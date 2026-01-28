package com.scylladb.cdc.debezium.connector.transforms;

import io.debezium.transforms.ExtractNewRecordState;
import java.util.Map;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

/**
 * Scylla-specific extension of Debezium's ExtractNewRecordState transform.
 *
 * <p>This transform handles both advanced and legacy output formats:
 *
 * <ul>
 *   <li>Advanced format: Values are direct (no unwrapping needed)
 *   <li>Legacy format: Non-PK values are wrapped in Cell structs ({@code {"value":
 *       <actual_value>}}) and need to be unwrapped
 * </ul>
 *
 * <p>When {@code cdc.output.format=legacy} (default), the transform unwraps Cell structs by
 * checking if a field's schema name ends with ".Cell" and has a single "value" field, then extracts
 * the inner value. When {@code cdc.output.format=advanced}, Cell unwrapping is skipped since
 * advanced format doesn't use Cell wrappers.
 */
public class ScyllaExtractNewRecordState<R extends ConnectRecord<R>>
    extends ExtractNewRecordState<R> {

  /**
   * Configuration key for CDC output format (matches ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY).
   */
  public static final String CDC_OUTPUT_FORMAT_CONFIG = "cdc.output.format";

  /** Advanced output format value. */
  private static final String CDC_OUTPUT_FORMAT_ADVANCED = "advanced";

  /** Field name used in legacy Cell structs to wrap values. */
  private static final String CELL_VALUE_FIELD = "value";

  private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

  /** Whether legacy format is enabled (requires Cell unwrapping). Defaults to true. */
  private boolean legacyFormat = true;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    Object formatValue = configs.get(CDC_OUTPUT_FORMAT_CONFIG);
    if (formatValue != null) {
      // Legacy format is the default; only disable Cell unwrapping for advanced format
      legacyFormat = !CDC_OUTPUT_FORMAT_ADVANCED.equalsIgnoreCase(formatValue.toString().trim());
    }
  }

  @Override
  public ConfigDef config() {
    ConfigDef config = super.config();
    config.define(
        CDC_OUTPUT_FORMAT_CONFIG,
        ConfigDef.Type.STRING,
        "legacy",
        ConfigDef.Importance.MEDIUM,
        "CDC output format. Default is 'legacy' which enables Cell struct unwrapping. Set to 'advanced' to disable Cell unwrapping.");
    return config;
  }

  @Override
  public R apply(final R record) {
    final R ret = super.apply(record);

    // Skip Cell unwrapping for advanced format (no Cell structs to unwrap)
    if (!legacyFormat) {
      return ret;
    }

    if (ret == null || !(ret.value() instanceof Struct)) {
      return ret;
    }

    final Struct value = (Struct) ret.value();

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema());
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      Object fieldValue = value.get(field);
      Schema fieldSchema = field.schema();

      // Check if this is a legacy Cell struct that needs unwrapping
      if (isCellSchema(fieldSchema) && fieldValue instanceof Struct) {
        Struct cellStruct = (Struct) fieldValue;
        fieldValue = cellStruct.get(CELL_VALUE_FIELD);
      }

      updatedValue.put(field.name(), fieldValue);
    }

    return ret.newRecord(
        ret.topic(),
        ret.kafkaPartition(),
        ret.keySchema(),
        ret.key(),
        updatedSchema,
        updatedValue,
        ret.timestamp());
  }

  @Override
  public void close() {
    super.close();
    schemaUpdateCache = null;
  }

  /**
   * Creates an updated schema that unwraps Cell structs for legacy format.
   *
   * <p>For legacy format fields wrapped in Cell structs, the schema is changed from the Cell struct
   * schema to the inner value's schema.
   */
  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      Schema fieldSchema = field.schema();

      // If this is a Cell schema, use the inner value's schema instead
      if (isCellSchema(fieldSchema)) {
        Field valueField = fieldSchema.field(CELL_VALUE_FIELD);
        if (valueField != null) {
          // Make the unwrapped schema optional since Cell values can be null
          Schema unwrappedSchema = valueField.schema();
          if (!unwrappedSchema.isOptional()) {
            unwrappedSchema = makeOptional(unwrappedSchema);
          }
          builder.field(field.name(), unwrappedSchema);
          continue;
        }
      }

      builder.field(field.name(), fieldSchema);
    }

    return builder.build();
  }

  /**
   * Checks if a schema represents a legacy Cell struct.
   *
   * <p>A Cell schema is identified by:
   *
   * <ul>
   *   <li>Being a STRUCT type
   *   <li>Having a schema name that ends with ".Cell"
   *   <li>Having exactly one field named "value"
   * </ul>
   */
  private boolean isCellSchema(Schema schema) {
    if (schema == null || schema.type() != Schema.Type.STRUCT) {
      return false;
    }

    String schemaName = schema.name();
    if (schemaName == null || !schemaName.endsWith(".Cell")) {
      return false;
    }

    // Verify it has exactly one field named "value"
    if (schema.fields().size() != 1) {
      return false;
    }

    return schema.field(CELL_VALUE_FIELD) != null;
  }

  /**
   * Creates an optional version of a schema.
   *
   * @param schema the schema to make optional
   * @return an optional version of the schema
   */
  private Schema makeOptional(Schema schema) {
    SchemaBuilder builder;
    switch (schema.type()) {
      case INT8:
        builder = SchemaBuilder.int8();
        break;
      case INT16:
        builder = SchemaBuilder.int16();
        break;
      case INT32:
        builder = SchemaBuilder.int32();
        break;
      case INT64:
        builder = SchemaBuilder.int64();
        break;
      case FLOAT32:
        builder = SchemaBuilder.float32();
        break;
      case FLOAT64:
        builder = SchemaBuilder.float64();
        break;
      case BOOLEAN:
        builder = SchemaBuilder.bool();
        break;
      case STRING:
        builder = SchemaBuilder.string();
        break;
      case BYTES:
        builder = SchemaBuilder.bytes();
        break;
      default:
        // For complex types (ARRAY, MAP, STRUCT), return the original schema unchanged.
        // These types are already handled correctly by Kafka Connect's schema system
        // and don't require special optional conversion.
        return schema;
    }

    if (schema.name() != null) {
      builder.name(schema.name());
    }
    if (schema.doc() != null) {
      builder.doc(schema.doc());
    }
    if (schema.defaultValue() != null) {
      builder.defaultValue(schema.defaultValue());
    }
    if (schema.parameters() != null) {
      for (java.util.Map.Entry<String, String> entry : schema.parameters().entrySet()) {
        builder.parameter(entry.getKey(), entry.getValue());
      }
    }

    return builder.optional().build();
  }
}
