package com.scylladb.cdc.debezium.connector.transforms;

import io.debezium.transforms.ExtractNewRecordState;
import java.util.List;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

/**
 * Transforms Scylla CDC records by extracting values from Cell wrappers.
 *
 * <p>ScyllaSchema wraps non-key columns in Cell structs with a single "value" field:
 *
 * <pre>{@code
 * {
 *   columnName: {
 *     value: <actual_data>  // may be null, scalar, array, or struct
 *   }
 * }
 * }</pre>
 *
 * <p>This transform extracts the inner value, producing:
 *
 * <pre>{@code
 * {
 *   columnName: <actual_data>
 * }
 * }</pre>
 *
 * <p>For non-frozen collections (SET, LIST, MAP, UDT), the value contains delta information:
 *
 * <ul>
 *   <li>SET/LIST: Array of elements (added and deleted elements in delta)
 *   <li>MAP: Array of {key, value} structs (null value indicates deletion)
 *   <li>UDT: Struct with field values (only modified fields present)
 * </ul>
 */
public class ScyllaExtractNewRecordState<R extends ConnectRecord<R>>
    extends ExtractNewRecordState<R> {
  private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

  /** {@inheritDoc} */
  @Override
  public R apply(final R record) {
    final R ret = super.apply(record);
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
      if (isCellField(field)) {
        Struct cellStruct = (Struct) value.get(field);
        Object extractedValue = cellStruct == null ? null : cellStruct.get("value");
        extractedValue = unwrapUdtElementsValue(extractedValue);
        updatedValue.put(field.name(), extractedValue);
      } else {
        updatedValue.put(field.name(), value.get(field));
      }
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

  /** Clears cached schema updates and releases resources. */
  @Override
  public void close() {
    super.close();
    schemaUpdateCache = null;
  }

  /**
   * Checks if a field is a Cell wrapper (struct with single "value" field).
   *
   * <p>ScyllaSchema wraps non-key columns in Cell structs to track null vs absent values.
   */
  private boolean isCellField(Field field) {
    if (field.schema().type() != Type.STRUCT) {
      return false;
    }
    List<Field> fields = field.schema().fields();
    return fields.size() == 1 && "value".equals(fields.get(0).name());
  }

  /**
   * Creates an updated schema that unwraps Cell fields.
   *
   * <p>Cell fields {@code {value: T}} become just {@code T}.
   */
  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      if (isCellField(field)) {
        Schema unwrappedSchema = unwrapUdtElementsSchema(field.schema().field("value").schema());
        builder.field(field.name(), unwrappedSchema);
      } else {
        builder.field(field.name(), field.schema());
      }
    }

    return builder.build();
  }

  /** Unwraps UDT "elements" wrappers in schema definitions, when present. */
  private Schema unwrapUdtElementsSchema(Schema schema) {
    if (schema != null && schema.type() == Type.STRUCT) {
      List<Field> fields = schema.fields();
      if (fields.size() == 1 && "elements".equals(fields.get(0).name())) {
        return fields.get(0).schema();
      }
    }
    return schema;
  }

  /** Unwraps UDT "elements" wrappers in value structs, when present. */
  private Object unwrapUdtElementsValue(Object value) {
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      Schema schema = struct.schema();
      if (schema != null
          && schema.type() == Type.STRUCT
          && schema.fields().size() == 1
          && schema.field("elements") != null) {
        return struct.get("elements");
      }
    }
    return value;
  }
}
