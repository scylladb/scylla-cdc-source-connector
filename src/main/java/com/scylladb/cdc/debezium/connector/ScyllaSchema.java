package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.ColumnType;
import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaSchema implements DatabaseSchema<CollectionId> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaSchema.class);
  public static final String FIELD_DIFF = "diff";

  private final String payloadKeyFieldName;

  private final Schema sourceSchema;
  private final ScyllaConnectorConfig configuration;
  private final SchemaNameAdjuster adjuster = SchemaNameAdjuster.create();
  private final Map<CollectionId, ScyllaCollectionSchema> dataCollectionSchemas = new HashMap<>();
  private final Map<CollectionId, ChangeSchema> cdcRowSchemas = new HashMap<>();
  private final Map<CollectionId, Schema> keySchemaCache = new HashMap<>();

  /** Creates a schema helper backed by the connector configuration and source schema. */
  public ScyllaSchema(ScyllaConnectorConfig configuration, Schema sourceSchema) {
    this.sourceSchema = sourceSchema;
    this.configuration = configuration;
    this.payloadKeyFieldName = configuration.getCdcIncludePkPayloadKeyName();
  }

  /** Returns the configured payload key field name. */
  public String getPayloadKeyFieldName() {
    return payloadKeyFieldName;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {}

  /** {@inheritDoc} */
  @Override
  public DataCollectionSchema schemaFor(CollectionId collectionId) {
    return dataCollectionSchemas.computeIfAbsent(collectionId, this::computeDataCollectionSchema);
  }

  /** Builds the Debezium collection schema for a given Scylla table. */
  private ScyllaCollectionSchema computeDataCollectionSchema(CollectionId collectionId) {
    ChangeSchema rowSchema = cdcRowSchemas.get(collectionId);

    if (rowSchema == null) {
      return null;
    }

    boolean includePayloadKey = configuration.getCdcIncludePk().inPayloadKey;

    Map<String, Schema> cellSchemas = computeCellSchemas(rowSchema, collectionId);
    Schema keySchema = computeKeySchema(rowSchema, collectionId);
    Schema beforeSchema = computeRowSchema(rowSchema, cellSchemas, collectionId, "Before");
    Schema afterSchema = computeRowSchema(rowSchema, cellSchemas, collectionId, "After");

    SchemaBuilder valueSchemaBuilder =
        SchemaBuilder.struct()
            .name(
                adjuster.adjust(
                    Envelope.schemaName(
                        configuration.getLogicalName()
                            + "."
                            + collectionId.getTableName().keyspace
                            + "."
                            + collectionId.getTableName().name)))
            .field(Envelope.FieldName.SOURCE, sourceSchema)
            .field(Envelope.FieldName.BEFORE, beforeSchema)
            .field(Envelope.FieldName.AFTER, afterSchema);

    // Add optional key field in payload if configured
    if (includePayloadKey) {
      valueSchemaBuilder.field(payloadKeyFieldName, keySchema);
    }

    valueSchemaBuilder
        .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
        .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
        .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
        .field(Envelope.FieldName.TIMESTAMP_US, Schema.OPTIONAL_INT64_SCHEMA)
        .field(Envelope.FieldName.TIMESTAMP_NS, Schema.OPTIONAL_INT64_SCHEMA);

    final Schema valueSchema = valueSchemaBuilder.build();
    final Envelope envelope = Envelope.fromSchema(valueSchema);

    return new ScyllaCollectionSchema(
        collectionId,
        keySchema,
        valueSchema,
        beforeSchema,
        afterSchema,
        null,
        cellSchemas,
        null,
        envelope);
  }

  /** Builds per-column schemas for non-key columns. */
  private Map<String, Schema> computeCellSchemas(
      ChangeSchema changeSchema, CollectionId collectionId) {
    Map<String, Schema> cellSchemas = new HashMap<>();
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      ColumnType colType = cdef.getBaseTableColumnType();
      if (colType == ColumnType.PARTITION_KEY || colType == ColumnType.CLUSTERING_KEY) continue;
      if (!isSupportedColumnSchema(cdef)) continue;

      Schema columnSchema = computeColumnSchema(changeSchema, cdef);
      cellSchemas.put(cdef.getColumnName(), columnSchema);
    }
    return cellSchemas;
  }

  /** Builds the Kafka Connect key schema for the table primary key. */
  private Schema computeKeySchema(ChangeSchema changeSchema, CollectionId collectionId) {
    // Return cached key schema if available for this keyspace.table
    Schema cachedKeySchema = keySchemaCache.get(collectionId);
    if (cachedKeySchema != null) {
      return cachedKeySchema;
    }

    SchemaBuilder keySchemaBuilder =
        SchemaBuilder.struct()
            .name(
                adjuster.adjust(
                    configuration.getLogicalName()
                        + "."
                        + collectionId.getTableName().keyspace
                        + "."
                        + collectionId.getTableName().name
                        + ".Key"));

    // Add partition key columns first to ensure proper primary key ordering
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (cdef.getBaseTableColumnType() != ColumnType.PARTITION_KEY) continue;
      if (!isSupportedColumnSchema(cdef)) continue;
      Schema columnSchema = computeColumnSchema(changeSchema, cdef);
      keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
    }

    // Add clustering key columns second
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (cdef.getBaseTableColumnType() != ColumnType.CLUSTERING_KEY) continue;
      if (!isSupportedColumnSchema(cdef)) continue;

      Schema columnSchema = computeColumnSchema(changeSchema, cdef);
      keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
    }

    Schema keySchema = keySchemaBuilder.build();
    keySchemaCache.put(collectionId, keySchema);
    return keySchema;
  }

  private String generateSchemaName(CollectionId collectionId, String suffix) {
    return adjuster.adjust(
        configuration.getLogicalName()
            + "."
            + collectionId.getTableName().keyspace
            + "."
            + collectionId.getTableName().name
            + "."
            + suffix);
  }

  private Schema computeRowSchema(
      ChangeSchema changeSchema,
      Map<String, Schema> cellSchemas,
      CollectionId collectionId,
      String suffix) {
    SchemaBuilder schemaBuilder =
        SchemaBuilder.struct().name(generateSchemaName(collectionId, suffix));
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (!isSupportedColumnSchema(cdef)) continue;

      if (cdef.getBaseTableColumnType() != ColumnType.PARTITION_KEY
          && cdef.getBaseTableColumnType() != ColumnType.CLUSTERING_KEY) {
        schemaBuilder =
            schemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
      } else {
        Schema columnSchema = computeColumnSchema(changeSchema, cdef);
        schemaBuilder = schemaBuilder.field(cdef.getColumnName(), columnSchema);
      }
    }
    return schemaBuilder.optional().build();
  }

  /** Computes the Kafka Connect schema for a column, handling non-frozen collections. */
  protected static Schema computeColumnSchema(
      ChangeSchema changeSchema, ChangeSchema.ColumnDefinition cdef) {
    if (isNonFrozenCollection(changeSchema, cdef)) {
      ChangeSchema.DataType type = cdef.getCdcLogDataType();
      switch (type.getCqlType()) {
        case SET:
        case LIST:
          {
            Schema elementSchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
            return SchemaBuilder.array(elementSchema).optional().build();
          }
        case MAP:
          {
            ChangeSchema.DataType keyType = type.getTypeArguments().get(0);
            Schema keySchema = computeColumnSchemaBasic(keyType);
            Schema valueSchema = computeColumnSchemaBasic(type.getTypeArguments().get(1));
            Schema entrySchema =
                SchemaBuilder.struct().field("key", keySchema).field("value", valueSchema).build();
            return SchemaBuilder.array(entrySchema).optional().build();
          }
        case UDT:
          {
            ChangeSchema.DataType udtType = unwrapUdtElementsType(type);
            SchemaBuilder udtSchema = SchemaBuilder.struct();
            for (Map.Entry<String, ChangeSchema.DataType> field :
                udtType.getUdtType().getFields().entrySet()) {
              Schema fieldSchema = computeColumnSchemaBasic(field.getValue());
              udtSchema = udtSchema.field(field.getKey(), fieldSchema);
            }
            return udtSchema.optional().build();
          }
        default:
          // Should be unreachable
          throw new UnsupportedOperationException(
              "Unsupported CQL type for non-frozen collection: " + type.getCqlType());
      }
    } else {
      return computeColumnSchemaBasic(cdef.getCdcLogDataType());
    }
  }

  /** Computes a Kafka Connect schema for a scalar or frozen collection type. */
  private static Schema computeColumnSchemaBasic(ChangeSchema.DataType type) {
    switch (type.getCqlType()) {
      case DATE:
        return Date.builder().optional().build();
      case TIME:
        // Using OPTIONAL_INT64_SCHEMA instead
        // of Time from Kafka Connect, because
        // Time from Kafka Connect has millisecond
        // precision (stored int32), while CQL TIME is
        // microsecond precision (stored int64).
        return Schema.OPTIONAL_INT64_SCHEMA;
      case SMALLINT:
        return Schema.OPTIONAL_INT16_SCHEMA;
      case TINYINT:
        return Schema.OPTIONAL_INT8_SCHEMA;
      case BLOB:
        return Schema.OPTIONAL_BYTES_SCHEMA;
      case BOOLEAN:
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case DOUBLE:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case FLOAT:
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case INT:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case TIMESTAMP:
        return Timestamp.builder().optional().build();
      case BIGINT:
      case COUNTER:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case ASCII:
      case DECIMAL:
      case UUID:
      case VARCHAR:
      case VARINT:
      case TIMEUUID:
      case INET:
      case DURATION:
      case TEXT:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case SET:
      case LIST:
        {
          Schema innerSchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
          return SchemaBuilder.array(innerSchema).optional().build();
        }
      case MAP:
        {
          Schema keySchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
          Schema valueSchema = computeColumnSchemaBasic(type.getTypeArguments().get(1));
          Schema entrySchema =
              SchemaBuilder.struct().field("key", keySchema).field("value", valueSchema).build();
          return SchemaBuilder.array(entrySchema).optional().build();
        }
      case TUPLE:
        {
          List<Schema> innerSchemas =
              type.getTypeArguments().stream()
                  .map(ScyllaSchema::computeColumnSchemaBasic)
                  .collect(Collectors.toList());
          SchemaBuilder tupleSchema = SchemaBuilder.struct();
          for (int i = 0; i < innerSchemas.size(); i++) {
            tupleSchema = tupleSchema.field("tuple_member_" + i, innerSchemas.get(i));
          }
          return tupleSchema.optional().build();
        }
      case UDT:
        {
          ChangeSchema.DataType udtType = unwrapUdtElementsType(type);
          SchemaBuilder udtSchema = SchemaBuilder.struct();
          for (Map.Entry<String, ChangeSchema.DataType> field :
              udtType.getUdtType().getFields().entrySet()) {
            udtSchema = udtSchema.field(field.getKey(), computeColumnSchemaBasic(field.getValue()));
          }
          return udtSchema.optional().build();
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  /** Returns true if the column's CDC type is supported by this connector. */
  protected static boolean isSupportedColumnSchema(ChangeSchema.ColumnDefinition cdef) {
    ChangeSchema.DataType type = cdef.getCdcLogDataType();
    switch (type.getCqlType()) {
      case ASCII:
      case BIGINT:
      case BLOB:
      case BOOLEAN:
      case COUNTER:
      case DATE:
      case DECIMAL:
      case DOUBLE:
      case DURATION:
      case FLOAT:
      case INET:
      case INT:
      case LIST:
      case MAP:
      case SET:
      case SMALLINT:
      case TEXT:
      case TIME:
      case TIMESTAMP:
      case TIMEUUID:
      case TINYINT:
      case UUID:
      case VARCHAR:
      case VARINT:
      case TUPLE:
      case UDT:
        return true;
      default:
        return false;
    }
  }

  /** Returns true when the column is a non-frozen collection tracked with element deltas. */
  protected static boolean isNonFrozenCollection(
      ChangeSchema changeSchema, ChangeSchema.ColumnDefinition cdef) {
    ChangeSchema.CqlType type = cdef.getCdcLogDataType().getCqlType();
    if (type == ChangeSchema.CqlType.LIST
        || type == ChangeSchema.CqlType.SET
        || type == ChangeSchema.CqlType.MAP
        || type == ChangeSchema.CqlType.UDT) {

      // FIXME: When isFrozen is fixed in scylla-cdc-java (PR #60),
      // replace with just a call to isFrozen.
      String deletedElementsColumnName = "cdc$deleted_elements_" + cdef.getColumnName();
      return changeSchema.getAllColumnDefinitions().stream()
          .anyMatch(c -> c.getColumnName().equals(deletedElementsColumnName));
    }
    return false;
  }

  /** Unwraps nested UDT "elements" wrappers used by Scylla CDC. */
  private static ChangeSchema.DataType unwrapUdtElementsType(ChangeSchema.DataType type) {
    if (type.getCqlType() != ChangeSchema.CqlType.UDT) {
      return type;
    }
    Map<String, ChangeSchema.DataType> fields = type.getUdtType().getFields();
    if (fields.size() == 1 && fields.containsKey("elements")) {
      ChangeSchema.DataType elementsType = fields.get("elements");
      if (elementsType != null && elementsType.getCqlType() == ChangeSchema.CqlType.UDT) {
        return elementsType;
      }
    }
    return type;
  }

  /** Updates cached schemas for a collection based on a newly observed change schema. */
  public ScyllaCollectionSchema updateChangeSchema(
      CollectionId collectionId, ChangeSchema cdcRowSchema) {
    cdcRowSchemas.put(collectionId, cdcRowSchema);
    dataCollectionSchemas.put(collectionId, computeDataCollectionSchema(collectionId));
    return dataCollectionSchemas.get(collectionId);
  }

  /**
   * Returns the cached key schema for the given keyspace.table, or null if not cached.
   *
   * @param collectionId the collection ID (keyspace.table)
   * @return the cached key schema, or null if not yet computed
   */
  public Schema getKeySchema(CollectionId collectionId) {
    return keySchemaCache.get(collectionId);
  }

  /** {@inheritDoc} */
  @Override
  public boolean tableInformationComplete() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isHistorized() {
    return false;
  }
}
