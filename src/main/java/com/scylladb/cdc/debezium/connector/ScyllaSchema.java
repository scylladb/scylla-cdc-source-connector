package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
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
  public static final String CELL_VALUE = "value";
  public static final String ELEMENTS_VALUE = "elements";
  public static final String REMOVED_ELEMENTS_VALUE = "removed_elements";
  public static final String MODE_VALUE = "mode";

  private final Schema sourceSchema;
  private final ScyllaConnectorConfig configuration;
  private final SchemaNameAdjuster adjuster = SchemaNameAdjuster.create();
  private final Map<CollectionId, ScyllaCollectionSchema> dataCollectionSchemas = new HashMap<>();
  private final Map<CollectionId, ChangeSchema> changeSchemas = new HashMap<>();

  public ScyllaSchema(ScyllaConnectorConfig configuration, Schema sourceSchema) {
    this.sourceSchema = sourceSchema;
    this.configuration = configuration;
  }

  @Override
  public void close() {}

  @Override
  public DataCollectionSchema schemaFor(CollectionId collectionId) {
    return dataCollectionSchemas.computeIfAbsent(collectionId, this::computeDataCollectionSchema);
  }

  private ScyllaCollectionSchema computeDataCollectionSchema(CollectionId collectionId) {
    ChangeSchema changeSchema = changeSchemas.get(collectionId);

    if (changeSchema == null) {
      return null;
    }

    Map<String, Schema> cellSchemas = computeCellSchemas(changeSchema, collectionId);
    Schema keySchema = computeKeySchema(changeSchema, collectionId);
    Schema beforeSchema = computeBeforeSchema(changeSchema, cellSchemas, collectionId);
    Schema afterSchema = computeAfterSchema(changeSchema, cellSchemas, collectionId);

    final Schema valueSchema =
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
            .field(Envelope.FieldName.AFTER, afterSchema)
            .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
            .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
            .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
            .field(Envelope.FieldName.TIMESTAMP_US, Schema.OPTIONAL_INT64_SCHEMA)
            .field(Envelope.FieldName.TIMESTAMP_NS, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    final Envelope envelope = Envelope.fromSchema(valueSchema);

    return new ScyllaCollectionSchema(
        collectionId, keySchema, valueSchema, beforeSchema, afterSchema, cellSchemas, envelope);
  }

  private Map<String, Schema> computeCellSchemas(
      ChangeSchema changeSchema, CollectionId collectionId) {
    Map<String, Schema> cellSchemas = new HashMap<>();
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY
          || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) continue;
      if (!isSupportedColumnSchema(changeSchema, cdef)) continue;

      Schema columnSchema =
          computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
      Schema cellSchema =
          SchemaBuilder.struct()
              .name(
                  adjuster.adjust(
                      configuration.getLogicalName()
                          + "."
                          + collectionId.getTableName().keyspace
                          + "."
                          + collectionId.getTableName().name
                          + "."
                          + cdef.getColumnName()
                          + ".Cell"))
              .field(CELL_VALUE, columnSchema)
              .optional()
              .build();
      cellSchemas.put(cdef.getColumnName(), cellSchema);
    }
    return cellSchemas;
  }

  private Schema computeKeySchema(ChangeSchema changeSchema, CollectionId collectionId) {
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
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY
          && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) continue;
      if (!isSupportedColumnSchema(changeSchema, cdef)) continue;
      Schema columnSchema =
          computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
      keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
    }

    return keySchemaBuilder.build();
  }

  private Schema computeAfterSchema(
      ChangeSchema changeSchema, Map<String, Schema> cellSchemas, CollectionId collectionId) {
    SchemaBuilder afterSchemaBuilder =
        SchemaBuilder.struct()
            .name(
                adjuster.adjust(
                    configuration.getLogicalName()
                        + "."
                        + collectionId.getTableName().keyspace
                        + "."
                        + collectionId.getTableName().name
                        + ".After"));
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (!isSupportedColumnSchema(changeSchema, cdef)) continue;
      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY
          && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) {
        afterSchemaBuilder =
            afterSchemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
      } else {
        Schema columnSchema =
            computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
        afterSchemaBuilder = afterSchemaBuilder.field(cdef.getColumnName(), columnSchema);
      }
    }
    return afterSchemaBuilder.optional().build();
  }

  private Schema computeBeforeSchema(
      ChangeSchema changeSchema, Map<String, Schema> cellSchemas, CollectionId collectionId) {
    SchemaBuilder beforeSchemaBuilder =
        SchemaBuilder.struct()
            .name(
                adjuster.adjust(
                    configuration.getLogicalName()
                        + "."
                        + collectionId.getTableName().keyspace
                        + "."
                        + collectionId.getTableName().name
                        + ".Before"));
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (!isSupportedColumnSchema(changeSchema, cdef)) continue;
      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY
          && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) {
        beforeSchemaBuilder =
            beforeSchemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
      } else {
        Schema columnSchema =
            computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
        beforeSchemaBuilder = beforeSchemaBuilder.field(cdef.getColumnName(), columnSchema);
      }
    }
    return beforeSchemaBuilder.optional().build();
  }

  protected static Schema computeColumnSchema(
      ChangeSchema changeSchema, ChangeSchema.ColumnDefinition cdef, CollectionsMode mode) {
    if (isNonFrozenCollection(changeSchema, cdef)) {
      switch (mode) {
        case DELTA:
          {
            SchemaBuilder builder = SchemaBuilder.struct();
            builder.field(MODE_VALUE, Schema.STRING_SCHEMA);

            ChangeSchema.DataType type = cdef.getCdcLogDataType();
            Schema elementsSchema;
            switch (type.getCqlType()) {
              case SET:
                {
                  Schema elementSchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                  Schema entrySchema =
                      SchemaBuilder.struct()
                          .field("element", elementSchema)
                          .field("added", Schema.BOOLEAN_SCHEMA)
                          .build();
                  elementsSchema = SchemaBuilder.array(entrySchema).required().build();
                  break;
                }
              case MAP:
                {
                  Schema keySchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                  Schema valueSchema = computeColumnSchemaBasic(type.getTypeArguments().get(1));
                  Schema entrySchema =
                      SchemaBuilder.struct()
                          .field("key", keySchema)
                          .field("value", valueSchema)
                          .build();
                  elementsSchema = SchemaBuilder.array(entrySchema).required().build();
                  break;
                }
              case UDT:
                {
                  SchemaBuilder udtSchema = SchemaBuilder.struct();
                  for (Map.Entry<String, ChangeSchema.DataType> field :
                      type.getUdtType().getFields().entrySet()) {
                    Schema fieldSchema = computeColumnSchemaBasic(field.getValue());
                    Schema cellSchema =
                        SchemaBuilder.struct().field(CELL_VALUE, fieldSchema).optional().build();
                    udtSchema = udtSchema.field(field.getKey(), cellSchema);
                  }
                  elementsSchema = udtSchema.required().build();
                  break;
                }
              case LIST:
                {
                  Schema valuesSchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                  Schema entrySchema =
                      SchemaBuilder.struct()
                          .field("key", Schema.STRING_SCHEMA)
                          .field("value", valuesSchema)
                          .build();
                  elementsSchema = SchemaBuilder.array(entrySchema).required().build();
                  break;
                }
              default:
                // Should be unreachable
                throw new UnsupportedOperationException(
                    "Unsupported CQL type for non-frozen collection: " + type.getCqlType());
            }

            builder.field(ELEMENTS_VALUE, elementsSchema);
            return builder.optional().build();
          }
        default:
          // There are no other modes right now
          throw new UnsupportedOperationException("Unsupported collections mode: " + mode);
      }
    } else {
      return computeColumnSchemaBasic(cdef.getCdcLogDataType());
    }
  }

  private static Schema computeColumnSchemaBasic(ChangeSchema.DataType type) {
    switch (type.getCqlType()) {
      case ASCII:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case BIGINT:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case BLOB:
        return Schema.OPTIONAL_BYTES_SCHEMA;
      case BOOLEAN:
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case COUNTER:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case DECIMAL:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case DOUBLE:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case FLOAT:
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case INT:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case TEXT:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case TIMESTAMP:
        return Timestamp.builder().optional().build();
      case UUID:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case VARCHAR:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case VARINT:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case TIMEUUID:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case INET:
        return Schema.OPTIONAL_STRING_SCHEMA;
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
      case DURATION:
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
          return SchemaBuilder.map(keySchema, valueSchema).optional().build();
        }
      case TUPLE:
        {
          List<Schema> innerSchemas =
              type.getTypeArguments().stream()
                  .map(innerType -> computeColumnSchemaBasic(innerType))
                  .collect(Collectors.toList());
          SchemaBuilder tupleSchema = SchemaBuilder.struct();
          for (int i = 0; i < innerSchemas.size(); i++) {
            tupleSchema = tupleSchema.field("tuple_member_" + i, innerSchemas.get(i));
          }
          return tupleSchema.optional().build();
        }
      case UDT:
        {
          SchemaBuilder udtSchema = SchemaBuilder.struct();
          for (Map.Entry<String, ChangeSchema.DataType> field :
              type.getUdtType().getFields().entrySet()) {
            udtSchema = udtSchema.field(field.getKey(), computeColumnSchemaBasic(field.getValue()));
          }
          return udtSchema.optional().build();
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  protected static boolean isSupportedColumnSchema(
      ChangeSchema changeSchema, ChangeSchema.ColumnDefinition cdef) {
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

  public ScyllaCollectionSchema updateChangeSchema(
      CollectionId collectionId, ChangeSchema changeSchema) {
    changeSchemas.put(collectionId, changeSchema);
    dataCollectionSchemas.put(collectionId, computeDataCollectionSchema(collectionId));
    return dataCollectionSchemas.get(collectionId);
  }

  @Override
  public boolean tableInformationComplete() {
    return false;
  }

  @Override
  public boolean isHistorized() {
    return false;
  }
}
