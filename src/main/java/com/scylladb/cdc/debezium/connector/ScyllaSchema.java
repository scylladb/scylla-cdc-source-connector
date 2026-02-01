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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScyllaSchema implements DatabaseSchema<CollectionId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaSchema.class);
    public static final String CELL_VALUE = "value";
    public static final String ELEMENTS_VALUE = "elements";
    public static final String REMOVED_ELEMENTS_VALUE = "removed_elements";
    public static final String MODE_VALUE = "mode";

  private final String payloadKeyFieldName;

  private final Schema sourceSchema;
  private final ScyllaConnectorConfig configuration;
  private final SchemaNameAdjuster adjuster = SchemaNameAdjuster.create();
  private final Map<CollectionId, ScyllaCollectionSchema> dataCollectionSchemas = new HashMap<>();
  private final Map<CollectionId, ChangeSchema> cdcRowSchemas = new HashMap<>();
  private final Map<CollectionId, Schema> keySchemaCache = new HashMap<>();

  public ScyllaSchema(ScyllaConnectorConfig configuration, Schema sourceSchema) {
    this.sourceSchema = sourceSchema;
    this.configuration = configuration;
    this.payloadKeyFieldName = configuration.getCdcIncludePkPayloadKeyName();
  }

  /** Returns the configured payload key field name. */
  public String getPayloadKeyFieldName() {
    return payloadKeyFieldName;
  }

  @Override
  public void close() {}

  @Override
  public DataCollectionSchema schemaFor(CollectionId collectionId) {
    return dataCollectionSchemas.computeIfAbsent(collectionId, this::computeDataCollectionSchema);
  }

  private ScyllaCollectionSchema computeDataCollectionSchema(CollectionId collectionId) {
    ChangeSchema rowSchema = cdcRowSchemas.get(collectionId);

    if (rowSchema == null) {
      return null;
    }
    LOGGER.info("Computing data collection schema for table: {}", collectionId.getTableName());

    boolean includePayloadKey = configuration.getCdcIncludePk().inPayloadKey;

    Map<String, Schema> cellSchemas = computeCellSchemas(rowSchema, collectionId);
    Schema keySchema = computeKeySchema(rowSchema, collectionId);
    Schema beforeSchema =
        computeRowSchema(
            rowSchema,
            cellSchemas,
            collectionId,
            "Before",
            configuration.getCdcIncludePk().inPayloadBefore);
    Schema afterSchema =
        computeRowSchema(
            rowSchema,
            cellSchemas,
            collectionId,
            "After",
            configuration.getCdcIncludePk().inPayloadAfter);

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
    LOGGER.info(
        "Computed value schema for table {}: fields={}",
        collectionId.getTableName(),
        valueSchema.fields().stream()
            .map(f -> f.name() + ":" + f.schema().type())
            .collect(Collectors.joining(", ")));
    final Envelope envelope = Envelope.fromSchema(valueSchema);

        Map<String, Schema> cellSchemas = computeCellSchemas(changeSchema, collectionId);
        Schema keySchema = computeKeySchema(changeSchema, collectionId);
        Schema beforeSchema = computeBeforeSchema(changeSchema, cellSchemas, collectionId);
        Schema afterSchema = computeAfterSchema(changeSchema, cellSchemas, collectionId);

        final Schema valueSchema = SchemaBuilder.struct()
                .name(adjuster.adjust(Envelope.schemaName(configuration.getLogicalName() + "." + collectionId.getTableName().keyspace + "." + collectionId.getTableName().name)))
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.BEFORE, beforeSchema)
                .field(Envelope.FieldName.AFTER, afterSchema)
                .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .build();

        final Envelope envelope = Envelope.fromSchema(valueSchema);

        return new ScyllaCollectionSchema(collectionId, keySchema, valueSchema, beforeSchema, afterSchema, cellSchemas, envelope);
    }

    private Map<String, Schema> computeCellSchemas(ChangeSchema changeSchema, CollectionId collectionId) {
        Map<String, Schema> cellSchemas = new HashMap<>();
        for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
            if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY
                    || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) continue;
            if (!isSupportedColumnSchema(changeSchema, cdef)) continue;

            Schema columnSchema = computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
            Schema cellSchema = SchemaBuilder.struct()
                    .name(adjuster.adjust(configuration.getLogicalName() + "." + collectionId.getTableName().keyspace + "." + collectionId.getTableName().name + "." + cdef.getColumnName() + ".Cell"))
                    .field(CELL_VALUE, columnSchema).optional().build();
            cellSchemas.put(cdef.getColumnName(), cellSchema);
        }
        return cellSchemas;
    }
    return cellSchemas;
  }

    private Schema computeKeySchema(ChangeSchema changeSchema, CollectionId collectionId) {
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct()
                .name(adjuster.adjust(configuration.getLogicalName() + "." + collectionId.getTableName().keyspace + "." + collectionId.getTableName().name + ".Key"));
        for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
            if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY
                    && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) continue;
            if (!isSupportedColumnSchema(changeSchema, cdef)) continue;
            Schema columnSchema = computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
            keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
        }

    // Add partition key columns first to ensure proper primary key ordering
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY) continue;

      Schema columnSchema = computeColumnSchema(cdef.getCdcLogDataType());
      keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
    }

    private Schema computeAfterSchema(ChangeSchema changeSchema, Map<String, Schema> cellSchemas, CollectionId collectionId) {
        SchemaBuilder afterSchemaBuilder = SchemaBuilder.struct()
                .name(adjuster.adjust(configuration.getLogicalName() + "." + collectionId.getTableName().keyspace + "." + collectionId.getTableName().name + ".After"));
        for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
            if (!isSupportedColumnSchema(changeSchema, cdef)) continue;
            if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) {
                afterSchemaBuilder = afterSchemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
            } else {
                Schema columnSchema = computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
                afterSchemaBuilder = afterSchemaBuilder.field(cdef.getColumnName(), columnSchema);
            }
        }
      } else {
        schemaBuilder =
            schemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
      }
    }
    return schemaBuilder.optional().build();
  }

    private Schema computeBeforeSchema(ChangeSchema changeSchema, Map<String, Schema> cellSchemas, CollectionId collectionId) {
        SchemaBuilder beforeSchemaBuilder = SchemaBuilder.struct()
                .name(adjuster.adjust(configuration.getLogicalName() + "." + collectionId.getTableName().keyspace + "." + collectionId.getTableName().name + ".Before"));
        for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
            if (!isSupportedColumnSchema(changeSchema, cdef)) continue;
            if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) {
                beforeSchemaBuilder = beforeSchemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
            } else {
                Schema columnSchema = computeColumnSchema(changeSchema, cdef, configuration.getCollectionsMode());
                beforeSchemaBuilder = beforeSchemaBuilder.field(cdef.getColumnName(), columnSchema);
            }
        } else {
            return computeColumnSchemaBasic(cdef.getCdcLogDataType());
        }
    }

    protected static Schema computeColumnSchema(ChangeSchema changeSchema, ChangeSchema.ColumnDefinition cdef, CollectionsMode mode) {
        if (isNonFrozenCollection(changeSchema, cdef)) {
            switch (mode) {
                case DELTA: {
                    SchemaBuilder builder = SchemaBuilder.struct();
                    builder.field(MODE_VALUE, Schema.STRING_SCHEMA);

                    ChangeSchema.DataType type = cdef.getCdcLogDataType();
                    Schema elementsSchema;
                    switch (type.getCqlType()) {
                        case SET: {
                            Schema valueSchema = SchemaBuilder.BOOLEAN_SCHEMA;
                            Schema keySchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                            elementsSchema = SchemaBuilder.map(keySchema, valueSchema).required().build();
                            break;
                        }
                        case MAP: {
                            Schema keySchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                            Schema valueSchema = computeColumnSchemaBasic(type.getTypeArguments().get(1));
                            elementsSchema = SchemaBuilder.map(keySchema, valueSchema).required().build();

                            break;
                        }
                        case UDT:{
                            SchemaBuilder udtSchema = SchemaBuilder.struct();
                            for (Map.Entry<String, ChangeSchema.DataType> field : type.getUdtType().getFields().entrySet()) {
                                Schema fieldSchema = computeColumnSchemaBasic(field.getValue());
                                Schema cellSchema = SchemaBuilder.struct()
                                        .field(CELL_VALUE, fieldSchema).optional().build();
                                udtSchema = udtSchema.field(field.getKey(), cellSchema);
                            }
                            elementsSchema = udtSchema.required().build();
                            break;
                        }
                        case LIST: {
                            Schema valuesSchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                            elementsSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, valuesSchema).required().build();
                            break;
                        }
                        default:
                            // Should be unreachable
                            throw new UnsupportedOperationException();
                    }

                    builder.field(ELEMENTS_VALUE, elementsSchema);
                    return builder.optional().build();
                }
                default:
                    // There are no other modes right now
                    throw new UnsupportedOperationException();
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
            case LIST: {
                Schema innerSchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                return SchemaBuilder.array(innerSchema).optional().build();
            }
            case MAP: {
                Schema keySchema = computeColumnSchemaBasic(type.getTypeArguments().get(0));
                Schema valueSchema = computeColumnSchemaBasic(type.getTypeArguments().get(1));
                return SchemaBuilder.map(keySchema, valueSchema).optional().build();
            }
            case TUPLE: {
                List<Schema> innerSchemas = type.getTypeArguments().stream()
                        .map(inner_type -> computeColumnSchemaBasic(inner_type)).collect(Collectors.toList());
                SchemaBuilder tupleSchema = SchemaBuilder.struct();
                for (int i = 0; i < innerSchemas.size(); i++) {
                    tupleSchema = tupleSchema.field("tuple_member_" + i, innerSchemas.get(i));
                }
                return tupleSchema.optional().build();
            }
            case UDT: {
                SchemaBuilder udtSchema = SchemaBuilder.struct();
                for (Map.Entry<String, ChangeSchema.DataType> field : type.getUdtType().getFields().entrySet()) {
                    udtSchema = udtSchema.field(field.getKey(), computeColumnSchemaBasic(field.getValue()));
                }
                return udtSchema.optional().build();
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    protected static boolean isSupportedColumnSchema(ChangeSchema changeSchema, ChangeSchema.ColumnDefinition cdef) {
        return true;
    }

    protected static boolean isNonFrozenCollection(ChangeSchema changeSchema, ChangeSchema.ColumnDefinition cdef) {
        ChangeSchema.CqlType type = cdef.getCdcLogDataType().getCqlType();
        if (type == ChangeSchema.CqlType.LIST || type == ChangeSchema.CqlType.SET
            || type == ChangeSchema.CqlType.MAP || type == ChangeSchema.CqlType.UDT) {

            // FIXME: When isFrozen is fixed in scylla-cdc-java (PR #60),
            // replace with just a call to isFrozen.
            String deletedElementsColumnName = "cdc$deleted_elements_" + cdef.getColumnName();
            return changeSchema.getAllColumnDefinitions().stream()
                    .anyMatch(c -> c.getColumnName().equals(deletedElementsColumnName));
        }
        return false;
    }

  /**
   * Computes a Kafka Connect schema for a scalar or frozen collection type.
   *
   * <p>For top-level columns, baseTableType is used to distinguish non-frozen lists (which are
   * stored as map&lt;timeuuid, V&gt; in the CDC log) from frozen maps with timeuuid keys. For
   * nested types, baseTableType should be null (nested types are always frozen).
   *
   * @param cdcLogType the data type as it appears in the CDC log
   * @param baseTableType the data type in the base table (used to distinguish non-frozen lists)
   * @param schemaNamePrefix prefix for nested struct schema names (e.g., "connector.ks.table.") to
   *     ensure global uniqueness across topics when using RecordNameStrategy
   */
  private static Schema computeColumnSchema(
      ChangeSchema.DataType cdcLogType,
      ChangeSchema.DataType baseTableType,
      String schemaNamePrefix) {
    switch (cdcLogType.getCqlType()) {
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
          List<ChangeSchema.DataType> typeArgs = cdcLogType.getTypeArguments();
          if (typeArgs == null || typeArgs.isEmpty()) {
            throw new IllegalStateException(
                cdcLogType.getCqlType() + " type must have exactly 1 type argument");
          }
          Schema innerSchema = computeColumnSchema(typeArgs.get(0), null, schemaNamePrefix);
          return SchemaBuilder.array(innerSchema).optional().build();
        }
      case MAP:
        {
          List<ChangeSchema.DataType> typeArgs = cdcLogType.getTypeArguments();
          if (typeArgs == null || typeArgs.size() != 2) {
            throw new IllegalStateException("MAP type must have exactly 2 type arguments");
          }
          // Check if this is a non-frozen list (stored as map<timeuuid, V> in CDC log).
          // Non-frozen lists have TIMEUUID keys AND base table type is LIST.
          // Without the base table type check, frozen map<timeuuid, V> would be
          // incorrectly treated as a non-frozen list.
          boolean isNonFrozenList =
              baseTableType != null
                  && baseTableType.getCqlType() == ChangeSchema.CqlType.LIST
                  && typeArgs.get(0).getCqlType() == ChangeSchema.CqlType.TIMEUUID;

          if (isNonFrozenList) {
            Schema valueSchema = computeColumnSchema(typeArgs.get(1), null, schemaNamePrefix);
            return SchemaBuilder.array(valueSchema).optional().build();
          }

          Schema keySchema = computeColumnSchema(typeArgs.get(0), null, schemaNamePrefix);
          Schema valueSchema = computeColumnSchema(typeArgs.get(1), null, schemaNamePrefix);
          // Generate a unique name for the map entry struct based on key/value types.
          // The schemaNamePrefix ensures global uniqueness across topics when using
          // RecordNameStrategy in Schema Registry.
          String entrySchemaName =
              schemaNamePrefix
                  + "MapEntry_"
                  + generateTypeName(typeArgs.get(0))
                  + "_"
                  + generateTypeName(typeArgs.get(1));
          Schema entrySchema =
              SchemaBuilder.struct()
                  .name(entrySchemaName)
                  .field("key", keySchema)
                  .field("value", valueSchema)
                  .build();
          return SchemaBuilder.array(entrySchema).optional().build();
        }
      case TUPLE:
        {
          List<Schema> innerSchemas =
              cdcLogType.getTypeArguments().stream()
                  .map(arg -> computeColumnSchema(arg, null, schemaNamePrefix))
                  .collect(Collectors.toList());
          // Generate a unique name for the tuple struct based on field types.
          // The schemaNamePrefix ensures global uniqueness across topics.
          String tupleSchemaName = schemaNamePrefix + generateTypeName(cdcLogType);
          SchemaBuilder tupleSchema = SchemaBuilder.struct().name(tupleSchemaName);
          for (int i = 0; i < innerSchemas.size(); i++) {
            // Use "field_N" naming for Avro compatibility (Avro field names cannot start with
            // digits)
            tupleSchema = tupleSchema.field("field_" + i, innerSchemas.get(i));
          }
          return tupleSchema.optional().build();
        }
      case UDT:
        {
          ChangeSchema.DataType udtDataType = unwrapUdtElementsType(cdcLogType);
          ChangeSchema.DataType.UdtType udt = udtDataType.getUdtType();
          if (udt == null || udt.getFields() == null) {
            throw new IllegalStateException("UDT type must have non-null UdtType with fields");
          }
          // Generate a unique name for the UDT struct based on its name.
          // The schemaNamePrefix ensures global uniqueness across topics, which is critical
          // for UDTs since different keyspaces may have UDTs with the same name but different
          // structures.
          String udtSchemaName = schemaNamePrefix + generateTypeName(udtDataType);
          SchemaBuilder udtSchema = SchemaBuilder.struct().name(udtSchemaName);
          for (Map.Entry<String, ChangeSchema.DataType> field : udt.getFields().entrySet()) {
            udtSchema =
                udtSchema.field(
                    field.getKey(), computeColumnSchema(field.getValue(), null, schemaNamePrefix));
          }
          return udtSchema.optional().build();
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Computes a Kafka Connect schema for primary key columns.
   *
   * <p>Primary key columns are always scalar types and don't need a schema name prefix since they
   * don't create nested struct schemas.
   */
  private static Schema computeColumnSchema(ChangeSchema.DataType type) {
    return computeColumnSchema(type, null, "");
  }

  private static ChangeSchema.DataType unwrapUdtElementsType(ChangeSchema.DataType type) {
    if (type.getCqlType() != ChangeSchema.CqlType.UDT) {
      return type;
    }
    ChangeSchema.DataType.UdtType udtType = type.getUdtType();
    if (udtType == null) {
      return type;
    }
    Map<String, ChangeSchema.DataType> fields = udtType.getFields();
    if (fields != null && fields.size() == 1 && fields.containsKey("elements")) {
      ChangeSchema.DataType elementsType = fields.get("elements");
      if (elementsType != null && elementsType.getCqlType() == ChangeSchema.CqlType.UDT) {
        return elementsType;
      }
    }
    return type;
  }

  /** Checks if the column is a primary key column (partition key or clustering key). */
  private static boolean isPrimaryKeyColumn(ChangeSchema.ColumnDefinition cdef) {
    ChangeSchema.ColumnType colType = cdef.getBaseTableColumnType();
    return colType == ChangeSchema.ColumnType.PARTITION_KEY
        || colType == ChangeSchema.ColumnType.CLUSTERING_KEY;
  }

  public ScyllaCollectionSchema updateChangeSchema(
      CollectionId collectionId, ChangeSchema cdcRowSchema) {
    cdcRowSchemas.put(collectionId, cdcRowSchema);
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
