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
  public static final String FIELD_DIFF = "diff";

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

  private Map<String, Schema> computeCellSchemas(
      ChangeSchema changeSchema, CollectionId collectionId) {
    Map<String, Schema> cellSchemas = new HashMap<>();
    String schemaNamePrefix = generateSchemaNamePrefix(collectionId);
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (isPrimaryKeyColumn(cdef)) continue;

      try {
        Schema columnSchema =
            computeColumnSchema(
                cdef.getCdcLogDataType(), cdef.getBaseTableDataType(), schemaNamePrefix);
        cellSchemas.put(cdef.getColumnName(), columnSchema);
        LOGGER.debug(
            "Computed cell schema for column {}: type={}, schema={}",
            cdef.getColumnName(),
            cdef.getCdcLogDataType().getCqlType(),
            columnSchema);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to compute cell schema for column {}: type={}",
            cdef.getColumnName(),
            cdef.getCdcLogDataType().getCqlType(),
            e);
        throw e;
      }
    }
    return cellSchemas;
  }

  private Schema computeKeySchema(ChangeSchema changeSchema, CollectionId collectionId) {
    // Return cached key schema if available for this keyspace.table
    Schema cachedKeySchema = keySchemaCache.get(collectionId);
    if (cachedKeySchema != null) {
      return cachedKeySchema;
    }

    SchemaBuilder keySchemaBuilder =
        SchemaBuilder.struct().name(generateSchemaNamePrefix(collectionId) + "Key");

    // Add partition key columns first to ensure proper primary key ordering
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY) continue;

      Schema columnSchema = computeColumnSchema(cdef.getCdcLogDataType());
      keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
    }

    // Add clustering key columns second
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) continue;

      Schema columnSchema = computeColumnSchema(cdef.getCdcLogDataType());
      keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
    }

    Schema keySchema = keySchemaBuilder.build();
    keySchemaCache.put(collectionId, keySchema);
    return keySchema;
  }

  /**
   * Generates a schema name prefix for nested types (maps, tuples, UDTs).
   *
   * <p>This prefix ensures globally unique schema names across different topics, which is required
   * when using Schema Registry with {@code RecordNameStrategy}. Without this prefix, two different
   * tables with the same nested type structure (e.g., {@code map<text, int>}) would generate
   * identical schema names like {@code MapEntry_TEXT_INT}, causing conflicts if the structures
   * differ or when schema evolution occurs independently per table.
   *
   * <p>With this prefix, schemas are namespaced per table: {@code
   * connector.keyspace.table.MapEntry_TEXT_INT}
   *
   * @param collectionId the collection (table) identifier
   * @return the schema name prefix including trailing dot
   */
  private String generateSchemaNamePrefix(CollectionId collectionId) {
    return adjuster.adjust(
            configuration.getLogicalName()
                + "."
                + collectionId.getTableName().keyspace
                + "."
                + collectionId.getTableName().name)
        + ".";
  }

  private Schema computeRowSchema(
      ChangeSchema changeSchema,
      Map<String, Schema> cellSchemas,
      CollectionId collectionId,
      String suffix,
      boolean includePk) {
    SchemaBuilder schemaBuilder =
        SchemaBuilder.struct().name(generateSchemaNamePrefix(collectionId) + suffix);
    for (ChangeSchema.ColumnDefinition cdef : changeSchema.getNonCdcColumnDefinitions()) {
      if (isPrimaryKeyColumn(cdef)) {
        if (includePk) {
          Schema columnSchema = computeColumnSchema(cdef.getCdcLogDataType());
          schemaBuilder = schemaBuilder.field(cdef.getColumnName(), columnSchema);
        }
      } else {
        schemaBuilder =
            schemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
      }
    }
    return schemaBuilder.optional().build();
  }

  /**
   * Generates a unique schema name for a CQL data type.
   *
   * <p>Avro requires all record types to have unique names. This method generates deterministic
   * names based on the type structure.
   */
  private static String generateTypeName(ChangeSchema.DataType dataType) {
    if (dataType == null) {
      return "Unknown";
    }
    ChangeSchema.CqlType cqlType = dataType.getCqlType();
    switch (cqlType) {
      case MAP:
        List<ChangeSchema.DataType> mapArgs = dataType.getTypeArguments();
        if (mapArgs != null && mapArgs.size() == 2) {
          return "Map_" + generateTypeName(mapArgs.get(0)) + "_" + generateTypeName(mapArgs.get(1));
        }
        return "Map";
      case LIST:
        List<ChangeSchema.DataType> listArgs = dataType.getTypeArguments();
        if (listArgs != null && !listArgs.isEmpty()) {
          return "List_" + generateTypeName(listArgs.get(0));
        }
        return "List";
      case SET:
        List<ChangeSchema.DataType> setArgs = dataType.getTypeArguments();
        if (setArgs != null && !setArgs.isEmpty()) {
          return "Set_" + generateTypeName(setArgs.get(0));
        }
        return "Set";
      case TUPLE:
        List<ChangeSchema.DataType> tupleArgs = dataType.getTypeArguments();
        if (tupleArgs != null && !tupleArgs.isEmpty()) {
          StringBuilder sb = new StringBuilder("Tuple");
          for (ChangeSchema.DataType arg : tupleArgs) {
            sb.append("_").append(generateTypeName(arg));
          }
          return sb.toString();
        }
        return "Tuple";
      case UDT:
        ChangeSchema.DataType.UdtType udtType = dataType.getUdtType();
        if (udtType != null && udtType.getName() != null) {
          // Sanitize UDT name for Avro (replace dots and other invalid chars)
          return "UDT_" + udtType.getName().replace(".", "_").replace("-", "_");
        }
        return "UDT";
      default:
        return cqlType.name();
    }
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
