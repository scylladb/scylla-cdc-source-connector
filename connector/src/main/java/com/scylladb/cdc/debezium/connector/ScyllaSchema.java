package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaSchema implements DatabaseSchema<CollectionId> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaSchema.class);
  public static final String CELL_VALUE = "value";

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
      if (!isSupportedColumnSchema(cdef)) continue;

      Schema columnSchema = computeColumnSchema(cdef);
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
      if (!isSupportedColumnSchema(cdef)) continue;

      Schema columnSchema = computeColumnSchema(cdef);
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
      if (!isSupportedColumnSchema(cdef)) continue;

      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY
          && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) {
        afterSchemaBuilder =
            afterSchemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
      } else {
        Schema columnSchema = computeColumnSchema(cdef);
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
      if (!isSupportedColumnSchema(cdef)) continue;

      if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY
          && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) {
        beforeSchemaBuilder =
            beforeSchemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
      } else {
        Schema columnSchema = computeColumnSchema(cdef);
        beforeSchemaBuilder = beforeSchemaBuilder.field(cdef.getColumnName(), columnSchema);
      }
    }
    return beforeSchemaBuilder.optional().build();
  }

  private Schema computeColumnSchema(ChangeSchema.ColumnDefinition cdef) {
    switch (cdef.getCdcLogDataType().getCqlType()) {
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
      case LIST:
      case MAP:
      case SET:
      case UDT:
      case TUPLE:
      default:
        throw new UnsupportedOperationException();
    }
  }

  protected static boolean isSupportedColumnSchema(ChangeSchema.ColumnDefinition cdef) {
    ChangeSchema.CqlType type = cdef.getCdcLogDataType().getCqlType();
    return type != ChangeSchema.CqlType.LIST
        && type != ChangeSchema.CqlType.MAP
        && type != ChangeSchema.CqlType.SET
        && type != ChangeSchema.CqlType.UDT
        && type != ChangeSchema.CqlType.TUPLE;
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
