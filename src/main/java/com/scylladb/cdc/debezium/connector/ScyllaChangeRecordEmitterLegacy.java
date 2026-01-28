package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Legacy emitter that wraps non-PK column values in Cell structs.
 *
 * <p>This is the v1 format where each non-primary-key column is wrapped in a Cell struct containing
 * a "value" field: {@code {"value": <actual_value>}}
 *
 * <p>Used when {@code cdc.output.format=legacy} is configured.
 */
public class ScyllaChangeRecordEmitterLegacy
    extends AbstractChangeRecordEmitter<ScyllaPartition, ScyllaCollectionSchema> {
  /** CDC column prefix for deleted column indicators. */
  private static final String CDC_DELETED_PREFIX = "cdc$deleted_";

  private final RawChange change;
  private final ScyllaSchemaLegacy schema;
  private final RawChange preImage;

  public ScyllaChangeRecordEmitterLegacy(
      ScyllaPartition partition,
      RawChange preImage,
      RawChange change,
      OffsetContext offsetContext,
      ScyllaSchemaLegacy schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig) {
    super(partition, offsetContext, clock, connectorConfig);
    this.change = change;
    this.schema = schema;
    this.preImage = preImage;
  }

  /**
   * Returns the raw CDC change being processed.
   *
   * @return the raw change from the CDC log
   */
  public RawChange getChange() {
    return change;
  }

  /**
   * Returns the legacy schema used for record emission.
   *
   * @return the legacy schema instance
   */
  public ScyllaSchemaLegacy getSchema() {
    return schema;
  }

  @Override
  public Envelope.Operation getOperation() {
    RawChange.OperationType operationType = this.change.getOperationType();
    switch (operationType) {
      case ROW_UPDATE:
        return Envelope.Operation.UPDATE;
      case ROW_INSERT:
        return Envelope.Operation.CREATE;
      case PARTITION_DELETE: // See comment in ScyllaChangesConsumerLegacy on the support of
      // partition deletes.
      case ROW_DELETE:
        return Envelope.Operation.DELETE;
      default:
        throw new IllegalStateException(
            String.format(
                "Unsupported operation type: %s for change %s",
                operationType, this.change.getId()));
    }
  }

  @Override
  protected void emitReadRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void emitCreateRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    scyllaCollectionSchema =
        this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

    Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
    Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
    fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

    Struct envelope;
    if (preImage != null) {
      Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
      fillStructWithChange(scyllaCollectionSchema, null, beforeStruct, preImage);
      envelope =
          generalizedEnvelope(
              scyllaCollectionSchema.getEnvelopeSchema().schema(),
              beforeStruct,
              afterStruct,
              getOffset().getSourceInfo(),
              getClock().currentTimeAsInstant(),
              Envelope.Operation.CREATE);
    } else {
      envelope =
          scyllaCollectionSchema
              .getEnvelopeSchema()
              .create(afterStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
    }

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        null);
  }

  @Override
  protected void emitUpdateRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    scyllaCollectionSchema =
        this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

    Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
    Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
    fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

    Struct envelope;
    if (preImage != null) {
      Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
      fillStructWithChange(scyllaCollectionSchema, null, beforeStruct, preImage);
      envelope =
          generalizedEnvelope(
              scyllaCollectionSchema.getEnvelopeSchema().schema(),
              beforeStruct,
              afterStruct,
              getOffset().getSourceInfo(),
              getClock().currentTimeAsInstant(),
              Envelope.Operation.UPDATE);
    } else {
      envelope =
          scyllaCollectionSchema
              .getEnvelopeSchema()
              .update(
                  null,
                  afterStruct,
                  getOffset().getSourceInfo(),
                  getClock().currentTimeAsInstant());
    }

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        null);
  }

  @Override
  protected void emitDeleteRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    scyllaCollectionSchema =
        this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

    Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
    Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
    if (preImage != null) {
      fillStructWithChange(scyllaCollectionSchema, keyStruct, beforeStruct, preImage);
    } else {
      fillStructWithChange(scyllaCollectionSchema, keyStruct, beforeStruct, change);
    }

    Struct envelope =
        scyllaCollectionSchema
            .getEnvelopeSchema()
            .delete(beforeStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        null);
  }

  private void fillStructWithChange(
      ScyllaCollectionSchema schema, Struct keyStruct, Struct valueStruct, RawChange change) {
    for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchemaLegacy.isSupportedColumnSchema(cdef)) continue;

      Object value = translateCellToKafka(change.getCell(cdef.getColumnName()));

      if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY
          || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) {
        valueStruct.put(cdef.getColumnName(), value);
        if (keyStruct != null) {
          keyStruct.put(cdef.getColumnName(), value);
        }
      } else {
        Boolean isDeleted =
            this.change.getCell(CDC_DELETED_PREFIX + cdef.getColumnName()).getBoolean();
        if (value != null || (isDeleted != null && isDeleted)) {
          Struct cell = new Struct(schema.cellSchema(cdef.getColumnName()));
          cell.put(ScyllaSchemaLegacy.CELL_VALUE, value);
          valueStruct.put(cdef.getColumnName(), cell);
        }
      }
    }
  }

  private Struct generalizedEnvelope(
      Schema schema,
      Object before,
      Object after,
      Struct source,
      Instant timestamp,
      Envelope.Operation operationType) {
    Struct struct = new Struct(schema);
    struct.put(Envelope.FieldName.OPERATION, operationType.code());
    if (before != null) {
      struct.put(Envelope.FieldName.BEFORE, before);
    }
    if (after != null) {
      struct.put(Envelope.FieldName.AFTER, after);
    }
    if (source != null) {
      struct.put(Envelope.FieldName.SOURCE, source);
    }
    if (timestamp != null) {
      struct.put(Envelope.FieldName.TIMESTAMP, timestamp.toEpochMilli());
    }
    return struct;
  }

  private Object translateCellToKafka(Cell cell) {
    ChangeSchema.DataType dataType = cell.getColumnDefinition().getCdcLogDataType();

    if (cell.getAsObject() == null) {
      return null;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DECIMAL) {
      return cell.getDecimal().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.UUID) {
      return cell.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.TIMEUUID) {
      return cell.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.VARINT) {
      return cell.getVarint().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.INET) {
      return cell.getInet().getHostAddress();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DATE) {
      CqlDate cqlDate = cell.getDate();
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.clear();
      // Months start from 0 in Calendar:
      calendar.set(cqlDate.getYear(), cqlDate.getMonth() - 1, cqlDate.getDay());
      return Date.from(calendar.toInstant());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DURATION) {
      return cell.getDuration().toString();
    }

    return cell.getAsObject();
  }
}
