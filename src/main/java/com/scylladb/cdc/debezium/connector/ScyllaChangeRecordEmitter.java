package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class ScyllaChangeRecordEmitter
    extends AbstractChangeRecordEmitter<ScyllaPartition, ScyllaCollectionSchema> {
  private final RawChange change;
  private final ScyllaSchema schema;
  private final RawChange preImage;

  public ScyllaChangeRecordEmitter(
      ScyllaPartition partition,
      RawChange preImage,
      RawChange change,
      OffsetContext offsetContext,
      ScyllaSchema schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig) {
    super(partition, offsetContext, clock, connectorConfig);
    this.change = change;
    this.schema = schema;
    this.preImage = preImage;
  }

  public RawChange getChange() {
    return change;
  }

  public ScyllaSchema getSchema() {
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
      case PARTITION_DELETE: // See comment in ScyllaChangesConsumer on the support of partition
      // deletes.
      case ROW_DELETE:
        return Envelope.Operation.DELETE;
      default:
        throw new RuntimeException(String.format("Unsupported operation type: %s.", operationType));
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

    private void fillStructWithChange(ScyllaCollectionSchema schema, Struct keyStruct, Struct valueStruct, RawChange change) {
        for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
            if (!ScyllaSchema.isSupportedColumnSchema(change.getSchema(), cdef)) continue;

            Schema cellSchema = schema.cellSchema(cdef.getColumnName());
            Schema innerSchema = cellSchema.field(ScyllaSchema.CELL_VALUE).schema();
            Object value = translateFieldToKafka(change.getCell(cdef.getColumnName()), innerSchema);

            if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) {
                valueStruct.put(cdef.getColumnName(), value);
                keyStruct.put(cdef.getColumnName(), value);
            } else {
                Boolean isDeleted = this.change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean();
                if (value != null || (isDeleted != null && isDeleted)) {
                    Struct cell = new Struct(cellSchema);
                    cell.put(ScyllaSchema.CELL_VALUE, value);
                    valueStruct.put(cdef.getColumnName(), cell);
                }
            }
        }
    }

    private Object translateFieldToKafka(Field field) {
       ChangeSchema.DataType dataType = field.getDataType();

    if (field.getAsObject() == null) {
      return null;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DECIMAL) {
      return field.getDecimal().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.UUID) {
      return field.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.TIMEUUID) {
      return field.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.VARINT) {
      return field.getVarint().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.INET) {
      return field.getInet().getHostAddress();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DATE) {
      CqlDate cqlDate = field.getDate();
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.clear();
      // Months start from 0 in Calendar:
      calendar.set(cqlDate.getYear(), cqlDate.getMonth() - 1, cqlDate.getDay());
      return Date.from(calendar.toInstant());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DURATION) {
      return field.getDuration().toString();
    }

       if (dataType.getCqlType() == ChangeSchema.CqlType.LIST) {
           Schema innerSchema = resultSchema.valueSchema();
           return field.getList().stream().map((element) -> this.translateFieldToKafka(element, innerSchema)).collect(Collectors.toList());
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.SET) {
           Schema innerSchema = resultSchema.valueSchema();
           return field.getSet().stream().map((element) -> this.translateFieldToKafka(element, innerSchema)).collect(Collectors.toList());
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.MAP) {
           Map<Field, Field> map = field.getMap();
           Map<Object, Object> kafkaMap = new LinkedHashMap<>();
           Schema keySchema = resultSchema.keySchema();
           Schema valueSchema = resultSchema.valueSchema();
           map.forEach((key, value) -> {
               Object kafkaKey = translateFieldToKafka(key, keySchema);
               Object kafkaValue = translateFieldToKafka(value, valueSchema);
               kafkaMap.put(kafkaKey, kafkaValue);
           });
           return kafkaMap;
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.TUPLE) {
           List<org.apache.kafka.connect.data.Field> fields_schemas = resultSchema.fields();
           Struct tupleStruct = new Struct(resultSchema);
           List<Field> tuple = field.getTuple();
           for (int i = 0; i < tuple.size(); i++) {
               tupleStruct.put("tuple_member_" + i, translateFieldToKafka(tuple.get(i), fields_schemas.get(i).schema()));
           }
           return tupleStruct;
       }

        if (dataType.getCqlType() == ChangeSchema.CqlType.UDT) {
            Struct udtStruct = new Struct(resultSchema);
            Map<String, Field> udt = field.getUDT();
            udt.forEach((name, value) -> {
                udtStruct.put(name, translateFieldToKafka(value, resultSchema.field(name).schema()));
            });
            return udtStruct;
        }

       return field.getAsObject();
    }
}
