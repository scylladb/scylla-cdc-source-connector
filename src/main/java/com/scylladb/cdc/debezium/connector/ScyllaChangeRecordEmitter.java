package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Struct;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class ScyllaChangeRecordEmitter extends AbstractChangeRecordEmitter<ScyllaCollectionSchema> {

    private final RawChange change;
    private final ScyllaSchema schema;

    public ScyllaChangeRecordEmitter(RawChange change, OffsetContext offsetContext, ScyllaSchema schema, Clock clock) {
        super(offsetContext, clock);
        this.change = change;
        this.schema = schema;
    }

    public RawChange getChange() {
        return change;
    }

    public ScyllaSchema getSchema() {
        return schema;
    }

    @Override
    protected Envelope.Operation getOperation() {
        RawChange.OperationType operationType = this.change.getOperationType();
        switch (operationType) {
            case ROW_UPDATE:
                return Envelope.Operation.UPDATE;
            case ROW_INSERT:
                return Envelope.Operation.CREATE;
            case PARTITION_DELETE: // See comment in ScyllaChangesConsumer on the support of partition deletes.
            case ROW_DELETE:
                return Envelope.Operation.DELETE;
            default:
                throw new RuntimeException(String.format("Unsupported operation type: %s.", operationType));
        }
    }

    @Override
    protected void emitReadRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void emitCreateRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        scyllaCollectionSchema = this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

        Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
        Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
        fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

        Struct envelope = scyllaCollectionSchema.getEnvelopeSchema().create(afterStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(scyllaCollectionSchema, getOperation(), keyStruct, envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        scyllaCollectionSchema = this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

        Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
        Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
        fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

        Struct envelope = scyllaCollectionSchema.getEnvelopeSchema().update(null, afterStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(scyllaCollectionSchema, getOperation(), keyStruct, envelope, getOffset(), null);
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        scyllaCollectionSchema = this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

        Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
        Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
        fillStructWithChange(scyllaCollectionSchema, keyStruct, beforeStruct, change);

        Struct envelope = scyllaCollectionSchema.getEnvelopeSchema().delete(beforeStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(scyllaCollectionSchema, getOperation(), keyStruct, envelope, getOffset(), null);
    }

    private void fillStructWithChange(ScyllaCollectionSchema schema, Struct keyStruct, Struct valueStruct, RawChange change) {
        for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
            if (!ScyllaSchema.isSupportedColumnSchema(change.getSchema(), cdef)) continue;

            Object value = translateFieldToKafka(change.getCell(cdef.getColumnName()));

            if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) {
                valueStruct.put(cdef.getColumnName(), value);
                keyStruct.put(cdef.getColumnName(), value);
            } else {
                Boolean isDeleted = this.change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean();
                if (value != null || (isDeleted != null && isDeleted)) {
                    Struct cell = new Struct(schema.cellSchema(cdef.getColumnName()));
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
           return field.getList().stream().map(this::translateFieldToKafka).collect(Collectors.toList());
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.SET) {
           return field.getSet().stream().map(this::translateFieldToKafka).collect(Collectors.toList());
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.MAP) {
           Map<Field, Field> map = field.getMap();
           Map<Object, Object> kafkaMap = new LinkedHashMap<>();
           map.forEach((key, value) -> {
               Object kafkaKey = translateFieldToKafka(key);
               Object kafkaValue = translateFieldToKafka(value);
               kafkaMap.put(kafkaKey, kafkaValue);
           });
           return kafkaMap;
       }

       return field.getAsObject();
    }
}
