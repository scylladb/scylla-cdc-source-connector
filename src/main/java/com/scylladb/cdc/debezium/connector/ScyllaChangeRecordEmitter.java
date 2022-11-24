package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.*;
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

            if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) {
                Object value = translateFieldToKafka(change.getCell(cdef.getColumnName()), schema.keySchema().field(cdef.getColumnName()).schema());
                valueStruct.put(cdef.getColumnName(), value);
                keyStruct.put(cdef.getColumnName(), value);
                continue;
            }

            Schema cellSchema = schema.cellSchema(cdef.getColumnName());
            if (ScyllaSchema.isNonFrozenCollection(change.getSchema(), cdef)) {
                Struct value = translateNonFrozenCollectionToKafka(valueStruct, change, cellSchema, cdef);
                valueStruct.put(cdef.getColumnName(), value);
                continue;
            }

            Schema innerSchema = cellSchema.field(ScyllaSchema.CELL_VALUE).schema();
            Object value = translateFieldToKafka(change.getCell(cdef.getColumnName()), innerSchema);
            Boolean isDeleted = this.change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean();

            if (value != null || (isDeleted != null && isDeleted)) {
                Struct cell = new Struct(cellSchema);
                cell.put(ScyllaSchema.CELL_VALUE, value);
                valueStruct.put(cdef.getColumnName(), cell);
            }
        }
    }

    private Struct translateNonFrozenCollectionToKafka(Struct valueStruct, RawChange change, Schema cellSchema, ChangeSchema.ColumnDefinition cdef) {
        Schema innerSchema = cellSchema.field(ScyllaSchema.CELL_VALUE).schema();
        Struct cell = new Struct(cellSchema);
        Struct value = new Struct(innerSchema);

        Cell elementsCell = change.getCell(cdef.getColumnName());
        Cell deletedElementsCell = change.getCell("cdc$deleted_elements_" + cdef.getColumnName());
        boolean isDeleted = Boolean.TRUE.equals(change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean());

        Object elements;
        boolean hasModified = false;
        switch (elementsCell.getDataType().getCqlType()) {
            case SET: {
                Schema elementsSchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
                Schema scyllaElementsSchema = SchemaBuilder.array(elementsSchema.keySchema()).build();
                List<Object> addedElements = (List<Object>) translateFieldToKafka(elementsCell, scyllaElementsSchema);
                List<Object> deletedElements = (List<Object>) translateFieldToKafka(deletedElementsCell, scyllaElementsSchema);
                Map<Object, Boolean> delta = new HashMap<>();
                if (addedElements != null) {
                    addedElements.forEach(element -> delta.put(element, true));
                }
                if (deletedElements != null) {
                    deletedElements.forEach(element -> delta.put(element, false));
                }

                hasModified = !delta.isEmpty();
                elements = delta;
                break;
            }
            case LIST:
            case MAP: {
                Schema elementsSchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
                Schema deletedElementsScyllaSchema = SchemaBuilder.array(elementsSchema.keySchema()).optional().build();
                Map<Object, Object> addedElements = (Map<Object, Object>) ObjectUtils.defaultIfNull(translateFieldToKafka(elementsCell, elementsSchema), new HashMap<>());
                List<Object> deletedKeys = (List<Object>)translateFieldToKafka(deletedElementsCell, deletedElementsScyllaSchema);
                if (deletedKeys != null) {
                    deletedKeys.forEach((key) -> {
                        addedElements.put(key, null);
                    });
                }

                hasModified = !addedElements.isEmpty();
                elements = addedElements;
                break;
            }
            case UDT: {
                List<Short> deletedKeysList = (List<Short>) translateFieldToKafka(deletedElementsCell, SchemaBuilder.array(Schema.INT16_SCHEMA).optional().build());
                Set<Short> deletedKeys;
                if (deletedKeysList == null) {
                    deletedKeys = new HashSet<>();
                } else {
                    deletedKeys = new HashSet<>(deletedKeysList);
                }

                Map<String, Field> elementsMap = elementsCell.getUDT();
                assert elementsMap instanceof LinkedHashMap;

                Schema udtSchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
                Struct udtStruct = new Struct(udtSchema);
                Short index = -1;
                for (Map.Entry<String, Field> element : elementsMap.entrySet()) {
                    index++;
                    if ((!element.getValue().isNull()) || deletedKeys.contains(index)) {
                        hasModified = true;
                        Schema fieldCellSchema = udtSchema.field(element.getKey()).schema();
                        Struct fieldCell = new Struct (fieldCellSchema);
                        if (element.getValue().isNull()) {
                            fieldCell.put(ScyllaSchema.CELL_VALUE, null);
                        } else {
                            fieldCell.put(ScyllaSchema.CELL_VALUE, translateFieldToKafka(element.getValue(), fieldCellSchema.field(ScyllaSchema.CELL_VALUE).schema()));
                        }
                        udtStruct.put(element.getKey(), fieldCell);
                    }
                }

                elements = udtStruct;
                break;
            }
            default:
                throw new RuntimeException("Unreachable");
        }

        if (!hasModified) {
            if (isDeleted) {
                cell.put(ScyllaSchema.CELL_VALUE, null);
                return cell;
            } else {
                return null;
            }
        }

        CollectionOperation mode = isDeleted ? CollectionOperation.OVERWRITE : CollectionOperation.MODIFY;
        value.put(ScyllaSchema.MODE_VALUE, mode.toString());
        value.put(ScyllaSchema.ELEMENTS_VALUE, elements);

        cell.put(ScyllaSchema.CELL_VALUE, value);
        return cell;
    }

    private Object translateFieldToKafka(Field field, Schema resultSchema) {
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
