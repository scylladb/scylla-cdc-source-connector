package com.scylladb.cdc.debezium.connector.transforms;

import io.debezium.transforms.ExtractNewRecordState;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema.Type;

import java.util.*;

public class ScyllaExtractFlattenedNewRecordState<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {
    private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

    @Override
    public R apply(final R record) {
        final R ret = super.apply(record);
        if (ret == null || !(ret.value() instanceof Struct)) {
            return ret;
        }

        final Struct value = (Struct) ret.value();

        Schema updatedSchema = makeUpdatedSchema(value);

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (isFlattenableField(field)) {
                Struct fieldValue = (Struct) value.get(field);
                if (fieldValue == null) {
                    updatedValue.put(field.name(), null);
                } else {
                    Map<Object, Object> fieldElementValue = (Map<Object, Object>) fieldValue.get("elements");
                    Set<Object> elementKeys = fieldElementValue.keySet();
                    if (elementKeys.size() == 0) {
                        return ret;
                    } else if (elementKeys.size() == 1) {
                        if(elementKeys.toArray()[0] instanceof Struct) {
                            updatedValue.put(field.name(),Arrays.asList(elementKeys.toArray()));
                            continue;
                        }
                        String timeUUIDfieldName = (String) elementKeys.toArray()[0];
                        try {
                            UUID.fromString(timeUUIDfieldName);
                            updatedValue.put(field.name(), fieldElementValue.get(timeUUIDfieldName));
                            } catch (IllegalArgumentException ex) {
                                List values = new ArrayList();
                                for (Object key : elementKeys) {
                                    Object innerFieldObject = fieldElementValue.get(key);
                                    values.add(innerFieldObject);
                                }
                            updatedValue.put(field.name(), values);
                            }
                    } else {
                        if(elementKeys.toArray()[0] instanceof Struct) {
                            updatedValue.put(field.name(),Arrays.asList(elementKeys.toArray()));
                            continue;
                        }
                        List<Struct> values = new ArrayList();
                        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
                        for (Object key : elementKeys) {
                            try {
                                UUID.fromString((String)key);
                                Object newValue = fieldElementValue.get(key);
                                    values.add((Struct) newValue);
                                    schemaBuilder.field((String)key);
                            } catch (IllegalArgumentException ex) {
                                Object innerFieldObject = fieldElementValue.get(key);
                                values.add((Struct) innerFieldObject);
                                schemaBuilder.field((String)key);
                            }
                            }
                        Field newField = new Field(field.name(), field.index(),SchemaBuilder.array(schemaBuilder.build()).build());
                        updatedValue.put(newField.name(), values);
                        }
                    }
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return ret.newRecord(ret.topic(), ret.kafkaPartition(), ret.keySchema(), ret.key(), updatedSchema, updatedValue, ret.timestamp());
    }

    @Override
    public void close() {
        super.close();
        schemaUpdateCache = null;
    }

    private boolean isFlattenableField(Field field) {
        if (field.schema().type() != Schema.Type.STRUCT) {
            return false;
        }
        if (field.schema().fields().size() != 2
                || !field.schema().fields().get(1).name().equals("elements")) {
            return false;
        }
        return true;
    }
    private Schema makeUpdatedSchema(Struct struct) {
        Schema schema = struct.schema();
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (isFlattenableField(field)) {
                Struct fieldValue = (Struct) struct.get(field);
                if (fieldValue == null) {
                    builder.field(field.name(), field.schema());
                    continue;
                }
                Map<String, Object> fieldElementValue = (Map<String, Object>) fieldValue.get("elements");
                Set<String> elementKeys = fieldElementValue.keySet();

                if (elementKeys == null || elementKeys.size() == 0) {
                    builder.field(field.name(), field.schema());
                    continue;
                } else if (elementKeys != null && elementKeys.size() == 1) {
                    if(elementKeys.toArray()[0] instanceof Struct){
                        Schema arraySchema = SchemaBuilder.array(((Struct) elementKeys.toArray()[0]).schema()).build();
                        builder.field(field.name(), arraySchema);
                        continue;
                    }
                    String timeUUIDfieldName = (String) elementKeys.toArray()[0];
                    try {
                        UUID.fromString(timeUUIDfieldName);
                        Struct fieldStruct = (Struct) fieldElementValue.get(timeUUIDfieldName);
                        builder.field(field.name(), fieldStruct.schema());
                    } catch (IllegalArgumentException ex) {
                        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
                        for (String key : elementKeys) {
                            Object innerFieldObject = fieldElementValue.get(key);
                        if (innerFieldObject instanceof Struct) {
                            Struct innerFieldStruct = (Struct) innerFieldObject;
                            schemaBuilder.field(key, innerFieldStruct.schema());
                        } else {
                            schemaBuilder.field(key, Schema.STRING_SCHEMA);
                        }
                        }
                        Schema arraySchema = SchemaBuilder.array(schemaBuilder.build()).build();
                        builder.field(field.name(), arraySchema);
                    }
                } else if (elementKeys != null && elementKeys.size() > 1) {
                    if(elementKeys.toArray()[0] instanceof Struct){
                        Schema arraySchema = SchemaBuilder.array(((Struct) elementKeys.toArray()[0]).schema()).build();
                        builder.field(field.name(), arraySchema);
                        continue;
                    }
                    String timeUUIDfieldName = (String) elementKeys.toArray()[0];
                    try {
                        UUID.fromString(timeUUIDfieldName);
                        Struct fieldStruct = (Struct) fieldElementValue.get(timeUUIDfieldName);
                        Schema arraySchema = SchemaBuilder.array(fieldStruct.schema()).build();
                        builder.field(field.name(), arraySchema);
                    } catch (IllegalArgumentException ex) {
                        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
                        for (String key : elementKeys) {
                            Object innerFieldObject = fieldElementValue.get(key);
                            if (innerFieldObject instanceof Struct) {
                                Struct innerFieldStruct = (Struct) innerFieldObject;
                                schemaBuilder.field(key, innerFieldStruct.schema());
                            } else {
                                schemaBuilder.field(key, Schema.STRING_SCHEMA);
                            }
                        }
                        Schema arraySchema = SchemaBuilder.array(schemaBuilder.build()).build();
                        builder.field(field.name(), arraySchema);
                    }
                }
            } else {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }
}
