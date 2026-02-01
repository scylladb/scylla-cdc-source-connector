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

public class ScyllaExtractNewRecordState<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {
    private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

    @Override
    public R apply(final R record) {
        final R ret = super.apply(record);
        if (ret == null || !(ret.value() instanceof Struct)) {
            return ret;
        }

        final Struct value = (Struct)ret.value();

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (isSimplifiableField(field)) {
                Struct fieldValue = (Struct) value.get(field);
                updatedValue.put(field.name(), fieldValue == null ? null : fieldValue.get("value"));
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

    private boolean isSimplifiableField(Field field) {
        if (field.schema().type() != Type.STRUCT) {
            return false;
        }

        if (field.schema().fields().size() != 1
         || field.schema().fields().get(0).name() != "value") {
            return false;
        }

        return true;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (isSimplifiableField(field)) {
                builder.field(field.name(), field.schema().field("value").schema());
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }
}
