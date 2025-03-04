package com.scylladb.cdc.debezium.connector.transforms;

import java.util.Map;
import io.debezium.data.Envelope;
import io.debezium.transforms.ExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteHandling;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

public class ScyllaExtractNewRecordState<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {
    private Cache<Integer, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    private DeleteHandling handleDeletes;

    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
    }


    @Override
    public R apply(final R record) {
        // Debezium by default does not emit a rewrite (delete.handling.mode)
        // when an after field is present. However, in ScyllaDB, we represent
        // the affected keys in the after field.
        boolean isDelete = false;
        boolean hasRewrite = false;

        if (record != null && (record.value() instanceof Struct) && record instanceof SourceRecord) {
            final SourceRecord sr = (SourceRecord) record;
                 if (Envelope.operationFor(sr) == Envelope.Operation.DELETE) {
                     isDelete = true;
                 }
        }

        final R ret = super.apply(record);
        if (ret == null || !(ret.value() instanceof Struct)) {
            return ret;
        }

        final Struct value = (Struct)ret.value();

        int schemaKey = getSchemaKey(value);

        Schema updatedSchema = schemaUpdateCache.get(schemaKey);
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), value);
            schemaUpdateCache.put(schemaKey, updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (isDroppableField(field, value)) {
                continue;
            }

            if (isSimplifiableField(field)) {
                Struct fieldValue = (Struct) value.get(field);
                updatedValue.put(field.name(), fieldValue == null ? null : fieldValue.get("value"));
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

    if (isDelete) {
           if (handleDeletes == DeleteHandling.REWRITE) {
                updatedValue.put(ExtractNewRecordStateConfigDefinition.DELETED_FIELD, "true");
       }
        }

        return ret.newRecord(ret.topic(), ret.kafkaPartition(), ret.keySchema(), ret.key(), updatedSchema, updatedValue, ret.timestamp());
    }

    @Override
    public void close() {
        super.close();
        schemaUpdateCache = null;
    }

    // getSchemaKey computes a unique schema according to the payload values
    // This ensures different values are stored with different keys in the LRU cache.
    private int getSchemaKey(Struct value) {
       Schema schema = value.schema();
       final int prime = 31;
       int hash = 1;

       hash += prime * hash + (schema.name() == null ? 0 : schema.name().hashCode());

       for (Field field : schema.fields()) {
            hash = prime * hash + field.name().hashCode();

            Object fieldValue = value.get(field);
            boolean isNull = fieldValue == null;
            hash = prime * hash + (isNull ? 0 : 1);

            if (!isNull && field.schema().type() == Type.STRUCT) {
                 hash = prime * hash + field.schema().type().hashCode();
            }
       }

       return hash;
    }

    // isDroppableField drops null Struct values within a Field. It does NOT drops empty Structs representing an actual delta mutation.
    // This allow subscribers to upsert deltas and converge to a stable result. For example, the following Type/Values will return:
    // False (don't drop) -- Type: STRUCT Value: Struct{}
    // True (drop)        -- Type: STRUCT Value: null
    private boolean isDroppableField(Field field, Struct value) {
        if (field.schema().type() == Type.STRUCT
            && (value.get(field) == null)) {
            return field.schema().isOptional();
        }

        return false;
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

    private Schema makeUpdatedSchema(Schema schema, Struct value) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (isDroppableField(field, value)) {
              continue;
            }

            if (isSimplifiableField(field)) {
                builder.field(field.name(), field.schema().field("value").schema());
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }
}
