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
    private Cache<String, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

    @Override
    public R apply(final R record) {
        final R ret = super.apply(record);
        if (ret == null || !(ret.value() instanceof Struct)) {
            return ret;
        }

        final Struct value = (Struct)ret.value();

        String schemaKey = createSchemaKey(value);

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

        return ret.newRecord(ret.topic(), ret.kafkaPartition(), ret.keySchema(), ret.key(), updatedSchema, updatedValue, ret.timestamp());
    }

    @Override
    public void close() {
        super.close();
        schemaUpdateCache = null;
    }

    // createSchemaKey computes a unique schema according to the payload values
    // This ensures different values are stored with different keys in the LRU cache.
    private String createSchemaKey(Struct value) {
       Schema schema = value.schema();
       StringBuilder key = new StringBuilder();
       key.append(schema.name()).append(":");

       for (Field field : schema.fields()) {
            key.append(field.name()).append("-");

            Object fieldValue = value.get(field);
            boolean isNull = fieldValue == null;
            key.append(isNull ? "N" : "P");

            // Struct types
            if (!isNull && field.schema().type() == Type.STRUCT) {
                key.append("-");
                Struct structValue = (Struct) fieldValue;
                for (Field nestedField : structValue.schema().fields()) {
                    if (structValue.get(nestedField) != null) {
                        key.append(nestedField.name()).append(";");
                    }
                }
            }
       }

       return key.toString();
    }

    // isDroppableField drops null Struct values within a Field. It does NOT drops empty Structs representing an actual delta mutation.
    // This allow subscribers to upsert deltas and converge to a stable result. For example, the following Type/Values will return:
    // False (don't drop) -- Type: STRUCT Value: Struct{}
    // True (drop)        -- Type: STRUCT Value: null
    private boolean isDroppableField(Field field, Struct value) {
        if (field.schema().type() == Type.STRUCT
            && (value.get(field) == null)) {
            return true;
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
