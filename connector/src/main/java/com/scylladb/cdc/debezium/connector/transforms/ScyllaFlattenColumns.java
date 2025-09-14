package com.scylladb.cdc.debezium.connector.transforms;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

public class ScyllaFlattenColumns<R extends ConnectRecord<R>> implements Transformation<R> {
  private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  private Cache<Schema, Schema> subSchemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

  @Override
  public R apply(final R record) {
    if (record.value() == null || !(record.value() instanceof Struct)) {
      return record;
    }
    Struct value = (Struct) record.value();

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema());
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);
    for (Field field : value.schema().fields()) {
      if (Objects.equals(field.name(), "before") || Objects.equals(field.name(), "after")) {
        Struct fieldValue = (Struct) value.get(field);
        if (fieldValue != null) {
          Struct updatedFieldValue = new Struct(makeUpdatedSubSchema(field.schema()));
          for (Field subField : field.schema().fields()) {
            if (isSimplifiableField(subField)) {
              Struct subFieldValue = (Struct) fieldValue.get(subField);
              updatedFieldValue.put(
                  subField.name(), subFieldValue == null ? null : subFieldValue.get("value"));
            } else {
              updatedFieldValue.put(subField.name(), fieldValue.get(subField));
            }
          }
          updatedValue.put(field.name(), updatedFieldValue);
        } else {
          updatedValue.put(field.name(), null);
        }
      } else {
        updatedValue.put(field.name(), value.get(field));
      }
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedSchema,
        updatedValue,
        record.timestamp());
  }

  private boolean isSimplifiableField(Field field) {
    if (field.schema().type() != Schema.Type.STRUCT) {
      return false;
    }

    if (field.schema().fields().size() != 1
        || !Objects.equals(field.schema().fields().get(0).name(), "value")) {
      return false;
    }

    return true;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      if (Objects.equals(field.name(), "before") || Objects.equals(field.name(), "after")) {
        builder.field(field.name(), makeUpdatedSubSchema(field.schema()));
      } else {
        builder.field(field.name(), field.schema());
      }
    }

    return builder.build();
  }

  private Schema makeUpdatedSubSchema(Schema subSchema) {
    Schema updatedSubSchema = subSchemaUpdateCache.get(subSchema);
    if (updatedSubSchema != null) {
      return updatedSubSchema;
    }

    final SchemaBuilder subBuilder = SchemaUtil.copySchemaBasics(subSchema, SchemaBuilder.struct());
    if (subSchema.isOptional()) {
      subBuilder.optional();
    }
    for (Field subField : subSchema.fields()) {
      if (isSimplifiableField(subField)) {
        subBuilder.field(subField.name(), subField.schema().field("value").schema());
      } else {
        subBuilder.field(subField.name(), subField.schema());
      }
    }
    updatedSubSchema = subBuilder.build();
    subSchemaUpdateCache.put(subSchema, updatedSubSchema);
    return updatedSubSchema;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
    subSchemaUpdateCache = null;
  }

  @Override
  public void configure(Map<String, ?> map) {}
}
