package com.scylladb.cdc.debezium.connector.transforms;

import io.debezium.transforms.ExtractNewRecordState;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

public class ScyllaExtractNewRecordState<R extends ConnectRecord<R>>
    extends ExtractNewRecordState<R> {
  private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

  @Override
  public R apply(final R record) {
    final R ret = super.apply(record);
    if (ret == null || !(ret.value() instanceof Struct)) {
      return ret;
    }

    final Struct value = (Struct) ret.value();

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema());
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      if (isSimplifiableField(field)) {
        Struct fieldValue = (Struct) value.get(field);
        Object extractedValue = fieldValue == null ? null : fieldValue.get("value");
        Schema innerSchema = field.schema().field("value").schema();

        if (isNonFrozenCollectionSchema(innerSchema) && extractedValue instanceof Struct) {
          Struct nonFrozenValue = (Struct) extractedValue;
          Schema flattenedSchema = updatedSchema.field(field.name()).schema();
          updatedValue.put(
              field.name(), flattenNonFrozenCollection(nonFrozenValue, flattenedSchema));
        } else {
          updatedValue.put(field.name(), extractedValue);
        }
      } else {
        updatedValue.put(field.name(), value.get(field));
      }
    }

    return ret.newRecord(
        ret.topic(),
        ret.kafkaPartition(),
        ret.keySchema(),
        ret.key(),
        updatedSchema,
        updatedValue,
        ret.timestamp());
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

    if (field.schema().fields().size() != 1 || field.schema().fields().get(0).name() != "value") {
      return false;
    }

    return true;
  }

  /**
   * Checks if a schema represents a non-frozen collection structure (has "mode" and "elements"
   * fields).
   */
  private boolean isNonFrozenCollectionSchema(Schema schema) {
    if (schema == null || schema.type() != Type.STRUCT) {
      return false;
    }
    Field modeField = schema.field("mode");
    Field elementsField = schema.field("elements");
    return modeField != null && elementsField != null && modeField.schema().type() == Type.STRING;
  }

  /** Determines if a non-frozen collection is a UDT (elements is a struct, not an array). */
  private boolean isNonFrozenUdtSchema(Schema elementsSchema) {
    return elementsSchema != null && elementsSchema.type() == Type.STRUCT;
  }

  /** Determines if a non-frozen collection is a SET (elements array has "element" and "added"). */
  private boolean isNonFrozenSetSchema(Schema elementsSchema) {
    if (elementsSchema == null || elementsSchema.type() != Type.ARRAY) {
      return false;
    }
    Schema entrySchema = elementsSchema.valueSchema();
    return entrySchema != null
        && entrySchema.type() == Type.STRUCT
        && entrySchema.field("element") != null
        && entrySchema.field("added") != null;
  }

  /**
   * Determines if a non-frozen collection is a MAP or LIST (elements array has "key" and "value").
   */
  private boolean isNonFrozenMapOrListSchema(Schema elementsSchema) {
    if (elementsSchema == null || elementsSchema.type() != Type.ARRAY) {
      return false;
    }
    Schema entrySchema = elementsSchema.valueSchema();
    return entrySchema != null
        && entrySchema.type() == Type.STRUCT
        && entrySchema.field("key") != null
        && entrySchema.field("value") != null;
  }

  /** Flattens a non-frozen collection value based on its type. */
  private Object flattenNonFrozenCollection(Struct nonFrozenValue, Schema flattenedSchema) {
    Struct elementsStruct = null;
    List<?> elementsList = null;
    Object elementsObj = nonFrozenValue.get("elements");

    if (elementsObj instanceof Struct) {
      elementsStruct = (Struct) elementsObj;
    } else if (elementsObj instanceof List) {
      elementsList = (List<?>) elementsObj;
    }

    Schema originalElementsSchema = nonFrozenValue.schema().field("elements").schema();

    if (isNonFrozenUdtSchema(originalElementsSchema) && elementsStruct != null) {
      return flattenNonFrozenUdt(elementsStruct, flattenedSchema);
    } else if (isNonFrozenSetSchema(originalElementsSchema) && elementsList != null) {
      return flattenNonFrozenSet(elementsList, flattenedSchema);
    } else if (isNonFrozenMapOrListSchema(originalElementsSchema) && elementsList != null) {
      return flattenNonFrozenMapOrList(elementsList, flattenedSchema);
    }

    // Unknown structure, return as-is
    return nonFrozenValue;
  }

  /** Flattens a non-frozen UDT: extracts field values from {fieldName: {value: x}} structure. */
  private Struct flattenNonFrozenUdt(Struct elementsStruct, Schema flattenedSchema) {
    Struct result = new Struct(flattenedSchema);
    for (Field field : elementsStruct.schema().fields()) {
      Object fieldCell = elementsStruct.get(field);
      if (fieldCell instanceof Struct) {
        Struct cellStruct = (Struct) fieldCell;
        result.put(field.name(), cellStruct.get("value"));
      } else {
        result.put(field.name(), null);
      }
    }
    return result;
  }

  /** Flattens a non-frozen SET: extracts elements with added=true. */
  @SuppressWarnings("unchecked")
  private List<Object> flattenNonFrozenSet(List<?> elementsList, Schema flattenedSchema) {
    List<Object> result = new ArrayList<>();
    for (Object entry : elementsList) {
      if (entry instanceof Struct) {
        Struct entryStruct = (Struct) entry;
        Boolean added = (Boolean) entryStruct.get("added");
        if (Boolean.TRUE.equals(added)) {
          result.add(entryStruct.get("element"));
        }
      }
    }
    return result;
  }

  /**
   * Flattens a non-frozen MAP/LIST: for MAP keeps [{key, value}] structs, for LIST outputs [value].
   */
  @SuppressWarnings("unchecked")
  private List<Object> flattenNonFrozenMapOrList(List<?> elementsList, Schema flattenedSchema) {
    List<Object> result = new ArrayList<>();
    Schema entrySchema = flattenedSchema.valueSchema();

    // Check if the flattened schema expects structs (MAP) or direct values (LIST)
    boolean isMapStyle = entrySchema != null && entrySchema.type() == Type.STRUCT;

    for (Object entry : elementsList) {
      if (entry instanceof Struct) {
        Struct entryStruct = (Struct) entry;
        Object key = entryStruct.get("key");
        Object value = entryStruct.get("value");

        if (isMapStyle) {
          // MAP: output as {key, value} structs (keep original format)
          Struct mapEntry = new Struct(entrySchema);
          mapEntry.put("key", key);
          mapEntry.put("value", value);
          result.add(mapEntry);
        } else {
          // LIST: output just the value (ignore the timeuuid key)
          result.add(value);
        }
      }
    }
    return result;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      if (isSimplifiableField(field)) {
        Schema innerSchema = field.schema().field("value").schema();
        if (isNonFrozenCollectionSchema(innerSchema)) {
          builder.field(field.name(), makeFlattenedNonFrozenSchema(innerSchema));
        } else {
          builder.field(field.name(), innerSchema);
        }
      } else {
        builder.field(field.name(), field.schema());
      }
    }

    return builder.build();
  }

  /** Creates a flattened schema for a non-frozen collection. */
  private Schema makeFlattenedNonFrozenSchema(Schema nonFrozenSchema) {
    Schema elementsSchema = nonFrozenSchema.field("elements").schema();

    if (isNonFrozenUdtSchema(elementsSchema)) {
      // UDT: flatten {fieldName: {value: x}} to {fieldName: x}
      SchemaBuilder udtBuilder = SchemaBuilder.struct();
      for (Field field : elementsSchema.fields()) {
        Schema cellSchema = field.schema();
        if (cellSchema.type() == Type.STRUCT && cellSchema.field("value") != null) {
          udtBuilder.field(field.name(), cellSchema.field("value").schema());
        } else {
          udtBuilder.field(field.name(), cellSchema);
        }
      }
      return udtBuilder.optional().build();
    } else if (isNonFrozenSetSchema(elementsSchema)) {
      // SET: flatten to array of element types
      Schema entrySchema = elementsSchema.valueSchema();
      Schema elementSchema = entrySchema.field("element").schema();
      return SchemaBuilder.array(elementSchema).optional().build();
    } else if (isNonFrozenMapOrListSchema(elementsSchema)) {
      // MAP/LIST: flatten to array of {key, value} structs or array of values
      Schema entrySchema = elementsSchema.valueSchema();
      Schema keySchema = entrySchema.field("key").schema();
      Schema valueSchema = entrySchema.field("value").schema();

      // Check if key is a string (timeuuid for LIST) or a primitive (MAP)
      // For LIST, the key is typically a string (timeuuid), for MAP it's the actual key type
      if (keySchema.type() == Type.STRING) {
        // LIST: just array of values
        return SchemaBuilder.array(valueSchema).optional().build();
      } else {
        // MAP: array of {key, value} structs
        Schema mapEntrySchema =
            SchemaBuilder.struct().field("key", keySchema).field("value", valueSchema).build();
        return SchemaBuilder.array(mapEntrySchema).optional().build();
      }
    }

    // Unknown, return as-is
    return nonFrozenSchema;
  }
}
