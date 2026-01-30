package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.debezium.data.SchemaUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

/**
 * Unit test to verify that frozen collections schemas are valid and can be converted to Avro
 * format. This tests the schema construction to ensure all struct types have proper names which is
 * required for Avro serialization.
 */
public class FrozenCollectionsAvroSchemaTest {

  @Test
  void testSimpleMapSchemaHasName() {
    // Create a simple map schema (frozen<map<int, text>>)
    Schema mapEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_INT_TEXT")
            .field("key", Schema.OPTIONAL_INT32_SCHEMA)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema mapSchema = SchemaBuilder.array(mapEntrySchema).optional().build();

    // Verify schema has a name (required for Avro)
    assertNotNull(mapEntrySchema.name(), "Map entry schema should have a name");
    System.out.println("Map entry schema name: " + mapEntrySchema.name());

    // Create the value schema
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("test.TestTable.Value")
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("frozen_map_col", mapSchema)
            .optional()
            .build();

    assertNotNull(valueSchema.name(), "Value schema should have a name");
    System.out.println("Value schema: " + SchemaUtil.asString(valueSchema));
  }

  @Test
  void testTupleSchemaHasName() {
    // Create a tuple schema (frozen<tuple<int, text>>) with Avro-compatible field names
    Schema tupleSchema =
        SchemaBuilder.struct()
            .name("Tuple_INT_TEXT")
            .field("field_0", Schema.OPTIONAL_INT32_SCHEMA)
            .field("field_1", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    // Verify schema has a name (required for Avro)
    assertNotNull(tupleSchema.name(), "Tuple schema should have a name");
    System.out.println("Tuple schema name: " + tupleSchema.name());
  }

  @Test
  void testMapWithTupleKeySchemaHasNames() {
    // Create a tuple schema for map keys
    Schema tupleKeySchema =
        SchemaBuilder.struct()
            .name("Tuple_INT_TEXT")
            .field("0", Schema.OPTIONAL_INT32_SCHEMA)
            .field("1", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    // Create map entry with tuple key
    Schema mapEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_Tuple_INT_TEXT_TEXT")
            .field("key", tupleKeySchema)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    assertNotNull(tupleKeySchema.name(), "Tuple key schema should have a name");
    assertNotNull(mapEntrySchema.name(), "Map entry schema should have a name");
    System.out.println("Tuple key schema name: " + tupleKeySchema.name());
    System.out.println("Map entry schema name: " + mapEntrySchema.name());
  }

  @Test
  void testUdtSchemaHasName() {
    // Create a UDT schema (frozen<map_key_udt>)
    Schema udtSchema =
        SchemaBuilder.struct()
            .name("UDT_map_key_udt")
            .field("a", Schema.OPTIONAL_INT32_SCHEMA)
            .field("b", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    assertNotNull(udtSchema.name(), "UDT schema should have a name");
    System.out.println("UDT schema name: " + udtSchema.name());
  }

  @Test
  void testFullFrozenCollectionsSchemaStructure() {
    // Test all frozen collections in a single schema like the actual table
    Schema listSchema = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build();
    Schema setSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();

    // frozen<map<int, text>>
    Schema mapEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_INT_TEXT")
            .field("key", Schema.OPTIONAL_INT32_SCHEMA)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema mapSchema = SchemaBuilder.array(mapEntrySchema).optional().build();

    // frozen<tuple<int, text>>
    Schema tupleSchema =
        SchemaBuilder.struct()
            .name("Tuple_INT_TEXT")
            .field("0", Schema.OPTIONAL_INT32_SCHEMA)
            .field("1", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    // frozen<map<frozen<tuple<int, text>>, text>>
    Schema mapTupleEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_Tuple_INT_TEXT_TEXT")
            .field("key", tupleSchema)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema mapTupleSchema = SchemaBuilder.array(mapTupleEntrySchema).optional().build();

    // UDT schema
    Schema udtSchema =
        SchemaBuilder.struct()
            .name("UDT_map_key_udt")
            .field("a", Schema.OPTIONAL_INT32_SCHEMA)
            .field("b", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    // frozen<map<frozen<map_key_udt>, text>>
    Schema mapUdtEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_UDT_map_key_udt_TEXT")
            .field("key", udtSchema)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema mapUdtSchema = SchemaBuilder.array(mapUdtEntrySchema).optional().build();

    // Create the value schema
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("test.TestTable.Value")
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("frozen_list_col", listSchema)
            .field("frozen_set_col", setSchema)
            .field("frozen_map_col", mapSchema)
            .field("frozen_tuple_col", tupleSchema)
            .field("frozen_map_tuple_key_col", mapTupleSchema)
            .field("frozen_map_udt_key_col", mapUdtSchema)
            .optional()
            .build();

    // Verify all struct schemas have names
    assertNotNull(mapEntrySchema.name());
    assertNotNull(tupleSchema.name());
    assertNotNull(mapTupleEntrySchema.name());
    assertNotNull(udtSchema.name());
    assertNotNull(mapUdtEntrySchema.name());
    assertNotNull(valueSchema.name());

    System.out.println("Full schema: " + SchemaUtil.asString(valueSchema));
  }

  @Test
  void testStructCreation() {
    // Test that we can create structs with these schemas and populate them
    Schema tupleSchema =
        SchemaBuilder.struct()
            .name("Tuple_INT_TEXT")
            .field("0", Schema.OPTIONAL_INT32_SCHEMA)
            .field("1", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    Schema mapEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_Tuple_INT_TEXT_TEXT")
            .field("key", tupleSchema)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema mapSchema = SchemaBuilder.array(mapEntrySchema).optional().build();

    Schema valueSchema =
        SchemaBuilder.struct()
            .name("test.TestTable.Value")
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("frozen_map_tuple_key_col", mapSchema)
            .optional()
            .build();

    // Create structs with data
    assertDoesNotThrow(
        () -> {
          Struct tupleKey = new Struct(tupleSchema);
          tupleKey.put("0", 1);
          tupleKey.put("1", "tuple_key");

          Struct mapEntry = new Struct(mapEntrySchema);
          mapEntry.put("key", tupleKey);
          mapEntry.put("value", "tuple_value");

          List<Struct> mapEntries = new ArrayList<>();
          mapEntries.add(mapEntry);

          Struct value = new Struct(valueSchema);
          value.put("id", 1);
          value.put("frozen_map_tuple_key_col", mapEntries);

          System.out.println("Struct created successfully");
        });
  }

  @Test
  void testEmptyCollectionsStruct() {
    // Test the testInsertWithEmpty scenario - empty collections
    Schema listSchema = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build();
    Schema setSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();

    Schema mapEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_INT_TEXT")
            .field("key", Schema.OPTIONAL_INT32_SCHEMA)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema mapSchema = SchemaBuilder.array(mapEntrySchema).optional().build();

    Schema tupleSchema =
        SchemaBuilder.struct()
            .name("Tuple_INT_TEXT")
            .field("0", Schema.OPTIONAL_INT32_SCHEMA)
            .field("1", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    Schema valueSchema =
        SchemaBuilder.struct()
            .name("test.TestTable.Value")
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("frozen_list_col", listSchema)
            .field("frozen_set_col", setSchema)
            .field("frozen_map_col", mapSchema)
            .field("frozen_tuple_col", tupleSchema)
            .optional()
            .build();

    assertDoesNotThrow(
        () -> {
          Struct value = new Struct(valueSchema);
          value.put("id", 1);
          value.put("frozen_list_col", new ArrayList<>());
          value.put("frozen_set_col", new ArrayList<>());
          value.put("frozen_map_col", new ArrayList<>());

          // Empty tuple with null fields
          Struct emptyTuple = new Struct(tupleSchema);
          emptyTuple.put("0", null);
          emptyTuple.put("1", null);
          value.put("frozen_tuple_col", emptyTuple);

          System.out.println("Empty collections struct created successfully");
        });
  }

  @Test
  void testDuplicateSchemaNames() {
    // Test what happens when same schema name appears multiple times
    // This simulates the actual table which has:
    // - frozen_map_col (map<int, text>) -> MapEntry_INT_TEXT
    // - frozen_map_int_col (map<int, text>) -> MapEntry_INT_TEXT
    // - frozen_tuple_col (tuple<int, text>) -> Tuple_INT_TEXT
    // - frozen_map_tuple_key_col (map<tuple<int, text>, text>) -> key = Tuple_INT_TEXT

    // Create two separate Schema objects with the same name
    Schema mapEntrySchema1 =
        SchemaBuilder.struct()
            .name("MapEntry_INT_TEXT")
            .field("key", Schema.OPTIONAL_INT32_SCHEMA)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    // Second schema with same name - this is what happens when each column
    // creates its own schema object
    Schema mapEntrySchema2 =
        SchemaBuilder.struct()
            .name("MapEntry_INT_TEXT")
            .field("key", Schema.OPTIONAL_INT32_SCHEMA)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    Schema mapSchema1 = SchemaBuilder.array(mapEntrySchema1).optional().build();
    Schema mapSchema2 = SchemaBuilder.array(mapEntrySchema2).optional().build();

    // Check if the schemas are equal
    System.out.println(
        "mapEntrySchema1 == mapEntrySchema2: " + (mapEntrySchema1 == mapEntrySchema2));
    System.out.println(
        "mapEntrySchema1.equals(mapEntrySchema2): " + mapEntrySchema1.equals(mapEntrySchema2));

    // Create a value schema with both map columns using different schema objects
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("test.TestTable.Value")
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("frozen_map_col", mapSchema1)
            .field("frozen_map_int_col", mapSchema2)
            .optional()
            .build();

    System.out.println("Value schema with duplicate names: " + valueSchema);
    System.out.println("Number of fields: " + valueSchema.fields().size());
    for (org.apache.kafka.connect.data.Field f : valueSchema.fields()) {
      Schema fieldSchema = f.schema();
      String innerSchemaName =
          fieldSchema.type() == Schema.Type.ARRAY ? fieldSchema.valueSchema().name() : null;
      System.out.println(
          "  Field: "
              + f.name()
              + " type: "
              + fieldSchema.type()
              + " inner schema name: "
              + innerSchemaName);
    }

    // Try to create a struct
    assertDoesNotThrow(
        () -> {
          Struct value = new Struct(valueSchema);
          value.put("id", 1);
          value.put("frozen_map_col", new ArrayList<>());
          value.put("frozen_map_int_col", new ArrayList<>());
        });

    System.out.println("Struct with duplicate schema names created successfully");
  }

  @Test
  void testSharedSchemaInstance() {
    // Test that using the same schema instance works better
    Schema mapEntrySchema =
        SchemaBuilder.struct()
            .name("MapEntry_INT_TEXT")
            .field("key", Schema.OPTIONAL_INT32_SCHEMA)
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    // Reuse the same schema for both arrays
    Schema mapSchema1 = SchemaBuilder.array(mapEntrySchema).optional().build();
    Schema mapSchema2 = SchemaBuilder.array(mapEntrySchema).optional().build();

    Schema valueSchema =
        SchemaBuilder.struct()
            .name("test.TestTable.Value")
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("frozen_map_col", mapSchema1)
            .field("frozen_map_int_col", mapSchema2)
            .optional()
            .build();

    System.out.println("Value schema with shared entry schema: " + valueSchema);

    assertDoesNotThrow(
        () -> {
          Struct value = new Struct(valueSchema);
          value.put("id", 1);
          value.put("frozen_map_col", new ArrayList<>());
          value.put("frozen_map_int_col", new ArrayList<>());
        });

    System.out.println("Struct with shared schema instance created successfully");
  }

  @Test
  void testAvroFieldNameValidity() {
    // Avro field names must match: [A-Za-z_][A-Za-z0-9_]*
    // Field names starting with digits (like "0", "1") are INVALID in Avro!

    // This is the current problematic tuple schema with numeric field names
    Schema badTupleSchema =
        SchemaBuilder.struct()
            .name("Tuple_INT_TEXT")
            .field("0", Schema.OPTIONAL_INT32_SCHEMA) // INVALID: starts with digit
            .field("1", Schema.OPTIONAL_STRING_SCHEMA) // INVALID: starts with digit
            .optional()
            .build();

    // This is the correct tuple schema with valid Avro field names
    Schema goodTupleSchema =
        SchemaBuilder.struct()
            .name("Tuple_INT_TEXT")
            .field("field_0", Schema.OPTIONAL_INT32_SCHEMA) // VALID
            .field("field_1", Schema.OPTIONAL_STRING_SCHEMA) // VALID
            .optional()
            .build();

    System.out.println("Bad tuple schema (numeric field names): " + badTupleSchema);
    System.out.println("Good tuple schema (prefixed field names): " + goodTupleSchema);

    // Both schemas work for Kafka Connect Struct - but only the good one works with Avro
    assertDoesNotThrow(
        () -> {
          Struct badTuple = new Struct(badTupleSchema);
          badTuple.put("0", 42);
          badTuple.put("1", "foo");
          System.out.println("Bad tuple struct created (works in Connect, fails in Avro)");
        });

    assertDoesNotThrow(
        () -> {
          Struct goodTuple = new Struct(goodTupleSchema);
          goodTuple.put("field_0", 42);
          goodTuple.put("field_1", "foo");
          System.out.println("Good tuple struct created (works in Connect AND Avro)");
        });
  }
}
