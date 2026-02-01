package com.scylladb.cdc.debezium.connector.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ScyllaExtractFlattenedNewRecordStateTest {

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        xform.configure(Collections.emptyMap());
    }

    private ScyllaExtractFlattenedNewRecordState<SinkRecord> xform = new ScyllaExtractFlattenedNewRecordState<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void testJavaSet() {

        final Schema keys = SchemaBuilder.struct().field("serviceid", Schema.STRING_SCHEMA)
                .field("servicename", Schema.STRING_SCHEMA).build();

        final Struct keysV = new Struct(keys);
        keysV.put("serviceid","serviceid_01");
        keysV.put("servicename","Payments");

        final Struct keysV1 = new Struct(keys);
        keysV1.put("serviceid","serviceid_02");
        keysV1.put("servicename","Webhook");
        Map params = new HashMap();
        params.put(keysV,true);
        params.put(keysV1,true);
        final Schema elements = SchemaBuilder.map(keys,Schema.BOOLEAN_SCHEMA)
                .build();

        final Schema serviceSchema = SchemaBuilder.struct().name("services")
                    .field("mode", Schema.STRING_SCHEMA)
                    .field("elements", elements)
                    .build();
        final Struct serviceV = new Struct(serviceSchema);
        serviceV.put("mode","OVERWRITE");
        serviceV.put("elements", params);

        Schema schema = SchemaBuilder.struct()
                .field("organization.Value", serviceSchema)
                .name("schema").build();
            final Struct value = new Struct(schema);
            value.put("organization.Value",serviceV);


            final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
            final SinkRecord transformedRecord = xform.apply(record);

            final Struct updatedValue = (Struct) transformedRecord.value();
            assertEquals(1, updatedValue.schema().fields().size());
            assertEquals(2, updatedValue.getArray("organization.Value").size());
        List<Struct> services = updatedValue.getArray("organization.Value");
        // Order from map keySet() is not guaranteed
        Set<String> serviceIds = new HashSet<>();
        Set<String> serviceNames = new HashSet<>();
        for (Struct s : services) {
            serviceIds.add((String) s.get("serviceid"));
            serviceNames.add((String) s.get("servicename"));
        }
        assertTrue(serviceIds.contains("serviceid_01"));
        assertTrue(serviceIds.contains("serviceid_02"));
        assertTrue(serviceNames.contains("Payments"));
        assertTrue(serviceNames.contains("Webhook"));
    }

    @Test
    public void testMapWithSingleUuidKey() {
        // MAP<timeuuid, UDT>: single UUID key -> struct value; transform outputs single struct
        Schema payloadSchema = SchemaBuilder.struct()
                .field("event_id", Schema.STRING_SCHEMA)
                .field("payload", Schema.STRING_SCHEMA)
                .build();

        Struct payload = new Struct(payloadSchema);
        payload.put("event_id", "evt-1");
        payload.put("payload", "data");

        String uuidKey = "550e8400-e29b-41d4-a716-446655440000";
        Map<String, Struct> elements = new LinkedHashMap<>();
        elements.put(uuidKey, payload);

        Schema elementsSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, payloadSchema).build();
        Schema mapStructSchema = SchemaBuilder.struct()
                .field("mode", Schema.STRING_SCHEMA)
                .field("elements", elementsSchema)
                .build();

        Struct mapStruct = new Struct(mapStructSchema);
        mapStruct.put("mode", "OVERWRITE");
        mapStruct.put("elements", elements);

        Schema schema = SchemaBuilder.struct()
                .field("events.Value", mapStructSchema)
                .name("schema").build();
        Struct value = new Struct(schema);
        value.put("events.Value", mapStruct);

        SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        SinkRecord transformedRecord = xform.apply(record);

        Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        Struct flattened = (Struct) updatedValue.get("events.Value");
        assertNotNull(flattened);
        assertEquals("evt-1", flattened.get("event_id"));
        assertEquals("data", flattened.get("payload"));
    }

    @Test
    public void testMapWithMultipleUuidKeys() {
        // MAP<timeuuid, UDT>: multiple UUID keys -> struct values; transform outputs array of structs
        Schema payloadSchema = SchemaBuilder.struct()
                .field("event_id", Schema.STRING_SCHEMA)
                .field("payload", Schema.STRING_SCHEMA)
                .build();

        Struct p1 = new Struct(payloadSchema);
        p1.put("event_id", "evt-1");
        p1.put("payload", "first");
        Struct p2 = new Struct(payloadSchema);
        p2.put("event_id", "evt-2");
        p2.put("payload", "second");

        Map<String, Struct> elements = new LinkedHashMap<>();
        elements.put("550e8400-e29b-41d4-a716-446655440001", p1);
        elements.put("550e8400-e29b-41d4-a716-446655440002", p2);

        Schema elementsSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, payloadSchema).build();
        Schema mapStructSchema = SchemaBuilder.struct()
                .field("mode", Schema.STRING_SCHEMA)
                .field("elements", elementsSchema)
                .build();

        Struct mapStruct = new Struct(mapStructSchema);
        mapStruct.put("mode", "OVERWRITE");
        mapStruct.put("elements", elements);

        Schema schema = SchemaBuilder.struct()
                .field("events.Value", mapStructSchema)
                .name("schema").build();
        Struct value = new Struct(schema);
        value.put("events.Value", mapStruct);

        SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        SinkRecord transformedRecord = xform.apply(record);

        Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        @SuppressWarnings("unchecked")
        List<Struct> flattened = (List<Struct>) updatedValue.get("events.Value");
        assertNotNull(flattened);
        assertEquals(2, flattened.size());
        assertEquals("evt-1", flattened.get(0).get("event_id"));
        assertEquals("first", flattened.get(0).get("payload"));
        assertEquals("evt-2", flattened.get(1).get("event_id"));
        assertEquals("second", flattened.get(1).get("payload"));
    }

    @Test
    public void testUdtsPreserved() {
        // UDT (struct with named fields, no "elements") is not flattenable -> passed through unchanged
        Schema addressSchema = SchemaBuilder.struct()
                .field("street", Schema.STRING_SCHEMA)
                .field("city", Schema.STRING_SCHEMA)
                .field("zip", Schema.STRING_SCHEMA)
                .build();

        Struct address = new Struct(addressSchema);
        address.put("street", "123 Main St");
        address.put("city", "Boston");
        address.put("zip", "02101");

        Schema schema = SchemaBuilder.struct()
                .field("user_id", Schema.INT32_SCHEMA)
                .field("address", addressSchema)
                .name("schema").build();
        Struct value = new Struct(schema);
        value.put("user_id", 42);
        value.put("address", address);

        SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        SinkRecord transformedRecord = xform.apply(record);

        Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(2, updatedValue.schema().fields().size());
        assertEquals(42, updatedValue.get("user_id"));

        Struct preservedAddress = (Struct) updatedValue.get("address");
        assertNotNull(preservedAddress);
        assertEquals("123 Main St", preservedAddress.get("street"));
        assertEquals("Boston", preservedAddress.get("city"));
        assertEquals("02101", preservedAddress.get("zip"));
    }

    @Test
    public void testMixedMapWithUuidKeysAndUdts() {
        // Record with MAP<timeuuid, UDT> (flattenable) and a plain UDT (passthrough)
        Schema payloadSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .build();

        Struct payload = new Struct(payloadSchema);
        payload.put("id", "p1");
        payload.put("data", "x");

        Map<String, Struct> mapElements = new LinkedHashMap<>();
        mapElements.put("550e8400-e29b-41d4-a716-446655440003", payload);

        Schema mapElementsSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, payloadSchema).build();
        Schema mapStructSchema = SchemaBuilder.struct()
                .field("mode", Schema.STRING_SCHEMA)
                .field("elements", mapElementsSchema)
                .build();

        Struct mapStruct = new Struct(mapStructSchema);
        mapStruct.put("mode", "OVERWRITE");
        mapStruct.put("elements", mapElements);

        Schema addressSchema = SchemaBuilder.struct()
                .field("street", Schema.STRING_SCHEMA)
                .field("city", Schema.STRING_SCHEMA)
                .build();
        Struct address = new Struct(addressSchema);
        address.put("street", "456 Oak Ave");
        address.put("city", "Seattle");

        Schema schema = SchemaBuilder.struct()
                .field("meta.Value", mapStructSchema)
                .field("location", addressSchema)
                .name("schema").build();
        Struct value = new Struct(schema);
        value.put("meta.Value", mapStruct);
        value.put("location", address);

        SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        SinkRecord transformedRecord = xform.apply(record);

        Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(2, updatedValue.schema().fields().size());

        Struct flattenedMap = (Struct) updatedValue.get("meta.Value");
        assertNotNull(flattenedMap);
        assertEquals("p1", flattenedMap.get("id"));
        assertEquals("x", flattenedMap.get("data"));

        Struct preservedLocation = (Struct) updatedValue.get("location");
        assertNotNull(preservedLocation);
        assertEquals("456 Oak Ave", preservedLocation.get("street"));
        assertEquals("Seattle", preservedLocation.get("city"));
    }
}