package com.scylladb.cdc.debezium.connector.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExampleOrderEventTransformTest {

    /** Input schema produced by ScyllaExtractNewRecordState with add.fields=op. */
    private static final Schema INPUT_SCHEMA = SchemaBuilder.struct()
            .optional()
            .field("order_id",     Schema.OPTIONAL_STRING_SCHEMA)
            .field("customer_id",  Schema.OPTIONAL_STRING_SCHEMA)
            .field("status",       Schema.OPTIONAL_STRING_SCHEMA)
            .field("total_amount", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("updated_at",   Schema.OPTIONAL_INT64_SCHEMA)
            .field("__op",         Schema.OPTIONAL_STRING_SCHEMA)
            .field("internal_col", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private ExampleOrderEventTransform<SourceRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new ExampleOrderEventTransform<>();
        transform.configure(Collections.emptyMap());
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    private SourceRecord makeRecord(Struct value) {
        return makeRecord(value, System.currentTimeMillis());
    }

    private SourceRecord makeRecord(Struct value, long timestamp) {
        return new SourceRecord(
                Map.of("partition", "0"),
                Map.of("offset", "0"),
                "orders.cdc",
                null,
                Schema.OPTIONAL_STRING_SCHEMA,
                "order-123",
                value == null ? null : value.schema(),
                value,
                timestamp);
    }

    private Struct makeInput(String op) {
        return new Struct(INPUT_SCHEMA)
                .put("order_id",     "ord-1")
                .put("customer_id",  "cust-42")
                .put("status",       "CONFIRMED")
                .put("total_amount", 99.95)
                .put("updated_at",   1_700_000_000_000L)
                .put("__op",         op)
                .put("internal_col", "ignored");
    }

    // -------------------------------------------------------------------------
    // Tombstone handling
    // -------------------------------------------------------------------------

    @Test
    void apply_tombstone_returnsNull() {
        assertNull(transform.apply(makeRecord(null)));
    }

    // -------------------------------------------------------------------------
    // Topic routing
    // -------------------------------------------------------------------------

    @Test
    void apply_routesToDefaultTopic() {
        SourceRecord result = transform.apply(makeRecord(makeInput("c")));
        assertNotNull(result);
        assertEquals("orders.order-event", result.topic());
    }

    @Test
    void apply_routesToCustomTopic() {
        ExampleOrderEventTransform<SourceRecord> custom = new ExampleOrderEventTransform<>();
        custom.configure(Map.of(ExampleOrderEventTransform.TARGET_TOPIC_CONFIG, "my.topic"));
        SourceRecord result = custom.apply(makeRecord(makeInput("u")));
        assertEquals("my.topic", result.topic());
        custom.close();
    }

    // -------------------------------------------------------------------------
    // Output schema structure — envelope with metadata + payload
    // -------------------------------------------------------------------------

    @Test
    void apply_outputHasMetadataAndPayloadFields() {
        SourceRecord result = transform.apply(makeRecord(makeInput("c")));
        Struct output = (Struct) result.value();

        assertNotNull(output.schema().field("metadata"), "missing metadata field");
        assertNotNull(output.schema().field("payload"),  "missing payload field");
        assertNull(output.schema().field("internal_col"), "internal_col must not appear in output");
    }

    // -------------------------------------------------------------------------
    // Metadata block
    // -------------------------------------------------------------------------

    @Test
    void metadata_create_hasCorrectEventNameAndStateImpact() {
        Struct output = (Struct) transform.apply(makeRecord(makeInput("c"))).value();
        Struct metadata = (Struct) output.get("metadata");

        assertEquals("ORDER_CREATED", metadata.get("event_name"));
        assertEquals("INSERT",        metadata.get("state_impact"));
    }

    @Test
    void metadata_update_hasCorrectEventNameAndStateImpact() {
        Struct output = (Struct) transform.apply(makeRecord(makeInput("u"))).value();
        Struct metadata = (Struct) output.get("metadata");

        assertEquals("ORDER_UPDATED", metadata.get("event_name"));
        assertEquals("UPDATE",        metadata.get("state_impact"));
    }

    @Test
    void metadata_delete_hasCorrectEventNameAndStateImpact() {
        Struct output = (Struct) transform.apply(makeRecord(makeInput("d"))).value();
        Struct metadata = (Struct) output.get("metadata");

        assertEquals("ORDER_DELETED", metadata.get("event_name"));
        assertEquals("DELETE",        metadata.get("state_impact"));
    }

    @Test
    void metadata_missingOp_fallsBackToUpdate() {
        // Input schema without __op — simulates missing add.fields=op config
        Schema schemaNoOp = SchemaBuilder.struct().optional()
                .field("order_id", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct input = new Struct(schemaNoOp).put("order_id", "ord-2");

        Struct output = (Struct) transform.apply(makeRecord(input)).value();
        Struct metadata = (Struct) output.get("metadata");

        assertEquals("ORDER_UPDATED", metadata.get("event_name"));
        assertEquals("UPDATE",        metadata.get("state_impact"));
    }

    @Test
    void metadata_eventId_isNonNullUUID() {
        Struct output = (Struct) transform.apply(makeRecord(makeInput("c"))).value();
        Struct metadata = (Struct) output.get("metadata");

        String eventId = (String) metadata.get("event_id");
        assertNotNull(eventId);
        // Must parse as a UUID without throwing
        assertDoesNotThrow(() -> java.util.UUID.fromString(eventId));
    }

    @Test
    void metadata_entityId_copiedFromOrderId() {
        Struct output = (Struct) transform.apply(makeRecord(makeInput("u"))).value();
        Struct metadata = (Struct) output.get("metadata");

        assertEquals("ord-1", metadata.get("entity_id"));
    }

    @Test
    void metadata_eventTs_copiedFromRecordTimestamp() {
        long ts = 1_700_000_000_000L;
        Struct output = (Struct) transform.apply(makeRecord(makeInput("c"), ts)).value();
        Struct metadata = (Struct) output.get("metadata");

        assertEquals(ts, metadata.get("event_ts"));
    }

    // -------------------------------------------------------------------------
    // Payload block
    // -------------------------------------------------------------------------

    @Test
    void payload_containsFullOrderSnapshot() {
        Struct output = (Struct) transform.apply(makeRecord(makeInput("u"))).value();
        Struct payload = (Struct) output.get("payload");

        assertEquals("ord-1",              payload.get("order_id"));
        assertEquals("cust-42",            payload.get("customer_id"));
        assertEquals("CONFIRMED",          payload.get("status"));
        assertEquals(99.95,                payload.get("total_amount"));
        assertEquals(1_700_000_000_000L,   payload.get("updated_at"));
    }

    @Test
    void payload_partialUpdate_missingFieldsAreNull() {
        Schema partialSchema = SchemaBuilder.struct().optional()
                .field("order_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("status",   Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct input = new Struct(partialSchema)
                .put("order_id", "ord-3")
                .put("status",   "SHIPPED");

        Struct output = (Struct) transform.apply(makeRecord(input)).value();
        Struct payload = (Struct) output.get("payload");

        assertEquals("ord-3",   payload.get("order_id"));
        assertEquals("SHIPPED", payload.get("status"));
        assertNull(payload.get("customer_id"));
        assertNull(payload.get("total_amount"));
        assertNull(payload.get("updated_at"));
    }

    // -------------------------------------------------------------------------
    // Kafka headers
    // -------------------------------------------------------------------------

    @Test
    void headers_allFourHeadersPresent() {
        SourceRecord result = transform.apply(makeRecord(makeInput("c")));
        Map<String, String> headers = headersAsMap(result);

        assertTrue(headers.containsKey(ExampleOrderEventTransform.HEADER_TRACE_ID),
                "missing trace-id header");
        assertTrue(headers.containsKey(ExampleOrderEventTransform.HEADER_SCHEMA_ID),
                "missing schema-id header");
        assertTrue(headers.containsKey(ExampleOrderEventTransform.HEADER_CONTENT_TYPE),
                "missing content-type header");
        assertTrue(headers.containsKey(ExampleOrderEventTransform.HEADER_EVENT_SOURCE),
                "missing event-source header");
    }

    @Test
    void headers_contentTypeIsProtobuf() {
        SourceRecord result = transform.apply(makeRecord(makeInput("u")));
        assertEquals("application/x-protobuf",
                headersAsMap(result).get(ExampleOrderEventTransform.HEADER_CONTENT_TYPE));
    }

    @Test
    void headers_schemaIdMatchesOutputSchemaName() {
        SourceRecord result = transform.apply(makeRecord(makeInput("c")));
        assertEquals(ExampleOrderEventTransform.OUTPUT_SCHEMA.name(),
                headersAsMap(result).get(ExampleOrderEventTransform.HEADER_SCHEMA_ID));
    }

    @Test
    void headers_traceId_isNonNullUUID() {
        SourceRecord result = transform.apply(makeRecord(makeInput("c")));
        String traceId = headersAsMap(result).get(ExampleOrderEventTransform.HEADER_TRACE_ID);
        assertNotNull(traceId);
        assertDoesNotThrow(() -> java.util.UUID.fromString(traceId));
    }

    @Test
    void headers_customEventSource_isApplied() {
        ExampleOrderEventTransform<SourceRecord> custom = new ExampleOrderEventTransform<>();
        custom.configure(Map.of(ExampleOrderEventTransform.EVENT_SOURCE_CONFIG, "my-service"));
        SourceRecord result = custom.apply(makeRecord(makeInput("u")));
        assertEquals("my-service",
                headersAsMap(result).get(ExampleOrderEventTransform.HEADER_EVENT_SOURCE));
        custom.close();
    }

    @Test
    void headers_defaultEventSource_isScyllaCdc() {
        SourceRecord result = transform.apply(makeRecord(makeInput("c")));
        assertEquals("scylla-cdc",
                headersAsMap(result).get(ExampleOrderEventTransform.HEADER_EVENT_SOURCE));
    }

    // -------------------------------------------------------------------------
    // Config definition
    // -------------------------------------------------------------------------

    @Test
    void config_definesBothConfigKeys() {
        Map<String, ?> keys = transform.config().configKeys();
        assertNotNull(keys.get(ExampleOrderEventTransform.TARGET_TOPIC_CONFIG));
        assertNotNull(keys.get(ExampleOrderEventTransform.EVENT_SOURCE_CONFIG));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Map<String, String> headersAsMap(SourceRecord record) {
        Map<String, String> map = new HashMap<>();
        for (Header header : record.headers()) {
            map.put(header.key(), header.value() == null ? null : header.value().toString());
        }
        return map;
    }
}
