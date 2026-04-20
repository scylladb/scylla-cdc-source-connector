package com.scylladb.cdc.debezium.connector.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExampleOrderEventTransformTest {

    private static final Schema INPUT_SCHEMA = SchemaBuilder.struct()
            .optional()
            .field("order_id",     Schema.OPTIONAL_STRING_SCHEMA)
            .field("customer_id",  Schema.OPTIONAL_STRING_SCHEMA)
            .field("status",       Schema.OPTIONAL_STRING_SCHEMA)
            .field("total_amount", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("updated_at",   Schema.OPTIONAL_INT64_SCHEMA)
            .field("internal_col", Schema.OPTIONAL_STRING_SCHEMA) // extra column — should be dropped
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
        return new SourceRecord(
                Map.of("partition", "0"),
                Map.of("offset", "0"),
                "orders.cdc",
                null,
                Schema.OPTIONAL_STRING_SCHEMA,
                "order-123",
                value == null ? null : value.schema(),
                value,
                System.currentTimeMillis());
    }

    @Test
    void apply_tombstone_returnsNull() {
        SourceRecord result = transform.apply(makeRecord(null));
        assertNull(result);
    }

    @Test
    void apply_normalRecord_routesToTargetTopic() {
        Struct input = new Struct(INPUT_SCHEMA)
                .put("order_id", "ord-1")
                .put("customer_id", "cust-42")
                .put("status", "CONFIRMED")
                .put("total_amount", 99.95)
                .put("updated_at", 1_700_000_000_000L)
                .put("internal_col", "ignored");

        SourceRecord result = transform.apply(makeRecord(input));

        assertNotNull(result);
        assertEquals("orders.order-event", result.topic());
    }

    @Test
    void apply_normalRecord_outputSchemaHasExpectedFields() {
        Struct input = new Struct(INPUT_SCHEMA)
                .put("order_id", "ord-1")
                .put("customer_id", "cust-42")
                .put("status", "CONFIRMED")
                .put("total_amount", 99.95)
                .put("updated_at", 1_700_000_000_000L)
                .put("internal_col", "ignored");

        SourceRecord result = transform.apply(makeRecord(input));
        Struct outputValue = (Struct) result.value();

        assertEquals("ord-1",              outputValue.get("order_id"));
        assertEquals("cust-42",            outputValue.get("customer_id"));
        assertEquals("CONFIRMED",          outputValue.get("status"));
        assertEquals(99.95,                outputValue.get("total_amount"));
        assertEquals(1_700_000_000_000L,   outputValue.get("updated_at"));
        // internal_col must NOT appear in the output schema
        assertNull(outputValue.schema().field("internal_col"));
    }

    @Test
    void apply_partialUpdate_missingFieldsAreNull() {
        // Only order_id and status are present — other fields absent from the struct
        Schema partialSchema = SchemaBuilder.struct()
                .optional()
                .field("order_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("status",   Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct input = new Struct(partialSchema)
                .put("order_id", "ord-2")
                .put("status",   "SHIPPED");

        SourceRecord result = transform.apply(makeRecord(input));
        Struct outputValue = (Struct) result.value();

        assertEquals("ord-2",   outputValue.get("order_id"));
        assertEquals("SHIPPED", outputValue.get("status"));
        assertNull(outputValue.get("customer_id"));
        assertNull(outputValue.get("total_amount"));
        assertNull(outputValue.get("updated_at"));
    }

    @Test
    void configure_customTargetTopic_isApplied() {
        ExampleOrderEventTransform<SourceRecord> customTransform = new ExampleOrderEventTransform<>();
        customTransform.configure(Map.of(ExampleOrderEventTransform.TARGET_TOPIC_CONFIG, "my.custom-topic"));

        Schema schema = SchemaBuilder.struct().optional()
                .field("order_id", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct input = new Struct(schema).put("order_id", "ord-3");

        SourceRecord result = customTransform.apply(makeRecord(input));
        assertEquals("my.custom-topic", result.topic());
        customTransform.close();
    }

    @Test
    void config_definesTargetTopicKey() {
        assertNotNull(transform.config().configKeys().get(ExampleOrderEventTransform.TARGET_TOPIC_CONFIG));
    }
}
