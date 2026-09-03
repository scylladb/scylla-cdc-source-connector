package com.scylladb.cdc.debezium.connector.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;
import java.util.UUID;

/**
 * Example Single Message Transform (SMT) that converts a Scylla CDC record for an {@code orders}
 * table into a client-ready {@code OrderEvent} Protobuf-envelope message.
 *
 * <p>This class demonstrates <strong>Level 2 customization</strong>: a custom SMT that ships
 * inside the connector plugin JAR. It is intended as a production-realistic starting point —
 * copy, rename, and adapt it for your own table schema and event conventions.
 *
 * <h2>Output format</h2>
 * <p>Each record's value is a {@link Struct} with two nested blocks:
 * <pre>
 * OrderEvent {
 *   metadata {
 *     event_id      String   — randomly generated UUID per message
 *     event_name    String   — ORDER_CREATED / ORDER_UPDATED / ORDER_DELETED (from CDC op)
 *     entity_id     String   — copied from order_id (the domain entity key)
 *     event_ts      Int64    — record timestamp in epoch milliseconds
 *     state_impact  String   — INSERT / UPDATE / DELETE (from CDC op)
 *   }
 *   payload {               — full Order row snapshot
 *     order_id      String
 *     customer_id   String
 *     status        String
 *     total_amount  Float64
 *     updated_at    Int64
 *   }
 * }
 * </pre>
 *
 * <p>In addition, four <b>Kafka headers</b> are added to every message:
 * <ul>
 *   <li>{@code trace-id} — a UUID generated per message (propagate from upstream in real systems)
 *   <li>{@code schema-id} — the fully-qualified output schema name
 *   <li>{@code content-type} — {@code application/x-protobuf} (informational; actual serialization
 *       is controlled by the connector's {@code value.converter})
 *   <li>{@code event-source} — configurable string identifying the CDC pipeline (default:
 *       {@code scylla-cdc})
 * </ul>
 *
 * <h2>Connector configuration</h2>
 * <p>Run this SMT <em>after</em> {@code ScyllaExtractNewRecordState}. Configure
 * {@code add.fields=op} on the extraction SMT so that the CDC operation type ({@code __op}) is
 * available as a value field for this transform to read:
 *
 * <pre>{@code
 * transforms=extractState,toOrderEvent
 *
 * transforms.extractState.type=com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState
 * transforms.extractState.cdc.output.format=advanced
 * transforms.extractState.add.fields=op
 * transforms.extractState.delete.tombstone.handling.mode=drop
 *
 * transforms.toOrderEvent.type=com.scylladb.cdc.debezium.connector.transforms.ExampleOrderEventTransform
 * transforms.toOrderEvent.target.topic=orders.order-event
 * transforms.toOrderEvent.event.source=my-service
 *
 * # Protobuf serialization (requires Confluent Schema Registry)
 * value.converter=io.confluent.connect.protobuf.ProtobufConverter
 * value.converter.schema.registry.url=http://schema-registry:8081
 * }</pre>
 *
 * <h2>Operation type mapping</h2>
 * <p>The {@code __op} field (added by {@code add.fields=op}) is mapped as follows:
 * <table border="1">
 *   <tr><th>__op</th><th>event_name</th><th>state_impact</th></tr>
 *   <tr><td>c</td><td>ORDER_CREATED</td><td>INSERT</td></tr>
 *   <tr><td>u</td><td>ORDER_UPDATED</td><td>UPDATE</td></tr>
 *   <tr><td>d</td><td>ORDER_DELETED</td><td>DELETE</td></tr>
 *   <tr><td>(absent)</td><td>ORDER_UPDATED</td><td>UPDATE</td></tr>
 * </table>
 *
 * <h2>Adapting this example</h2>
 * <ol>
 *   <li>Rename the class and update the {@code event_name} prefix (e.g. {@code SHIPMENT_}).
 *   <li>Update {@link #PAYLOAD_SCHEMA} and {@link #buildPayload} to match your table columns.
 *   <li>Change {@link #ENTITY_ID_FIELD} to your table's primary key column name.
 *   <li>Adjust {@link #SCHEMA_ID_VALUE} to your Protobuf schema's fully-qualified name.
 * </ol>
 */
public class ExampleOrderEventTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    // -------------------------------------------------------------------------
    // Configuration keys
    // -------------------------------------------------------------------------

    /** Kafka topic to route transformed records to. */
    public static final String TARGET_TOPIC_CONFIG = "target.topic";
    private static final String TARGET_TOPIC_DEFAULT = "orders.order-event";

    /** Value of the {@code event-source} Kafka header. */
    public static final String EVENT_SOURCE_CONFIG = "event.source";
    private static final String EVENT_SOURCE_DEFAULT = "scylla-cdc";

    // -------------------------------------------------------------------------
    // CDC op codes (produced by ScyllaExtractNewRecordState with add.fields=op)
    // -------------------------------------------------------------------------

    private static final String OP_CREATE = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";

    // -------------------------------------------------------------------------
    // Domain field names (payload / source record)
    // -------------------------------------------------------------------------

    /** Name of the field to copy into metadata.entity_id. Change to your PK column. */
    private static final String ENTITY_ID_FIELD  = "order_id";

    private static final String FIELD_ORDER_ID     = "order_id";
    private static final String FIELD_CUSTOMER_ID  = "customer_id";
    private static final String FIELD_STATUS       = "status";
    private static final String FIELD_TOTAL_AMOUNT = "total_amount";
    private static final String FIELD_UPDATED_AT   = "updated_at";

    /** The CDC operation type field added by {@code add.fields=op}. */
    private static final String FIELD_OP = "__op";

    // -------------------------------------------------------------------------
    // Kafka header names
    // -------------------------------------------------------------------------

    public static final String HEADER_TRACE_ID     = "trace-id";
    public static final String HEADER_SCHEMA_ID    = "schema-id";
    public static final String HEADER_CONTENT_TYPE = "content-type";
    public static final String HEADER_EVENT_SOURCE = "event-source";

    private static final String CONTENT_TYPE_VALUE  = "application/x-protobuf";
    private static final String SCHEMA_ID_VALUE     = "com.example.orders.OrderEvent";

    // -------------------------------------------------------------------------
    // Output schemas
    // -------------------------------------------------------------------------

    /** Metadata block schema. */
    static final Schema METADATA_SCHEMA = SchemaBuilder.struct()
            .name("com.example.orders.OrderEvent.Metadata")
            .field("event_id",     Schema.STRING_SCHEMA)
            .field("event_name",   Schema.STRING_SCHEMA)
            .field("entity_id",    Schema.OPTIONAL_STRING_SCHEMA)
            .field("event_ts",     Schema.OPTIONAL_INT64_SCHEMA)
            .field("state_impact", Schema.STRING_SCHEMA)
            .build();

    /** Payload block schema — full Order row snapshot. */
    static final Schema PAYLOAD_SCHEMA = SchemaBuilder.struct()
            .name("com.example.orders.OrderEvent.Payload")
            .optional()
            .field(FIELD_ORDER_ID,     Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_CUSTOMER_ID,  Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_STATUS,       Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_TOTAL_AMOUNT, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(FIELD_UPDATED_AT,   Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    /** Top-level OrderEvent envelope schema. */
    static final Schema OUTPUT_SCHEMA = SchemaBuilder.struct()
            .name(SCHEMA_ID_VALUE)
            .field("metadata", METADATA_SCHEMA)
            .field("payload",  PAYLOAD_SCHEMA)
            .build();

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private String targetTopic;
    private String eventSource;

    // -------------------------------------------------------------------------
    // Transformation lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void configure(Map<String, ?> configs) {
        Object topic = configs.get(TARGET_TOPIC_CONFIG);
        targetTopic = (topic != null) ? topic.toString() : TARGET_TOPIC_DEFAULT;

        Object src = configs.get(EVENT_SOURCE_CONFIG);
        eventSource = (src != null) ? src.toString() : EVENT_SOURCE_DEFAULT;
    }

    @Override
    public R apply(R record) {
        // Drop tombstones (DELETE events represented as null-value records)
        if (record.value() == null) {
            return null;
        }

        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Struct input = (Struct) record.value();
        String op = getOptionalField(input, FIELD_OP, String.class);

        Struct metadata = buildMetadata(input, op, record.timestamp());
        Struct payload  = buildPayload(input);

        Struct output = new Struct(OUTPUT_SCHEMA)
                .put("metadata", metadata)
                .put("payload",  payload);

        // Build the new record targeting the configured topic
        R result = record.newRecord(
                targetTopic,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                OUTPUT_SCHEMA,
                output,
                record.timestamp());

        // Add custom Kafka headers
        addHeaders(result, op);

        return result;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(TARGET_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        TARGET_TOPIC_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Kafka topic to route transformed OrderEvent records to. "
                                + "Default: " + TARGET_TOPIC_DEFAULT)
                .define(EVENT_SOURCE_CONFIG,
                        ConfigDef.Type.STRING,
                        EVENT_SOURCE_DEFAULT,
                        ConfigDef.Importance.LOW,
                        "Value written to the 'event-source' Kafka header. "
                                + "Default: " + EVENT_SOURCE_DEFAULT);
    }

    @Override
    public void close() {
        // No resources to release
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the metadata block from the CDC record and derived operation fields.
     *
     * @param input the flattened CDC value Struct (after ScyllaExtractNewRecordState)
     * @param op    the CDC operation code ({@code c}/{@code u}/{@code d}), or null if absent
     * @param timestampMs record timestamp in epoch milliseconds, or null
     */
    protected Struct buildMetadata(Struct input, String op, Long timestampMs) {
        return new Struct(METADATA_SCHEMA)
                .put("event_id",     UUID.randomUUID().toString())
                .put("event_name",   resolveEventName(op))
                .put("entity_id",    getOptionalField(input, ENTITY_ID_FIELD, String.class))
                .put("event_ts",     timestampMs)
                .put("state_impact", resolveStateImpact(op));
    }

    /**
     * Builds the payload block — the full Order row snapshot from the CDC after-image.
     *
     * @param input the flattened CDC value Struct
     */
    protected Struct buildPayload(Struct input) {
        return new Struct(PAYLOAD_SCHEMA)
                .put(FIELD_ORDER_ID,     getOptionalField(input, FIELD_ORDER_ID,     String.class))
                .put(FIELD_CUSTOMER_ID,  getOptionalField(input, FIELD_CUSTOMER_ID,  String.class))
                .put(FIELD_STATUS,       getOptionalField(input, FIELD_STATUS,        String.class))
                .put(FIELD_TOTAL_AMOUNT, getOptionalField(input, FIELD_TOTAL_AMOUNT, Double.class))
                .put(FIELD_UPDATED_AT,   getOptionalField(input, FIELD_UPDATED_AT,   Long.class));
    }

    /**
     * Adds the four standard Kafka headers to the record.
     *
     * <p>Headers are added to the record returned by {@link ConnectRecord#newRecord} — the
     * {@link org.apache.kafka.connect.header.Headers} object is mutable and changes are reflected
     * in the record passed to the next SMT or to the producer.
     */
    protected void addHeaders(R record, String op) {
        record.headers().addString(HEADER_TRACE_ID,     UUID.randomUUID().toString());
        record.headers().addString(HEADER_SCHEMA_ID,    SCHEMA_ID_VALUE);
        record.headers().addString(HEADER_CONTENT_TYPE, CONTENT_TYPE_VALUE);
        record.headers().addString(HEADER_EVENT_SOURCE, eventSource);
    }

    /**
     * Maps a CDC operation code to a domain event name.
     *
     * <p>Falls back to {@code ORDER_UPDATED} when {@code __op} is absent (e.g. the upstream SMT
     * was not configured with {@code add.fields=op}).
     */
    protected String resolveEventName(String op) {
        if (op == null) return "ORDER_UPDATED";
        switch (op) {
            case OP_CREATE: return "ORDER_CREATED";
            case OP_DELETE: return "ORDER_DELETED";
            default:        return "ORDER_UPDATED";
        }
    }

    /**
     * Maps a CDC operation code to a state impact label.
     *
     * <p>Falls back to {@code UPDATE} when {@code __op} is absent.
     */
    protected String resolveStateImpact(String op) {
        if (op == null) return "UPDATE";
        switch (op) {
            case OP_CREATE: return "INSERT";
            case OP_DELETE: return "DELETE";
            default:        return "UPDATE";
        }
    }

    /**
     * Safely reads a typed field from a {@link Struct}, returning null if the field does not exist
     * in the schema or its value is of an unexpected type.
     */
    @SuppressWarnings("unchecked")
    protected <T> T getOptionalField(Struct struct, String fieldName, Class<T> type) {
        Field field = struct.schema().field(fieldName);
        if (field == null) {
            return null;
        }
        Object value = struct.get(field);
        if (value == null || !type.isInstance(value)) {
            return null;
        }
        return (T) value;
    }
}
