package com.scylladb.cdc.debezium.connector.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

/**
 * Example Single Message Transform (SMT) that converts a Scylla CDC record for an {@code orders}
 * table into a compact, application-specific {@code OrderEvent} message.
 *
 * <p>This class demonstrates <strong>Level 2 customization</strong>: writing your own SMT that
 * ships inside the connector plugin JAR. It is intended as a starting point — copy, rename, and
 * adapt it for your own table schema and output format.
 *
 * <h2>What it does</h2>
 * <ul>
 *   <li>Reads the <em>after-image</em> fields from a Debezium/Scylla CDC {@code SourceRecord}
 *       (after {@link ScyllaExtractNewRecordState} or equivalent unwrapping has already run).
 *   <li>Emits a new record with a simplified, flat schema containing only application-level fields:
 *       {@code order_id}, {@code customer_id}, {@code status}, {@code total_amount},
 *       {@code updated_at}.
 *   <li>Routes the record to the configurable topic (default: {@code orders.order-event}).
 *   <li>Drops tombstone records (null value) — DELETE events produce no output.
 * </ul>
 *
 * <h2>Expected upstream record schema</h2>
 * <p>The record value must be a {@link Struct} with at least the fields listed above (all optional).
 * Run this SMT <em>after</em> {@code ScyllaExtractNewRecordState} so that Cell wrappers have
 * already been removed.
 *
 * <h2>Connector configuration</h2>
 * <pre>{@code
 * transforms=extractState,toOrderEvent
 * transforms.extractState.type=com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState
 * transforms.toOrderEvent.type=com.scylladb.cdc.debezium.connector.transforms.ExampleOrderEventTransform
 * transforms.toOrderEvent.target.topic=orders.order-event
 * }</pre>
 *
 * <h2>Adapting this example</h2>
 * <ol>
 *   <li>Rename the class and update the field list to match your table.
 *   <li>Adjust the output {@link Schema} in {@link #OUTPUT_SCHEMA}.
 *   <li>Override {@link #buildOutputValue} for any field mapping or computation logic.
 *   <li>Register the class name in the connector config ({@code transforms.*.type}).
 * </ol>
 */
public class ExampleOrderEventTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    /** Configuration key for the output topic. */
    public static final String TARGET_TOPIC_CONFIG = "target.topic";
    private static final String TARGET_TOPIC_DEFAULT = "orders.order-event";

    /** Names of the source fields this transform reads from the upstream CDC record. */
    private static final String FIELD_ORDER_ID = "order_id";
    private static final String FIELD_CUSTOMER_ID = "customer_id";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_TOTAL_AMOUNT = "total_amount";
    private static final String FIELD_UPDATED_AT = "updated_at";

    /**
     * The output schema for the {@code OrderEvent} message.
     *
     * <p>All fields are optional so that partial updates (where only some columns changed) do not
     * cause schema validation failures. Consumers should treat a missing field as "not changed".
     */
    static final Schema OUTPUT_SCHEMA = SchemaBuilder.struct()
            .name("com.example.orders.OrderEvent")
            .optional()
            .field(FIELD_ORDER_ID,     Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_CUSTOMER_ID,  Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_STATUS,       Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_TOTAL_AMOUNT, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(FIELD_UPDATED_AT,   Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private String targetTopic;

    @Override
    public void configure(Map<String, ?> configs) {
        Object topic = configs.get(TARGET_TOPIC_CONFIG);
        targetTopic = (topic != null) ? topic.toString() : TARGET_TOPIC_DEFAULT;
    }

    @Override
    public R apply(R record) {
        // Drop tombstones (DELETE events)
        if (record.value() == null) {
            return null;
        }

        if (!(record.value() instanceof Struct)) {
            // Pass through records whose value is not a Struct unchanged
            return record;
        }

        Struct inputValue = (Struct) record.value();
        Struct outputValue = buildOutputValue(inputValue);

        return record.newRecord(
                targetTopic,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                OUTPUT_SCHEMA,
                outputValue,
                record.timestamp());
    }

    /**
     * Maps fields from the CDC after-image {@link Struct} to the output {@code OrderEvent} struct.
     *
     * <p>Each field is read with a null-safe helper so that absent or null columns in the CDC
     * record produce a null value in the output (preserving partial-update semantics).
     *
     * @param input the upstream Struct (after Cell unwrapping by a prior SMT)
     * @return a new Struct conforming to {@link #OUTPUT_SCHEMA}
     */
    protected Struct buildOutputValue(Struct input) {
        Struct output = new Struct(OUTPUT_SCHEMA);
        output.put(FIELD_ORDER_ID,     getOptionalField(input, FIELD_ORDER_ID,     String.class));
        output.put(FIELD_CUSTOMER_ID,  getOptionalField(input, FIELD_CUSTOMER_ID,  String.class));
        output.put(FIELD_STATUS,       getOptionalField(input, FIELD_STATUS,        String.class));
        output.put(FIELD_TOTAL_AMOUNT, getOptionalField(input, FIELD_TOTAL_AMOUNT, Double.class));
        output.put(FIELD_UPDATED_AT,   getOptionalField(input, FIELD_UPDATED_AT,   Long.class));
        return output;
    }

    /**
     * Safely reads a field from a {@link Struct}, returning null if the field does not exist in
     * the schema or its value is null.
     */
    @SuppressWarnings("unchecked")
    private <T> T getOptionalField(Struct struct, String fieldName, Class<T> type) {
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

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(TARGET_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        TARGET_TOPIC_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "The Kafka topic to route transformed records to. "
                                + "Default: " + TARGET_TOPIC_DEFAULT);
    }

    @Override
    public void close() {
        // No resources to release
    }
}
