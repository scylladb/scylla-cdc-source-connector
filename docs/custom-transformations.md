# Custom Output Transformations Guide

This guide explains how to customize the format of records published by the Scylla CDC Source
Connector. Three levels of customization are covered, from simplest to most powerful.

---

## Overview

The Scylla CDC Source Connector produces [Debezium-style CDC records][debezium-format] by default.
If you need a different record format — a different topic name, fewer fields, a renamed schema, or
computed values — you can apply **Single Message Transforms (SMTs)** in the Kafka Connect
configuration, or write your own SMT class.

---

## Level 1: Config-only — chaining built-in SMTs

Kafka Connect supports chaining multiple transforms via a comma-separated `transforms` list.
No code changes are required.

**Use case:** Strip the Debezium envelope and flatten Cell wrappers, then rename the topic.

```properties
# Strip the Debezium "after" envelope and unwrap Scylla legacy Cell structs
transforms=extractState,reroute
transforms.extractState.type=com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState
transforms.extractState.cdc.output.format=legacy

# Reroute all records to a single topic (built-in Kafka Connect SMT)
transforms.reroute.type=org.apache.kafka.connect.transforms.ReplaceField$Value
transforms.reroute.topic.regex=.*
transforms.reroute.topic.replacement=orders.order-event
```

**Available built-in SMTs provided by this connector:**

| SMT class | What it does |
|-----------|-------------|
| `ScyllaExtractNewRecordState` | Unwraps the Debezium envelope; for legacy format also removes Cell struct wrappers |
| `ScyllaFlattenColumns` | Flattens nested column structs into a flat key→value record |

Kafka Connect's own transform library (`org.apache.kafka.connect.transforms.*`) adds further
options: field dropping/renaming, topic routing by regex, timestamp conversion, and more.

---

## Level 2: Custom SMT in the connector JAR

Write a Java class implementing `org.apache.kafka.connect.transforms.Transformation<R>`. Place it
in the same Maven module as the connector. It will be included in the fat JAR automatically — no
extra build steps or SPI registration are needed.

### Example: `ExampleOrderEventTransform`

The class [`ExampleOrderEventTransform`][example-smt] ships with this connector as a reference
implementation. It converts a CDC record for an `orders` table into a compact `OrderEvent` message
on the topic `orders.order-event`.

#### What it does

1. Drops tombstone (DELETE) records — returns `null` to suppress the record downstream.
2. Reads five application-level fields from the upstream Struct (`order_id`, `customer_id`,
   `status`, `total_amount`, `updated_at`).
3. Emits a new record with a clean, flat schema and routes it to the configured target topic.
4. Silently drops any CDC-internal or extra columns that are not part of the output schema.

#### Step-by-step: adapting the example for your table

1. **Copy and rename** `ExampleOrderEventTransform.java` — e.g. `ShipmentEventTransform.java`.

2. **Update the field list** to match your table's columns:
   ```java
   private static final String FIELD_SHIPMENT_ID   = "shipment_id";
   private static final String FIELD_CARRIER        = "carrier";
   private static final String FIELD_TRACKING_CODE  = "tracking_code";
   ```

3. **Define the output schema**:
   ```java
   static final Schema OUTPUT_SCHEMA = SchemaBuilder.struct()
           .name("com.example.shipping.ShipmentEvent")
           .optional()
           .field(FIELD_SHIPMENT_ID,  Schema.OPTIONAL_STRING_SCHEMA)
           .field(FIELD_CARRIER,      Schema.OPTIONAL_STRING_SCHEMA)
           .field(FIELD_TRACKING_CODE, Schema.OPTIONAL_STRING_SCHEMA)
           .build();
   ```

4. **Override `buildOutputValue`** if you need computed fields or type coercions:
   ```java
   @Override
   protected Struct buildOutputValue(Struct input) {
       Struct output = new Struct(OUTPUT_SCHEMA);
       output.put(FIELD_SHIPMENT_ID,  getOptionalField(input, FIELD_SHIPMENT_ID, String.class));
       output.put(FIELD_CARRIER,      getOptionalField(input, FIELD_CARRIER, String.class));
       // Computed: combine two source fields
       String tracking = getOptionalField(input, "carrier_prefix", String.class);
       String code     = getOptionalField(input, "shipment_number", String.class);
       if (tracking != null && code != null) {
           output.put(FIELD_TRACKING_CODE, tracking + "-" + code);
       }
       return output;
   }
   ```

5. **Configure the connector** (runs the new SMT _after_ Cell unwrapping):
   ```properties
   transforms=extractState,toShipmentEvent
   transforms.extractState.type=com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState
   transforms.toShipmentEvent.type=com.scylladb.cdc.debezium.connector.transforms.ShipmentEventTransform
   transforms.toShipmentEvent.target.topic=logistics.shipment-event
   ```

#### Run transform chain order

Transforms run left to right. Always put `ScyllaExtractNewRecordState` (or equivalent) before your
custom transform so that Cell wrappers are stripped first:

```
raw CDC record
  → ScyllaExtractNewRecordState   (removes Debezium envelope + Cell wrappers)
  → YourCustomTransform           (reads flat fields, emits OrderEvent)
  → Kafka topic
```

---

## Level 3: User-supplied SMT JAR

If you cannot (or prefer not to) modify the connector source, you can package your SMT in a
**separate JAR** and deploy it alongside the connector.

### Option A: Same plugin directory (shared classloader)

Place your JAR in the same directory as the connector plugin JAR. Kafka Connect loads all JARs in
a plugin directory with the same classloader, so your SMT can reference connector classes
(e.g. `ScyllaExtractNewRecordState`) if needed.

```
/kafka/plugins/
  scylla-cdc-source-connector/
    scylla-cdc-source-connector-*.jar   ← connector
    my-order-transforms-1.0.jar         ← your SMT JAR
```

### Option B: Separate plugin directory (isolated classloader)

Place your JAR in its own plugin directory. It will have an isolated classloader and can only see
standard Kafka Connect types (`ConnectRecord`, `Schema`, `Struct`, etc.). Useful if you want strict
isolation or your SMT has conflicting dependencies.

```
/kafka/plugins/
  scylla-cdc-source-connector/
    scylla-cdc-source-connector-*.jar
  my-order-transforms/
    my-order-transforms-1.0.jar         ← isolated classloader
```

### Packaging your SMT

Your `pom.xml` needs only the `kafka-connect-api` dependency (provided scope):

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>connect-api</artifactId>
    <version>3.7.0</version>
    <scope>provided</scope>
</dependency>
```

Build with `mvn package` and copy the resulting JAR to the plugin directory. No SPI registration
(no `META-INF/services/` file) is required — Kafka Connect discovers SMTs by class name from the
connector configuration.

### Configuration

```properties
transforms=extractState,toOrderEvent
transforms.extractState.type=com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState
transforms.toOrderEvent.type=com.example.orders.MyOrderEventTransform
transforms.toOrderEvent.target.topic=orders.order-event
```

---

## External offset management (advanced)

By default, `SourceRecord` objects carry `sourcePartition` / `sourceOffset` maps that Kafka Connect
persists in its internal offset topic. If you need the connector to checkpoint progress to an
external store (Redis, a database, etc.) instead, you can use the `commitRecord` and `commit`
hooks on the `SourceTask`:

- `commitRecord(SourceRecord, RecordMetadata)` — called after each record is successfully produced.
- `commit()` — called periodically as a bulk flush hint.

Setting `sourcePartition` and `sourceOffset` to `null` disables Connect's internal offset storage
for those records. Note that this breaks exactly-once but preserves at-least-once delivery.

For standalone Java consumers (not Kafka Connect), use `CDCConsumer.withStateStore(CDCStateStore)`
from `scylla-cdc-lib` to plug in a custom checkpoint backend. See the
[scylla-cdc-java README][state-store-readme] for details.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `ClassNotFoundException` for your SMT | JAR not in plugin directory | Copy JAR to same plugin dir as connector |
| `NullPointerException` in `buildOutputValue` | Field missing from CDC schema | Use null-safe field accessor; all output fields should be `OPTIONAL` |
| Records appear with old schema after redeploy | Schema registry cached old schema | Bump the schema version or use `schema.registry.compatibility=NONE` |
| Tombstones still appearing in output topic | Your SMT returns the record instead of `null` | Return `null` from `apply()` for tombstones |

[debezium-format]: https://debezium.io/documentation/reference/stable/connectors/cassandra.html
[example-smt]: ../src/main/java/com/scylladb/cdc/debezium/connector/transforms/ExampleOrderEventTransform.java
[state-store-readme]: https://github.com/scylladb/scylla-cdc-java#checkpoint-persistence-cdcstatestore
