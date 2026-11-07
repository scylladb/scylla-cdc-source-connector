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
options: field dropping/renaming, topic routing by regex, timestamp conversion, and more. See
the [Kafka Connect included transformations reference][connect-transforms] for the full list.

---

## Level 2: Custom SMT in the connector JAR

Write a Java class implementing `org.apache.kafka.connect.transforms.Transformation<R>`. Place it
in the same Maven module as the connector. It will be included in the fat JAR automatically — no
extra build steps or SPI registration are needed.

### Example: `ExampleOrderEventTransform`

The class [`ExampleOrderEventTransform`][example-smt] ships with this connector as a
production-realistic reference implementation. It models the following real-world requirements:

- Publish to a custom topic (`orders.order-event`) in an application-specific envelope format
- Wrap every record in a metadata block + full row-snapshot payload
- Derive domain event semantics (`ORDER_CREATED`, `ORDER_UPDATED`, `ORDER_DELETED`) from the CDC
  operation type
- Add four custom Kafka headers per message (`trace-id`, `schema-id`, `content-type`,
  `event-source`)
- Serialize as Protobuf (via the connector's `value.converter` — see
  [Protobuf serialization](#protobuf-serialization) below)

#### Output format

Each record's value is a `Struct` with two nested blocks:

```
OrderEvent {
  metadata {
    event_id      String   — randomly generated UUID per message
    event_name    String   — ORDER_CREATED / ORDER_UPDATED / ORDER_DELETED
    entity_id     String   — copied from order_id (the domain entity key)
    event_ts      Int64    — record timestamp in epoch milliseconds
    state_impact  String   — INSERT / UPDATE / DELETE
  }
  payload {               — full Order row snapshot
    order_id      String
    customer_id   String
    status        String
    total_amount  Float64
    updated_at    Int64
  }
}
```

#### Kafka headers

Four headers are added to every message:

| Header | Value |
|--------|-------|
| `trace-id` | UUID generated per message (propagate from upstream in real systems) |
| `schema-id` | Fully-qualified output schema name (`com.example.orders.OrderEvent`) |
| `content-type` | `application/x-protobuf` (informational; serialization is controlled by `value.converter`) |
| `event-source` | Configurable string, default `scylla-cdc` |

Headers are added by calling `record.headers().addString(key, value)` on the `ConnectRecord`
returned by `newRecord()`. The `Headers` object is mutable; changes are visible to subsequent
SMTs and to the Kafka producer.

#### Operation type mapping

The `__op` field is added by `ScyllaExtractNewRecordState` when configured with `add.fields=op`:

| `__op` | `event_name` | `state_impact` |
|--------|-------------|----------------|
| `c` | `ORDER_CREATED` | `INSERT` |
| `u` | `ORDER_UPDATED` | `UPDATE` |
| `d` | `ORDER_DELETED` | `DELETE` |
| _(absent)_ | `ORDER_UPDATED` | `UPDATE` |

#### Full connector configuration

```properties
transforms=extractState,toOrderEvent

# Unwrap Debezium envelope; expose CDC op type as __op field
transforms.extractState.type=com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState
transforms.extractState.cdc.output.format=advanced
transforms.extractState.add.fields=op
transforms.extractState.delete.tombstone.handling.mode=drop

# Build the nested OrderEvent envelope with metadata + payload, add headers
transforms.toOrderEvent.type=com.scylladb.cdc.debezium.connector.transforms.ExampleOrderEventTransform
transforms.toOrderEvent.target.topic=orders.order-event
transforms.toOrderEvent.event.source=my-service

# Serialize as Protobuf (requires Confluent Schema Registry)
value.converter=io.confluent.connect.protobuf.ProtobufConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

#### Transform chain order

Transforms run left to right. `ScyllaExtractNewRecordState` must run first so that `__op` is
promoted to a value field and Cell wrappers are stripped before the custom transform reads them:

```
raw CDC record (Debezium envelope)
  → ScyllaExtractNewRecordState   (strips envelope, promotes __op, drops Cell wrappers)
  → ExampleOrderEventTransform    (builds OrderEvent struct, adds Kafka headers)
  → value.converter               (serializes Struct to Protobuf bytes)
  → Kafka topic: orders.order-event
```

#### Step-by-step: adapting the example for your table

1. **Copy and rename** `ExampleOrderEventTransform.java` — e.g. `ShipmentEventTransform.java`.

2. **Update the entity ID field** — set `ENTITY_ID_FIELD` to your table's primary key column:
   ```java
   private static final String ENTITY_ID_FIELD = "shipment_id";
   ```

3. **Update `event_name` prefix** — override `resolveEventName` / `resolveStateImpact`:
   ```java
   @Override
   protected String resolveEventName(String op) {
       if (OP_CREATE.equals(op)) return "SHIPMENT_CREATED";
       if (OP_DELETE.equals(op)) return "SHIPMENT_DELETED";
       return "SHIPMENT_UPDATED";
   }
   ```

4. **Update `PAYLOAD_SCHEMA` and `buildPayload`** to match your table's columns:
   ```java
   static final Schema PAYLOAD_SCHEMA = SchemaBuilder.struct()
           .name("com.example.shipping.ShipmentEvent.Payload")
           .optional()
           .field("shipment_id",   Schema.OPTIONAL_STRING_SCHEMA)
           .field("carrier",       Schema.OPTIONAL_STRING_SCHEMA)
           .field("tracking_code", Schema.OPTIONAL_STRING_SCHEMA)
           .build();

   @Override
   protected Struct buildPayload(Struct input) {
       return new Struct(PAYLOAD_SCHEMA)
               .put("shipment_id",   getOptionalField(input, "shipment_id",   String.class))
               .put("carrier",       getOptionalField(input, "carrier",        String.class))
               .put("tracking_code", getOptionalField(input, "tracking_code",  String.class));
   }
   ```

5. **Update `SCHEMA_ID_VALUE`** to your Protobuf schema's fully-qualified name:
   ```java
   private static final String SCHEMA_ID_VALUE = "com.example.shipping.ShipmentEvent";
   ```

6. **Register the class name in the connector config**:
   ```properties
   transforms.toShipmentEvent.type=com.scylladb.cdc.debezium.connector.transforms.ShipmentEventTransform
   transforms.toShipmentEvent.target.topic=logistics.shipment-event
   ```

---

## Protobuf serialization

The SMT produces a Kafka Connect `Struct` — a schema-aware in-memory object. Serialization to
Protobuf bytes happens downstream, controlled by the connector's `value.converter` setting. The
SMT itself does not produce Protobuf bytes directly.

### With Confluent Schema Registry (recommended)

```properties
value.converter=io.confluent.connect.protobuf.ProtobufConverter
value.converter.schema.registry.url=http://schema-registry:8081
# Optional: match the Protobuf message name to the Connect schema name
value.converter.scrub.invalid.names=true
```

Add the converter to your Kafka Connect plugin path:

```xml
<!-- pom.xml dependency for packaging -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-connect-protobuf-converter</artifactId>
    <version>7.6.0</version>
</dependency>
```

The converter derives the Protobuf `.proto` schema from the Connect `Schema` automatically and
registers it in the Schema Registry. The `schema.name` of your `OUTPUT_SCHEMA` becomes the
Protobuf message name (e.g. `com.example.orders.OrderEvent`), which is also written to the
`schema-id` header.

### Without Schema Registry

If you manage your `.proto` files and serialization manually, set:

```properties
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```

And perform Protobuf serialization yourself inside `buildPayload` / `apply` using the generated
Protobuf builder, then return a `ConnectRecord` with a `byte[]` value. This bypasses Connect's
schema system and is suitable for environments where Schema Registry is not available.

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
transforms.extractState.add.fields=op
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
| `event_name` is always `ORDER_UPDATED` | `__op` field missing | Add `add.fields=op` to `ScyllaExtractNewRecordState` config |
| `NullPointerException` in `buildPayload` | Field missing from CDC schema | Use `getOptionalField`; all output fields should be `OPTIONAL` |
| Protobuf deserialization error on consumer | Schema mismatch | Ensure `SCHEMA_ID_VALUE` matches the registered `.proto` message name |
| Records appear with old schema after redeploy | Schema registry cached old schema | Bump the schema version or use `schema.registry.compatibility=NONE` |
| Tombstones still appearing in output topic | SMT returns record instead of `null` | Return `null` from `apply()` for `record.value() == null` |
| Kafka headers not visible to consumer | Consumer not reading headers | Check consumer `header.deserializer` configuration |

[debezium-format]: https://debezium.io/documentation/reference/stable/connectors/cassandra.html
[example-smt]: ../src/main/java/com/scylladb/cdc/debezium/connector/transforms/ExampleOrderEventTransform.java
[state-store-readme]: https://github.com/scylladb/scylla-cdc-java#checkpoint-persistence-cdcstatestore
[connect-transforms]: https://kafka.apache.org/42/kafka-connect/user-guide/#included-transformations
