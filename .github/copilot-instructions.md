# AI Agent Instructions for Scylla CDC Source Connector

These instructions help AI coding agents quickly work productively in this repo.

## Big Picture
- **Purpose:** A Debezium-based Kafka Connect source connector that reads ScyllaDB CDC logs and emits row change events (`INSERT`, `UPDATE`, `DELETE`).
- **Core flow:** Scylla session → CDC readers per vnode/task → Debezium Change Event Source → Kafka Connect records (topic per table). Handles schema/topology changes, at-least-once delivery, offsets.
- **Packaging:** Maven builds a fat JAR and a Confluent Hub component package.

## Key Components (Java)
- `com.scylladb.cdc.debezium.connector.ScyllaConnector`: Kafka Connect connector entry.
- `ScyllaConnectorConfig`: Parses connector config; includes Scylla and collection mode settings.
- `ScyllaTaskContext` + `ScyllaConnectorTask`: Task lifecycle, partitioning across vnodes.
- `ScyllaChangeEventSourceFactory`: Wires snapshot/streaming sources.
- `ScyllaStreamingChangeEventSource` and `ScyllaSnapshotChangeEventSource`: Core readers producing Debezium events.
- `ScyllaChangeRecordEmitter`: Builds Connect `SourceRecord` payloads and schemas.
- `ScyllaSourceInfoStructMaker` / `SourceInfo`: Source metadata fields (`ts_ms`, `ts_us`, keyspace/table, logical name).
- `ScyllaCollectionSchema`: Schema-related class; non-frozen collections are emitted as **delta** payloads (no mode field) as described in the README.
- `transforms/ScyllaExtractNewState`: SMT flattening one-field Cell wrappers for easier sinks (similar to Debezium ExtractNewRecordState).

## Data Model & Topics
- **Topic naming:** `logicalName.keyspace.table` from `scylla.name` (see README examples).
- **Keys:** Primary key columns only.
- **Values:** `op`, `before`, `after`, `source`, `ts_ms`; non-PK columns wrapped in single-field `Cell` structs to distinguish non-modification vs `NULL`.

### Collections specifics
- **Frozen collections:** Represented as a single Cell `value` field; empty vs `NULL` is preserved and asserted by `ScyllaTypesIT.canReplicateFrozenCollectionsEdgeCases`.
- **Non-frozen collections (delta mode):**
  - Represented as a Cell whose `value` carries the delta payload (no `mode` field).
  - `Set<T>`: array of elements touched by the change; additions/removals are not distinguished.
  - `List<T>`: array of `{value: T}` structs; removals are `{value: null}` and list element keys/timeuuid values are not emitted.
  - `Map<K,V>`: array of `{key, value}` structs; removals have `value: null`.
  - `UDT`: struct with only modified fields present; removed fields are `null`.
  - When there are no element-level deltas on INSERT, Scylla CDC does not distinguish explicit empty (`[]/{}`) from `NULL`; the connector may emit a **top-level null** for that column in this ambiguous case.
  - For non-INSERT operations that delete the entire non-frozen collection, the connector emits a Cell with `value = null`.

Refer to `ScyllaTypesIT.canReplicateNonFrozenCollectionsEdgeCases` for concrete examples and assertions of this behavior.

## Build & Artifacts
- **Build fat JAR:**
  ```bash
  mvn clean package
  ls target/fat-jar/
  ```
- **Confluent package:** Generated via `kafka-connect-maven-plugin`; artifacts under `target/components/`.
- **Java version:** Target 1.8; Debezium 1.4.1.Final; Kafka 2.6.0.

## Run & Local Setup
- **Kafka Connect plugin:** Copy fat JAR to Kafka Connect `plugin.path`.
- **Example config:** See `connector-config/scylla-cdc-connector.json`.
- **Start standalone (OSS Kafka):**
  ```bash
  # Create properties from JSON (or use Control Center UI)
  bin/connect-standalone.sh config/connect-standalone.properties connector.properties
  ```

### Minimal Local Run (Docker)
- **Build connector:**
  ```bash
  mvn clean package
  ```
- **Start required containers:**
Launch Scylla, Zookeeper, Kafka, and Kafka Connect containers (see `README-QUICKSTART.md` for full commands and port mappings). Example:

  ```bash
  # Start Scylla
  docker run -d --name scylla --hostname scylla -p 9042:9042 scylladb/scylla
  # Start Zookeeper
  docker run -d --name zookeeper -p 2181:2181 zookeeper:3.6
  # Start Kafka (example, adjust as needed)
  docker run -d --name kafka -p 9092:9092 --env ... bitnami/kafka:latest
  # Start Kafka Connect
  docker run -d --name connect -p 8083:8083 -v $PWD/target/fat-jar:/usr/share/java/kafka/plugins/scylla-cdc -e CONNECT_PLUGIN_PATH=/usr/share/java/kafka/plugins ... bitnami/kafka-connect:latest
  ```
- **Deploy connector JAR:**
Ensure the built connector JAR is available in the Kafka Connect container's plugin path (as shown above with `-v $PWD/target/fat-jar:/usr/share/java/kafka/plugins/scylla-cdc`).
- **Start connector:**
Post connector-config/scylla-cdc-connector.json to the Kafka Connect REST API:
  ```bash
  curl -X POST -H "Content-Type: application/json" --data @connector-config/scylla-cdc-connector.json http://localhost:8083/connectors
  ```

### Avro Converter Example
- **Worker config (typical):**
  ```properties
  key.converter=io.confluent.connect.avro.AvroConverter
  value.converter=io.confluent.connect.avro.AvroConverter
  key.converter.schema.registry.url=http://schema-registry:8081
  value.converter.schema.registry.url=http://schema-registry:8081
  ```
- **Connector config overrides:** If setting at connector level, add to `config` in `connector-config/scylla-cdc-connector.json`:
  ```json
  {
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
  ```
- Ensure Schema Registry is running (via `docker-compose.yml`).

### Control Center Deployment
- **Place connector JARs:** Move built fat JAR into Control Center’s Kafka Connect `share/java` (or configured `plugin.path`).
- **Launch GUI:** Visit `http://localhost:9021`, add connector `ScyllaConnector (Source Connector)`, then set:
  - Hosts: e.g., `scylla:9042`
  - Namespace: `ScyllaCollectionsTestNamespace`
  - Table names: `quickstart_keyspace.user_profiles,...`
  - Converters: JSON or Avro as above
- **Verify messages:** Topic `logicalName.keyspace.table` (e.g., `ScyllaCollectionsTestNamespace.quickstart_keyspace.orders`).

## Configuration Highlights
- Required: `scylla.cluster.ip.addresses`, `scylla.name`, `scylla.table.names`.
- Optional: `scylla.user`, `scylla.password`, `scylla.consistency.level`, TCP keepalive.

- Converters: JSON or Avro via standard Kafka Connect converters.

## Debugging & Offsets
- **Offsets:** Uses Kafka Connect offset storage; controlled by `offset.flush.interval.ms`; resumes from saved CDC positions.
- **Logs:** Flogger + log4j backend; ensure Connect worker logs include connector class logs.
- **Lag:** Compare `source.ts_ms` vs top-level `ts_ms`.

## Project Conventions
- **Schema building:** Centralized in `ScyllaChangeRecordEmitter` and `ScyllaCollectionSchema`; prefer extending rather than custom emitters.

- **Topic creation:** Can be auto-enabled via `auto.create.topics.enable`.
- **Compatibility:** Kafka 2.6.0+ only; no pre/postimage support; partition/range deletes ignored by design.

## Useful Files & Paths
- `README.md`: Deep dive on event formats, examples, collections.
- `README-QUICKSTART.md`: Control Center and OSS Kafka setup guide.
- `pom.xml`: Plugin packaging config and dependency versions.
- `src/main/java/com/scylladb/cdc/debezium/connector/`: Connector core implementation.



---
Questions or missing pieces? Tell me where guidance was unclear (build/run steps, config fields, or data format nuances) and I’ll refine this for your workflows.
