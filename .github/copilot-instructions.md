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
- `ScyllaCollectionSchema`, `CollectionsMode`, `CollectionOperation`: Collection (frozen/non-frozen) representations and delta mode logic.
- `transforms/ScyllaExtractNewState`: SMT flattening one-field Cell wrappers for easier sinks (similar to Debezium ExtractNewRecordState).

## Data Model & Topics
- **Topic naming:** `logicalName.keyspace.table` from `scylla.name` (see README examples).
- **Keys:** Primary key columns only.
- **Values:** `op`, `before`, `after`, `source`, `ts_ms`; non-PK columns wrapped in single-field `Cell` structs to distinguish non-modification vs `NULL`.
- **Collections:**
  - Frozen: `List/Set → array(T)`, `Map → array([key,value])`, `UDT → struct`.
  - Non-frozen delta mode: struct `{mode, elements}`; remove indicated by `null` values; UDT fields as per-Cell wrappers.

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
- **Docker Compose:** Check `docker-compose.yml` and `manage-docker.sh` for orchestrating Scylla/Kafka in docs/testing scenarios.

### Minimal Local Run (Docker Compose)
- **Bring up stack:**
  ```bash
  # From repo root
  docker compose up -d
  # or helper script if present
  ./manage-docker.sh up
  ```
- **Build connector:**
  ```bash
  mvn clean package
  ```
- **Deploy to Kafka Connect:** Copy `target/fat-jar/scylla-cdc-source-connector-*-jar-with-dependencies.jar` into the Connect `plugin.path` inside the Connect container, then restart the worker.
- **Start connector (JSON):** Post `connector-config/scylla-cdc-connector.json` to Connect’s REST API (`http://localhost:8083/connectors`).

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
- Collections: `scylla.collection.mode` and `scylla.collection.operation` control non-frozen behavior.
- Converters: JSON or Avro via standard Kafka Connect converters.

## Debugging & Offsets
- **Offsets:** Uses Kafka Connect offset storage; controlled by `offset.flush.interval.ms`; resumes from saved CDC positions.
- **Logs:** Flogger + log4j backend; ensure Connect worker logs include connector class logs.
- **Lag:** Compare `source.ts_ms` vs top-level `ts_ms`.

## Project Conventions
- **Schema building:** Centralized in `ScyllaChangeRecordEmitter` and `ScyllaCollectionSchema`; prefer extending rather than custom emitters.
- **SMT usage:** Use `transforms/ScyllaExtractNewState` when downstream requires flattened fields; understand trade-off (NULL vs non-modification).
- **Topic creation:** Can be auto-enabled via `auto.create.topics.enable`.
- **Compatibility:** Kafka 2.6.0+ only; no pre/postimage support; partition/range deletes ignored by design.

## Useful Files & Paths
- `README.md`: Deep dive on event formats, examples, collections.
- `README-QUICKSTART.md`: Control Center and OSS Kafka setup guide.
- `pom.xml`: Plugin packaging config and dependency versions.
- `src/main/java/com/scylladb/cdc/debezium/connector/`: Connector core implementation.
- `src/main/java/.../transforms/`: SMTs.
- `connector-config/scylla-cdc-connector.json`: JSON example config.

---
Questions or missing pieces? Tell me where guidance was unclear (build/run steps, config fields, or data format nuances) and I’ll refine this for your workflows.