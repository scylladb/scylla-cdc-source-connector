# Scylla CDC Source Connector

## Overview

Scylla CDC Source Connector is a source connector capturing row-level changes in the tables of a Scylla cluster. It is a Debezium connector, compatible with Kafka Connect (with Kafka 2.6.0+) and built on top of scylla-cdc-java library.

The connector reads the CDC log for specified tables and produces Kafka messages for each row-level `INSERT`, `UPDATE` or `DELETE` operation. The connector is able to split reading the CDC log accross multiple processes: the connector can start a separate Kafka Connect task for reading each [Vnode of Scylla cluster](https://docs.scylladb.com/architecture/ringarchitecture/) allowing for high throughput. You can limit the number of started tasks by using `tasks.max` property.

Scylla CDC Source Connector seamlessly handles schema changes and topology changes (adding, removing nodes from Scylla cluster). The connector is fault-tolerant, retrying reading data from Scylla in case of failure. It periodically saves the current position in Scylla CDC log using Kafka Connect offset tracking (configurable by `offset.flush.interval.ms` parameter). If the connector is stopped, it is able to resume reading from previously saved offset. Scylla CDC Source Connector has at-least-once semantics.

The connector has the following capabilities:
- Kafka Connect connector using Debezium framework
- Replication of row-level changes from Scylla using [Scylla CDC](https://docs.scylladb.com/using-scylla/cdc/):
    - `INSERT`
    - `UPDATE`
    - `DELETE` (single row deletes)
- High scalability - able to split work accross multiple Kafka Connect workers
- Fault tolerant - connector periodically saves its progress and can resume from previously saved offset (with at-least-once semantics)
- Support for many standard Kafka Connect converters, such as JSON and Avro
- Compatible with standard Kafka Connect transformations
- Metadata about CDC events - each generated Kafka message contains information about source, such as timestamp and table name
- Seamless handling of schema changes and topology changes (adding, removing nodes from Scylla cluster)
- Preimage and postimage support ([optional](#preimagepostimage-configuration)) - messages generated for row-level changes can have their [`before`](#data-change-event-value) and/or [`after`](#data-change-event-value) fields filled with complete row state from corresponding preimage/postimage rows.
- Full support for collection types (`LIST`, `SET`, `MAP`) and User Defined Types (`UDT`) - see [Collection and UDT Type Support](#collection-and-udt-type-support)

The connector has the following limitations:
- Only Kafka 2.6.0+ is supported
- Only row-level operations are produced (`INSERT`, `UPDATE`, `DELETE`):
    - Row range deletes - those changes are ignored
    - Partition deletes - see [Partition Delete Support](#partition-delete-support) section below
- By default, changes only contain those columns that were modified, not the entire row before/after change. To include full row state, configure [preimage/postimage support](#preimagepostimage-configuration). More information [here](#cell-representation)

### Compatibility

[![Integration testing](https://github.com/scylladb/scylla-cdc-source-connector/actions/workflows/pr-verify.yml/badge.svg?branch=master)](https://github.com/scylladb/scylla-cdc-source-connector/actions/workflows/pr-verify.yml?query=branch%3Amaster)

The connector is tested against the latest Apache Kafka 3.x, 4.x and currently last 6 minor Confluent Platform releases.
It should remain compatible with those versions.

Below is a summary of the Debezium and Kafka (Connect API) dependency versions used by the last few tagged connector releases.

| Connector version | Debezium version    | Connect API |
|-------------------|---------------------|-------------|
| v1.2.6            | 2.6.2.Final         | 3.9.1       |
| v1.2.5            | 2.6.2.Final         | 3.3.1       |
| v1.2.4            | 2.6.2.Final         | 3.3.1       |
| v1.2.3            | 2.6.2.Final         | 3.3.1       |
| v1.2.2            | 1.4.1.Final         | 3.3.1       |

For older releases, consult the corresponding tag's `pom.xml` for the `<debezium.version>` and `<kafka.version>` properties.

## Connector installation

### Prebuilt images

You can download the connector as a prebuilt package:
1. JAR with dependencies from [github releases](https://github.com/scylladb/scylla-cdc-source-connector/releases) (fat JAR)
2. [Confluent Hub](https://www.confluent.io/hub/scylladb/scylla-cdc-source-connector) package (ZIP)

The artifacts are also available in [Maven Central Repository](https://central.sonatype.com/artifact/com.scylladb/scylla-cdc-source-connector) - we recommend using the "JAR with dependencies" file there.

#### Building from source

You can also build the connector from source by using the following commands:
```bash
git clone https://github.com/scylladb/scylla-cdc-source-connector.git
cd scylla-cdc-source-connector
mvn clean package
```

The connector JAR file will be available in `scylla-cdc-kafka-connect/target/fat-jar` directory.


### Installation

Copy the JAR file with connector into your Kafka Connect deployment and append the directory containing the connector to your Kafka Connect's plugin path (`plugin.path` configuration property).

## Configuration

Scylla CDC Source Connector exposes many configuration properties. These are the most important:

| Property                                                   | Required | Description |
|------------------------------------------------------------| --- | --- |
| (until 1.2.2)`scylla.name`<br/>(since 1.2.3)`topic.prefix` | **Yes** | A unique name that identifies the Scylla cluster and that is used as a prefix for all schemas, topics. The logical name allows you to easily differentiate between your different Scylla cluster deployments. Each distinct Scylla installation should have a separate namespace and be monitored by at most one Scylla CDC Source Connector. It should consist of alphanumeric or underscore (`_`) characters. |
| `scylla.cluster.ip.addresses`                              | **Yes** | List of IP addresses of nodes in the Scylla cluster that the connector will use to open initial connections to the cluster. In the form of a comma-separated list of pairs <IP>:<PORT> (`host1:port1,host2:port2`). |
| `scylla.table.names`                                       | **Yes** | List of CDC-enabled table names for connector to read. See [Change Data Capture (CDC)](https://docs.scylladb.com/using-scylla/cdc/) for more information about configuring CDC on Scylla. Provided as a comma-separated list of pairs `<keyspace name>.<table name>`. |
| `scylla.user`                                              | No | The username to connect to Scylla with. If not set, no authorization is done. |
| `scylla.password`                                          | No | The password to connect to Scylla with. If not set, no authorization is done. |

### Choosing an Output Format

> **⚠️ Deprecation Warning:** The legacy output format is deprecated and will be removed in version 3.0.0. New deployments should use the advanced format (`cdc.output.format=advanced`). Existing deployments using legacy format should plan to migrate before upgrading to 3.0.0.

The connector supports two output formats. Choose based on your requirements:

| Feature | Legacy Format (Default) | Advanced Format |
|---------|------------------------|-----------------|
| Configuration | `cdc.output.format=legacy` | `cdc.output.format=advanced` |
| Column value representation | Wrapped in Cell structs (`{"value": ...}`) | Direct values |
| Preimage support | `experimental.preimages.enabled=true` | `cdc.include.before=full\|only-updated` |
| Postimage support | **Not supported** | `cdc.include.after=full\|only-updated` |
| Primary key placement options | Fixed | Configurable via `cdc.include.primary-key.placement` |
| Recommended for | Existing deployments, backward compatibility | New deployments, cleaner message structure |

See [Legacy Format Details](#legacy-format-details) and [Advanced Format Details](#advanced-format-details) for complete documentation.

See additional configuration properties in the ["Advanced administration"](#advanced-administration) section.

Example configuration (as `.properties` file):
```
name=ScyllaCDCSourceConnector
connector.class=com.scylladb.cdc.debezium.connector.ScyllaConnector
# use scylla.name instead for versions < 1.2.3
topic.prefix=MyScyllaCluster
scylla.cluster.ip.addresses=127.0.0.1:9042,127.0.0.2:9042
scylla.table.names=ks.my_table

tasks.max=10
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true

auto.create.topics.enable=true
```

This configuration will capture row-level changes in the `ks.my_table` table from Scylla cluster (`127.0.0.1`, `127.0.0.2`). Change data capture events will appear on `MyScyllaCluster.ks.my_table` Kafka topic encoded as JSONs with schema information.

Note that some of the unspecified properties will be set according to the defaults or your common connector configuration
defined by your Kafka Connect installation. For example if you skip converter settings, they can be different depending
on the Kafka Connect version you're using.

Scylla CDC Source Connector writes events to a separate Kafka topic for each source Scylla table. The topic name will be: `logicalName.keyspaceName.tableName` (logical name configured by `topic.prefix` property). You can turn on automatic topic creation by using the `auto.create.topics.enable` property.

## Data change events
Scylla CDC Source Connector generates a data change event for each row-level `INSERT`, `UPDATE` or `DELETE` operation. Each event consists of key and value.

Debezium and Kafka Connect are designed around continuous streams of event messages, and the structure of these events may change over time. This could be difficult for consumers to deal with, so to make it easy Kafka Connect makes each event self-contained. Every message key and value has two parts: a schema and payload. The schema describes the structure of the payload, while the payload contains the actual data.

> **Important:** The connector supports two output formats: **Legacy** (default) and **Advanced**. The format affects the structure of messages, particularly how non-primary-key column values are represented. See [Output Format Configuration](#output-format-configuration) for details on choosing a format.

### Data change event key
The data change event's key will contain a field for each column in the primary key (partition key and clustering key).

For example, given this Scylla table and `INSERT` operation:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

INSERT INTO ks.t(pk, ck, v) VALUES (1, 1, 'example row');
```

The data change event's key will look like this (with JSON serializer and schema enabled):
```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": true,
        "field": "ck"
      },
      {
        "type": "int32",
        "optional": true,
        "field": "pk"
      }
    ],
    "optional": false,
    "name": "MyScyllaCluster.ks.my_table.Key"
  },
  "payload": {
    "ck": 1,
    "pk": 1
  }
}
```

### Data change event value
Data change event's value consists of a schema and a payload section. The payload of every data change events contains the following fields:

- `op`: type of operation. `c` for `INSERT`, `u` for `UPDATE`, `d` for `DELETE`.
- `before`: an optional field with state of the row before the event occurred. Present in `DELETE` data change events.
- `after`: an optional field with state of the row after the event occurred. Present in `UPDATE` and `INSERT` data change events.
- `ts_ms`: time at which connector processed the event.
- `source`: metadata about the source of event:
    - `name`: logical name of Scylla cluster (`scylla.name`).
    - `ts_ms`: the time that the change was made in the database (in milliseconds). You can compute a difference between `source.ts_ms` and (top-level) `ts_ms` to determine the lag between the source Scylla change and the connector.
    - `ts_us`: the time that the change was made in the database (in microseconds).
    - `keyspace_name`, `table_name`: the name of keyspace and table this data change event originated from. 

---

## Legacy Format Details

> **⚠️ Deprecation Warning:** The legacy output format is deprecated and will be removed in version 3.0.0. Please migrate to the advanced format. See [Migrating from Legacy to Advanced Format](#migrating-from-legacy-to-advanced-format).

This section describes the message structure and behavior specific to the **legacy output format** (`cdc.output.format=legacy`, the default). For advanced format, see [Advanced Format Details](#advanced-format-details).

### Cell Representation

Operations in Scylla, such as `INSERT` or `UPDATE`, do not have to modify all columns of a row. In the legacy output format, to differentiate between non-modification of column and inserting/updating `NULL`, all non-primary-key columns are wrapped with structure containing a single `value` field. For example, given this Scylla table and `UPDATE` operation:

```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

INSERT INTO ks.t(pk, ck, v) VALUES (1, 1, 'example row');
UPDATE ks.t SET v = 'new value' WHERE pk = 1 AND ck = 1;
```

The `v` column will be represented as:
```json
...
    "v": {
        "value": "new value"
    }
...
```

In case of `UPDATE` setting `v` to `NULL`:
```
UPDATE ks.t SET v = NULL WHERE pk = 1 AND ck = 1;
```

The `v` column will be represented as:
```json
...
    "v": {
        "value": null
    }
...
```

If the operation did not modify the `v` column, the data event will contain the following representation of `v`:
```json
...
    "v": null
...
```

See `UPDATE` example for full data change event's value.

### Legacy Format Preimage Support

To enable preimage support in legacy format, use the `experimental.preimages.enabled` configuration option:

```properties
experimental.preimages.enabled=true
```

> **Note:** Legacy format does not support postimages. For postimage support, use the [advanced format](#advanced-format-details) with `cdc.include.after` configuration.

### Single Message Transformations (SMTs) for Legacy Format

The connector provides two single message transformations (SMTs) that are primarily useful for the legacy format: `ScyllaExtractNewRecordState` and `ScyllaFlattenColumns`.

#### `ScyllaExtractNewRecordState`
`ScyllaExtractNewRecordState` (class: `com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState`) works exactly like `io.debezium.transforms.ExtractNewRecordState` (in fact it is called underneath), but also flattens structure by extracting values from Cell structs (the `{"value": ...}` wrapper used in legacy format).

> **Note:** This transform works with both output formats. For legacy format (the default), it unwraps Cell structs. For advanced format (where values are already direct), it is effectively a pass-through for the value extraction part.

Such transformation makes message structure simpler (and easier to use with e.g. Elasticsearch), but when used with legacy format, it makes it impossible to differentiate between NULL value and non-modification.
If the message is as following:
```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": true,
        "field": "ck"
      },
      {
        "type": "int32",
        "optional": true,
        "field": "pk"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "int32",
            "optional": true,
            "field": "value"
          }
        ],
        "optional": true,
        "name": "NS2.ks.t.v.Cell",
        "field": "v"
      }
    ],
    "optional": false,
    "name": "NS2.ks.t.After"
  },
  "payload": {
    "ck": 2,
    "pk": 20,
    "v": {
      "value": 3
    }
  }
}
```
then the same message transformed by `ScyllaExtractNewRecordState` would be:
```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": true,
        "field": "ck"
      },
      {
        "type": "int32",
        "optional": true,
        "field": "pk"
      },
      {
        "type": "int32",
        "optional": true,
        "field": "v"
      }
    ],
    "optional": false,
    "name": "NS2.ks.t.After"
  },
  "payload": {
    "ck": 2,
    "pk": 20,
    "v": 3
  }
}
```
Notice how `v` field is no longer packed in `value`.

#### `ScyllaFlattenColumns`

`ScyllaFlattenColumns` (class: `com.scylladb.cdc.debezium.connector.transforms.ScyllaFlattenColumns`) flattens columns that are wrapped in Cell structures (the `{"value": ...}` wrapper used in legacy format), such as:
```json
"v": {
  "value": 3
}
```
transforming it into:
```json
"v": 3
```

Compared to `ScyllaExtractNewRecordState` transformation, `ScyllaFlattenColumns` does not remove any additional metadata or modify the message in any other way.

For example, running the transformation on this message:
```json
{
  "source": {
    "version": "1.1.4",
    "connector": "scylla",
    "name": "SMTExample",
    "ts_ms": 1706890860030,
    "snapshot": {
      "string": "false"
    },
    "db": "ks",
    "keyspace_name": "ks",
    "table_name": "t",
    "ts_us": 1706890860030414
  },
  "before": null,
  "after": {
    "SMTExample.ks.t.Before": {
      "ck": 7,
      "pk": 1,
      "v": {
        "value": 7
      }
    }
  },
  "op": {
    "string": "c"
  },
  "ts_ms": {
    "long": 1706890892952
  },
  "transaction": null
}
```
will result in the following message:
```json
{
  "source": {
    "version": "1.1.4",
    "connector": "scylla",
    "name": "SMTExample",
    "ts_ms": 1706890860030,
    "snapshot": {
      "string": "false"
    },
    "db": "ks",
    "keyspace_name": "ks",
    "table_name": "t",
    "ts_us": 1706890860030414
  },
  "before": null,
  "after": {
    "SMTExample.ks.t.Before": {
      "ck": 7,
      "pk": 1,
      "v": 7
    }
  },
  "op": {
    "string": "c"
  },
  "ts_ms": {
    "long": 1706890892952
  },
  "transaction": null
}
```
while `ScyllaExtractNewRecordState` would produce:
```json
{
  "ck": 7,
  "pk": 1,
  "v": 7
}
```

### Legacy Format Examples

The following examples show data change events in **legacy format** (the default). Notice how non-primary-key columns like `v` are wrapped in Cell structs (`{"value": ...}`).

#### `INSERT` example (Legacy Format)
Given this Scylla table and `INSERT` operation:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

INSERT INTO ks.t(pk, ck, v) VALUES (1, 1, 'example row');
```

The connector will generate the following data change event's value (with JSON serializer and schema enabled):
```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "struct",
        "fields": [
          {
            "type": "string",
            "optional": false,
            "field": "version"
          },
          {
            "type": "string",
            "optional": false,
            "field": "connector"
          },
          {
            "type": "string",
            "optional": false,
            "field": "name"
          },
          {
            "type": "int64",
            "optional": false,
            "field": "ts_ms"
          },
          {
            "type": "int64",
            "optional": false,
            "field": "ts_us"
          },
          {
            "type": "string",
            "optional": true,
            "name": "io.debezium.data.Enum",
            "version": 1,
            "parameters": {
              "allowed": "true,last,false"
            },
            "default": "false",
            "field": "snapshot"
          },
          {
            "type": "string",
            "optional": false,
            "field": "db"
          },
          {
            "type": "string",
            "optional": false,
            "field": "keyspace_name"
          },
          {
            "type": "string",
            "optional": false,
            "field": "table_name"
          }
        ],
        "optional": false,
        "name": "com.scylladb.cdc.debezium.connector",
        "field": "source"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "int32",
            "optional": true,
            "field": "ck"
          },
          {
            "type": "int32",
            "optional": true,
            "field": "pk"
          },
          {
            "type": "struct",
            "fields": [
              {
                "type": "string",
                "optional": true,
                "field": "value"
              }
            ],
            "optional": true,
            "name": "MyScyllaCluster.ks.my_table.v.Cell",
            "field": "v"
          }
        ],
        "optional": true,
        "name": "MyScyllaCluster.ks.my_table.Before",
        "field": "before"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "int32",
            "optional": true,
            "field": "ck"
          },
          {
            "type": "int32",
            "optional": true,
            "field": "pk"
          },
          {
            "type": "struct",
            "fields": [
              {
                "type": "string",
                "optional": true,
                "field": "value"
              }
            ],
            "optional": true,
            "name": "MyScyllaCluster.ks.my_table.v.Cell",
            "field": "v"
          }
        ],
        "optional": true,
        "name": "MyScyllaCluster.ks.my_table.After",
        "field": "after"
      },
      {
        "type": "string",
        "optional": true,
        "field": "op"
      },
      {
        "type": "int64",
        "optional": true,
        "field": "ts_ms"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "string",
            "optional": false,
            "field": "id"
          },
          {
            "type": "int64",
            "optional": false,
            "field": "total_order"
          },
          {
            "type": "int64",
            "optional": false,
            "field": "data_collection_order"
          }
        ],
        "optional": true,
        "field": "transaction"
      }
    ],
    "optional": false,
    "name": "MyScyllaCluster.ks.my_table.Envelope"
  },
  "payload": {
    "source": {
      "version": "1.0.1-SNAPSHOT",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1611578778701,
      "ts_us": 1611578778701813,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "my_table"
    },
    "before": null,
    "after": {
      "ck": 1,
      "pk": 1,
      "v": {
        "value": "example row"
      }
    },
    "op": "c",
    "ts_ms": 1611578838754,
    "transaction": null
  }
}
```

#### `UPDATE` example (Legacy Format)
Given this Scylla table and `UPDATE` operations:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

UPDATE ks.t SET v = 'new value' WHERE pk = 1 AND ck = 1;
UPDATE ks.t SET v = NULL WHERE pk = 1 AND ck = 1;
```

The connector will generate the following data change event's value (with JSON serializer and schema enabled) for the first `UPDATE`. Note that `schema` is ommitted as it is the same as in `INSERT` example:
```json
{
  "schema": {},
  "payload": {
    "source": {
      "version": "1.0.1-SNAPSHOT",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1611578808701,
      "ts_us": 1611578808701321,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "my_table"
    },
    "before": null,
    "after": {
      "ck": 1,
      "pk": 1,
      "v": {
        "value": "new value"
      }
    },
    "op": "u",
    "ts_ms": 1611578868758,
    "transaction": null
  }
}
```

Data change event's value for the second `UPDATE`:
```json
{
  "schema": {},
  "payload": {
    "source": {
      "version": "1.0.1-SNAPSHOT",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1611578808701,
      "ts_us": 1611578808701341,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "my_table"
    },
    "before": null,
    "after": {
      "ck": 1,
      "pk": 1,
      "v": {
        "value": null
      }
    },
    "op": "u",
    "ts_ms": 1611578868758,
    "transaction": null
  }
}
```

#### `DELETE` example (Legacy Format)
Given this Scylla table and `DELETE` operation:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

DELETE FROM ks.t WHERE pk = 1 AND ck = 1;
```

The connector will generate the following data change event's value (with JSON serializer and schema enabled). Note that `schema` is ommitted as it is the same as in `INSERT` example:
```json
{
  "schema": {},
  "payload": {
    "source": {
      "version": "1.0.1-SNAPSHOT",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1611578808701,
      "ts_us": 1611578808701919,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "my_table"
    },
    "before": {
      "ck": 1,
      "pk": 1,
      "v": null
    },
    "after": null,
    "op": "d",
    "ts_ms": 1611578868759,
    "transaction": null
  }
}
```

---

## Advanced Format Details

This section describes the message structure and behavior specific to the **advanced output format** (`cdc.output.format=advanced`). For legacy format details, see [Legacy Format Details](#legacy-format-details).

### Message Structure

In the advanced format, non-primary-key column values are emitted **directly** without the Cell struct wrapper. This results in cleaner, more compact messages.

### Advanced Format Examples

The following examples show data change events in **advanced format**. Notice how non-primary-key columns like `v` contain values directly (no `{"value": ...}` wrapper).

#### `INSERT` example (Advanced Format)

Given this Scylla table and `INSERT` operation:
```sql
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

INSERT INTO ks.t(pk, ck, v) VALUES (1, 1, 'example row');
```

The connector will generate the following data change event's value (payload only, schema omitted for brevity):
```json
{
  "payload": {
    "source": {
      "version": "1.0.1-SNAPSHOT",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1611578778701,
      "ts_us": 1611578778701813,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "my_table"
    },
    "before": null,
    "after": {
      "ck": 1,
      "pk": 1,
      "v": "example row"
    },
    "op": "c",
    "ts_ms": 1611578838754,
    "transaction": null
  }
}
```

Note: The `v` column value is `"example row"` directly, not `{"value": "example row"}` as in legacy format.

#### `UPDATE` example (Advanced Format)

Given an `UPDATE` operation:
```sql
UPDATE ks.t SET v = 'new value' WHERE pk = 1 AND ck = 1;
```

The connector will generate:
```json
{
  "payload": {
    "source": {
      "version": "1.0.1-SNAPSHOT",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1611578808701,
      "ts_us": 1611578808701321,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "my_table"
    },
    "before": null,
    "after": {
      "ck": 1,
      "pk": 1,
      "v": "new value"
    },
    "op": "u",
    "ts_ms": 1611578868758,
    "transaction": null
  }
}
```

#### `DELETE` example (Advanced Format)

Given a `DELETE` operation:
```sql
DELETE FROM ks.t WHERE pk = 1 AND ck = 1;
```

The connector will generate:
```json
{
  "payload": {
    "source": {
      "version": "1.0.1-SNAPSHOT",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1611578808701,
      "ts_us": 1611578808701919,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "my_table"
    },
    "before": {
      "ck": 1,
      "pk": 1,
      "v": null
    },
    "after": null,
    "op": "d",
    "ts_ms": 1611578868759,
    "transaction": null
  }
}
```

### Advanced Format Preimage/Postimage Support

The advanced format supports flexible preimage and postimage configuration through the `cdc.include.before` and `cdc.include.after` options. See [Preimage/Postimage Configuration](#preimagepostimage-configuration) for detailed documentation.

Example configuration:
```properties
cdc.output.format=advanced
cdc.include.before=full
cdc.include.after=full
```

#### `UPDATE` with Preimage/Postimage (Advanced Format)

With `cdc.include.before=full` and `cdc.include.after=full`, an `UPDATE` will include complete row state:
```json
{
  "payload": {
    "source": { ... },
    "before": {
      "ck": 1,
      "pk": 1,
      "v": "old value"
    },
    "after": {
      "ck": 1,
      "pk": 1,
      "v": "new value"
    },
    "op": "u",
    "ts_ms": 1611578868758,
    "transaction": null
  }
}
```

---

## Collection and UDT Type Support

The connector fully supports Scylla collection types (`LIST`, `SET`, `MAP`) and User Defined Types (`UDT`). Both frozen and non-frozen collections are supported, with different CDC behavior for each.

### Frozen vs Non-Frozen Collections

| Type | Frozen | Non-Frozen |
|------|--------|------------|
| **CDC Behavior** | Entire collection is replaced atomically | Element-level changes (add, remove, update) |
| **Message Content** | Complete collection value | Only the modified elements |
| **Use Case** | Small, rarely modified collections | Large collections with frequent element updates |

### Collection Type Representations

#### LIST

Lists are represented as JSON arrays:

```json
{
  "after": {
    "pk": 1,
    "my_list": ["item1", "item2", "item3"]
  }
}
```

For non-frozen lists, Scylla CDC tracks element-level changes. The connector reconstructs the list from the CDC log entries.

#### SET

Sets are represented as JSON arrays (order is not guaranteed):

```json
{
  "after": {
    "pk": 1,
    "my_set": ["value1", "value2", "value3"]
  }
}
```

#### MAP

Maps are represented as arrays of key-value structs:

```json
{
  "after": {
    "pk": 1,
    "my_map": [
      {"key": "key1", "value": "value1"},
      {"key": "key2", "value": "value2"}
    ]
  }
}
```

This representation is used instead of a JSON object to support non-string key types.

### User Defined Types (UDT)

UDTs are represented as structs with fields matching the UDT definition:

```sql
CREATE TYPE ks.address (
    street text,
    city text,
    zip int
);

CREATE TABLE ks.users (
    id int PRIMARY KEY,
    home_address frozen<address>
) WITH cdc = {'enabled': true};
```

The resulting message:

```json
{
  "after": {
    "id": 1,
    "home_address": {
      "street": "123 Main St",
      "city": "Springfield",
      "zip": 12345
    }
  }
}
```

### Nested Collections and UDTs

The connector supports nested structures such as:
- `LIST<frozen<UDT>>`
- `MAP<text, frozen<LIST<int>>>`
- UDTs containing collection fields

Nested types are represented using the same conventions as top-level types.

### Scylla Table Configuration

To enable CDC for tables with collections:

```sql
-- Frozen collection (atomic updates only)
CREATE TABLE ks.with_frozen (
    pk int PRIMARY KEY,
    data frozen<list<text>>
) WITH cdc = {'enabled': true};

-- Non-frozen collection (element-level tracking)
CREATE TABLE ks.with_non_frozen (
    pk int PRIMARY KEY,
    data list<text>
) WITH cdc = {'enabled': true};
```

---

## Advanced administration

### Advanced configuration parameters

In addition to the configuration parameters described in the ["Configuration"](#configuration) section, Scylla CDC Source Connector exposes the following (non-required) configuration parameters:

| Property                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `scylla.query.time.window.size`  | The size of windows queried by the connector. Changes are queried using `SELECT` statements with time restriction with width defined by this parameter. Value expressed in milliseconds.                                                                                                                                                                                                                                                                                                                              |
| `scylla.confidence.window.size`  | The size of the confidence window. It is necessary for the connector to avoid reading too fresh data from the CDC log due to the eventual consistency of Scylla. The problem could appear when a newer write reaches a replica before some older write. For a short period of time, when reading, it is possible for the replica to return only the newer write. The connector mitigates this problem by not reading a window of most recent changes (controlled by this parameter). Value expressed in milliseconds. |
| `scylla.consistency.level`       | The consistency level of CDC table read queries. This consistency level is used only for read queries to the CDC log table. By default, `QUORUM` level is used.                                                                                                                                                                                                                                                                                                                                                       |
| `scylla.local.dc`                | The name of Scylla local datacenter. This local datacenter name will be used to setup the connection to Scylla to prioritize sending requests to the nodes in the local datacenter. If not set, no particular datacenter will be prioritized.                                                                                                                                                                                                                                                                         |


### Output Format Configuration

The connector supports two output formats for CDC messages, controlled by the `cdc.output.format` configuration option.

For detailed documentation with examples, see:
- [Legacy Format Details](#legacy-format-details) - Default format with Cell struct wrappers
- [Advanced Format Details](#advanced-format-details) - Direct values, advanced preimage/postimage support

#### Configuration Options

| Property            | Default | Values | Description |
|---------------------|---------|--------|-------------|
| `cdc.output.format` | `legacy` | `legacy`, `advanced` | Specifies the output format for CDC messages. See format descriptions below. |
| `experimental.preimages.enabled` | `false` | `true`, `false` | Enable preimage support in legacy mode only. For advanced mode, use `cdc.include.before` instead. |

#### Format Descriptions

| Format | Description |
|--------|-------------|
| `legacy` | **Default.** V1 format where non-PK column values are wrapped in Cell structs (`{"value": <actual_value>}`). Uses `experimental.preimages.enabled` for simple preimage support. Does not support postimages or advanced configuration options. |
| `advanced` | Non-PK column values are emitted directly without wrapping. Supports advanced preimage/postimage configuration via `cdc.include.before` and `cdc.include.after`. Supports flexible primary key placement via `cdc.include.primary-key.placement`. |

#### Legacy Format (Default) - Deprecated

> **⚠️ Deprecated:** Will be removed in version 3.0.0. Use advanced format for new deployments.

The legacy format wraps non-PK column values in Cell structs:
```json
{
  "after": {
    "pk": 1,
    "ck": 1,
    "v": {
      "value": "example value"
    }
  }
}
```

Key characteristics:
- Non-PK columns are wrapped in `{"value": <actual_value>}` structs
- Use `experimental.preimages.enabled=true` for preimage support (not `cdc.include.before`)
- **Postimages are not supported** in legacy mode
- The `cdc.include.*` configuration options do not apply in legacy mode

#### Advanced Format

The advanced format emits column values directly:
```json
{
  "after": {
    "pk": 1,
    "ck": 1,
    "v": "example value"
  }
}
```

This format supports:
- Preimage and postimage configuration via `cdc.include.before` and `cdc.include.after`
- Flexible primary key placement via `cdc.include.primary-key.placement`
- Cleaner message structure without Cell wrappers

#### Migrating from Legacy to Advanced Format

If you want to migrate from the legacy format to the advanced format:

1. **Schema changes**: The advanced format has a different message schema (no Cell wrappers). Downstream consumers may need updates.
2. **Configuration changes**:
   - Add `cdc.output.format=advanced`
   - Remove `experimental.preimages.enabled=true`
   - Add `cdc.include.before=full` (or `only-updated`) if you need preimages
   - Optionally configure `cdc.include.after` for postimage support
3. **SMT changes**: If using `ScyllaExtractNewRecordState` or `ScyllaFlattenColumns` transforms, they will work with both formats (they are no-ops for advanced format since values aren't wrapped).

#### Advanced Format Example Configuration

```properties
# Use advanced output format
cdc.output.format=advanced

# Enable preimage support (advanced mode)
cdc.include.before=full
```

### Preimage/Postimage Configuration (Advanced Format Only)

> **Note:** This section applies to the **advanced output format** (`cdc.output.format=advanced`). For legacy format (the default), use `experimental.preimages.enabled` instead. See [Legacy Format Details](#legacy-format-details).

The connector supports including the complete row state before and/or after a change in CDC messages. This is useful when you need the full context of a row, not just the columns that were modified.

#### Configuration Options

| Property            | Default | Values | Description |
|---------------------|---------|--------|-------------|
| `cdc.include.before` | `none`  | `none`, `full`, `only-updated` | Specifies whether to include the 'before' state of the row in CDC messages. Requires the Scylla table to have preimage enabled (`WITH cdc = {'preimage': true}`) for `full` or `only-updated` modes. |
| `cdc.include.after`  | `none`  | `none`, `full`, `only-updated` | Specifies whether to include the 'after' state of the row in CDC messages. Requires the Scylla table to have postimage enabled (`WITH cdc = {'postimage': true}`) for `full` or `only-updated` modes. |
| `cdc.include.primary-key.placement` | `kafka-key,payload-after,payload-before` | Comma-separated list of: `kafka-key`, `payload-after`, `payload-before`, `payload-key`, `kafka-headers` | Specifies where primary key (PK) and clustering key (CK) columns should be included in the output. See [Primary Key Placement](#primary-key-placement) for details. |
| `cdc.include.primary-key.payload-key-name` | `key` | Any valid field name | Specifies the field name for the primary key object in the message payload when `payload-key` is included in `cdc.include.primary-key.placement`. |
| `cdc.incomplete.task.timeout.ms` | `15000` | Positive integer (milliseconds) | Timeout for incomplete CDC tasks waiting for preimage/postimage events. Tasks that remain incomplete longer than this duration are dropped and logged as errors. |

#### Mode Descriptions

| Mode | Description |
|------|-------------|
| `none` | The field (`before` or `after`) will be `null`. No preimage/postimage data is fetched from Scylla. |
| `full` | The field will contain the complete row state with all columns. For `before`, this shows the full row before the change. For `after`, this shows the full row after the change. |
| `only-updated` | The field will contain only the columns that were modified by the operation (plus primary key columns based on `cdc.include.primary-key.placement`). This reduces message size when you only care about what changed. |

#### Behavior by Operation Type

| Operation | `before` field | `after` field |
|-----------|----------------|---------------|
| **INSERT** | Always `null` (no previous state) | Full image regardless of mode (all columns with values) |
| **UPDATE** | Depends on mode: `full` = all columns, `only-updated` = only modified columns, `none` = null | Depends on mode: `full` = all columns, `only-updated` = only modified columns, `none` = null |
| **DELETE** | Full preimage regardless of mode (all columns) | Always `null` (row was deleted) |

#### Scylla Table Configuration

To use preimage/postimage support, you must enable the corresponding CDC options on your Scylla table:

```sql
-- Enable preimage only
CREATE TABLE ks.my_table (
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true, 'preimage': true};

-- Enable postimage only
CREATE TABLE ks.my_table (
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true, 'postimage': true};

-- Enable both preimage and postimage
CREATE TABLE ks.my_table (
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};
```

#### Connector Configuration Examples

```properties
# Include full row state before and after changes
cdc.include.before=full
cdc.include.after=full

# Include only modified columns (reduces message size for UPDATE operations)
cdc.include.before=only-updated
cdc.include.after=only-updated

# Mixed mode: full before state, only modified columns after
cdc.include.before=full
cdc.include.after=only-updated
```

#### Use Cases for `only-updated` Mode

The `only-updated` mode is particularly useful when:
- **Reducing message size**: When tables have many columns but updates typically modify only a few, `only-updated` significantly reduces Kafka message sizes.
- **Change-focused consumers**: When downstream consumers only need to know what changed rather than the complete row state.
- **Audit trails**: When combined with `cdc.include.before=full`, you get complete "before" state for auditing while keeping "after" minimal.

#### Validation

The connector validates that the Scylla table's CDC options match the connector configuration:
- If you configure `cdc.include.before=full` or `cdc.include.before=only-updated` but the table does not have preimage enabled, the connector will report a configuration error at startup.
- If you configure `cdc.include.after=full` or `cdc.include.after=only-updated` but the table does not have postimage enabled, the connector will report a configuration error at startup.

#### Primary Key Placement (Advanced Format Only)

The `cdc.include.primary-key.placement` option controls where the primary key (partition key and clustering key) columns appear in the generated Kafka messages. This provides flexibility for different consumption patterns and downstream system requirements.

> **Note:** This option only applies to the advanced output format (`cdc.output.format=advanced`).

##### Available Locations

| Location | Description |
|----------|-------------|
| `kafka-key` | Include PK/CK columns in the Kafka record key. This is essential for proper message partitioning, ordering, and log compaction. |
| `payload-after` | Include PK/CK columns inside the `after` field in the message value. Useful when consumers need complete row data in the after image. |
| `payload-before` | Include PK/CK columns inside the `before` field in the message value. Useful when consumers need complete row data in the before image. |
| `payload-key` | Include PK/CK columns as a top-level `key` object in the message value. The field name can be customized using `cdc.include.primary-key.payload-key-name`. |
| `kafka-headers` | Include PK/CK columns as Kafka message headers. Useful for routing or filtering without parsing the message body. |

##### Configuration Examples

```properties
# Default: PK in Kafka key and both before/after payloads
cdc.include.primary-key.placement=kafka-key,payload-after,payload-before

# Minimal: Only in Kafka key (for partitioning/compaction)
cdc.include.primary-key.placement=kafka-key

# Include PK as separate top-level field named "primaryKey"
cdc.include.primary-key.placement=kafka-key,payload-key
cdc.include.primary-key.payload-key-name=primaryKey

# Include PK in headers for routing without message parsing
cdc.include.primary-key.placement=kafka-key,kafka-headers
```

##### Use Cases

- **Log compaction**: Always include `kafka-key` to ensure proper compaction behavior based on the row's primary key.
- **Simplified consumers**: Use `payload-key` to provide a dedicated key object separate from the before/after images.
- **Header-based routing**: Use `kafka-headers` when downstream systems need to route or filter messages based on PK values without deserializing the message body.
- **Minimal message size**: Remove `payload-after` and `payload-before` if PK columns are only needed in the Kafka key.

### Partition Delete Support

The connector's handling of partition deletes (`DELETE FROM table WHERE pk = ?`) depends on the table schema and Scylla version.

#### Tables with Clustering Keys

For tables that have clustering keys, partition deletes are **not supported** and are ignored by the connector. This is because a partition delete in such tables can affect multiple rows, and Scylla CDC does not provide individual row-level information for these operations.

```sql
-- Table with clustering key - partition deletes are IGNORED
CREATE TABLE ks.with_ck (
    pk int,
    ck int,
    v text,
    PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

DELETE FROM ks.with_ck WHERE pk = 1;  -- This delete is IGNORED by the connector
```

#### Tables without Clustering Keys (Scylla 2026.1.0+)

For tables that have **only a partition key** (no clustering key), partition deletes are effectively single-row deletes. Starting with **Scylla 2026.1.0**, these operations are fully supported:

- The connector emits a `DELETE` event (`op: "d"`) for the operation
- When preimage is enabled on the table and `cdc.include.before` is set to `full` or `only-updated`, the `before` field will be populated with the row state before deletion
- Scylla generates a `PRE_IMAGE` record for these operations, allowing the connector to include the complete row data

```sql
-- Table without clustering key - partition deletes ARE SUPPORTED (Scylla 2026.1.0+)
CREATE TABLE ks.without_ck (
    pk int PRIMARY KEY,
    v text
) WITH cdc = {'enabled': true, 'preimage': true};

DELETE FROM ks.without_ck WHERE pk = 1;  -- This delete IS captured with preimage data
```

**Connector configuration for partition delete preimage:**
```properties
cdc.include.before=full
# or
cdc.include.before=only-updated
```

#### Version Compatibility

| Scylla Version | Table Type | Partition Delete Support |
|----------------|------------|-------------------------|
| All versions | With clustering key | Not supported (ignored) |
| < 2026.1.0 | Without clustering key | Supported, but no preimage data |
| >= 2026.1.0 | Without clustering key | Fully supported with preimage data |

### Configuration for large Scylla clusters
#### Offset (progress) storage
Scylla CDC Source Connector reads the CDC log by quering on [Vnode](https://docs.scylladb.com/architecture/ringarchitecture/) granularity level. It uses Kafka Connect to store current progress (offset) for each Vnode. By default, there are 256 Vnodes per each Scylla node. Kafka Connect stores those offsets in its `connect-offsets` internal topic, but it could grow large in case of big Scylla clusters. You can minimize this topic size, by adjusting the following configuration options on this topic:

1. `segment.bytes` or `segment.ms` - lowering them will make the compaction process trigger more often.
2. `cleanup.policy=delete` and setting `retention.ms` to at least the TTL value of your Scylla CDC table (in milliseconds; Scylla default is 24 hours). Using this configuration, older offsets will be deleted. By setting `retention.ms` to at least the TTL value of your Scylla CDC table, we make sure to delete only those offsets that have already expired in the source Scylla CDC table.

#### `tasks.max` property
By adjusting `tasks.max` property, you can configure how many Kafka Connect worker tasks will be started. By scaling up the number of nodes in your Kafka Connect cluster (and `tasks.max` number), you can achieve higher throughput. In general, the `tasks.max` property should be greater or equal the number of nodes in Kafka Connect cluster, to allow the connector to start on each node. `tasks.max` property should also be greater or equal the number of nodes in your Scylla cluster, especially if those nodes have high shard count (32 or greater) as they have a large number of [CDC Streams](https://docs.scylladb.com/using-scylla/cdc/cdc-streams/).

### Throttling the connector on first start

When the connector starts for the first time, it may need to catch up on a large backlog of historical CDC data. During this catch-up phase the connector can issue rapid-fire queries to Scylla, consuming significant cluster resources. The following configuration parameters allow you to throttle the connector and control the rate at which it reads data.

#### How the connector reads CDC data

The connector reads the CDC log in a series of **time windows**. Each window is a `SELECT` query covering a slice of the CDC log defined by `scylla.query.time.window.size` (default: `30000` ms). After reading one window, the connector moves to the next one. When it reaches data that is too recent (within `scylla.confidence.window.size` of the present), it waits before proceeding. On first start the connector has many windows to read through sequentially, and this is where throttling matters.

#### Configuration parameters

| Property | Default | Description |
| --- | --- | --- |
| `scylla.minimal.wait.for.window.time` | `0` (disabled) | Minimum time in milliseconds between reading consecutive CDC log windows. This is the **primary throttling knob**. Setting it to a positive value introduces a mandatory pause after each window read. |
| `scylla.query.time.window.size` | `30000` | Size of each query window in milliseconds. Smaller windows mean less data per query. |
| `scylla.query.options.fetch.size` | `0` (driver default, typically `5000`) | Number of rows fetched per CQL page within each query. A smaller value reduces per-query memory usage but increases the number of network round trips. |
| `poll.interval.ms` | `500` | How often (in milliseconds) Kafka Connect polls the connector's internal queue for records. Higher values reduce how frequently batches are sent to Kafka. |
| `max.batch.size` | `2048` | Maximum number of records returned per poll cycle. Smaller values cap the throughput on the Kafka side. |
| `max.queue.size` | `8192` | Maximum size of the internal in-memory queue between the CDC reader and Kafka Connect. When the queue is full, the CDC reader blocks, creating backpressure toward Scylla. |

#### Calculating throttle values

To estimate the right values, start from the **target throughput** you want to allow and work backwards.

**Step 1: Decide your target read rate.**

For example, you want the connector to read at most **R = 1 window per second** during catch-up.

**Step 2: Set `scylla.minimal.wait.for.window.time`.**

This parameter directly controls the pause between windows. If each window read takes approximately `T_read` milliseconds, set:

```
scylla.minimal.wait.for.window.time = (1000 / R) - T_read
```

For example, if each window read takes ~200 ms and you want 1 window/s:

```
scylla.minimal.wait.for.window.time = 1000 - 200 = 800
```

If you are unsure how long a window read takes, start with `scylla.minimal.wait.for.window.time = 1000` and adjust down from there.

**Step 3: Adjust the window size if needed.**

Each window covers `scylla.query.time.window.size` milliseconds of CDC log data. The default of 30 seconds is reasonable for most workloads. If your tables have a very high write rate and each window returns a large amount of data, reduce the window size:

```
scylla.query.time.window.size = 10000
```

This makes each query smaller, which reduces per-query load on Scylla. The trade-off is that the connector needs more windows to cover the same time span.

**Step 4: Limit the output side.**

To prevent bursts on the Kafka side, reduce batch and queue sizes:

```
poll.interval.ms = 1000
max.batch.size = 512
max.queue.size = 2048
```

When `max.queue.size` is small, the internal queue fills up faster and the CDC reader blocks until Kafka Connect drains it. This creates natural backpressure.

**Step 5: Reduce CQL page size for memory-constrained environments.**

If Scylla nodes are under memory pressure, limit how many rows the driver fetches per network round trip:

```
scylla.query.options.fetch.size = 1000
```

#### Example: gentle first-start configuration

The following configuration keeps the connector well-behaved during initial catch-up:

```properties
# Pause 1 second between each window read
scylla.minimal.wait.for.window.time=1000

# Read 10-second windows instead of 30-second
scylla.query.time.window.size=10000

# Fetch 1000 rows per CQL page
scylla.query.options.fetch.size=1000

# Slow down the Kafka output side
poll.interval.ms=1000
max.batch.size=512
max.queue.size=2048
```

Once the connector has caught up with the real-time CDC stream, these throttles remain in effect but have minimal impact since the connector naturally waits for new data to appear. You can relax the values after catch-up by updating the connector configuration.
