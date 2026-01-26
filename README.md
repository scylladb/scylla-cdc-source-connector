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

The connector has the following limitations:
- Only Kafka 2.6.0+ is supported
- Only row-level operations are produced (`INSERT`, `UPDATE`, `DELETE`):
    - Row range deletes - those changes are ignored
    - Partition deletes - see [Partition Delete Support](#partition-delete-support) section below
- No support for collection types (`LIST`, `SET`, `MAP`) and `UDT` - columns with those types are omitted from generated messages
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

#### Cell representation
Operations in Scylla, such as `INSERT` or `UPDATE`, do not have to modify all columns of a row. To differentiate between non-modification of column and inserting/updating `NULL`, all non-primary-key columns are wrapped with structure containing a single `value` field. For example, given this Scylla table and `UPDATE` operation:

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

See `UPDATE` example for full  data change event's value.

#### Single Message Transformations (SMTs)
The connector provides two single message transformations (SMTs): `ScyllaExtractNewRecordState` (class: `com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState`) and `ScyllaFlattenColumns` (`com.scylladb.cdc.debezium.connector.transforms.ScyllaFlattenColumns`).

##### `ScyllaExtractNewRecordState`
`ScyllaExtractNewRecordState` works like exactly like `io.debezium.transforms.ExtractNewRecordState` (in fact it is called underneath), but also flattens structure by extracting values from the aforementioned single-field structures.
Such transformation makes message structure simpler (and easier to use with e.g. Elasticsearch), but it makes it impossible to differentiate between NULL value and non-modification.
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

##### `ScyllaFlattenColumns`
`ScyllaFlattenColumns` flattens columns that are wrapped in `value` structure, such as:
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

### `INSERT` example
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

### `UPDATE` example
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

### `DELETE` example
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

## Advanced administration

### Advanced configuration parameters

In addition to the configuration parameters described in the ["Configuration"](#configuration) section, Scylla CDC Source Connector exposes the following (non-required) configuration parameters:

| Property                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `scylla.query.time.window.size`  | The size of windows queried by the connector. Changes are queried using `SELECT` statements with time restriction with width defined by this parameter. Value expressed in milliseconds.                                                                                                                                                                                                                                                                                                                              |
| `scylla.confidence.window.size`  | The size of the confidence window. It is necessary for the connector to avoid reading too fresh data from the CDC log due to the eventual consistency of Scylla. The problem could appear when a newer write reaches a replica before some older write. For a short period of time, when reading, it is possible for the replica to return only the newer write. The connector mitigates this problem by not reading a window of most recent changes (controlled by this parameter). Value expressed in milliseconds. |
| `scylla.consistency.level`       | The consistency level of CDC table read queries. This consistency level is used only for read queries to the CDC log table. By default, `QUORUM` level is used.                                                                                                                                                                                                                                                                                                                                                       |
| `scylla.local.dc`                | The name of Scylla local datacenter. This local datacenter name will be used to setup the connection to Scylla to prioritize sending requests to the nodes in the local datacenter. If not set, no particular datacenter will be prioritized.                                                                                                                                                                                                                                                                         |


### Preimage/Postimage Configuration

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

#### Primary Key Placement

The `cdc.include.primary-key.placement` option controls where the primary key (partition key and clustering key) columns appear in the generated Kafka messages. This provides flexibility for different consumption patterns and downstream system requirements.

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
