# Confluent Kafka Quickstart for Scylla CDC Source Connector

This guide will walk you through setting up and using the Scylla CDC Source Connector with Kafka Connect using the provided scripts.

## Prerequisites

- Clone this repository
- Have Java 11 or later and Maven installed
- Make sure you fulfill the requirements listed at https://docs.confluent.io/platform/current/get-started/platform-quickstart.html

## Setup Process

### 1. Clone the repository

```bash
git clone https://github.com/scylladb/scylla-cdc-source-connector.git
cd scylla-cdc-source-connector
```

### 2. Build connector and setup the environment

If you don't want to be bothered with the details simply run:

```bash
chmod +x cp-quickstart/setup-containers.sh
./cp-quickstart/setup-containers.sh
```

This script will first build the connector using `mvn clean package` so make sure that you are on the correct version branch.
Afterwards it will start the necessary Docker containers. All of them are described in `docker-compose.yml`.
When all the containers are up and running, it will create a `demo_keyspace.users` table with CDC enabled and insert 4 rows into it.

Once the script completes wait a few seconds and the environment to run the connector will be ready.

### 3. Set up the connector

Run the `setup-connector.sh` script to register the Scylla CDC Source Connector with Kafka Connect:

```bash
chmod +x cp-quickstart/setup-connector.sh
./cp-quickstart/setup-connector.sh
```

This will launch the connector configured to monitor changes to the `demo_keyspace.users` table and publish them to a Kafka topic.
If you wish to adjust the configuration you can simply edit the configuration inside `setup-connector.sh`.
Default configuration assumes the same setup as in `setup-containers.sh` script.

### 4. Test the connector

After setting up the connector, you can test it by making changes to the Scylla database and observing the messages in the Kafka topic.

Connect to Scylla and make a change:

```bash
docker exec -it scylla cqlsh
```

In the cqlsh prompt:

```sql
INSERT INTO demo_keyspace.users (user_id, username, email, age, registration_date)
VALUES (uuid(), 'new_user', 'new_user@example.com', 33, toTimestamp(now()));

INSERT INTO demo_keyspace.users (user_id, username, email, age, registration_date)
VALUES (dba11045-4dc3-48c2-b607-04a123d100e0, 'another_user', 'another_user@example.com', 33, toTimestamp(now()));

UPDATE demo_keyspace.users SET age = 29 WHERE user_id = dba11045-4dc3-48c2-b607-04a123d100e0;

DELETE FROM demo_keyspace.users WHERE user_id = dba11045-4dc3-48c2-b607-04a123d100e0;
```

Wait a bit and view the messages in the Kafka topic:

```bash
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic scylla-cluster.demo_keyspace.users --from-beginning
```

You should see JSON messages representing the rows from `users_scylla_cdc_log` table.
First ones should look similar to this:
```json
{"source":{"version":"1.2.6-SNAPSHOT","connector":"scylla","name":"scylla-cluster","ts_ms":1755710756661,"snapshot":"false","db":"demo_keyspace","sequence":null,"ts_us":1755710756661369,"ts_ns":1755710756661000000,"keyspace_name":"demo_keyspace","table_name":"users"},"before":null,"after":{"age":{"value":24},"email":{"value":"alice@example.com"},"registration_date":{"value":1755710756661},"user_id":"89e95901-443a-4c92-a4d5-248aab1223c1","username":{"value":"alice_williams"}},"op":"c","ts_ms":1755710810004,"transaction":null,"ts_us":1755710810004787,"ts_ns":1755710810004787000}
{"source":{"version":"1.2.6-SNAPSHOT","connector":"scylla","name":"scylla-cluster","ts_ms":1755710756656,"snapshot":"false","db":"demo_keyspace","sequence":null,"ts_us":1755710756656914,"ts_ns":1755710756656000000,"keyspace_name":"demo_keyspace","table_name":"users"},"before":null,"after":{"age":{"value":28},"email":{"value":"john@example.com"},"registration_date":{"value":1755710756657},"user_id":"dba11045-4dc3-48c2-b607-04a123d100e0","username":{"value":"john_doe"}},"op":"c","ts_ms":1755710900858,"transaction":null,"ts_us":1755710900858696,"ts_ns":1755710900858696000}
```

### 5. Cleanup

When you're done, clean up the environment using the `cleanup.sh` script:

```bash
chmod +x cp-quickstart/cleanup.sh
./cp-quickstart/cleanup.sh
```

This will stop and remove all the Docker containers.

## Troubleshooting

### Check connector status

```bash
curl -s http://localhost:8083/connectors/scylla-cdc-source-connector/status | jq
```

### View Kafka Connect logs

```bash
docker logs connect
```

### Restart the connector

```bash
curl -X POST http://localhost:8083/connectors/scylla-cdc-source-connector/restart
```
