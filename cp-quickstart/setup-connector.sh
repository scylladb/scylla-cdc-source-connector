#!/bin/bash
set -e

# Colors for better output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up Scylla CDC Source Connector...${NC}"

# Check if Kafka Connect is running
echo -e "${YELLOW}Checking if Kafka Connect is running...${NC}"
if ! curl -s http://localhost:8083/ > /dev/null; then
  echo "Kafka Connect is not running. Please make sure the containers are started using setup-containers.sh first."
  exit 1
fi

echo -e "${YELLOW}Waiting for Kafka Connect REST API to be ready...${NC}"
READY_CODE=""
for i in {1..60}; do
  READY_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connector-plugins || true)
  if [ "$READY_CODE" = "200" ]; then
    echo -e "${GREEN}Kafka Connect REST API is ready.${NC}"
    break
  fi
  echo "Waiting for Connect... ($i/60)"
  sleep 2
done
if [ "$READY_CODE" != "200" ]; then
  echo "Kafka Connect REST API not ready after waiting."
  exit 1
fi

# Check if Scylla is running
echo -e "${YELLOW}Checking if Scylla is running...${NC}"
if ! docker exec scylla cqlsh -e "describe keyspaces" > /dev/null 2>&1; then
  echo "Scylla is not running. Please make sure the containers are started using setup-containers.sh first."
  exit 1
fi

echo -e "${YELLOW}Creating connector configuration...${NC}"

echo -e "${YELLOW}Verifying ScyllaConnector plugin is available...${NC}"
if ! curl -s http://localhost:8083/connector-plugins | grep -q 'com.scylladb.cdc.debezium.connector.ScyllaConnector'; then
  echo -e "${YELLOW}ScyllaConnector plugin not listed by Connect.${NC}"
  echo "Troubleshooting tips:"
  echo "- Ensure mvn build completed and jars exist under target/components/packages"
  echo "- Check Connect logs for plugin scanning"
  exit 1
fi

CONNECTOR_NAME="scylla-cdc-source-connector"
CONNECTOR_CONFIG=$(cat <<'JSON'
{
  "name": "scylla-cdc-source-connector",
  "config": {
    "tasks.max": "3",
    "connector.class": "com.scylladb.cdc.debezium.connector.ScyllaConnector",
    "scylla.cluster.ip.addresses": "scylla:9042",
    "topic.prefix": "scylla_cluster",
    "scylla.table.names": "demo_keyspace.users",
    "scylla.consistency.level": "ONE",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "experimental.preimages.enabled": "false"
  }
}
JSON
)

# For PUT /connectors/{name}/config, Kafka Connect expects only the flat config object,
# not the wrapper with "name" and "config".
CONNECTOR_CONFIG_ONLY=$(cat <<'JSON'
{
  "tasks.max": "3",
  "connector.class": "com.scylladb.cdc.debezium.connector.ScyllaConnector",
  "scylla.cluster.ip.addresses": "scylla:9042",
  "topic.prefix": "scylla_cluster",
  "scylla.table.names": "demo_keyspace.users",
  "scylla.consistency.level": "ONE",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false",
  "experimental.preimages.enabled": "false"
}
JSON
)

echo -e "${YELLOW}Registering (or updating) Scylla CDC Source Connector...${NC}"
GET_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:8083/connectors/${CONNECTOR_NAME}" || true)
if [ "$GET_STATUS" = "200" ]; then
  curl -s -X PUT "http://localhost:8083/connectors/${CONNECTOR_NAME}/config" \
       -H "Content-Type: application/json" \
       -d "${CONNECTOR_CONFIG_ONLY}"
  echo -e "${YELLOW}Restarting existing connector to load latest classes...${NC}"
  curl -X POST http://localhost:8083/connectors/${CONNECTOR_NAME}/restart
else
  curl -s -X POST "http://localhost:8083/connectors" \
       -H "Content-Type: application/json" \
       -d "${CONNECTOR_CONFIG}"
  echo -e "${YELLOW}Connector created; restart is not required on first creation.${NC}"
fi
echo -e "${GREEN}Connector setup complete!${NC}"
echo -e "${YELLOW}You can check the connector status at: ${GREEN}http://localhost:8083/connectors/scylla-cdc-source-connector/status${NC}"
echo -e "${YELLOW}CDC changes will be published to Kafka topics with names like: ${GREEN}scylla_cluster.demo_keyspace.users${NC}"
echo -e "${YELLOW}To consume messages from the topic, you can run:${NC}"
echo -e "${GREEN}docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic scylla_cluster.demo_keyspace.users --from-beginning${NC}"
echo -e "${YELLOW}Note that with default configuration it may take 30 seconds for the first changes to be sent by the connector:${NC}"
