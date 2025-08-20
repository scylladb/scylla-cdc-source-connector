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

# Check if Scylla is running
echo -e "${YELLOW}Checking if Scylla is running...${NC}"
if ! docker exec -it scylla cqlsh -e "describe keyspaces" > /dev/null 2>&1; then
  echo "Scylla is not running. Please make sure the containers are started using setup-containers.sh first."
  exit 1
fi

echo -e "${YELLOW}Creating connector configuration...${NC}"

# Create the connector
echo -e "${YELLOW}Registering Scylla CDC Source Connector with Kafka Connect...${NC}"
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
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
    "value.converter.schemas.enable": "false"
  }
}'

echo -e "${GREEN}Connector setup complete!${NC}"
echo -e "${YELLOW}You can check the connector status at: ${GREEN}http://localhost:8083/connectors/scylla-cdc-source-connector/status${NC}"
echo -e "${YELLOW}CDC changes will be published to Kafka topics with names like: ${GREEN}scylla-cluster.demo_keyspace.users${NC}"
echo -e "${YELLOW}To consume messages from the topic, you can run:${NC}"
echo -e "${GREEN}docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic scylla-cluster.demo_keyspace.users --from-beginning${NC}"
echo -e "${YELLOW}Note that with default configuration it may take 30 seconds for the first changes to be sent by the connector:${NC}"
