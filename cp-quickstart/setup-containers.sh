#!/bin/bash
set -e

# Colors for better output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Building Scylla CDC Source Connector...${NC}"
# Navigate to the project root and build the connector
cd "$(dirname "$0")/.."
mvn clean package -DskipTests


# Compose function for compatibility and clarity
compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
  else
    echo "Neither 'docker compose' nor 'docker-compose' is available. Please install one of them."
    exit 1
  fi
}


echo -e "${YELLOW}Starting containers with docker compose...${NC}"
# Navigate back to cp-quickstart directory
cd cp-quickstart
compose down -v
compose up -d --force-recreate

echo -e "${YELLOW}Waiting for Kafka Connect REST API to be ready...${NC}"
READY_CODE=""
for i in {1..60}; do
  READY_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/ || true)
  if [ "$READY_CODE" = "200" ]; then
    echo -e "${GREEN}Kafka Connect REST API is ready.${NC}"
    break
  fi
  echo "Waiting for Connect... ($i/60)"
  sleep 2
done
if [ "$READY_CODE" != "200" ]; then
  echo "Kafka Connect REST API not ready after waiting, continuing anyway."
fi

echo -e "${YELLOW}Verifying loaded Scylla connector JARs in Connect...${NC}"
docker exec connect bash -lc 'echo "Loaded Scylla JARs:"; find /usr/share -type f -name "*scylla*jar" -printf "%TY-%Tm-%Td %TT %p\n" | sort || true' || true

echo -e "${YELLOW}Checking Kafka Connect plugin discovery logs...${NC}"
docker logs connect 2>&1 | grep -i -E "Scanning plugin path|Added plugin|ScyllaConnector|scylla-cdc" || true

echo -e "${YELLOW}Waiting for Scylla to be ready...${NC}"
# Wait for Scylla to be ready
for i in {1..30}; do
  if docker exec scylla cqlsh -e "describe keyspaces" > /dev/null 2>&1; then
    echo -e "${GREEN}Scylla is ready!${NC}"
    break
  fi
  echo "Waiting for Scylla to start... ($i/30)"
  sleep 5
  if [ $i -eq 30 ]; then
    echo "Timed out waiting for Scylla to start"
    exit 1
  fi
done

echo -e "${YELLOW}Creating keyspace and table with CDC enabled...${NC}"
# Create keyspace and table with CDC
docker exec scylla cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS demo_keyspace
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS demo_keyspace.users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    email TEXT,
    age INT,
    registration_date TIMESTAMP
) WITH cdc = {'enabled': true};

INSERT INTO demo_keyspace.users (user_id, username, email, age, registration_date)
VALUES (uuid(), 'john_doe', 'john@example.com', 28, toTimestamp(now()));

INSERT INTO demo_keyspace.users (user_id, username, email, age, registration_date)
VALUES (uuid(), 'jane_smith', 'jane@example.com', 32, toTimestamp(now()));

INSERT INTO demo_keyspace.users (user_id, username, email, age, registration_date)
VALUES (uuid(), 'bob_johnson', 'bob@example.com', 45, toTimestamp(now()));

INSERT INTO demo_keyspace.users (user_id, username, email, age, registration_date)
VALUES (uuid(), 'alice_williams', 'alice@example.com', 24, toTimestamp(now()));

DESCRIBE demo_keyspace.users;
SELECT * FROM demo_keyspace.users;"

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${YELLOW}CDC-enabled table created with sample data in Scylla${NC}"
echo -e "${YELLOW}You can now configure the Scylla CDC Source Connector to consume changes from this table${NC}"
echo -e "Connect to Scylla: ${GREEN}docker exec -it scylla cqlsh${NC}"
echo -e "Kafka Connect available at : ${GREEN}http://localhost:8083${NC}"
