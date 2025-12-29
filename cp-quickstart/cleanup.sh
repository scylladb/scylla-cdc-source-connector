#!/bin/bash
set -e

# Colors for better output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Cleaning up Scylla CDC Source Connector environment...${NC}"


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

# Navigate to the cp-quickstart directory
cd "$(dirname "$0")"


# Stop and remove all containers
echo -e "${YELLOW}Stopping and removing all containers...${NC}"
compose down -v

echo -e "${GREEN}Cleanup complete!${NC}"
echo -e "${YELLOW}All containers and networks have been removed.${NC}"
