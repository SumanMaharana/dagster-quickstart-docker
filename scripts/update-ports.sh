#!/bin/bash

# update-ports.sh - Update configuration files with new port assignments
# Usage: ./update-ports.sh <webserver_port> <grpc_port> <dagster_pg_port> <source_pg_port> <warehouse_pg_port>

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse arguments
if [ $# -ne 5 ]; then
    echo "Usage: $0 <webserver_port> <grpc_port> <dagster_pg_port> <source_pg_port> <warehouse_pg_port>"
    exit 1
fi

WEBSERVER_PORT=$1
GRPC_PORT=$2
DAGSTER_PG_PORT=$3
SOURCE_PG_PORT=$4
WAREHOUSE_PG_PORT=$5

echo -e "${BLUE}=== Updating Configuration with New Ports ===${NC}\n"

# Create or update .env file
ENV_FILE="$PROJECT_ROOT/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Creating new .env file from template..."
    cp "$PROJECT_ROOT/.env.template" "$ENV_FILE"
fi

echo "Updating $ENV_FILE..."

# Update ports in .env file using sed
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/^DAGSTER_WEBSERVER_PORT=.*/DAGSTER_WEBSERVER_PORT=$WEBSERVER_PORT/" "$ENV_FILE"
    sed -i '' "s/^DAGSTER_GRPC_PORT=.*/DAGSTER_GRPC_PORT=$GRPC_PORT/" "$ENV_FILE"
    sed -i '' "s/^DAGSTER_POSTGRES_PORT=.*/DAGSTER_POSTGRES_PORT=$DAGSTER_PG_PORT/" "$ENV_FILE"
    sed -i '' "s/^SOURCE_POSTGRES_PORT=.*/SOURCE_POSTGRES_PORT=$SOURCE_PG_PORT/" "$ENV_FILE"
    sed -i '' "s/^WAREHOUSE_POSTGRES_PORT=.*/WAREHOUSE_POSTGRES_PORT=$WAREHOUSE_PG_PORT/" "$ENV_FILE"
else
    # Linux
    sed -i "s/^DAGSTER_WEBSERVER_PORT=.*/DAGSTER_WEBSERVER_PORT=$WEBSERVER_PORT/" "$ENV_FILE"
    sed -i "s/^DAGSTER_GRPC_PORT=.*/DAGSTER_GRPC_PORT=$GRPC_PORT/" "$ENV_FILE"
    sed -i "s/^DAGSTER_POSTGRES_PORT=.*/DAGSTER_POSTGRES_PORT=$DAGSTER_PG_PORT/" "$ENV_FILE"
    sed -i "s/^SOURCE_POSTGRES_PORT=.*/SOURCE_POSTGRES_PORT=$SOURCE_PG_PORT/" "$ENV_FILE"
    sed -i "s/^WAREHOUSE_POSTGRES_PORT=.*/WAREHOUSE_POSTGRES_PORT=$WAREHOUSE_PG_PORT/" "$ENV_FILE"
fi

echo -e "${GREEN}✓${NC} Updated .env file:"
echo "  - Dagster Webserver Port: $WEBSERVER_PORT"
echo "  - Dagster gRPC Port: $GRPC_PORT"
echo "  - Dagster PostgreSQL Port: $DAGSTER_PG_PORT"
echo "  - Source PostgreSQL Port: $SOURCE_PG_PORT"
echo "  - Warehouse PostgreSQL Port: $WAREHOUSE_PG_PORT"

echo
echo -e "${GREEN}✓ Port configuration updated successfully${NC}"
echo -e "${YELLOW}Note: Changes will take effect when you run docker-compose up${NC}"
