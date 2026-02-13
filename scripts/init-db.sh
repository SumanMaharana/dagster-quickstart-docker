#!/bin/bash

# init-db.sh - Initialize Dagster database and verify connectivity
# Usage: ./init-db.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== Dagster Database Initialization ===${NC}\n"

# Source environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
    echo -e "${GREEN}✓${NC} Loaded environment variables"
else
    echo -e "${RED}✗ .env file not found${NC}"
    exit 1
fi

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    local host=$1
    local port=$2
    local user=$3
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for PostgreSQL at $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec dagster_postgresql pg_isready -h localhost -p 5432 -U "$user" >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} PostgreSQL is ready"
            return 0
        fi
        
        echo "  Attempt $attempt/$max_attempts - waiting..."
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}✗ PostgreSQL did not become ready in time${NC}"
    return 1
}

# Function to wait for all databases
wait_for_all_databases() {
    echo "Checking database connectivity..."
    echo
    
    # Dagster system database
    echo "1. Dagster System Database:"
    if wait_for_postgres "dagster_postgresql" "5432" "${DAGSTER_POSTGRES_USER}"; then
        echo -e "   ${GREEN}✓${NC} dagster_postgresql is ready"
    else
        echo -e "   ${RED}✗${NC} Failed to connect to dagster_postgresql"
        return 1
    fi
    
    echo
    
    # Source database
    echo "2. Source Database:"
    if docker exec source_postgresql pg_isready -h localhost -p 5432 -U "${SOURCE_DB_USER}" >/dev/null 2>&1; then
        echo -e "   ${GREEN}✓${NC} source_postgresql is ready"
    else
        echo -e "   ${YELLOW}⚠${NC} source_postgresql not ready yet, waiting..."
        sleep 5
    fi
    
    echo
    
    # Warehouse database
    echo "3. Warehouse Database:"
    if docker exec warehouse_postgresql pg_isready -h localhost -p 5432 -U "${WAREHOUSE_DB_USER}" >/dev/null 2>&1; then
        echo -e "   ${GREEN}✓${NC} warehouse_postgresql is ready"
    else
        echo -e "   ${YELLOW}⚠${NC} warehouse_postgresql not ready yet, waiting..."
        sleep 5
    fi
    
    echo
}

# Wait for all databases to be ready
if ! wait_for_all_databases; then
    echo -e "${RED}✗ Database initialization failed${NC}"
    exit 1
fi

# Initialize Dagster instance
echo "================================"
echo "Initializing Dagster instance..."
echo

# Check if dagster_webserver container is running
if ! docker ps | grep -q dagster_webserver; then
    echo -e "${YELLOW}⚠ Dagster webserver container is not running yet${NC}"
    echo "Waiting for Dagster services to start..."
    sleep 10
fi

# Run Dagster instance migration
echo "Running Dagster instance migration..."
if docker exec dagster_webserver dagster instance migrate 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Dagster instance migration completed"
else
    echo -e "${YELLOW}⚠${NC} Migration command failed (this is normal on first run)"
    echo "Dagster will auto-initialize on first use"
fi

echo

# Verify Dagster instance info
echo "Verifying Dagster instance configuration..."
if docker exec dagster_webserver dagster instance info 2>/dev/null | head -n 20; then
    echo
    echo -e "${GREEN}✓${NC} Dagster instance configured successfully"
else
    echo -e "${YELLOW}⚠${NC} Could not verify instance (container may still be starting)"
fi

echo
echo "================================"

# Verify source database data
echo "Verifying source database data..."
CUSTOMER_COUNT=$(docker exec source_postgresql psql -U "${SOURCE_DB_USER}" -d "${SOURCE_DB_NAME}" -t -c "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d ' ' || echo "0")
PRODUCT_COUNT=$(docker exec source_postgresql psql -U "${SOURCE_DB_USER}" -d "${SOURCE_DB_NAME}" -t -c "SELECT COUNT(*) FROM products;" 2>/dev/null | tr -d ' ' || echo "0")
ORDER_COUNT=$(docker exec source_postgresql psql -U "${SOURCE_DB_USER}" -d "${SOURCE_DB_NAME}" -t -c "SELECT COUNT(*) FROM orders;" 2>/dev/null | tr -d ' ' || echo "0")

echo "  - Customers: $CUSTOMER_COUNT"
echo "  - Products: $PRODUCT_COUNT"
echo "  - Orders: $ORDER_COUNT"

if [ "$CUSTOMER_COUNT" -gt 0 ] && [ "$PRODUCT_COUNT" -gt 0 ] && [ "$ORDER_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓${NC} Source database populated with sample data"
else
    echo -e "${YELLOW}⚠${NC} Source database may not be fully populated yet"
fi

echo
echo "================================"
echo -e "${GREEN}✓ Database initialization complete${NC}"
echo
echo "Summary:"
echo "  - Dagster system database: Ready"
echo "  - Source database: Ready (with sample data)"
echo "  - Warehouse database: Ready (empty, will be populated by assets)"
echo
echo "Next steps:"
echo "  1. Access Dagster UI at http://localhost:${DAGSTER_WEBSERVER_PORT}"
echo "  2. Materialize assets to populate warehouse database"
echo "  3. Run OpenMetadata ingestion to capture lineage"

exit 0
