#!/usr/bin/env bash

# check-ports.sh - Intelligent port availability checker with automatic alternative port suggestion
# Usage: ./check-ports.sh [--fix]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Ports to check (from .env.template defaults)
DAGSTER_WEBSERVER_PORT=3000
DAGSTER_GRPC_PORT=3001
DAGSTER_POSTGRES_PORT=5432
SOURCE_POSTGRES_PORT=5433
WAREHOUSE_POSTGRES_PORT=5434

# Flag for fixing mode
FIX_MODE=false

# Parse arguments
if [[ "$1" == "--fix" ]]; then
    FIX_MODE=true
fi

echo -e "${BLUE}=== Dagster Port Availability Checker ===${NC}\n"

# Function to check if a port is in use
check_port() {
    local port=$1
    local service_name=$2
    
    # Try multiple methods for cross-platform compatibility
    
    # Method 1: lsof (best for macOS)
    if command -v lsof &> /dev/null; then
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
            return 1  # Port in use
        fi
    # Method 2: netstat (fallback)
    elif command -v netstat &> /dev/null; then
        if netstat -an 2>/dev/null | grep -q "LISTEN.*[.:]$port "; then
            return 1  # Port in use
        fi
    # Method 3: nc (netcat)
    elif command -v nc &> /dev/null; then
        if nc -z localhost $port 2>/dev/null; then
            return 1  # Port in use
        fi
    else
        echo -e "${YELLOW}⚠ Warning: No port checking tool available (lsof, netstat, nc)${NC}"
        return 0  # Assume port is free
    fi
    
    return 0  # Port is free
}

# Function to find next available port
find_available_port() {
    local start_port=$1
    local max_attempts=100
    
    for ((i=0; i<max_attempts; i++)); do
        local test_port=$((start_port + i))
        if check_port $test_port "temp"; then
            echo $test_port
            return 0
        fi
    done
    
    echo -e "${RED}✗ Could not find available port starting from $start_port${NC}" >&2
    return 1
}

# Variables to store port values (instead of associative array)
NEW_WEBSERVER_PORT=$DAGSTER_WEBSERVER_PORT
NEW_GRPC_PORT=$DAGSTER_GRPC_PORT
NEW_DAGSTER_PG_PORT=$DAGSTER_POSTGRES_PORT
NEW_SOURCE_PG_PORT=$SOURCE_POSTGRES_PORT
NEW_WAREHOUSE_PG_PORT=$WAREHOUSE_POSTGRES_PORT

# Counter for conflicts
CONFLICTS=0

# Check each port
echo "Checking required ports..."
echo

# Dagster Webserver
if check_port $DAGSTER_WEBSERVER_PORT "Dagster Webserver"; then
    echo -e "${GREEN}✓${NC} Port $DAGSTER_WEBSERVER_PORT (Dagster Webserver) is available"
else
    echo -e "${RED}✗${NC} Port $DAGSTER_WEBSERVER_PORT (Dagster Webserver) is in use"
    CONFLICTS=$((CONFLICTS + 1))
    
    if [[ "$FIX_MODE" == true ]]; then
        NEW_WEBSERVER_PORT=$(find_available_port $((DAGSTER_WEBSERVER_PORT + 1)))
        echo -e "  ${BLUE}→${NC} Suggesting alternative: $NEW_WEBSERVER_PORT"
    fi
fi

# Dagster gRPC
if check_port $DAGSTER_GRPC_PORT "Dagster gRPC"; then
    echo -e "${GREEN}✓${NC} Port $DAGSTER_GRPC_PORT (Dagster gRPC) is available"
else
    echo -e "${RED}✗${NC} Port $DAGSTER_GRPC_PORT (Dagster gRPC) is in use"
    CONFLICTS=$((CONFLICTS + 1))
    
    if [[ "$FIX_MODE" == true ]]; then
        NEW_GRPC_PORT=$(find_available_port $((DAGSTER_GRPC_PORT + 1)))
        echo -e "  ${BLUE}→${NC} Suggesting alternative: $NEW_GRPC_PORT"
    fi
fi

# Dagster PostgreSQL
if check_port $DAGSTER_POSTGRES_PORT "Dagster PostgreSQL"; then
    echo -e "${GREEN}✓${NC} Port $DAGSTER_POSTGRES_PORT (Dagster PostgreSQL) is available"
else
    echo -e "${RED}✗${NC} Port $DAGSTER_POSTGRES_PORT (Dagster PostgreSQL) is in use"
    CONFLICTS=$((CONFLICTS + 1))
    
    if [[ "$FIX_MODE" == true ]]; then
        NEW_DAGSTER_PG_PORT=$(find_available_port $((DAGSTER_POSTGRES_PORT + 1)))
        echo -e "  ${BLUE}→${NC} Suggesting alternative: $NEW_DAGSTER_PG_PORT"
    fi
fi

# Source PostgreSQL
if check_port $SOURCE_POSTGRES_PORT "Source PostgreSQL"; then
    echo -e "${GREEN}✓${NC} Port $SOURCE_POSTGRES_PORT (Source PostgreSQL) is available"
else
    echo -e "${RED}✗${NC} Port $SOURCE_POSTGRES_PORT (Source PostgreSQL) is in use"
    CONFLICTS=$((CONFLICTS + 1))
    
    if [[ "$FIX_MODE" == true ]]; then
        NEW_SOURCE_PG_PORT=$(find_available_port $((SOURCE_POSTGRES_PORT + 1)))
        echo -e "  ${BLUE}→${NC} Suggesting alternative: $NEW_SOURCE_PG_PORT"
    fi
fi

# Warehouse PostgreSQL
if check_port $WAREHOUSE_POSTGRES_PORT "Warehouse PostgreSQL"; then
    echo -e "${GREEN}✓${NC} Port $WAREHOUSE_POSTGRES_PORT (Warehouse PostgreSQL) is available"
else
    echo -e "${RED}✗${NC} Port $WAREHOUSE_POSTGRES_PORT (Warehouse PostgreSQL) is in use"
    CONFLICTS=$((CONFLICTS + 1))
    
    if [[ "$FIX_MODE" == true ]]; then
        NEW_WAREHOUSE_PG_PORT=$(find_available_port $((WAREHOUSE_POSTGRES_PORT + 1)))
        echo -e "  ${BLUE}→${NC} Suggesting alternative: $NEW_WAREHOUSE_PG_PORT"
    fi
fi

echo
echo "================================"

# Summary
if [ $CONFLICTS -eq 0 ]; then
    echo -e "${GREEN}✓ All required ports are available${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠ Found $CONFLICTS port conflict(s)${NC}"
    
    if [[ "$FIX_MODE" == true ]]; then
        echo
        echo "Updating configuration files with alternative ports..."
        
        # Call update-ports.sh with new port mappings
        "$SCRIPT_DIR/update-ports.sh" \
            "$NEW_WEBSERVER_PORT" \
            "$NEW_GRPC_PORT" \
            "$NEW_DAGSTER_PG_PORT" \
            "$NEW_SOURCE_PG_PORT" \
            "$NEW_WAREHOUSE_PG_PORT"
        
        echo -e "${GREEN}✓ Configuration files updated with alternative ports${NC}"
        exit 0
    else
        echo
        echo "Run with --fix flag to automatically update configuration with alternative ports:"
        echo -e "${BLUE}  ./scripts/check-ports.sh --fix${NC}"
        echo
        echo "Or manually resolve conflicts and re-run the check."
        exit 1
    fi
fi
