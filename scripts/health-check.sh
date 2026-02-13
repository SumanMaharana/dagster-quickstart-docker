#!/bin/bash

# health-check.sh - Poll services until healthy or timeout
# Usage: ./health-check.sh [--timeout SECONDS]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Default timeout (seconds)
TIMEOUT=180
START_TIME=$(date +%s)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--timeout SECONDS]"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}=== Dagster Health Check ===${NC}"
echo "Timeout: ${TIMEOUT}s"
echo

# Source environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Function to check elapsed time
check_timeout() {
    local current_time=$(date +%s)
    local elapsed=$((current_time - START_TIME))
    
    if [ $elapsed -ge $TIMEOUT ]; then
        echo -e "\n${RED}✗ Timeout reached (${TIMEOUT}s)${NC}"
        return 1
    fi
    
    return 0
}

# Function to check container health
check_container_health() {
    local container=$1
    local status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
    
    if [ "$status" = "healthy" ]; then
        return 0
    fi
    return 1
}

# Function to check container running
check_container_running() {
    local container=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        return 0
    fi
    return 1
}

# Check PostgreSQL databases
echo "Checking PostgreSQL databases..."
POSTGRES_CONTAINERS=("source_postgresql")

for container in "${POSTGRES_CONTAINERS[@]}"; do
    echo -n "  - $container: "
    while ! check_container_health "$container"; do
        if ! check_timeout; then exit 1; fi
        echo -n "."
        sleep 2
    done
    echo -e " ${GREEN}✓ healthy${NC}"
done

echo

# Check Dagster user code
echo "Checking Dagster user code..."
echo -n "  - dagster_user_code: "
while ! check_container_running "dagster_user_code"; do
    if ! check_timeout; then exit 1; fi
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}✓ running${NC}"

echo

# Check Dagster daemon
echo "Checking Dagster daemon..."
echo -n "  - dagster_daemon: "
while ! check_container_running "dagster_daemon"; do
    if ! check_timeout; then exit 1; fi
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}✓ running${NC}"

echo

# Check Dagster webserver
echo "Checking Dagster webserver..."
echo -n "  - dagster_webserver: "
while ! check_container_health "dagster_webserver"; do
    if ! check_timeout; then exit 1; fi
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}✓ healthy${NC}"

echo

# Check Dagster API endpoint
echo "Checking Dagster API endpoint..."
echo -n "  - http://localhost:${DAGSTER_WEBSERVER_PORT}/server_info: "
while ! curl -sf "http://localhost:${DAGSTER_WEBSERVER_PORT}/server_info" >/dev/null 2>&1; do
    if ! check_timeout; then exit 1; fi
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}✓ responding${NC}"

echo
echo "================================"

ELAPSED=$(($(date +%s) - START_TIME))
echo -e "${GREEN}✓ All services healthy${NC} (took ${ELAPSED}s)"
echo
echo "Dagster UI: http://localhost:${DAGSTER_WEBSERVER_PORT}"

exit 0
