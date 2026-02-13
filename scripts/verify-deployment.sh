#!/bin/bash

# verify-deployment.sh - Comprehensive deployment verification
# Usage: ./verify-deployment.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

CHECKS_PASSED=0
CHECKS_FAILED=0

echo -e "${BLUE}=== Comprehensive Deployment Verification ===${NC}\n"

# Source environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo -e "${RED}✗ .env file not found${NC}"
    exit 1
fi

# Helper function for checks
check() {
    local name=$1
    local command=$2
    
    echo -n "Checking $name... "
    
    if eval "$command" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ passed${NC}"
        ((CHECKS_PASSED++))
        return 0
    else
        echo -e "${RED}✗ failed${NC}"
        ((CHECKS_FAILED++))
        return 1
    fi
}

# ============================================
# CONTAINER HEALTH CHECKS
# ============================================
echo -e "${BLUE}1. Container Health Checks${NC}"

check "dagster_postgresql health" \
    "[ \$(docker inspect --format='{{.State.Health.Status}}' dagster_postgresql) = 'healthy' ]"

check "source_postgresql health" \
    "[ \$(docker inspect --format='{{.State.Health.Status}}' source_postgresql) = 'healthy' ]"

check "warehouse_postgresql health" \
    "[ \$(docker inspect --format='{{.State.Health.Status}}' warehouse_postgresql) = 'healthy' ]"

check "dagster_webserver health" \
    "[ \$(docker inspect --format='{{.State.Health.Status}}' dagster_webserver) = 'healthy' ]"

check "dagster_user_code running" \
    "docker ps --format '{{.Names}}' | grep -q '^dagster_user_code$'"

check "dagster_daemon running" \
    "docker ps --format '{{.Names}}' | grep -q '^dagster_daemon$'"

echo

# ============================================
# API ENDPOINT CHECKS
# ============================================
echo -e "${BLUE}2. API Endpoint Checks${NC}"

check "Dagster webserver responding" \
    "curl -sf http://localhost:${DAGSTER_WEBSERVER_PORT}/server_info"

check "Dagster GraphQL endpoint" \
    "curl -sf -X POST http://localhost:${DAGSTER_WEBSERVER_PORT}/graphql \
         -H 'Content-Type: application/json' \
         -d '{\"query\": \"{ __schema { types { name } } }\"}'"

echo

# ============================================
# DATABASE CONNECTIVITY
# ============================================
echo -e "${BLUE}3. Database Connectivity${NC}"

check "Dagster database accessible" \
    "docker exec dagster_postgresql psql -U ${DAGSTER_POSTGRES_USER} -d ${DAGSTER_POSTGRES_DB} -c '\\l'"

check "Source database accessible" \
    "docker exec source_postgresql psql -U ${SOURCE_DB_USER} -d ${SOURCE_DB_NAME} -c '\\l'"

check "Warehouse database accessible" \
    "docker exec warehouse_postgresql psql -U ${WAREHOUSE_DB_USER} -d ${WAREHOUSE_DB_NAME} -c '\\l'"

echo

# ============================================
# DATA VERIFICATION
# ============================================
echo -e "${BLUE}4. Data Verification${NC}"

# Check source database tables
CUSTOMER_COUNT=$(docker exec source_postgresql psql -U ${SOURCE_DB_USER} -d ${SOURCE_DB_NAME} -t -c "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d ' ' || echo "0")
PRODUCT_COUNT=$(docker exec source_postgresql psql -U ${SOURCE_DB_USER} -d ${SOURCE_DB_NAME} -t -c "SELECT COUNT(*) FROM products;" 2>/dev/null | tr -d ' ' || echo "0")
ORDER_COUNT=$(docker exec source_postgresql psql -U ${SOURCE_DB_USER} -d ${SOURCE_DB_NAME} -t -c "SELECT COUNT(*) FROM orders;" 2>/dev/null | tr -d ' ' || echo "0")

echo "  Source database records:"
echo "    - Customers: $CUSTOMER_COUNT"
echo "    - Products: $PRODUCT_COUNT"
echo "    - Orders: $ORDER_COUNT"

if [ "$CUSTOMER_COUNT" -gt 0 ] && [ "$PRODUCT_COUNT" -gt 0 ] && [ "$ORDER_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}✓ Source data populated${NC}"
    ((CHECKS_PASSED++))
else
    echo -e "  ${RED}✗ Source data not populated${NC}"
    ((CHECKS_FAILED++))
fi

echo

# ============================================
# DAGSTER CONFIGURATION
# ============================================
echo -e "${BLUE}5. Dagster Configuration${NC}"

check "Dagster instance configured" \
    "docker exec dagster_webserver dagster instance info"

# Try to list code locations
echo -n "Checking code locations... "
if docker exec dagster_webserver dagster code-location list 2>/dev/null | grep -q "dagster_code_location"; then
    echo -e "${GREEN}✓ passed${NC}"
    ((CHECKS_PASSED++))
else
    echo -e "${YELLOW}⚠ code location not loaded yet (may still be starting)${NC}"
fi

echo

# ============================================
# NETWORK CONNECTIVITY
# ============================================
echo -e "${BLUE}6. Inter-Service Connectivity${NC}"

check "Dagster → PostgreSQL connectivity" \
    "docker exec dagster_webserver nc -z dagster_postgresql 5432"

check "Dagster → Source DB connectivity" \
    "docker exec dagster_webserver nc -z source_postgresql 5432"

check "Dagster → Warehouse DB connectivity" \
    "docker exec dagster_webserver nc -z warehouse_postgresql 5432"

check "Dagster → User Code connectivity" \
    "docker exec dagster_webserver nc -z dagster_user_code 3001"

echo

# ============================================
# OPENMETADATA INTEGRATION
# ============================================
echo -e "${BLUE}7. OpenMetadata Integration (Optional)${NC}"

if curl -sf http://localhost:8585/api/v1/health >/dev/null 2>&1; then
    echo -e "  ${GREEN}✓ OpenMetadata accessible at http://localhost:8585${NC}"
    ((CHECKS_PASSED++))
    
    # Check if dagster-creds.yaml exists
    if [ -f "$PROJECT_ROOT/dagster-creds.yaml" ]; then
        echo -e "  ${GREEN}✓ dagster-creds.yaml found${NC}"
        ((CHECKS_PASSED++))
    else
        echo -e "  ${YELLOW}⚠ dagster-creds.yaml not found${NC}"
    fi
else
    echo -e "  ${YELLOW}⚠ OpenMetadata not accessible (expected if running separately)${NC}"
fi

echo

# ============================================
# SUMMARY
# ============================================
echo "================================"
echo -e "${BLUE}Verification Summary${NC}"
echo "================================"
echo "Checks passed: ${GREEN}${CHECKS_PASSED}${NC}"
echo "Checks failed: ${RED}${CHECKS_FAILED}${NC}"
echo "================================"
echo

if [ $CHECKS_FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All critical checks passed!${NC}"
    echo
    echo "Deployment is ready for use:"
    echo "  - Dagster UI: http://localhost:${DAGSTER_WEBSERVER_PORT}"
    echo "  - GraphQL API: http://localhost:${DAGSTER_WEBSERVER_PORT}/graphql"
    echo
    echo "Next steps:"
    echo "  1. Materialize assets in Dagster UI"
    echo "  2. Run OpenMetadata ingestion: metadata ingest -c dagster-creds.yaml"
    echo "  3. View lineage in OpenMetadata UI"
    exit 0
else
    echo -e "${RED}✗ Some checks failed. Review the output above.${NC}"
    echo
    echo "Common issues:"
    echo "  - Services still starting up (wait a bit and re-run)"
    echo "  - Port conflicts (check with: docker ps)"
    echo "  - Review logs: docker-compose logs"
    exit 1
fi
