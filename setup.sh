#!/bin/bash

# setup.sh - Master setup script for Dagster Docker environment
# Usage: ./setup.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

clear

echo -e "${BOLD}${BLUE}"
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║                                                           ║"
echo "║         Dagster + OpenMetadata Integration Setup         ║"
echo "║                                                           ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo

# Change to project root
cd "$PROJECT_ROOT"

# ============================================
# STEP 1: Pre-flight Checks
# ============================================
echo -e "${BLUE}${BOLD}Step 1: Pre-flight Checks${NC}"
echo "================================"

# Check if Docker is running
echo -n "Checking Docker... "
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running${NC}"
    echo "Please start Docker Desktop and try again."
    exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"

# Check if docker-compose is available
echo -n "Checking docker-compose... "
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ docker-compose not found${NC}"
    echo "Please install docker-compose and try again."
    exit 1
fi
echo -e "${GREEN}✓ docker-compose found${NC}"

echo

# ============================================
# STEP 2: Port Availability Check
# ============================================
echo -e "${BLUE}${BOLD}Step 2: Port Availability Check${NC}"
echo "================================"

if ./scripts/check-ports.sh; then
    echo -e "${GREEN}✓ All ports available${NC}"
else
    echo
    echo -e "${YELLOW}Some ports are in use.${NC}"
    read -p "Automatically find and use alternative ports? (y/n): " -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        ./scripts/check-ports.sh --fix
        echo -e "${GREEN}✓ Ports configured${NC}"
    else
        echo -e "${RED}Please resolve port conflicts and try again.${NC}"
        exit 1
    fi
fi

echo

# ============================================
# STEP 3: Network Detection
# ============================================
echo -e "${BLUE}${BOLD}Step 3: Docker Network Detection${NC}"
echo "================================"

./scripts/detect-network.sh

echo

# ============================================
# STEP 4: Configuration Generation
# ============================================
echo -e "${BLUE}${BOLD}Step 4: Configuration Generation${NC}"
echo "================================"

./scripts/generate-config.sh

echo

# ============================================
# STEP 5: Build and Start Services
# ============================================
echo -e "${BLUE}${BOLD}Step 5: Build and Start Services${NC}"
echo "================================"

echo "Building Docker images..."
docker-compose build

echo
echo "Starting services..."
docker-compose up -d

echo -e "${GREEN}✓ Services started${NC}"

echo

# ============================================
# STEP 6: Health Checks
# ============================================
echo -e "${BLUE}${BOLD}Step 6: Health Checks${NC}"
echo "================================"

echo "Waiting for services to be healthy (this may take 1-2 minutes)..."
echo

if ./scripts/health-check.sh --timeout 180; then
    echo -e "${GREEN}✓ All services healthy${NC}"
else
    echo -e "${RED}✗ Some services failed to become healthy${NC}"
    echo
    echo "Check logs with: docker-compose logs"
    exit 1
fi

echo

# ============================================
# STEP 7: Database Initialization
# ============================================
echo -e "${BLUE}${BOLD}Step 7: Database Initialization${NC}"
echo "================================"

# Wait a bit for services to fully stabilize
sleep 5

./scripts/init-db.sh

echo

# ============================================
# STEP 8: Verification
# ============================================
echo -e "${BLUE}${BOLD}Step 8: Deployment Verification${NC}"
echo "================================"

./scripts/verify-deployment.sh

# ============================================
# COMPLETION
# ============================================
echo
echo -e "${BOLD}${GREEN}"
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║                                                           ║"
echo "║              ✓ Setup Complete!                            ║"
echo "║                                                           ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo

# Source .env for port info
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

echo -e "${BOLD}Access Points:${NC}"
echo "  • Dagster UI: ${BLUE}http://localhost:${DAGSTER_WEBSERVER_PORT:-3000}${NC}"
echo "  • GraphQL API: ${BLUE}http://localhost:${DAGSTER_WEBSERVER_PORT:-3000}/graphql${NC}"
if curl -sf http://localhost:8585/api/v1/health >/dev/null 2>&1; then
    echo "  • OpenMetadata: ${BLUE}http://localhost:8585${NC}"
fi

echo
echo -e "${BOLD}Next Steps:${NC}"
echo "  1. Open Dagster UI in your browser"
echo "  2. Navigate to Assets tab"
echo "  3. Materialize all assets to populate warehouse"
echo "  4. Run OpenMetadata ingestion:"
echo "     ${BLUE}metadata ingest -c dagster-creds.yaml${NC}"
echo "  5. View lineage in OpenMetadata UI"

echo
echo -e "${BOLD}Useful Commands:${NC}"
echo "  • View logs:         ${BLUE}docker-compose logs -f${NC}"
echo "  • Stop services:     ${BLUE}docker-compose down${NC}"
echo "  • Restart services:  ${BLUE}docker-compose restart${NC}"
echo "  • Full cleanup:      ${BLUE}./scripts/cleanup.sh --full${NC}"
echo "  • Health check:      ${BLUE}./scripts/health-check.sh${NC}"
echo "  • Verify deployment: ${BLUE}./scripts/verify-deployment.sh${NC}"

echo
echo -e "${YELLOW}For troubleshooting, see README.md${NC}"
echo

exit 0
