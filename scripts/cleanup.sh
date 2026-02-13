#!/bin/bash

# cleanup.sh - Stop and optionally remove Dagster Docker environment
# Usage: ./cleanup.sh [--full]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

FULL_CLEANUP=false

# Parse arguments
if [[ "$1" == "--full" ]]; then
    FULL_CLEANUP=true
fi

echo -e "${BLUE}=== Dagster Cleanup ===${NC}\n"

cd "$PROJECT_ROOT"

if [ "$FULL_CLEANUP" = true ]; then
    echo -e "${YELLOW}⚠ FULL CLEANUP MODE${NC}"
    echo "This will:"
    echo "  - Stop all containers"
    echo "  - Remove containers"
    echo "  - Remove volumes (ALL DATA WILL BE LOST)"
    echo "  - Remove generated config files"
    echo
    read -p "Are you sure? (yes/no): " -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        echo "Cleanup cancelled."
        exit 0
    fi
    
    echo "Performing full cleanup..."
    echo
    
    # Stop containers
    echo "Stopping containers..."
    docker-compose down -v --remove-orphans
    
    # Remove generated config files
    echo "Removing generated config files..."
    [ -f dagster.yaml ] && rm dagster.yaml && echo "  - Removed dagster.yaml"
    [ -f workspace.yaml ] && rm workspace.yaml && echo "  - Removed workspace.yaml"
    [ -f .env ] && rm .env && echo "  - Removed .env"
    
    # Remove dagster_home directory
    if [ -d dagster_home ]; then
        echo "Removing dagster_home directory..."
        rm -rf dagster_home
        echo "  - Removed dagster_home/"
    fi
    
    # Remove logs directory
    if [ -d logs ]; then
        echo "Removing logs directory..."
        rm -rf logs
        echo "  - Removed logs/"
    fi
    
    # List remaining Docker volumes
    echo
    echo "Docker volumes for this project:"
    docker volume ls | grep "dagster" || echo "  (none found)"
    
    echo
    echo -e "${GREEN}✓ Full cleanup complete${NC}"
    echo
    echo "To start fresh:"
    echo "  ./setup.sh"
    
else
    # Soft cleanup - just stop containers
    echo "Stopping Dagster services..."
    echo "(Volumes and data will be preserved)"
    echo
    
    docker-compose down
    
    echo
    echo -e "${GREEN}✓ Services stopped${NC}"
    echo
    echo "Data and volumes preserved."
    echo
    echo "To restart:"
    echo "  docker-compose up -d"
    echo
    echo "For full cleanup (removes all data):"
    echo "  ./scripts/cleanup.sh --full"
fi

exit 0
