#!/bin/bash

# detect-network.sh - Detect existing OpenMetadata Docker network or create shared network
# Usage: ./detect-network.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== Docker Network Detection ===${NC}\n"

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running${NC}"
    echo "Please start Docker and try again."
    exit 1
fi

# Search for OpenMetadata related networks
echo "Searching for OpenMetadata Docker networks..."

OPENMETADATA_NETWORKS=$(docker network ls --filter "name=openmetadata" --format "{{.Name}}" 2>/dev/null || true)

if [ -z "$OPENMETADATA_NETWORKS" ]; then
    echo -e "${YELLOW}⚠ No OpenMetadata network found${NC}"
    echo
    
    # Check for any networks with 'metadata' in the name
    METADATA_NETWORKS=$(docker network ls --filter "name=metadata" --format "{{.Name}}" 2>/dev/null || true)
    
    if [ -n "$METADATA_NETWORKS" ]; then
        echo "Found networks containing 'metadata':"
        echo "$METADATA_NETWORKS" | while read -r net; do
            echo "  - $net"
        done
        echo
    fi
    
    # Search for common network names
    COMMON_NETWORKS=("openmetadata_app-network" "openmetadata_default" "openmetadata" "app-network" "metadata-network")
    FOUND_NETWORK=""
    
    for net_name in "${COMMON_NETWORKS[@]}"; do
        if docker network inspect "$net_name" >/dev/null 2>&1; then
            FOUND_NETWORK="$net_name"
            echo -e "${GREEN}✓ Found network: $net_name${NC}"
            break
        fi
    done
    
    if [ -z "$FOUND_NETWORK" ]; then
        # No existing network found, create a new one
        NETWORK_NAME="dagster-openmetadata-net"
        echo "Creating new shared network: $NETWORK_NAME"
        
        if docker network create "$NETWORK_NAME" >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Created network: $NETWORK_NAME${NC}"
            FOUND_NETWORK="$NETWORK_NAME"
            EXTERNAL_NETWORK="false"
        else
            echo -e "${RED}✗ Failed to create network${NC}"
            exit 1
        fi
    else
        EXTERNAL_NETWORK="true"
    fi
else
    # Use the first OpenMetadata network found
    FOUND_NETWORK=$(echo "$OPENMETADATA_NETWORKS" | head -n 1)
    echo -e "${GREEN}✓ Found OpenMetadata network: $FOUND_NETWORK${NC}"
    EXTERNAL_NETWORK="true"
    
    # Show network details
    echo
    echo "Network details:"
    docker network inspect "$FOUND_NETWORK" --format "  Driver: {{.Driver}}" 2>/dev/null || true
    docker network inspect "$FOUND_NETWORK" --format "  Subnet: {{range .IPAM.Config}}{{.Subnet}}{{end}}" 2>/dev/null || true
    
    # List containers on this network
    CONTAINERS=$(docker network inspect "$FOUND_NETWORK" --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null || true)
    if [ -n "$CONTAINERS" ]; then
        echo "  Containers:"
        for container in $CONTAINERS; do
            echo "    - $container"
        done
    fi
fi

echo
echo "================================"
echo -e "${GREEN}Network Configuration:${NC}"
echo "  Network Name: $FOUND_NETWORK"
echo "  External: $EXTERNAL_NETWORK"

# Update .env file
ENV_FILE="$PROJECT_ROOT/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo
    echo "Creating .env file from template..."
    cp "$PROJECT_ROOT/.env.template" "$ENV_FILE"
fi

echo
echo "Updating $ENV_FILE with network configuration..."

# Update network settings
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/^NETWORK_NAME=.*/NETWORK_NAME=$FOUND_NETWORK/" "$ENV_FILE"
    sed -i '' "s/^EXTERNAL_NETWORK=.*/EXTERNAL_NETWORK=$EXTERNAL_NETWORK/" "$ENV_FILE"
else
    # Linux
    sed -i "s/^NETWORK_NAME=.*/NETWORK_NAME=$FOUND_NETWORK/" "$ENV_FILE"
    sed -i "s/^EXTERNAL_NETWORK=.*/EXTERNAL_NETWORK=$EXTERNAL_NETWORK/" "$ENV_FILE"
fi

echo -e "${GREEN}✓ Network configuration saved to .env${NC}"

# Export variables for other scripts
export NETWORK_NAME="$FOUND_NETWORK"
export EXTERNAL_NETWORK="$EXTERNAL_NETWORK"

echo
echo -e "${GREEN}✓ Network detection complete${NC}"

if [ "$EXTERNAL_NETWORK" == "true" ]; then
    echo -e "${YELLOW}Note: Dagster will connect to existing network: $FOUND_NETWORK${NC}"
    echo "Ensure OpenMetadata containers are on this network for communication."
else
    echo -e "${YELLOW}Note: Created new network: $FOUND_NETWORK${NC}"
    echo "Connect your OpenMetadata containers to this network for integration."
fi

exit 0
