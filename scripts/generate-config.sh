#!/bin/bash

# generate-config.sh - Generate Dagster configuration files from templates
# Usage: ./generate-config.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== Generating Dagster Configuration Files ===${NC}\n"

# Check if .env file exists
ENV_FILE="$PROJECT_ROOT/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo -e "${YELLOW}⚠ .env file not found, creating from template...${NC}"
    cp "$PROJECT_ROOT/.env.template" "$ENV_FILE"
    echo -e "${GREEN}✓ Created .env file${NC}"
    echo -e "${YELLOW}Please review and update .env with your specific configuration${NC}"
fi

# Source environment variables
set -a
source "$ENV_FILE"
set +a

echo "Loaded environment variables from .env"
echo

# Check if envsubst is available
if ! command -v envsubst &> /dev/null; then
    echo -e "${RED}✗ envsubst command not found${NC}"
    echo "Installing gettext (contains envsubst)..."
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install gettext
            brew link --force gettext
        else
            echo -e "${RED}Homebrew not found. Please install gettext manually.${NC}"
            exit 1
        fi
    else
        # Linux
        sudo apt-get update && sudo apt-get install -y gettext-base
    fi
fi

# Generate dagster.yaml
echo "Generating dagster.yaml..."
if [ -f "$PROJECT_ROOT/dagster.yaml.template" ]; then
    envsubst < "$PROJECT_ROOT/dagster.yaml.template" > "$PROJECT_ROOT/dagster.yaml"
    echo -e "${GREEN}✓${NC} Generated dagster.yaml"
else
    echo -e "${YELLOW}⚠ dagster.yaml.template not found, will be created by Docker container${NC}"
fi

# Generate workspace.yaml
echo "Generating workspace.yaml..."
if [ -f "$PROJECT_ROOT/workspace.yaml.template" ]; then
    envsubst < "$PROJECT_ROOT/workspace.yaml.template" > "$PROJECT_ROOT/workspace.yaml"
    echo -e "${GREEN}✓${NC} Generated workspace.yaml"
else
    echo -e "${YELLOW}⚠ workspace.yaml.template not found, will be created by Docker container${NC}"
fi

# Create dagster_home directory if it doesn't exist
DAGSTER_HOME_DIR="$PROJECT_ROOT/dagster_home"
if [ ! -d "$DAGSTER_HOME_DIR" ]; then
    mkdir -p "$DAGSTER_HOME_DIR"
    echo -e "${GREEN}✓${NC} Created dagster_home directory"
fi

# Create logs directory
LOGS_DIR="$PROJECT_ROOT/logs"
if [ ! -d "$LOGS_DIR" ]; then
    mkdir -p "$LOGS_DIR"
    echo -e "${GREEN}✓${NC} Created logs directory"
fi

# Copy config files to dagster_home for Docker volume mounting
echo "Copying config files to dagster_home..."
if [ -f "$PROJECT_ROOT/dagster.yaml" ]; then
    cp "$PROJECT_ROOT/dagster.yaml" "$DAGSTER_HOME_DIR/dagster.yaml"
    echo -e "${GREEN}✓${NC} Copied dagster.yaml to dagster_home/"
fi
if [ -f "$PROJECT_ROOT/workspace.yaml" ]; then
    cp "$PROJECT_ROOT/workspace.yaml" "$DAGSTER_HOME_DIR/workspace.yaml"
    echo -e "${GREEN}✓${NC} Copied workspace.yaml to dagster_home/"
fi

echo
echo "================================"
echo -e "${GREEN}✓ Configuration generation complete${NC}"
echo
echo "Generated files:"
[ -f "$PROJECT_ROOT/dagster.yaml" ] && echo "  - dagster.yaml"
[ -f "$PROJECT_ROOT/workspace.yaml" ] && echo "  - workspace.yaml"
echo "  - dagster_home/"
echo "  - logs/"
echo
echo "Configuration summary:"
echo "  - Dagster Webserver Port: ${DAGSTER_WEBSERVER_PORT}"
echo "  - Dagster gRPC Port: ${DAGSTER_GRPC_PORT}"
echo "  - Network: ${NETWORK_NAME} (external: ${EXTERNAL_NETWORK})"
echo "  - Dagster DB: ${DAGSTER_POSTGRES_HOST}:${DAGSTER_POSTGRES_PORT}"
echo "  - Source DB: ${SOURCE_DB_HOST}:${SOURCE_DB_PORT}"
echo "  - Warehouse DB: ${WAREHOUSE_DB_HOST}:${WAREHOUSE_DB_PORT}"

exit 0
