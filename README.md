# Dagster + OpenMetadata Integration

Fully automated Dockerized Dagster environment with real PostgreSQL databases for testing OpenMetadata lineage capture.

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                    Docker Network                         │
│                                                           │
│  ┌──────────────┐     ┌──────────────┐   ┌──────────────┐ │
│  │   Dagster    │────▶│   Dagster    │──▶│   Dagster    │ │
│  │  Webserver   │     │    Daemon    │   │  User Code   │ │
│  │  (Port 3000) │     │              │   │  (gRPC 3001) │ │
│  └──────────────┘     └──────────────┘   └──────────────┘ │
│         │                    │                   │        │
│         └────────────────────┴───────────────────┘        │
│                              │                            │
│                    ┌─────────▼─────────┐                  │
│                    │  dagster_postgres │                  │
│                    │  (System Storage) │                  │
│                    └───────────────────┘                  │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │             Data Pipeline Flow                      │  │
│  │                                                     │  │
│  │  ┌───────────────┐          ┌──────────────┐        │  │
│  │  │source_postgres│─────────▶│warehouse_    │        │  │
│  │  │               │  Assets  │postgres      │        │  │
│  │  │ • customers   │  Read &  │              │        │  │
│  │  │ • products    │  Write   │• enriched_*  │        │  │
│  │  │ • orders      │          │• summaries   │        │  │
│  │  └───────────────┘          └──────────────┘        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                           │
│         Lineage captured by OpenMetadata ▼                │
└───────────────────────────────────────────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   OpenMetadata         │
                    │   (External/Existing)  │
                    │   http://localhost:8585│
                    └────────────────────────┘
```

## Features

✨ **Automated Setup**
- Intelligent port detection and auto-configuration
- Docker network discovery and integration
- Health checks and comprehensive verification

🗄️ **Real Database Lineage**
- Source PostgreSQL with realistic sample data (customers, orders, products)
- Warehouse PostgreSQL for transformed data
- Complex relationships with foreign keys for testing lineage

📊 **Lineage-Aware Assets**
- `raw_customers`, `raw_products`, `raw_orders` (source reads)
- `enriched_customers`, `enriched_products` (transformations)
- `customer_order_summary` (multi-source aggregation)
- Rich metadata for OpenMetadata capture

🔧 **Complete Tooling**
- One-command setup script
- Health monitoring
- Deployment verification
- Cleanup utilities

## Quick Start

### Prerequisites

- Docker Desktop running
- 8GB+ RAM recommended
- OpenMetadata instance running locally (optional, can be added later)

### Installation

```bash
# Clone or navigate to the dagster directory
cd /path/to/dagster

# Run the automated setup
./setup.sh
```

The setup script will:
1. ✅ Check Docker availability
2. 🔍 Detect port conflicts (auto-fix if needed)
3. 🌐 Discover/create Docker network
4. ⚙️ Generate configuration files
5. 🏗️ Build and start all services
6. 🏥 Wait for services to be healthy
7. 💾 Initialize databases with sample data
8. ✅ Verify complete deployment

### Manual Setup (Alternative)

If you prefer step-by-step control:

```bash
# 1. Check and fix port conflicts
./scripts/check-ports.sh --fix

# 2. Detect Docker network
./scripts/detect-network.sh

# 3. Generate configs
./scripts/generate-config.sh

# 4. Start services
docker-compose up -d

# 5. Wait for health
./scripts/health-check.sh

# 6. Initialize databases
./scripts/init-db.sh

# 7. Verify deployment
./scripts/verify-deployment.sh
```

## Usage

### Access Dagster UI

Open [http://localhost:3000](http://localhost:3000) in your browser.

### Materialize Assets

1. Navigate to **Assets** tab in Dagster UI
2. Select all assets
3. Click **Materialize selected**
4. Watch the lineage graph as assets execute

### Database Exploration

**Source Database** (Port 5433):
```bash
docker exec -it source_postgresql psql -U source_user -d source_db

# Explore tables
\dt
SELECT * FROM customers LIMIT 5;
SELECT * FROM products WHERE category = 'Electronics';
SELECT * FROM orders WHERE status = 'delivered';
```

**Warehouse Database** (Port 5434):
```bash
docker exec -it warehouse_postgresql psql -U warehouse_user -d warehouse_db

# Check transformed data
SELECT * FROM enriched_customers LIMIT 5;
SELECT * FROM customer_order_summary ORDER BY total_spent DESC;
```

### OpenMetadata Integration

Once your assets are materialized:

```bash
# Run OpenMetadata ingestion
metadata ingest -c dagster-creds.yaml
```

Then view lineage in OpenMetadata UI at [http://localhost:8585](http://localhost:8585)

## Project Structure

```
dagster/
├── setup.sh                      # Master setup orchestrator
├── docker-compose.yml            # Docker services definition
├── Dockerfile                    # Dagster user code container
├── requirements.txt              # Python dependencies
├── dagster-creds.yaml            # OpenMetadata ingestion config
│
├── .env.template                 # Environment variables template
├── dagster.yaml.template         # Dagster instance config template
├── workspace.yaml.template       # Workspace config template
│
├── dagster_code/                 # Python application
│   ├── __init__.py              # Definitions
│   ├── resources.py             # Database resources
│   └── assets.py                # Lineage-aware assets
│
├── docker/
│   └── postgres-init/
│       └── source/
│           ├── 01-create-schemas.sql  # Table definitions
│           └── 02-seed-data.sql       # Sample data
│
└── scripts/                      # Automation scripts
    ├── check-ports.sh           # Port conflict detection
    ├── update-ports.sh          # Port configuration
    ├── detect-network.sh        # Docker network discovery
    ├── generate-config.sh       # Config file generation
    ├── init-db.sh               # Database initialization
    ├── health-check.sh          # Service health monitoring
    ├── verify-deployment.sh     # Comprehensive verification
    └── cleanup.sh               # Cleanup utility
```

## Lineage Flow

The sample assets demonstrate complex lineage relationships:

```
Source Database (source_postgresql)
│
├─ customers ──┐
│              │
├─ products    │
│              ├──▶ raw_customers ──▶ enriched_customers ──┐
├─ orders ─────┤                                           │
│              └──▶ raw_orders ─────────────────────────────┼──▶ customer_order_summary
└─ order_items                                            │
                                                          │
                    raw_products ──▶ enriched_products ──┘

                          ▼
            Warehouse Database (warehouse_postgresql)
```

## Configuration

### Environment Variables

Key variables in `.env`:

```bash
# Ports
DAGSTER_WEBSERVER_PORT=3000
DAGSTER_GRPC_PORT=3001
DAGSTER_POSTGRES_PORT=5432
SOURCE_POSTGRES_PORT=5433
WAREHOUSE_POSTGRES_PORT=5434

# Network
NETWORK_NAME=dagster-openmetadata-net
EXTERNAL_NETWORK=false

# Database Credentials
DAGSTER_POSTGRES_USER=dagster_user
DAGSTER_POSTGRES_PASSWORD=dagster_pass
SOURCE_DB_USER=source_user
SOURCE_DB_PASSWORD=source_pass
WAREHOUSE_DB_USER=warehouse_user
WAREHOUSE_DB_PASSWORD=warehouse_pass
```

### Custom Assets

To add your own assets, edit [dagster_code/assets.py](dagster_code/assets.py):

```python
from dagster import asset, AssetIn

@asset(
    group_name="my_group",
    ins={"upstream_asset": AssetIn()}
)
def my_new_asset(context, source_db, upstream_asset):
    # Your transformation logic
    pass
```

## Useful Commands

### Service Management

```bash
# View logs (all services)
docker-compose logs -f

# View specific service logs
docker-compose logs -f dagster_webserver

# Restart services
docker-compose restart

# Stop services (keep data)
docker-compose down

# Stop and remove all data
./scripts/cleanup.sh --full
```

### Health & Diagnostics

```bash
# Check service health
./scripts/health-check.sh

# Comprehensive verification
./scripts/verify-deployment.sh

# Check container status
docker-compose ps

# Check port usage
./scripts/check-ports.sh
```

### Dagster CLI

```bash
# Execute commands in Dagster container
docker exec dagster_webserver dagster instance info
docker exec dagster_webserver dagster asset list
docker exec dagster_webserver dagster code-location list
```

## Troubleshooting

### Port Conflicts

**Issue**: Port already in use

**Solution**:
```bash
./scripts/check-ports.sh --fix
```

### Services Not Starting

**Issue**: Containers fail to start or become unhealthy

**Steps**:
1. Check logs: `docker-compose logs`
2. Verify Docker resources (8GB+ RAM recommended)
3. Check disk space
4. Try full cleanup and restart:
   ```bash
   ./scripts/cleanup.sh --full
   ./setup.sh
   ```

### Database Connection Errors

**Issue**: Assets fail with connection errors

**Check**:
```bash
# Verify database health
docker exec dagster_postgresql pg_isready -U dagster_user
docker exec source_postgresql pg_isready -U source_user
docker exec warehouse_postgresql pg_isready -U warehouse_user

# Test connectivity from Dagster
docker exec dagster_webserver nc -z source_postgresql 5432
```

### Code Location Not Loading

**Issue**: Assets not visible in UI

**Steps**:
1. Check user code logs: `docker-compose logs dagster_user_code`
2. Verify workspace config: `docker exec dagster_webserver cat /opt/dagster/dagster_home/workspace.yaml`
3. Restart user code: `docker-compose restart dagster_user_code`

### OpenMetadata Ingestion Fails

**Issue**: `metadata ingest` command fails

**Check**:
1. Verify Dagster is accessible:
   ```bash
   curl http://localhost:3000/server_info
   ```
2. Update [dagster-creds.yaml](dagster-creds.yaml) if using different ports
3. Ensure OpenMetadata and Dagster are on same Docker network
4. Check OpenMetadata JWT token is valid

### Network Issues

**Issue**: Services can't communicate

**Solution**:
```bash
# List Docker networks
docker network ls

# Inspect network
docker network inspect dagster-openmetadata-net

# Reconnect services
docker-compose down
./scripts/detect-network.sh
docker-compose up -d
```

## Development

### Adding New Assets

1. Edit [dagster_code/assets.py](dagster_code/assets.py)
2. Reload code location in Dagster UI or restart:
   ```bash
   docker-compose restart dagster_user_code
   ```

### Modifying Database Schema

1. Edit SQL files in `docker/postgres-init/source/`
2. Rebuild database:
   ```bash
   ./scripts/cleanup.sh --full
   ./setup.sh
   ```

### Python Dependencies

Add to [requirements.txt](requirements.txt) then rebuild:
```bash
docker-compose build
docker-compose up -d
```

## Performance Tuning

### Increase Concurrent Runs

Edit [dagster.yaml.template](dagster.yaml.template):
```yaml
run_coordinator:
  config:
    max_concurrent_runs: 20  # Increase from 10
```

### Database Connection Pooling

For production, consider adding pgbouncer or adjusting PostgreSQL settings.

### Resource Limits

Edit [docker-compose.yml](docker-compose.yml) to add resource constraints:
```yaml
services:
  dagster_webserver:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
```

## Data Model

### Source Database Schema

**customers**: customer demographic data
- `customer_id` (PK), `first_name`, `last_name`, `email`, `address`, etc.

**products**: product catalog
- `product_id` (PK), `product_name`, `category`, `price`, `stock_quantity`

**orders**: order headers
- `order_id` (PK), `customer_id` (FK), `order_date`, `total_amount`, `status`

**order_items**: order line items
- `order_item_id` (PK), `order_id` (FK), `product_id` (FK), `quantity`, `unit_price`

### Warehouse Database Schema

**enriched_customers**: transformed customer data
- Adds: `full_name`, `full_address`, `email_domain`

**enriched_products**: product analytics
- Adds: `stock_status`, `price_tier`, `inventory_value`

**customer_order_summary**: aggregated metrics
- Customer lifetime value, order counts, segmentation

## Contributing

To extend this setup:

1. Fork/copy the project
2. Add your custom assets in `dagster_code/`
3. Update SQL schema as needed
4. Test with `./setup.sh`
5. Update documentation

## License

This is a development/testing setup. Adjust for production use with proper security, backups, and monitoring.

## Support

For issues:
- Check logs: `docker-compose logs`
- Run verification: `./scripts/verify-deployment.sh`
- Review Docker status: `docker-compose ps`
- Check Dagster docs: https://docs.dagster.io
- Check OpenMetadata docs: https://docs.open-metadata.org

---

**Happy Lineage Testing! 🚀**
