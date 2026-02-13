# Dagster + PostgreSQL Setup Guide

## Overview

This is a fully automated Dagster environment with PostgreSQL backend, designed for testing data lineage with OpenMetadata. The setup uses a single PostgreSQL database with multiple schemas for simplicity.

---

## Architecture

### Single Database Design

We use **one PostgreSQL database** (`postgres`) with three schemas:

```
source_postgresql (Container)
├── public schema       → Dagster system tables (runs, schedules, events)
├── source schema       → Source data (customers, orders, products)
└── warehouse schema    → Transformed data (enriched tables, aggregations)
```

### Components

| Component | Container Name | Port | Purpose |
|-----------|---------------|------|---------|
| PostgreSQL | `source_postgresql` | 5432 | All data storage |
| Dagster Webserver | `dagster_webserver` | 3000 | Web UI |
| Dagster User Code | `dagster_user_code` | 3001 | gRPC API |
| Dagster Daemon | `dagster_daemon` | - | Background services |

---

## PostgreSQL Database

### Connection Details

**Database Credentials:**
```yaml
Host: source_postgresql (inside Docker network)
      localhost (from host machine)
Port: 5432
User: postgres
Password: postgres
Database: postgres
```

### Schema Structure

#### 1. Public Schema (Dagster System)
Contains Dagster's internal tables:
- `runs` - Execution runs
- `event_logs` - Asset materialization events
- `schedules` - Scheduled jobs
- `jobs` - Job definitions
- And more...

#### 2. Source Schema (Source Data)
Production-like source tables with realistic relational data:

**Tables:**
- `source.customers` - Customer information (15 records)
  - customer_id, first_name, last_name, email, phone, address, city, state, zip_code
  
- `source.products` - Product catalog (16 records)
  - product_id, product_name, category, price, stock_quantity, supplier, description
  
- `source.orders` - Customer orders (15 records)
  - order_id, customer_id (FK), order_date, total_amount, status, shipping details
  
- `source.order_items` - Order line items (29 records)
  - order_item_id, order_id (FK), product_id (FK), quantity, unit_price, total_price

**Views:**
- `source.customer_order_summary` - Aggregated customer statistics
- `source.product_sales_summary` - Product performance metrics

#### 3. Warehouse Schema (Transformed Data)
Created dynamically by Dagster assets:
- `warehouse.enriched_customers` - Enhanced customer data
- `warehouse.enriched_products` - Enhanced product data
- `warehouse.customer_order_summary` - Customer analytics

---

## Connecting to PostgreSQL

### From Host Machine

```bash
# Using psql command line
psql -h localhost -p 5432 -U postgres -d postgres

# List all schemas
\dn

# List tables in source schema
\dt source.*

# Query source data
SELECT * FROM source.customers LIMIT 5;

# Check warehouse tables (after materializing assets)
SELECT * FROM warehouse.enriched_customers LIMIT 5;
```

### From Docker Container

```bash
# Execute psql inside container
docker exec -it source_postgresql psql -U postgres -d postgres

# Run direct queries
docker exec -it source_postgresql psql -U postgres -c "SELECT COUNT(*) FROM source.customers;"

# List all schemas
docker exec -it source_postgresql psql -U postgres -c "\dn"

# Describe source tables
docker exec -it source_postgresql psql -U postgres -c "\d+ source.customers"
```

### Using Python (from Dagster assets)

```python
import psycopg2
import pandas as pd

# Connection parameters
conn = psycopg2.connect(
    host="source_postgresql",  # or "localhost" from host
    port=5432,
    user="postgres",
    password="postgres",
    database="postgres"
)

# Query source schema
with conn.cursor() as cur:
    cur.execute("SELECT * FROM source.customers")
    results = cur.fetchall()

# Using pandas
df = pd.read_sql("SELECT * FROM source.customers", conn)

# Write to warehouse schema
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE warehouse.my_table AS
        SELECT * FROM source.customers
    """)
    conn.commit()

conn.close()
```

### Using DBeaver / pgAdmin / TablePlus

**Connection Settings:**
- Host: `localhost`
- Port: `5432`
- Database: `postgres`
- Username: `postgres`
- Password: `postgres`
- SSL Mode: Disable

---

## Dagster Information

### What is Dagster?

Dagster is a data orchestration platform that helps you:
- Build data pipelines with dependencies
- Track data lineage automatically
- Schedule and monitor jobs
- Manage data assets as code
- Version and test data pipelines

### Architecture Components

#### 1. Dagster Webserver
- **URL:** http://localhost:3000
- **Purpose:** Web-based UI for managing and monitoring pipelines
- **Features:**
  - Visualize asset lineage graph
  - Materialize assets on-demand
  - View run history and logs
  - Monitor schedules and sensors
  - Debug pipeline failures

#### 2. Dagster User Code (gRPC Server)
- **Port:** 3001
- **Purpose:** Hosts your Python code (assets, jobs, resources)
- **Location:** `/opt/dagster/app/dagster_code/`
- **Files:**
  - `__init__.py` - Definitions export
  - `assets.py` - Asset definitions
  - `resources.py` - Database resources

#### 3. Dagster Daemon
- **Purpose:** Background process for:
  - Running schedules
  - Sensor evaluations
  - Run queue management
  - Backfills
  - Auto-materialization

### Configuration Files

#### dagster.yaml (Instance Configuration)
Located at: `/opt/dagster/dagster_home/dagster.yaml`

Configures:
- PostgreSQL storage for runs, schedules, and events
- Run coordinator settings
- Launcher configuration

```yaml
schedule_storage:
  module: dagster_postgres.schedule_storage
  class: PostgresScheduleStorage
  config:
    postgres_db:
      username: postgres
      password: postgres
      hostname: source_postgresql
      db_name: postgres
      port: 5432

run_storage:
  module: dagster_postgres.run_storage
  class: PostgresRunStorage
  config:
    postgres_db:
      username: postgres
      password: postgres
      hostname: source_postgresql
      db_name: postgres
      port: 5432

event_log_storage:
  module: dagster_postgres.event_log
  class: PostgresEventLogStorage
  config:
    postgres_db:
      username: postgres
      password: postgres
      hostname: source_postgresql
      db_name: postgres
      port: 5432
```

#### workspace.yaml (Code Location)
Located at: `/opt/dagster/dagster_home/workspace.yaml`

Points to your code location:
```yaml
load_from:
  - grpc_server:
      host: dagster_user_code
      port: 3001
      location_name: dagster_code
```

---

## Dagster Assets

### Asset Definitions

Our setup includes 6 assets organized in a lineage flow:

```
source.customers ──→ raw_customers ──→ enriched_customers ──┐
                                                              ├─→ customer_order_summary
source.orders ────→ raw_orders ─────────────────────────────┘

source.products ──→ raw_products ──→ enriched_products
```

#### Source Assets (Read from DB)

**1. raw_customers**
- **Source:** `source.customers`
- **Type:** Extraction asset
- **Output:** Pandas DataFrame
- **Schema:** source
- **Records:** 15 customers

**2. raw_products**
- **Source:** `source.products`
- **Type:** Extraction asset
- **Output:** Pandas DataFrame
- **Schema:** source
- **Records:** 16 products

**3. raw_orders**
- **Source:** `source.orders` + `source.order_items`
- **Type:** Extraction asset (with JOIN)
- **Output:** Pandas DataFrame
- **Schema:** source
- **Records:** 15 orders, 29 line items

#### Warehouse Assets (Write to DB)

**4. enriched_customers**
- **Input:** raw_customers
- **Output:** `warehouse.enriched_customers` table
- **Transformation:** Adds computed columns, data quality flags

**5. enriched_products**
- **Input:** raw_products
- **Output:** `warehouse.enriched_products` table
- **Transformation:** Category normalization, price analysis

**6. customer_order_summary**
- **Input:** enriched_customers, raw_orders
- **Output:** `warehouse.customer_order_summary` table
- **Transformation:** Aggregated customer analytics

### How to Materialize Assets

#### Via Web UI
1. Open http://localhost:3000
2. Navigate to **Assets** tab
3. Select assets to materialize
4. Click **Materialize selected**
5. Monitor progress in **Runs** tab

#### Via CLI (inside container)
```bash
# Materialize a single asset
docker exec dagster_user_code dagster asset materialize \
  -m dagster_code -a raw_customers

# Materialize all assets
docker exec dagster_user_code dagster asset materialize \
  -m dagster_code --select "*"

# Materialize with dependencies
docker exec dagster_user_code dagster asset materialize \
  -m dagster_code -a enriched_customers
```

#### Via Python API
```python
from dagster import materialize
from dagster_code import defs

# Materialize single asset
result = materialize(
    [defs.get_asset_graph().get("raw_customers")],
    instance=DagsterInstance.get()
)

# Materialize multiple assets
result = materialize(
    [
        defs.get_asset_graph().get("raw_customers"),
        defs.get_asset_graph().get("enriched_customers")
    ],
    instance=DagsterInstance.get()
)
```

---

## Resource Configuration

### PostgreSQL Resources

Defined in `dagster_code/resources.py`:

```python
class PostgresResource(ConfigurableResource):
    """PostgreSQL database resource for Dagster assets."""
    
    host: str
    port: int = 5432
    user: str
    password: str
    database: str
    
    @contextmanager
    def get_connection(self):
        """Get a database connection."""
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

# Source database resource
source_db = PostgresResource(
    host="source_postgresql",
    port=5432,
    user="postgres",
    password="postgres",
    database="postgres"
)

# Warehouse database resource (same DB, different schema usage)
warehouse_db = PostgresResource(
    host="source_postgresql",
    port=5432,
    user="postgres",
    password="postgres",
    database="postgres"
)
```

Assets access these via dependency injection:
```python
@asset
def raw_customers(context: AssetExecutionContext, source_db: PostgresResource):
    with source_db.get_connection() as conn:
        df = pd.read_sql("SELECT * FROM source.customers", conn)
    return df
```

---

## Network Configuration

### Docker Network

**Network Name:** `dagster-openmetadata-net`
- **Type:** Bridge
- **Subnet:** 172.18.0.0/16
- **External:** Yes (shared with OpenMetadata)

### Container Communication

Within the Docker network:
- Containers reference each other by service name
- `dagster_webserver` → `dagster_user_code:3001`
- `dagster_user_code` → `source_postgresql:5432`
- All services → `source_postgresql:5432`

From host machine:
- Access via `localhost` with mapped ports
- Dagster UI: `localhost:3000`
- PostgreSQL: `localhost:5432`

### Network Commands

```bash
# Inspect network
docker network inspect dagster-openmetadata-net

# List containers on network
docker network inspect dagster-openmetadata-net \
  --format '{{range .Containers}}{{.Name}} {{end}}'

# Check connectivity
docker exec dagster_user_code ping -c 1 source_postgresql
```

---

## Environment Variables

Key environment variables in `.env`:

```bash
# Network Configuration
NETWORK_NAME=dagster-openmetadata-net
EXTERNAL_NETWORK=false

# Port Configuration
DAGSTER_WEBSERVER_PORT=3000
DAGSTER_GRPC_PORT=3001
DAGSTER_POSTGRES_PORT=5432

# Dagster System Database
DAGSTER_POSTGRES_HOST=source_postgresql
DAGSTER_POSTGRES_USER=postgres
DAGSTER_POSTGRES_PASSWORD=postgres
DAGSTER_POSTGRES_DB=postgres

# Source Database (same as system)
SOURCE_DB_HOST=source_postgresql
SOURCE_DB_USER=postgres
SOURCE_DB_PASSWORD=postgres
SOURCE_DB_NAME=postgres
SOURCE_DB_PORT=5432

# Warehouse Database (same as system, different schema)
WAREHOUSE_DB_HOST=source_postgresql
WAREHOUSE_DB_USER=postgres
WAREHOUSE_DB_PASSWORD=postgres
WAREHOUSE_DB_NAME=postgres
WAREHOUSE_DB_PORT=5432

# Dagster Configuration
DAGSTER_HOME=/opt/dagster/dagster_home
DAGSTER_CURRENT_IMAGE=dagster-user-code:latest

# OpenMetadata Integration
OPENMETADATA_HOST=http://localhost:8585
OPENMETADATA_API_ENDPOINT=http://localhost:8585/api

# Docker Configuration
COMPOSE_PROJECT_NAME=dagster-om-integration
```

---

## Common Operations

### Start/Stop Services

```bash
# Start all services (automated setup)
./setup.sh

# Start services manually
docker-compose up -d

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v

# Restart specific service
docker-compose restart dagster_webserver

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f dagster_webserver

# Check service status
docker-compose ps
```

### Database Operations

```bash
# Connect to database
docker exec -it source_postgresql psql -U postgres

# Run query from host
docker exec -it source_postgresql psql -U postgres -c "SELECT COUNT(*) FROM source.customers;"

# Backup database
docker exec source_postgresql pg_dump -U postgres postgres > backup.sql

# Restore database
cat backup.sql | docker exec -i source_postgresql psql -U postgres postgres

# Check database size
docker exec source_postgresql psql -U postgres -c "
  SELECT schemaname, 
         pg_size_pretty(sum(pg_total_relation_size(schemaname||'.'||tablename))::bigint) as size
  FROM pg_tables 
  WHERE schemaname IN ('source', 'warehouse')
  GROUP BY schemaname;
"

# List all tables with row counts
docker exec source_postgresql psql -U postgres -c "
  SELECT schemaname, tablename, 
         (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
  FROM (
    SELECT table_name, table_schema,
           query_to_xml(format('SELECT COUNT(*) as cnt FROM %I.%I', table_schema, table_name), false, true, '') as xml_count
    FROM information_schema.tables
    WHERE table_schema IN ('source', 'warehouse')
  ) t(tablename, schemaname, xml_count);
"
```

### Dagster Operations

```bash
# Check Dagster instance info
docker exec dagster_webserver dagster instance info

# List all assets
docker exec dagster_user_code dagster asset list -m dagster_code

# View asset lineage
docker exec dagster_user_code dagster asset lineage -m dagster_code raw_customers

# Check workspace
docker exec dagster_webserver dagster workspace list

# Run health check
./scripts/health-check.sh

# Full deployment verification
./scripts/verify-deployment.sh

# Initialize database
./scripts/init-db.sh
```

---

## Troubleshooting

### Check Service Status

```bash
# Check all containers
docker-compose ps

# Check specific service health
docker inspect dagster_webserver --format='{{.State.Health.Status}}'

# View detailed logs
docker logs dagster_webserver --tail 100 -f

# Check container resource usage
docker stats --no-stream
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
docker exec source_postgresql pg_isready -U postgres

# Check PostgreSQL logs
docker logs source_postgresql --tail 50

# Verify schemas exist
docker exec source_postgresql psql -U postgres -c "\dn"

# Check connections to database
docker exec source_postgresql psql -U postgres -c "
  SELECT datname, usename, application_name, client_addr, state
  FROM pg_stat_activity
  WHERE datname = 'postgres';
"

# Verify source data
docker exec source_postgresql psql -U postgres -c "
  SELECT 'customers' as table_name, COUNT(*) FROM source.customers
  UNION ALL
  SELECT 'products', COUNT(*) FROM source.products
  UNION ALL
  SELECT 'orders', COUNT(*) FROM source.orders;
"
```

### Dagster Webserver Not Starting

```bash
# Check configuration files
cat dagster_home/dagster.yaml
cat dagster_home/workspace.yaml

# Verify environment variables
docker exec dagster_webserver env | grep DAGSTER

# Test workspace connection
docker exec dagster_webserver dagster workspace list

# Check if user code is responding
docker exec dagster_user_code ps aux | grep dagster

# Test gRPC endpoint
docker exec dagster_user_code netstat -an | grep 3001
```

### Asset Materialization Failures

```bash
# View run logs in UI
# Navigate to: http://localhost:3000 → Runs → Select failed run

# Check asset logs via CLI
docker exec dagster_user_code dagster asset list -m dagster_code

# Verify database permissions
docker exec source_postgresql psql -U postgres -c "
  SELECT schema_name, schema_owner 
  FROM information_schema.schemata 
  WHERE schema_name IN ('source', 'warehouse');
"

# Test Python imports
docker exec dagster_user_code python -c "
from dagster_code.resources import get_source_db_resource
print('Import successful')
"

# Test database connection from Python
docker exec dagster_user_code python -c "
from dagster_code.resources import get_source_db_resource
db = get_source_db_resource()
with db.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
        print('Connection successful:', cur.fetchone())
"
```

### Common Error Messages

**Error: "No dagster instance configuration file found"**
- **Solution:** Run `cp dagster.yaml dagster_home/`

**Error: "Found config for storage which is incompatible"**
- **Solution:** Remove `storage:` section from dagster.yaml, keep individual storage configs

**Error: "No arguments given and no [tool.dagster] block in pyproject.toml"**
- **Solution:** Add `-w /opt/dagster/dagster_home/workspace.yaml` to webserver command

**Error: "relation does not exist"**
- **Solution:** Check schema prefix (use `source.table_name` not just `table_name`)

---

## Data Lineage for OpenMetadata

### How Lineage is Captured

Dagster automatically tracks:
1. **Asset Dependencies:** Explicit upstream/downstream relationships
2. **Table References:** SQL queries and table access patterns
3. **Transformation Logic:** Python code transformations
4. **Metadata:** Row counts, column info, execution times

### Metadata Included in Assets

Each asset contains rich metadata:
```python
@asset(
    group_name="source_data",
    description="Raw customer data from source database",
    metadata={
        "source": "source_postgresql",
        "schema": "source",
        "table": "customers"
    }
)
def raw_customers(context: AssetExecutionContext, source_db: PostgresResource):
    # ... asset code ...
    
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(len(df)),
            "columns": MetadataValue.json(list(df.columns)),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )
```

### Connecting OpenMetadata

1. **Configure ingestion** in `dagster-creds.yaml`:
```yaml
source:
  type: dagster
  serviceName: dagster
  serviceConnection:
    config:
      host: dagster_webserver
      port: 3000
  sourceConfig:
    config:
      type: DatabaseMetadata
```

2. **Run ingestion:**
```bash
metadata ingest -c dagster-creds.yaml
```

3. **View lineage** in OpenMetadata UI at http://localhost:8585

The lineage will show:
```
PostgreSQL Tables (source schema)
    ↓
Dagster Assets (raw_*)
    ↓
Dagster Assets (enriched_*)
    ↓
PostgreSQL Tables (warehouse schema)
```

---

## Automated Setup Scripts

The project includes automated scripts in `scripts/` directory:

### Master Setup Script
```bash
./setup.sh
```
**Steps:**
1. Pre-flight checks (Docker, docker-compose)
2. Port availability check
3. Network detection (finds OpenMetadata network)
4. Configuration generation
5. Build and start services
6. Health checks
7. Database initialization
8. Deployment verification

### Individual Scripts

**check-ports.sh** - Verify port availability
```bash
./scripts/check-ports.sh
./scripts/check-ports.sh --fix  # Auto-find alternative ports
```

**detect-network.sh** - Find OpenMetadata Docker network
```bash
./scripts/detect-network.sh
```

**generate-config.sh** - Generate dagster.yaml and workspace.yaml
```bash
./scripts/generate-config.sh
```

**health-check.sh** - Check service health
```bash
./scripts/health-check.sh
./scripts/health-check.sh --timeout 60
```

**verify-deployment.sh** - Full verification
```bash
./scripts/verify-deployment.sh
```

**init-db.sh** - Initialize databases
```bash
./scripts/init-db.sh
```

**cleanup.sh** - Clean up resources
```bash
./scripts/cleanup.sh           # Stop services only
./scripts/cleanup.sh --full    # Remove everything including volumes
```

---

## File Structure

```
dagster/
├── dagster_code/              # Python application code
│   ├── __init__.py           # Definitions export
│   ├── assets.py             # Asset definitions (6 assets)
│   └── resources.py          # Database resources
│
├── dagster_home/             # Dagster instance configuration
│   ├── dagster.yaml          # System configuration
│   ├── workspace.yaml        # Code location config
│   └── logs/                 # Run logs
│
├── docker/                   # Docker-specific files
│   └── postgres-init/
│       └── source/           # PostgreSQL initialization scripts
│           ├── 01-create-schemas.sql      # Create schemas and tables
│           ├── 02-seed-data.sql           # Load sample data
│           └── 03-create-warehouse-schema.sql  # Warehouse schema
│
├── scripts/                  # Automation scripts
│   ├── check-ports.sh        # Port availability checker
│   ├── cleanup.sh            # Cleanup utility
│   ├── detect-network.sh     # Network detection
│   ├── generate-config.sh    # Config generator
│   ├── health-check.sh       # Health monitoring
│   ├── init-db.sh            # Database initializer
│   ├── update-ports.sh       # Port updater
│   └── verify-deployment.sh  # Deployment verifier
│
├── logs/                     # Application logs
├── docker-compose.yml        # Service definitions
├── Dockerfile               # Dagster image definition
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables
├── .env.template            # Environment template
├── dagster.yaml.template    # Dagster config template
├── workspace.yaml.template  # Workspace config template
├── dagster-creds.yaml       # OpenMetadata credentials
├── DAGSTER_INFO.md          # This documentation
└── setup.sh                 # Master setup script
```

---

## Data Model

### Source Schema ERD

```
┌─────────────────┐
│   customers     │
│─────────────────│
│ customer_id (PK)│◄────┐
│ first_name      │     │
│ last_name       │     │
│ email           │     │
│ phone           │     │
│ address         │     │
└─────────────────┘     │
                        │
                        │ FK
                        │
┌─────────────────┐     │
│     orders      │     │
│─────────────────│     │
│ order_id (PK)   │◄────┼────┐
│ customer_id (FK)├─────┘    │
│ order_date      │          │
│ total_amount    │          │ FK
│ status          │          │
└─────────────────┘          │
                             │
┌─────────────────┐          │
│   order_items   │          │
│─────────────────│          │
│ order_item_id PK│          │
│ order_id (FK)   ├──────────┘
│ product_id (FK) ├──────┐
│ quantity        │      │
│ unit_price      │      │
└─────────────────┘      │
                         │ FK
┌─────────────────┐      │
│    products     │      │
│─────────────────│      │
│ product_id (PK) │◄─────┘
│ product_name    │
│ category        │
│ price           │
│ stock_quantity  │
└─────────────────┘
```

### Sample Data Queries

```sql
-- Customer with most orders
SELECT c.first_name, c.last_name, COUNT(o.order_id) as order_count
FROM source.customers c
JOIN source.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY order_count DESC
LIMIT 5;

-- Top selling products
SELECT p.product_name, SUM(oi.quantity) as total_sold, SUM(oi.total_price) as revenue
FROM source.products p
JOIN source.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sold DESC
LIMIT 5;

-- Orders by status
SELECT status, COUNT(*) as count, SUM(total_amount) as total
FROM source.orders
GROUP BY status;

-- Product categories
SELECT category, COUNT(*) as product_count, AVG(price) as avg_price
FROM source.products
GROUP BY category
ORDER BY product_count DESC;
```

---

## Performance Tuning

### PostgreSQL Optimization

```sql
-- Create indexes on foreign keys
CREATE INDEX IF NOT EXISTS idx_orders_customer ON source.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON source.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON source.order_items(product_id);

-- Analyze tables for query optimization
ANALYZE source.customers;
ANALYZE source.products;
ANALYZE source.orders;
ANALYZE source.order_items;

-- Vacuum to reclaim space
VACUUM ANALYZE source.customers;
```

### Dagster Configuration

```yaml
# In dagster.yaml
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 10  # Adjust based on resources
```

### Docker Resource Limits

```yaml
# In docker-compose.yml
services:
  source_postgresql:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G
```

---

## Security Considerations

### Production Recommendations

1. **Change Default Passwords**
```bash
# Update in .env file
DAGSTER_POSTGRES_PASSWORD=your_secure_password_here
SOURCE_DB_PASSWORD=your_secure_password_here
```

2. **Use Secret Management**
```python
# In resources.py
import os
from dagster import EnvVar

source_db = PostgresResource(
    host=EnvVar("SOURCE_DB_HOST"),
    password=EnvVar("SOURCE_DB_PASSWORD"),  # Read from env
    # ...
)
```

3. **Enable PostgreSQL SSL**
```yaml
# In docker-compose.yml
environment:
  POSTGRES_HOST_AUTH_METHOD: scram-sha-256
```

4. **Restrict Network Access**
```yaml
# Remove port mappings for internal services
# Only expose webserver to host
```

---

## Support & Resources

### Dagster Documentation
- **Official Docs:** https://docs.dagster.io
- **Concepts:** https://docs.dagster.io/concepts
- **API Reference:** https://docs.dagster.io/api
- **Tutorial:** https://docs.dagster.io/tutorial

### PostgreSQL Documentation
- **Official Docs:** https://www.postgresql.org/docs/
- **psql Guide:** https://www.postgresql.org/docs/current/app-psql.html
- **SQL Commands:** https://www.postgresql.org/docs/current/sql-commands.html

### Docker Documentation
- **Compose Docs:** https://docs.docker.com/compose/
- **Networking:** https://docs.docker.com/network/

### Local Resources
- **Dagster UI:** http://localhost:3000
- **Dagster GraphQL:** http://localhost:3000/graphql
- **OpenMetadata:** http://localhost:8585

### Community
- **Dagster Slack:** https://dagster.io/slack
- **GitHub Issues:** https://github.com/dagster-io/dagster/issues

---

## Quick Reference Card

| Action | Command |
|--------|---------|
| Start everything | `./setup.sh` |
| Open Dagster UI | http://localhost:3000 |
| Connect to DB | `docker exec -it source_postgresql psql -U postgres` |
| Query source data | `SELECT * FROM source.customers;` |
| Query warehouse | `SELECT * FROM warehouse.enriched_customers;` |
| View logs | `docker-compose logs -f` |
| Stop services | `docker-compose down` |
| Health check | `./scripts/health-check.sh` |
| Materialize assets | Click "Materialize" in UI or run via CLI |
| List assets | `docker exec dagster_user_code dagster asset list -m dagster_code` |
| Check schemas | `docker exec source_postgresql psql -U postgres -c "\dn"` |
| Full cleanup | `./scripts/cleanup.sh --full` |
| Restart service | `docker-compose restart dagster_webserver` |
| Backup database | `docker exec source_postgresql pg_dump -U postgres postgres > backup.sql` |

---

## Appendix: Common SQL Queries

### Database Administration

```sql
-- Check database size
SELECT pg_database.datname, 
       pg_size_pretty(pg_database_size(pg_database.datname)) AS size
FROM pg_database;

-- List all schemas
SELECT schema_name FROM information_schema.schemata;

-- List all tables with sizes
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Active connections
SELECT datname, usename, application_name, client_addr, state, query
FROM pg_stat_activity
WHERE datname = 'postgres';
```

### Data Exploration

```sql
-- Row counts for all tables
SELECT 'customers' as table_name, COUNT(*) FROM source.customers
UNION ALL SELECT 'products', COUNT(*) FROM source.products
UNION ALL SELECT 'orders', COUNT(*) FROM source.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM source.order_items;

-- Table column information
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'source' AND table_name = 'customers'
ORDER BY ordinal_position;

-- Check for NULL values
SELECT 
  COUNT(*) as total_rows,
  COUNT(email) as emails,
  COUNT(*) - COUNT(email) as missing_emails
FROM source.customers;
```

---

**Last Updated:** January 12, 2026  
**Version:** 1.0  
**Maintainer:** Automated Setup System  
**Repository:** Local Development Environment
