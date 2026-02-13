# 🎉 Dagster + OpenMetadata Setup Complete!

## ✅ What Was Created

A fully automated Dagster Docker environment with real PostgreSQL databases for testing OpenMetadata lineage capture.

## 📁 Project Structure

```
dagster/
├── 🚀 setup.sh                          # ONE-COMMAND SETUP - Run this!
├── 🐳 docker-compose.yml                # 5 services: 3 databases + Dagster
├── 📦 Dockerfile                        # Python container definition
├── 📋 requirements.txt                  # Python dependencies
├── 📖 README.md                         # Complete documentation
├── 🔧 .env.template                     # Environment configuration template
├── ⚙️ dagster.yaml.template             # Dagster instance config
├── ⚙️ workspace.yaml.template           # Code location config
├── 🔗 dagster-creds.yaml                # OpenMetadata ingestion (FIXED!)
│
├── 🐍 dagster_code/                     # Python Application
│   ├── __init__.py                     # Definitions with resources
│   ├── resources.py                    # PostgreSQL resources
│   └── assets.py                       # 6 lineage-aware assets
│
├── 🗄️ docker/
│   └── postgres-init/source/
│       ├── 01-create-schemas.sql       # Tables with foreign keys
│       └── 02-seed-data.sql            # 15 customers, 16 products, 15 orders
│
└── 🛠️ scripts/                          # Automation Scripts
    ├── check-ports.sh                  # Intelligent port detection
    ├── update-ports.sh                 # Auto-reconfigure ports
    ├── detect-network.sh               # Find OpenMetadata network
    ├── generate-config.sh              # Create config files
    ├── init-db.sh                      # Initialize databases
    ├── health-check.sh                 # Wait for services
    ├── verify-deployment.sh            # Comprehensive checks
    └── cleanup.sh                      # Teardown utility
```

## 🚀 Quick Start

### Run Setup (Automated)

```bash
./setup.sh
```

This single command will:
1. ✅ Check Docker is running
2. 🔍 Detect and fix port conflicts automatically
3. 🌐 Find your OpenMetadata Docker network (or create shared network)
4. ⚙️ Generate all configuration files
5. 🏗️ Build Docker images
6. 🚢 Start 5 containers (3 databases + Dagster daemon + webserver)
7. 🏥 Wait for all services to be healthy
8. 💾 Initialize databases with realistic sample data
9. ✅ Verify everything is working

**Estimated time: 2-3 minutes**

### What You Get

**🗄️ Three PostgreSQL Databases:**
- `dagster_postgresql` (port 5432) - Dagster system storage
- `source_postgresql` (port 5433) - Source data with customers/orders/products
- `warehouse_postgresql` (port 5434) - Transformed/enriched data

**📊 Six Assets with Lineage:**
1. `raw_customers` - Read from source DB
2. `raw_products` - Read from source DB
3. `raw_orders` - Read from source DB (joins orders + order_items)
4. `enriched_customers` - Transform customers, write to warehouse
5. `enriched_products` - Transform products, write to warehouse
6. `customer_order_summary` - Aggregate from multiple sources

**🔗 Lineage Flow:**
```
source DB → raw_* assets → enriched_* assets → warehouse DB
                    ↓
        customer_order_summary (multi-source)
```

## 🎯 Next Steps

### 1. Access Dagster UI

Open: **http://localhost:3000**

### 2. Materialize Assets

- Navigate to **Assets** tab
- Select all 6 assets
- Click **"Materialize selected"**
- Watch the beautiful lineage graph! 📈

### 3. Explore Databases

**Source database:**
```bash
docker exec -it source_postgresql psql -U source_user -d source_db
\dt  # List tables
SELECT * FROM customers LIMIT 5;
```

**Warehouse database:**
```bash
docker exec -it warehouse_postgresql psql -U warehouse_user -d warehouse_db
SELECT * FROM customer_order_summary ORDER BY total_spent DESC;
```

### 4. Run OpenMetadata Ingestion

```bash
metadata ingest -c dagster-creds.yaml
```

Then view lineage in OpenMetadata at **http://localhost:8585** 🎉

## 🔧 Configuration Details

### Fixed Issues
- ✅ Fixed typo: `locahost` → `dagster_webserver` in dagster-creds.yaml
- ✅ Removed unnecessary token field (Dagster OSS doesn't need authentication)
- ✅ Configured Docker network for OpenMetadata communication

### Environment Variables

See `.env` (created from `.env.template`):
- Ports: 3000 (Dagster), 5432/5433/5434 (PostgreSQL)
- Network: Auto-detected or created
- Database credentials: All configured

### Sample Data

**15 Customers** with realistic names, emails, addresses  
**16 Products** across categories (Electronics, Furniture, Stationery, Books)  
**15 Orders** spanning 90 days with multiple line items  
**Complex relationships** with foreign keys for realistic lineage

## 📚 Useful Commands

```bash
# View all logs
docker-compose logs -f

# Check service health
./scripts/health-check.sh

# Verify everything
./scripts/verify-deployment.sh

# Stop services (keep data)
docker-compose down

# Full cleanup (delete everything)
./scripts/cleanup.sh --full

# Restart from scratch
./scripts/cleanup.sh --full && ./setup.sh
```

## 🐛 Troubleshooting

### Port conflicts?
```bash
./scripts/check-ports.sh --fix
```

### Services not starting?
```bash
docker-compose logs
./scripts/health-check.sh
```

### Need to rebuild?
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## 📊 Architecture Highlights

### Intelligent Automation
- **Port detection**: Uses `lsof`, `netstat`, `nc` with fallbacks
- **Network discovery**: Finds existing OpenMetadata networks automatically
- **Health checks**: Proper dependency ordering with Docker health checks
- **Verification**: 20+ automated checks for deployment validation

### Real Database Lineage
- **Source → Warehouse**: Realistic ETL pipeline
- **Foreign keys**: Proper relational data model
- **Transformations**: Column-level lineage with derived fields
- **Aggregations**: Multi-table joins and summaries

### Production-Ready Patterns
- **Resource management**: Configurable PostgreSQL resources
- **Error handling**: Comprehensive logging and error messages
- **Idempotency**: Scripts can be run multiple times safely
- **Documentation**: Extensive inline comments and README

## 🎓 Learning Resources

- **Dagster Docs**: https://docs.dagster.io
- **OpenMetadata Docs**: https://docs.open-metadata.org
- **Sample Assets**: See `dagster_code/assets.py` for patterns
- **Database Schema**: See `docker/postgres-init/source/*.sql`

## ✨ Key Features

✅ **One-command setup** - `./setup.sh` does everything  
✅ **Automatic port detection** - No manual configuration needed  
✅ **Docker network discovery** - Seamless OpenMetadata integration  
✅ **Real databases** - PostgreSQL with actual relational data  
✅ **Foreign key relationships** - Complex lineage testing  
✅ **Health checks** - Proper startup ordering  
✅ **Comprehensive verification** - Know when things go wrong  
✅ **Complete documentation** - README with all the details  

## 🎉 You're Ready!

Everything is set up and ready to test Dagster lineage with OpenMetadata!

**Start here:**
```bash
./setup.sh
```

Then open http://localhost:3000 and start materializing assets! 🚀

---

**Need help?** Check the [README.md](README.md) for detailed documentation and troubleshooting.

**Found an issue?** All scripts have error handling and helpful messages.

**Happy lineage testing!** 📊✨
