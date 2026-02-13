# SQL Lineage Tracking Pipeline

This document describes the SQL lineage tracking job and assets created for demonstrating how to extract SQL queries from Dagster for lineage tracking in tools like OpenMetadata.

## Overview

The `sql_lineage_tracking` job includes three assets that explicitly track SQL queries in their metadata. This allows lineage tools to parse the SQL and extract table dependencies without having to infer them from the data flow.

## Assets

### 1. customer_order_metrics

**Source Tables:**
- `source.customers`
- `source.orders`

**Destination Table:**
- `warehouse.customer_order_metrics`

**SQL Pattern:**
```sql
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date
FROM source.customers c
LEFT JOIN source.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state
```

**Metadata Captured:**
- `sql_query`: Complete SQL query formatted in markdown
- `source_tables`: List of source tables
- `destination_table`: Target warehouse table
- `join_type`: Type of join used (LEFT JOIN)
- `join_key`: Column used for joining
- `aggregations`: List of aggregate functions used
- `lineage_type`: Classification as "sql_transformation"

### 2. product_sales_metrics

**Source Tables:**
- `source.products`
- `source.order_items`
- `source.orders`

**Destination Table:**
- `warehouse.product_sales_metrics`

**SQL Pattern:**
```sql
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock_quantity,
    COUNT(DISTINCT oi.order_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.total_price) as total_revenue,
    AVG(oi.quantity) as avg_quantity_per_order,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM source.products p
LEFT JOIN source.order_items oi ON p.product_id = oi.product_id
LEFT JOIN source.orders o ON oi.order_id = o.order_id
GROUP BY p.product_id, p.product_name, p.category, p.price, p.stock_quantity
```

**Metadata Captured:**
- `sql_query`: Complete SQL query with multiple joins
- `source_tables`: All three source tables
- `destination_table`: Target warehouse table
- `join_type`: "LEFT JOIN (multiple)"
- `join_keys`: All join columns used
- `aggregations`: Aggregate functions (COUNT, SUM, AVG)
- `derived_columns`: Columns calculated post-query (revenue_per_customer)
- `lineage_type`: "sql_transformation"

### 3. daily_order_summary_sql

**Source Tables:**
- `source.orders`

**Destination Table:**
- `warehouse.daily_order_summary_sql`

**SQL Pattern:**
```sql
-- SELECT query
SELECT 
    DATE(order_date) as order_date,
    COUNT(order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_order_value,
    MIN(total_amount) as min_order_value,
    MAX(total_amount) as max_order_value
FROM source.orders
GROUP BY DATE(order_date)

-- INSERT...SELECT pattern
INSERT INTO warehouse.daily_order_summary_sql 
    (order_date, total_orders, unique_customers, total_sales, avg_order_value, min_order_value, max_order_value)
[SELECT query above]
```

**Metadata Captured:**
- `select_query`: The SELECT portion of the query
- `insert_select_query`: Full INSERT...SELECT statement
- `create_table_query`: DDL for table creation
- `source_tables`: Source table(s)
- `destination_table`: Target warehouse table
- `aggregations`: All aggregate functions used
- `group_by`: Grouping columns
- `lineage_type`: "sql_insert_select"

## Job Configuration

**Job Name:** `sql_lineage_tracking`

**Job Tags:**
```python
{
    "pipeline": "sql_lineage",
    "lineage_type": "explicit_sql_queries",
    "query_types": "SELECT_JOIN,INSERT_SELECT",
    "source_tables": "source.customers,source.products,source.orders,source.order_items",
    "destination_tables": "warehouse.customer_order_metrics,warehouse.product_sales_metrics,warehouse.daily_order_summary_sql"
}
```

## How to Use for Lineage Tracking

### 1. Run the Job

```bash
# Via UI: Navigate to Jobs → sql_lineage_tracking → Launch Run
# Via CLI (if using dagster CLI)
dagster job execute -j sql_lineage_tracking
```

### 2. Extract SQL Queries via GraphQL

```graphql
query {
  assetNodeOrError(assetKey: {path: ["customer_order_metrics"]}) {
    ... on AssetNode {
      assetKey {
        path
      }
      assetMaterializations(limit: 1) {
        materializationEvent {
          assetMaterialization {
            metadataEntries {
              label
              ... on TextMetadataEntry {
                text
              }
              ... on MarkdownMetadataEntry {
                mdStr
              }
            }
          }
        }
      }
    }
  }
}
```

### 3. OpenMetadata Integration

The explicit SQL queries in metadata can be extracted by OpenMetadata's Dagster connector to build lineage graphs:

1. **Configure dagster-creds.yaml** (already done):
   ```yaml
   source:
     type: dagster
     serviceName: dagster_service
     serviceConnection:
       config:
         type: Dagster
         host: localhost
         port: 3000
   ```

2. **Run Ingestion**:
   ```bash
   metadata ingest -c dagster-creds.yaml
   ```

3. **Lineage Extraction**:
   - OpenMetadata will parse the `sql_query` metadata entries
   - Extract table names from FROM, JOIN clauses
   - Build lineage graph showing data flow
   - Display JOIN relationships and aggregation patterns

## Benefits of Explicit SQL Tracking

1. **Accurate Lineage**: SQL queries provide precise table dependencies
2. **Join Analysis**: Can analyze which columns are used for joins
3. **Transformation Logic**: SQL shows exactly how data is transformed
4. **Aggregation Tracking**: Clear view of which metrics are calculated
5. **Multi-Table Lineage**: Complex queries with multiple joins are fully captured
6. **Audit Trail**: SQL queries serve as documentation of transformations

## Asset Group

All SQL lineage assets belong to the `sql_lineage` group:

```python
@asset(
    group_name="sql_lineage",
    metadata={
        "database": "postgres",
        "schema": "warehouse",
        "table": "customer_order_metrics",
        "query_type": "SELECT_JOIN"
    }
)
```

## Query Types Demonstrated

1. **SELECT_JOIN**: Simple SELECT with LEFT JOIN and aggregations
2. **SELECT_JOIN (multiple)**: Multiple LEFT JOINs in a single query
3. **INSERT_SELECT**: INSERT...SELECT pattern for warehouse loading

## Verification

To verify all assets are loaded:

```bash
# Check total jobs
docker exec dagster_user_code python -c "from dagster_code import all_jobs; print(f'Total jobs: {len(all_jobs)}')"

# List all assets
curl -s http://localhost:3000/graphql -H "Content-Type: application/json" \
  -d '{"query": "{ assetsOrError { ... on AssetConnection { nodes { key { path } } } } }"}' \
  | python3 -m json.tool | grep -E "(customer_order_metrics|product_sales_metrics|daily_order_summary_sql)"
```

## Summary

This pipeline demonstrates best practices for SQL lineage tracking in Dagster:

- ✅ Explicit SQL queries stored in asset metadata
- ✅ Comprehensive metadata including source/destination tables
- ✅ Different query patterns (JOIN, INSERT...SELECT)
- ✅ Ready for OpenMetadata ingestion
- ✅ Complete with DDL and indexes
- ✅ Production-ready error handling and logging

The job serves as a reference implementation for teams that need to extract SQL queries from Dagster for lineage tracking in external tools.
