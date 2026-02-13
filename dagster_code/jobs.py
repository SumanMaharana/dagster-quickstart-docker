"""
Dagster Jobs and Schedules

Defines jobs that group assets together and schedules to run them automatically.
"""

from dagster import (
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
)


# ============================================
# JOB DEFINITIONS
# ============================================

# Job to extract all source data (raw assets)
extract_source_data_job = define_asset_job(
    name="extract_source_data",
    description="Extract all data from source database (customers, products, orders)",
    selection=AssetSelection.groups("source_data"),
    tags={
        "type": "extraction",
        "source": "source_postgresql"
    }
)

# Job to enrich and transform data
transform_data_job = define_asset_job(
    name="transform_data",
    description="Transform and enrich data, load into warehouse schema",
    selection=AssetSelection.groups("warehouse_data"),
    tags={
        "type": "transformation",
        "target": "warehouse"
    }
)

# Job to run the full pipeline (all assets)
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    description="Run complete data pipeline from source extraction to warehouse loading",
    selection=AssetSelection.all(),
    tags={
        "type": "full_pipeline",
        "priority": "high"
    }
)

# Job to materialize specific customer-related assets
customer_pipeline_job = define_asset_job(
    name="customer_pipeline",
    description="Extract and enrich customer data only",
    selection=AssetSelection.keys("raw_customers", "enriched_customers", "customer_order_summary"),
    tags={
        "type": "partial_pipeline",
        "domain": "customers"
    }
)

# Job to materialize specific product-related assets
product_pipeline_job = define_asset_job(
    name="product_pipeline",
    description="Extract and enrich product data only",
    selection=AssetSelection.keys("raw_products", "enriched_products"),
    tags={
        "type": "partial_pipeline",
        "domain": "products"
    }
)

# Job to materialize warehouse tables with full qualified names
warehouse_tables_job = define_asset_job(
    name="warehouse_tables",
    description="Materialize order analytics tables (order_items_analytics, daily_sales_summary)",
    selection=AssetSelection.keys("raw_order_items", "order_items_analytics", "daily_sales_summary"),
    tags={
        "type": "warehouse_load",
        "schema": "warehouse",
        "source_tables": "source.order_items,source.orders",
        "destination_tables": "warehouse.order_items_analytics,warehouse.daily_sales_summary"
    }
)

# Job for business intelligence pipeline with new source and destination tables
bi_analytics_job = define_asset_job(
    name="bi_analytics",
    description="Business Intelligence: Product performance and customer LTV analytics",
    selection=AssetSelection.keys(
        "source_product_sales",
        "source_customer_orders",
        "product_performance_analytics",
        "customer_lifetime_value"
    ),
    tags={
        "type": "bi_analytics",
        "domain": "business_intelligence",
        "source_tables": "source.product_sales_summary,source.customer_order_summary",
        "destination_tables": "warehouse.product_performance_analytics,warehouse.customer_lifetime_value"
    }
)

# Job with hierarchical asset keys (database.schema.table)
hierarchical_pipeline_job = define_asset_job(
    name="hierarchical_pipeline",
    description="Pipeline with hierarchical asset keys: postgres.schema.table structure",
    selection=AssetSelection.keys(
        ["postgres", "source", "customers"],
        ["postgres", "source", "products"],
        ["postgres", "source", "orders"],
        ["postgres", "warehouse", "customer_insights"],
        ["postgres", "warehouse", "product_summary"]
    ),
    tags={
        "type": "hierarchical_pipeline",
        "key_structure": "database.schema.table",
        "source_schema": "postgres.source",
        "destination_schema": "postgres.warehouse",
        "source_tables": "postgres.source.customers,postgres.source.products,postgres.source.orders",
        "destination_tables": "postgres.warehouse.customer_insights,postgres.warehouse.product_summary"
    }
)

# SQL Lineage Tracking Job
sql_lineage_job = define_asset_job(
    name="sql_lineage_tracking",
    selection=AssetSelection.keys(
        "customer_order_metrics",
        "product_sales_metrics",
        "daily_order_summary_sql"
    ),
    description="Job demonstrating SQL query tracking for lineage extraction",
    tags={
        "pipeline": "sql_lineage",
        "lineage_type": "explicit_sql_queries",
        "query_types": "SELECT_JOIN,INSERT_SELECT",
        "source_tables": "source.customers,source.products,source.orders,source.order_items",
        "destination_tables": "warehouse.customer_order_metrics,warehouse.product_sales_metrics,warehouse.daily_order_summary_sql"
    }
)


# ============================================
# SCHEDULE DEFINITIONS
# ============================================

# Daily full pipeline run at 2 AM
daily_full_pipeline_schedule = ScheduleDefinition(
    name="daily_full_pipeline",
    job=full_pipeline_job,
    cron_schedule="0 2 * * *",  # 2 AM every day
    description="Run the complete data pipeline daily at 2 AM"
)

# Hourly source data extraction
hourly_extraction_schedule = ScheduleDefinition(
    name="hourly_extraction",
    job=extract_source_data_job,
    cron_schedule="0 * * * *",  # Every hour at minute 0
    description="Extract source data every hour"
)

# Every 6 hours transformation
transformation_schedule = ScheduleDefinition(
    name="transformation_schedule",
    job=transform_data_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    description="Transform and load data into warehouse every 6 hours"
)

# Weekly customer analysis (Sundays at 3 AM)
weekly_customer_analysis_schedule = ScheduleDefinition(
    name="weekly_customer_analysis",
    job=customer_pipeline_job,
    cron_schedule="0 3 * * 0",  # 3 AM every Sunday
    description="Weekly customer data refresh and analysis"
)

# Daily BI analytics run (Mondays at 8 AM)
daily_bi_analytics_schedule = ScheduleDefinition(
    name="daily_bi_analytics",
    job=bi_analytics_job,
    cron_schedule="0 8 * * 1",  # 8 AM every Monday
    description="Weekly business intelligence analytics refresh"
)


# ============================================
# EXPORTS
# ============================================

# List of all jobs
all_jobs = [
    extract_source_data_job,
    transform_data_job,
    full_pipeline_job,
    customer_pipeline_job,
    product_pipeline_job,
    warehouse_tables_job,
    bi_analytics_job,
    hierarchical_pipeline_job,
    sql_lineage_job,
]

# List of all schedules
all_schedules = [
    daily_full_pipeline_schedule,
    hourly_extraction_schedule,
    transformation_schedule,
    weekly_customer_analysis_schedule,
    daily_bi_analytics_schedule,
]
