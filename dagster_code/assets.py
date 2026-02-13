"""
Dagster Assets with Database Lineage

Defines assets that read from source database and write to warehouse database,
with explicit lineage metadata for OpenMetadata capture.
"""

import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    AssetIn,
)
from dagster_code.resources import PostgresResource


# ============================================
# SOURCE ASSETS (Read from source database)
# ============================================

@asset(
    group_name="source_data",
    description="Raw customer data from source database",
    metadata={
        "source": "source_postgresql",
        "schema": "source",
        "table": "customers"
    }
)
def raw_customers(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """
    Extract customer data from source database.
    
    This asset reads from the customers table in the source PostgreSQL database.
    """
    context.log.info("Reading customers from source database...")
    
    with source_db.get_connection() as conn:
        # Read customers from source
        query = "SELECT * FROM source.customers ORDER BY customer_id"
        df = pd.read_sql(query, conn)
        
        context.log.info(f"Retrieved {len(df)} customers")
        
        # Log metadata
        context.add_output_metadata({
            "row_count": len(df),
            "columns": MetadataValue.json(list(df.columns)),
            "preview": MetadataValue.md(df.head(10).to_markdown()),
            "source_table": "source.customers"
        })
        
    return df


@asset(
    group_name="source_data",
    description="Raw product data from source database",
    metadata={
        "source": "source_postgresql",
        "schema": "source",
        "table": "products"
    }
)
def raw_products(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """
    Extract product data from source database.
    
    This asset reads from the products table in the source PostgreSQL database.
    """
    context.log.info("Reading products from source database...")
    
    with source_db.get_connection() as conn:
        query = "SELECT * FROM source.products ORDER BY product_id"
        df = pd.read_sql(query, conn)
        
        context.log.info(f"Retrieved {len(df)} products")
        
        context.add_output_metadata({
            "row_count": len(df),
            "columns": MetadataValue.json(list(df.columns)),
            "preview": MetadataValue.md(df.head(10).to_markdown()),
            "source_table": "source.products"
        })
        
    return df


@asset(
    group_name="source_data",
    description="Raw order data from source database",
    metadata={
        "source": "source_postgresql",
        "schema": "source",
        "table": "orders"
    }
)
def raw_orders(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """
    Extract order data from source database.
    
    This asset reads from the orders table in the source PostgreSQL database.
    """
    context.log.info("Reading orders from source database...")
    
    with source_db.get_connection() as conn:
        query = """
            SELECT o.*, oi.product_id, oi.quantity, oi.unit_price, oi.total_price
            FROM source.orders o
            JOIN source.order_items oi ON o.order_id = oi.order_id
            ORDER BY o.order_id, oi.order_item_id
        """
        df = pd.read_sql(query, conn)
        
        context.log.info(f"Retrieved {len(df)} order items")
        
        context.add_output_metadata({
            "row_count": len(df),
            "columns": MetadataValue.json(list(df.columns)),
            "preview": MetadataValue.md(df.head(10).to_markdown()),
            "source_tables": "source.orders, source.order_items"
        })
        
    return df


# ============================================
# TRANSFORMED ASSETS (Write to warehouse)
# ============================================

@asset(
    group_name="warehouse_data",
    description="Enriched customer data with derived fields",
    ins={"raw_customers": AssetIn()},
    metadata={
        "destination": "source_postgresql",
        "schema": "warehouse",
        "table": "enriched_customers"
    }
)
def enriched_customers(
    context: AssetExecutionContext,
    source_db: PostgresResource,
    warehouse_db: PostgresResource,
    raw_customers
) -> pd.DataFrame:
    """
    Transform and enrich customer data, then write to warehouse.
    
    Lineage: raw_customers -> enriched_customers
    """
    context.log.info("Transforming customer data...")
    
    # Use passed DataFrame instead of re-reading
    df = raw_customers.copy()
    
    # Transform: Add derived fields (vectorized operations)
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    df['full_address'] = df['address'] + ', ' + df['city'] + ', ' + df['state'] + ' ' + df['zip_code']
    df['email_domain'] = df['email'].str.split('@').str[1]
    
    context.log.info(f"Transformed {len(df)} customer records")
    
    # Write to warehouse - DDL operations first
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.enriched_customers CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.enriched_customers (
                    customer_id INTEGER PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    full_name VARCHAR(200),
                    email VARCHAR(255),
                    email_domain VARCHAR(255),
                    phone VARCHAR(20),
                    address VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    zip_code VARCHAR(20),
                    country VARCHAR(100),
                    full_address TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_enriched_customers_email ON warehouse.enriched_customers(email);
                CREATE INDEX idx_enriched_customers_state ON warehouse.enriched_customers(state);
                CREATE INDEX idx_enriched_customers_email_domain ON warehouse.enriched_customers(email_domain);
            """)
    
    # Insert data using SQLAlchemy engine (after DDL completes)
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('enriched_customers', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
        
    context.log.info(f"Loaded {len(df)} records to warehouse")
    
    # Add metadata and return DataFrame for downstream assets
    context.add_output_metadata({
        "row_count": len(df),
        "columns": MetadataValue.json(list(df.columns)),
        "destination_table": "warehouse.enriched_customers",
        "destination_database": "source_db",
        "transformation": "Added full_name, full_address, email_domain fields",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df


@asset(
    group_name="warehouse_data",
    description="Enriched product data with category analysis",
    ins={"raw_products": AssetIn()},
    metadata={
        "destination": "source_postgresql",
        "schema": "warehouse",
        "table": "enriched_products"
    }
)
def enriched_products(
    context: AssetExecutionContext,
    source_db: PostgresResource,
    warehouse_db: PostgresResource,
    raw_products
) -> pd.DataFrame:
    """
    Transform and enrich product data, then write to warehouse.
    
    Lineage: raw_products -> enriched_products
    """
    context.log.info("Transforming product data...")
    
    # Use passed DataFrame instead of re-reading
    df = raw_products.copy()
    
    # Transform: Add derived fields (vectorized operations)
    df['stock_status'] = pd.cut(df['stock_quantity'], 
                                 bins=[-1, 0, 19, float('inf')], 
                                 labels=['Out of Stock', 'Low Stock', 'In Stock']).astype(str)
    df['price_tier'] = pd.cut(df['price'], 
                              bins=[0, 50, 200, float('inf')], 
                              labels=['Budget', 'Mid-Range', 'Premium']).astype(str)
    df['inventory_value'] = df['price'] * df['stock_quantity']
    
    context.log.info(f"Transformed {len(df)} product records")
    
    # Write to warehouse - DDL operations first
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.enriched_products CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.enriched_products (
                    product_id INTEGER PRIMARY KEY,
                    product_name VARCHAR(255),
                    category VARCHAR(100),
                    price DECIMAL(10, 2),
                    stock_quantity INTEGER,
                    stock_status VARCHAR(50),
                    price_tier VARCHAR(50),
                    inventory_value DECIMAL(15, 2),
                    supplier VARCHAR(255),
                    description TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_enriched_products_category ON warehouse.enriched_products(category);
                CREATE INDEX idx_enriched_products_stock_status ON warehouse.enriched_products(stock_status);
                CREATE INDEX idx_enriched_products_price_tier ON warehouse.enriched_products(price_tier);
            """)
    
    # Insert data using SQLAlchemy engine (after DDL completes)
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('enriched_products', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    context.log.info(f"Loaded {len(df)} records to warehouse")
    
    # Add metadata and return DataFrame for downstream assets
    context.add_output_metadata({
        "row_count": len(df),
        "columns": MetadataValue.json(list(df.columns)),
        "destination_table": "warehouse.enriched_products",
        "destination_database": "source_db",
        "transformation": "Added stock_status, price_tier, inventory_value fields",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df


@asset(
    group_name="warehouse_data",
    description="Customer order summary with aggregated metrics",
    ins={
        "raw_customers": AssetIn(),
        "raw_orders": AssetIn(),
        "enriched_customers": AssetIn()
    },
    metadata={
        "destination": "source_postgresql",
        "schema": "warehouse",
        "table": "customer_order_summary"
    }
)
def customer_order_summary(
    context: AssetExecutionContext,
    source_db: PostgresResource,
    warehouse_db: PostgresResource,
    raw_customers,
    raw_orders,
    enriched_customers
) -> MaterializeResult:
    """
    Create aggregated customer order summary in warehouse.
    
    Lineage: raw_customers, raw_orders, enriched_customers -> customer_order_summary
    """
    context.log.info("Creating customer order summary...")
    
    # Read enriched customers from warehouse schema
    with warehouse_db.get_connection() as conn:
        customers_df = pd.read_sql("SELECT * FROM warehouse.enriched_customers", conn)
    
    # Read orders from source with schema prefix
    with source_db.get_connection() as conn:
        orders_query = """
            SELECT 
                customer_id,
                COUNT(DISTINCT order_id) as total_orders,
                SUM(total_amount) as total_spent,
                AVG(total_amount) as avg_order_value,
                MAX(order_date) as last_order_date,
                MIN(order_date) as first_order_date
            FROM source.orders
            GROUP BY customer_id
        """
        orders_agg = pd.read_sql(orders_query, conn)
    
    # Merge customer data with order aggregations
    summary_df = customers_df.merge(orders_agg, on='customer_id', how='left')
    
    # Fill nulls for customers without orders (vectorized)
    summary_df[['total_orders', 'total_spent', 'avg_order_value']] = summary_df[['total_orders', 'total_spent', 'avg_order_value']].fillna(0)
    summary_df['total_orders'] = summary_df['total_orders'].astype(int)
    
    # Add customer segment (vectorized with pd.cut)
    summary_df['customer_segment'] = pd.cut(summary_df['total_spent'], 
                                            bins=[0, 100, 1000, float('inf')], 
                                            labels=['New', 'Regular', 'VIP']).astype(str)
    
    context.log.info(f"Created summary for {len(summary_df)} customers")
    
    # Write to warehouse - DDL operations first
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.customer_order_summary CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.customer_order_summary (
                    customer_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(200),
                    email VARCHAR(255),
                    email_domain VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    total_orders INTEGER,
                    total_spent DECIMAL(10, 2),
                    avg_order_value DECIMAL(10, 2),
                    last_order_date TIMESTAMP,
                    first_order_date TIMESTAMP,
                    customer_segment VARCHAR(50)
                )
            """)
            cur.execute("""
                CREATE INDEX idx_customer_summary_segment ON warehouse.customer_order_summary(customer_segment);
                CREATE INDEX idx_customer_summary_state ON warehouse.customer_order_summary(state);
                CREATE INDEX idx_customer_summary_total_spent ON warehouse.customer_order_summary(total_spent);
            """)
    
    # Select relevant columns and insert data (after DDL completes)
    output_df = summary_df[[
        'customer_id', 'full_name', 'email', 'email_domain', 
        'city', 'state', 'total_orders', 'total_spent', 
        'avg_order_value', 'last_order_date', 'first_order_date', 
        'customer_segment'
    ]]
    
    engine = warehouse_db.get_engine()
    try:
        output_df.to_sql('customer_order_summary', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    context.log.info(f"Loaded {len(output_df)} records to warehouse")
    
    # Customer segment breakdown
    segment_counts = summary_df['customer_segment'].value_counts().to_dict()
    
    return MaterializeResult(
        metadata={
            "row_count": len(output_df),
            "columns": MetadataValue.json(list(output_df.columns)),
            "destination_table": "warehouse.customer_order_summary",
            "destination_database": "source_db",
            "transformation": "Aggregated orders per customer with segmentation",
            "customer_segments": MetadataValue.json(segment_counts),
            "preview": MetadataValue.md(output_df.head(5).to_markdown())
        }
    )


# ============================================
# NEW PIPELINE ASSETS (Order Items Analytics)
# ============================================

@asset(
    group_name="order_analytics",
    description="Raw order items data from source database",
    metadata={
        "source": "source_postgresql",
        "schema": "source",
        "table": "order_items"
    }
)
def raw_order_items(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """
    Extract order items data from source database.
    
    This asset reads from the order_items table in the source PostgreSQL database.
    """
    context.log.info("Reading order items from source database...")
    
    with source_db.get_connection() as conn:
        query = "SELECT * FROM source.order_items ORDER BY order_id, order_item_id"
        df = pd.read_sql(query, conn)
        
        context.log.info(f"Retrieved {len(df)} order items")
        
        context.add_output_metadata({
            "row_count": len(df),
            "columns": MetadataValue.json(list(df.columns)),
            "preview": MetadataValue.md(df.head(10).to_markdown()),
            "source_table": "source.order_items"
        })
        
    return df


@asset(
    group_name="order_analytics",
    description="Enriched order items with product and pricing analytics",
    ins={
        "raw_order_items": AssetIn(),
        "raw_products": AssetIn()
    },
    metadata={
        "destination": "source_postgresql",
        "schema": "warehouse",
        "table": "order_items_analytics"
    }
)
def order_items_analytics(
    context: AssetExecutionContext,
    warehouse_db: PostgresResource,
    raw_order_items,
    raw_products
) -> pd.DataFrame:
    """
    Create enriched order items with product information and analytics.
    
    Lineage: raw_order_items, raw_products -> order_items_analytics
    """
    context.log.info("Creating order items analytics...")
    
    # Merge order items with product data
    df = raw_order_items.merge(
        raw_products[['product_id', 'product_name', 'category', 'supplier']], 
        on='product_id', 
        how='left'
    )
    
    # Calculate analytics fields
    df['revenue'] = df['quantity'] * df['unit_price']
    df['discount_amount'] = df['unit_price'] * df['quantity'] - df['total_price']
    df['discount_percentage'] = (df['discount_amount'] / (df['unit_price'] * df['quantity']) * 100).fillna(0)
    df['is_bulk_order'] = df['quantity'] > 10
    
    context.log.info(f"Transformed {len(df)} order items")
    
    # Write to warehouse - DDL operations first
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.order_items_analytics CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.order_items_analytics (
                    order_item_id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    product_id INTEGER,
                    product_name VARCHAR(255),
                    category VARCHAR(100),
                    supplier VARCHAR(255),
                    quantity INTEGER,
                    unit_price DECIMAL(10, 2),
                    total_price DECIMAL(10, 2),
                    revenue DECIMAL(10, 2),
                    discount_amount DECIMAL(10, 2),
                    discount_percentage DECIMAL(5, 2),
                    is_bulk_order BOOLEAN,
                    created_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_order_items_order_id ON warehouse.order_items_analytics(order_id);
                CREATE INDEX idx_order_items_product_id ON warehouse.order_items_analytics(product_id);
                CREATE INDEX idx_order_items_category ON warehouse.order_items_analytics(category);
            """)
    
    # Insert data using SQLAlchemy engine
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('order_items_analytics', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    context.log.info(f"Loaded {len(df)} records to warehouse")
    
    # Add metadata and return DataFrame
    context.add_output_metadata({
        "row_count": len(df),
        "columns": MetadataValue.json(list(df.columns)),
        "destination_table": "warehouse.order_items_analytics",
        "total_revenue": f"${df['revenue'].sum():.2f}",
        "total_discount": f"${df['discount_amount'].sum():.2f}",
        "bulk_orders_pct": f"{(df['is_bulk_order'].sum() / len(df) * 100):.1f}%",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df


@asset(
    group_name="order_analytics",
    description="Daily sales summary aggregated by date",
    ins={"raw_orders": AssetIn()},
    metadata={
        "destination": "source_postgresql",
        "schema": "warehouse",
        "table": "daily_sales_summary"
    }
)
def daily_sales_summary(
    context: AssetExecutionContext,
    warehouse_db: PostgresResource,
    raw_orders
) -> pd.DataFrame:
    """
    Create daily sales summary from order data.
    
    Lineage: raw_orders -> daily_sales_summary
    """
    context.log.info("Creating daily sales summary...")
    
    # Convert order_date to date only and aggregate
    df = raw_orders.copy()
    df['order_date'] = pd.to_datetime(df['order_date']).dt.date
    
    summary = df.groupby('order_date').agg({
        'order_id': 'count',
        'customer_id': 'nunique',
        'total_amount': ['sum', 'mean', 'min', 'max']
    }).reset_index()
    
    # Flatten column names
    summary.columns = [
        'sale_date', 'total_orders', 'unique_customers', 
        'total_revenue', 'avg_order_value', 'min_order_value', 'max_order_value'
    ]
    
    context.log.info(f"Created summary for {len(summary)} days")
    
    # Write to warehouse - DDL operations first
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.daily_sales_summary CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.daily_sales_summary (
                    sale_date DATE PRIMARY KEY,
                    total_orders INTEGER,
                    unique_customers INTEGER,
                    total_revenue DECIMAL(12, 2),
                    avg_order_value DECIMAL(10, 2),
                    min_order_value DECIMAL(10, 2),
                    max_order_value DECIMAL(10, 2)
                )
            """)
            cur.execute("""
                CREATE INDEX idx_daily_sales_date ON warehouse.daily_sales_summary(sale_date);
            """)
    
    # Insert data using SQLAlchemy engine
    engine = warehouse_db.get_engine()
    try:
        summary.to_sql('daily_sales_summary', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    context.log.info(f"Loaded {len(summary)} records to warehouse")
    
    # Add metadata and return DataFrame
    context.add_output_metadata({
        "row_count": len(summary),
        "columns": MetadataValue.json(list(summary.columns)),
        "destination_table": "warehouse.daily_sales_summary",
        "date_range": f"{summary['sale_date'].min()} to {summary['sale_date'].max()}",
        "total_revenue": f"${summary['total_revenue'].sum():.2f}",
        "preview": MetadataValue.md(summary.head(10).to_markdown())
    })
    
    return summary


# ============================================
# BUSINESS INTELLIGENCE PIPELINE (New Sources)
# ============================================

@asset(
    group_name="business_intelligence",
    description="Product sales performance data from source aggregated view",
    metadata={
        "source": "source_postgresql",
        "schema": "source",
        "table": "product_sales_summary"
    }
)
def source_product_sales(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """
    Extract product sales summary from source database aggregated view.
    
    This asset reads from product_sales_summary in the source schema.
    """
    context.log.info("Reading product sales summary from source...")
    
    with source_db.get_connection() as conn:
        query = "SELECT * FROM source.product_sales_summary ORDER BY total_revenue DESC"
        df = pd.read_sql(query, conn)
        
        context.log.info(f"Retrieved {len(df)} product sales records")
        
        context.add_output_metadata({
            "row_count": len(df),
            "columns": MetadataValue.json(list(df.columns)),
            "total_revenue": f"${df['total_revenue'].sum():.2f}",
            "top_product": df.iloc[0]['product_name'] if len(df) > 0 else "N/A",
            "preview": MetadataValue.md(df.head(10).to_markdown()),
            "source_table": "source.product_sales_summary"
        })
        
    return df


@asset(
    group_name="business_intelligence",
    description="Customer order summary data from source aggregated view",
    metadata={
        "source": "source_postgresql",
        "schema": "source",
        "table": "customer_order_summary"
    }
)
def source_customer_orders(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """
    Extract customer order summary from source database aggregated view.
    
    This asset reads from customer_order_summary in the source schema.
    """
    context.log.info("Reading customer order summary from source...")
    
    with source_db.get_connection() as conn:
        query = "SELECT * FROM source.customer_order_summary ORDER BY total_spent DESC"
        df = pd.read_sql(query, conn)
        
        context.log.info(f"Retrieved {len(df)} customer order records")
        
        context.add_output_metadata({
            "row_count": len(df),
            "columns": MetadataValue.json(list(df.columns)),
            "total_customers": len(df),
            "total_revenue": f"${df['total_spent'].sum():.2f}",
            "top_customer": df.iloc[0]['email'] if len(df) > 0 else "N/A",
            "preview": MetadataValue.md(df.head(10).to_markdown()),
            "source_table": "source.customer_order_summary"
        })
        
    return df


@asset(
    group_name="business_intelligence",
    description="Product performance analytics with category insights",
    ins={"source_product_sales": AssetIn()},
    metadata={
        "destination": "source_postgresql",
        "schema": "warehouse",
        "table": "product_performance_analytics"
    }
)
def product_performance_analytics(
    context: AssetExecutionContext,
    warehouse_db: PostgresResource,
    source_product_sales
) -> pd.DataFrame:
    """
    Create product performance analytics with rankings and category analysis.
    
    Lineage: source.product_sales_summary -> warehouse.product_performance_analytics
    """
    context.log.info("Creating product performance analytics...")
    
    df = source_product_sales.copy()
    
    # Add analytics columns (handle division by zero)
    df['avg_order_quantity'] = df.apply(
        lambda x: x['total_quantity_sold'] / x['times_ordered'] if x['times_ordered'] > 0 else 0,
        axis=1
    )
    df['avg_revenue_per_order'] = df.apply(
        lambda x: x['total_revenue'] / x['times_ordered'] if x['times_ordered'] > 0 else 0,
        axis=1
    )
    
    # Use nullable integer type (Int64) to handle potential NaN values
    df['revenue_rank'] = df['total_revenue'].rank(ascending=False, method='dense', na_option='bottom').astype('Int64')
    df['popularity_rank'] = df['times_ordered'].rank(ascending=False, method='dense', na_option='bottom').astype('Int64')
    
    # Performance tier
    df['performance_tier'] = pd.cut(
        df['total_revenue'],
        bins=[0, df['total_revenue'].quantile(0.33), df['total_revenue'].quantile(0.67), float('inf')],
        labels=['Low Performer', 'Average Performer', 'Top Performer']
    ).astype(str)
    
    # Category stats
    category_totals = df.groupby('category')['total_revenue'].sum()
    df['category_revenue_share'] = df.apply(
        lambda x: (x['total_revenue'] / category_totals[x['category']] * 100) if x['category'] in category_totals else 0,
        axis=1
    )
    
    context.log.info(f"Transformed {len(df)} product performance records")
    
    # Write to warehouse - DDL first
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.product_performance_analytics CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.product_performance_analytics (
                    product_id INTEGER PRIMARY KEY,
                    product_name VARCHAR(255),
                    category VARCHAR(100),
                    price DECIMAL(10, 2),
                    times_ordered BIGINT,
                    total_quantity_sold BIGINT,
                    total_revenue DECIMAL(15, 2),
                    avg_order_quantity DECIMAL(10, 2),
                    avg_revenue_per_order DECIMAL(10, 2),
                    revenue_rank INTEGER,
                    popularity_rank INTEGER,
                    performance_tier VARCHAR(50),
                    category_revenue_share DECIMAL(5, 2),
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_product_perf_revenue_rank ON warehouse.product_performance_analytics(revenue_rank);
                CREATE INDEX idx_product_perf_category ON warehouse.product_performance_analytics(category);
                CREATE INDEX idx_product_perf_tier ON warehouse.product_performance_analytics(performance_tier);
            """)
    
    # Insert data
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('product_performance_analytics', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    context.log.info(f"Loaded {len(df)} records to warehouse")
    
    context.add_output_metadata({
        "row_count": len(df),
        "columns": MetadataValue.json(list(df.columns)),
        "destination_table": "warehouse.product_performance_analytics",
        "top_performers": len(df[df['performance_tier'] == 'Top Performer']),
        "total_revenue": f"${df['total_revenue'].sum():.2f}",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df


@asset(
    group_name="business_intelligence",
    description="Customer lifetime value and segmentation analytics",
    ins={"source_customer_orders": AssetIn()},
    metadata={
        "destination": "source_postgresql",
        "schema": "warehouse",
        "table": "customer_lifetime_value"
    }
)
def customer_lifetime_value(
    context: AssetExecutionContext,
    warehouse_db: PostgresResource,
    source_customer_orders
) -> pd.DataFrame:
    """
    Create customer lifetime value analytics with segmentation.
    
    Lineage: source.customer_order_summary -> warehouse.customer_lifetime_value
    """
    context.log.info("Creating customer lifetime value analytics...")
    
    df = source_customer_orders.copy()
    
    # Calculate analytics (handle division by zero)
    df['avg_order_value'] = df.apply(
        lambda x: x['total_spent'] / x['total_orders'] if x['total_orders'] > 0 else 0,
        axis=1
    )
    df['customer_tenure_days'] = (df['last_order_date'] - df['first_order_date']).dt.days
    df['orders_per_month'] = df['total_orders'] / (df['customer_tenure_days'] / 30.0 + 1)
    
    # LTV segmentation
    df['ltv_segment'] = pd.cut(
        df['total_spent'],
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['Bronze', 'Silver', 'Gold', 'Platinum']
    ).astype(str)
    
    # Engagement level
    df['engagement_level'] = pd.cut(
        df['orders_per_month'],
        bins=[0, 0.5, 2, float('inf')],
        labels=['Low', 'Medium', 'High']
    ).astype(str)
    
    # Recency category
    days_since_order = (pd.Timestamp.now() - df['last_order_date']).dt.days
    df['recency_status'] = pd.cut(
        days_since_order,
        bins=[-1, 30, 90, float('inf')],
        labels=['Active', 'At Risk', 'Churned']
    ).astype(str)
    
    context.log.info(f"Transformed {len(df)} customer LTV records")
    
    # Write to warehouse - DDL first
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.customer_lifetime_value CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.customer_lifetime_value (
                    customer_id INTEGER PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(255),
                    total_orders BIGINT,
                    total_spent DECIMAL(12, 2),
                    avg_order_value DECIMAL(10, 2),
                    customer_tenure_days INTEGER,
                    orders_per_month DECIMAL(10, 2),
                    first_order_date TIMESTAMP,
                    last_order_date TIMESTAMP,
                    ltv_segment VARCHAR(50),
                    engagement_level VARCHAR(50),
                    recency_status VARCHAR(50),
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_customer_ltv_segment ON warehouse.customer_lifetime_value(ltv_segment);
                CREATE INDEX idx_customer_engagement ON warehouse.customer_lifetime_value(engagement_level);
                CREATE INDEX idx_customer_recency ON warehouse.customer_lifetime_value(recency_status);
            """)
    
    # Insert data
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('customer_lifetime_value', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    context.log.info(f"Loaded {len(df)} records to warehouse")
    
    # Segment breakdown
    segment_counts = df['ltv_segment'].value_counts().to_dict()
    
    context.add_output_metadata({
        "row_count": len(df),
        "columns": MetadataValue.json(list(df.columns)),
        "destination_table": "warehouse.customer_lifetime_value",
        "ltv_segments": MetadataValue.json(segment_counts),
        "total_ltv": f"${df['total_spent'].sum():.2f}",
        "avg_customer_value": f"${df['total_spent'].mean():.2f}",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df


# ============================================
# HIERARCHICAL KEY ASSETS (Database.Schema.Table)
# ============================================

@asset(
    key=["postgres", "source", "customers"],
    group_name="hierarchical_pipeline",
    description="Customers with hierarchical asset key: postgres.source.customers",
    metadata={
        "database": "postgres",
        "schema": "source",
        "table": "customers"
    }
)
def postgres__source__customers(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """Extract customers with hierarchical key structure."""
    context.log.info("Reading customers with hierarchical key...")
    
    with source_db.get_connection() as conn:
        df = pd.read_sql("SELECT * FROM source.customers ORDER BY customer_id", conn)
        
        context.add_output_metadata({
            "row_count": len(df),
            "database": "postgres",
            "schema": "source",
            "table": "customers",
            "asset_key_path": "postgres/source/customers"
        })
        
    return df


@asset(
    key=["postgres", "source", "products"],
    group_name="hierarchical_pipeline",
    description="Products with hierarchical asset key: postgres.source.products",
    metadata={
        "database": "postgres",
        "schema": "source",
        "table": "products"
    }
)
def postgres__source__products(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """Extract products with hierarchical key structure."""
    context.log.info("Reading products with hierarchical key...")
    
    with source_db.get_connection() as conn:
        df = pd.read_sql("SELECT * FROM source.products ORDER BY product_id", conn)
        
        context.add_output_metadata({
            "row_count": len(df),
            "database": "postgres",
            "schema": "source",
            "table": "products",
            "asset_key_path": "postgres/source/products"
        })
        
    return df


@asset(
    key=["postgres", "source", "orders"],
    group_name="hierarchical_pipeline",
    description="Orders with hierarchical asset key: postgres.source.orders",
    metadata={
        "database": "postgres",
        "schema": "source",
        "table": "orders"
    }
)
def postgres__source__orders(context: AssetExecutionContext, source_db: PostgresResource) -> pd.DataFrame:
    """Extract orders with hierarchical key structure."""
    context.log.info("Reading orders with hierarchical key...")
    
    with source_db.get_connection() as conn:
        df = pd.read_sql("SELECT * FROM source.orders ORDER BY order_id", conn)
        
        context.add_output_metadata({
            "row_count": len(df),
            "database": "postgres",
            "schema": "source",
            "table": "orders",
            "asset_key_path": "postgres/source/orders"
        })
        
    return df


@asset(
    key=["postgres", "warehouse", "customer_insights"],
    ins={
        "customers": AssetIn(key=["postgres", "source", "customers"]),
        "orders": AssetIn(key=["postgres", "source", "orders"])
    },
    group_name="hierarchical_pipeline",
    description="Customer insights with hierarchical asset key: postgres.warehouse.customer_insights",
    metadata={
        "database": "postgres",
        "schema": "warehouse",
        "table": "customer_insights"
    }
)
def postgres__warehouse__customer_insights(
    context: AssetExecutionContext,
    warehouse_db: PostgresResource,
    customers,
    orders
) -> pd.DataFrame:
    """Create customer insights with hierarchical key structure."""
    context.log.info("Creating customer insights with hierarchical key...")
    
    # Aggregate orders per customer
    order_stats = orders.groupby('customer_id').agg({
        'order_id': 'count',
        'total_amount': ['sum', 'mean']
    }).reset_index()
    order_stats.columns = ['customer_id', 'order_count', 'total_spent', 'avg_order_value']
    
    # Merge with customer data
    df = customers.merge(order_stats, on='customer_id', how='left')
    df[['order_count', 'total_spent', 'avg_order_value']] = df[['order_count', 'total_spent', 'avg_order_value']].fillna(0)
    
    # Add insight categories
    df['customer_tier'] = pd.cut(
        df['total_spent'],
        bins=[0, 200, 500, float('inf')],
        labels=['Basic', 'Premium', 'Elite']
    ).astype(str)
    
    context.log.info(f"Created insights for {len(df)} customers")
    
    # Write to warehouse
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.customer_insights CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.customer_insights (
                    customer_id INTEGER PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(255),
                    order_count INTEGER,
                    total_spent DECIMAL(12, 2),
                    avg_order_value DECIMAL(10, 2),
                    customer_tier VARCHAR(50),
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_customer_insights_tier ON warehouse.customer_insights(customer_tier);
            """)
    
    engine = warehouse_db.get_engine()
    try:
        df[['customer_id', 'first_name', 'last_name', 'email', 'order_count', 'total_spent', 'avg_order_value', 'customer_tier']].to_sql(
            'customer_insights', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse'
        )
    finally:
        engine.dispose()
    
    context.add_output_metadata({
        "row_count": len(df),
        "database": "postgres",
        "schema": "warehouse",
        "table": "customer_insights",
        "asset_key_path": "postgres/warehouse/customer_insights",
        "tier_breakdown": MetadataValue.json(df['customer_tier'].value_counts().to_dict())
    })
    
    return df


@asset(
    key=["postgres", "warehouse", "product_summary"],
    ins={"products": AssetIn(key=["postgres", "source", "products"])},
    group_name="hierarchical_pipeline",
    description="Product summary with hierarchical asset key: postgres.warehouse.product_summary",
    metadata={
        "database": "postgres",
        "schema": "warehouse",
        "table": "product_summary"
    }
)
def postgres__warehouse__product_summary(
    context: AssetExecutionContext,
    warehouse_db: PostgresResource,
    products
) -> pd.DataFrame:
    """Create product summary with hierarchical key structure."""
    context.log.info("Creating product summary with hierarchical key...")
    
    df = products.copy()
    
    # Add summary fields
    df['stock_category'] = pd.cut(
        df['stock_quantity'],
        bins=[-1, 0, 10, 50, float('inf')],
        labels=['Out of Stock', 'Critical', 'Low', 'Normal']
    ).astype(str)
    
    df['price_category'] = pd.cut(
        df['price'],
        bins=[0, 25, 75, 150, float('inf')],
        labels=['Budget', 'Economy', 'Standard', 'Premium']
    ).astype(str)
    
    df['inventory_value'] = df['price'] * df['stock_quantity']
    
    context.log.info(f"Created summary for {len(df)} products")
    
    # Write to warehouse
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.product_summary CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.product_summary (
                    product_id INTEGER PRIMARY KEY,
                    product_name VARCHAR(255),
                    category VARCHAR(100),
                    price DECIMAL(10, 2),
                    stock_quantity INTEGER,
                    stock_category VARCHAR(50),
                    price_category VARCHAR(50),
                    inventory_value DECIMAL(15, 2),
                    summarized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_product_summary_category ON warehouse.product_summary(category);
                CREATE INDEX idx_product_summary_stock ON warehouse.product_summary(stock_category);
            """)
    
    engine = warehouse_db.get_engine()
    try:
        df[['product_id', 'product_name', 'category', 'price', 'stock_quantity', 'stock_category', 'price_category', 'inventory_value']].to_sql(
            'product_summary', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse'
        )
    finally:
        engine.dispose()
    
    context.add_output_metadata({
        "row_count": len(df),
        "database": "postgres",
        "schema": "warehouse",
        "table": "product_summary",
        "asset_key_path": "postgres/warehouse/product_summary",
        "total_inventory_value": f"${df['inventory_value'].sum():.2f}"
    })
    
    return df


# ============================================
# SQL LINEAGE TRACKING PIPELINE
# ============================================

@asset(
    group_name="sql_lineage",
    description="Customer orders with explicit SQL query for lineage tracking",
    metadata={
        "database": "postgres",
        "schema": "warehouse",
        "table": "customer_order_metrics",
        "query_type": "SELECT_JOIN"
    }
)
def customer_order_metrics(context: AssetExecutionContext, source_db: PostgresResource, warehouse_db: PostgresResource) -> pd.DataFrame:
    """
    Create customer order metrics with explicit SQL query tracking for lineage.
    
    This asset demonstrates SQL query extraction for lineage tracking tools.
    """
    context.log.info("Creating customer order metrics with SQL lineage...")
    
    # Define the SQL query explicitly for lineage tracking
    sql_query = """
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
        ORDER BY total_revenue DESC NULLS LAST
    """
    
    # Execute query
    with source_db.get_connection() as conn:
        df = pd.read_sql(sql_query, conn)
    
    context.log.info(f"Retrieved {len(df)} customer order metrics")
    
    # Write to warehouse with DDL
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.customer_order_metrics CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.customer_order_metrics (
                    customer_id INTEGER PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    order_count BIGINT,
                    total_revenue DECIMAL(12, 2),
                    avg_order_value DECIMAL(10, 2),
                    last_order_date TIMESTAMP,
                    first_order_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_customer_order_metrics_state ON warehouse.customer_order_metrics(state);
                CREATE INDEX idx_customer_order_metrics_revenue ON warehouse.customer_order_metrics(total_revenue);
            """)
    
    # Insert data
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('customer_order_metrics', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    # Add comprehensive metadata including SQL query for lineage
    context.add_output_metadata({
        "row_count": len(df),
        "database": "postgres",
        "schema": "warehouse",
        "table": "customer_order_metrics",
        "sql_query": MetadataValue.md(f"```sql\n{sql_query}\n```"),
        "source_tables": ["source.customers", "source.orders"],
        "destination_table": "warehouse.customer_order_metrics",
        "join_type": "LEFT JOIN",
        "join_key": "customer_id",
        "aggregations": ["COUNT", "SUM", "AVG", "MAX", "MIN"],
        "lineage_type": "sql_transformation",
        "total_revenue": f"${df['total_revenue'].sum():.2f}",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df


@asset(
    group_name="sql_lineage",
    description="Product sales metrics with explicit SQL query for lineage tracking",
    metadata={
        "database": "postgres",
        "schema": "warehouse",
        "table": "product_sales_metrics",
        "query_type": "SELECT_JOIN"
    }
)
def product_sales_metrics(context: AssetExecutionContext, source_db: PostgresResource, warehouse_db: PostgresResource) -> pd.DataFrame:
    """
    Create product sales metrics with explicit SQL query tracking for lineage.
    
    This asset demonstrates complex SQL query extraction for lineage tracking.
    """
    context.log.info("Creating product sales metrics with SQL lineage...")
    
    # Define the SQL query explicitly for lineage tracking
    sql_query = """
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
        ORDER BY total_revenue DESC NULLS LAST
    """
    
    # Execute query
    with source_db.get_connection() as conn:
        df = pd.read_sql(sql_query, conn)
    
    # Add derived metrics
    df['revenue_per_customer'] = df['total_revenue'] / df['unique_customers']
    df['revenue_per_customer'] = df['revenue_per_customer'].fillna(0)
    
    context.log.info(f"Retrieved {len(df)} product sales metrics")
    
    # Write to warehouse with DDL
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.product_sales_metrics CASCADE")
            cur.execute("""
                CREATE TABLE warehouse.product_sales_metrics (
                    product_id INTEGER PRIMARY KEY,
                    product_name VARCHAR(255),
                    category VARCHAR(100),
                    price DECIMAL(10, 2),
                    stock_quantity INTEGER,
                    times_ordered BIGINT,
                    total_quantity_sold BIGINT,
                    total_revenue DECIMAL(15, 2),
                    avg_quantity_per_order DECIMAL(10, 2),
                    unique_customers BIGINT,
                    revenue_per_customer DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX idx_product_sales_metrics_category ON warehouse.product_sales_metrics(category);
                CREATE INDEX idx_product_sales_metrics_revenue ON warehouse.product_sales_metrics(total_revenue);
            """)
    
    # Insert data
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('product_sales_metrics', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    # Add comprehensive metadata including SQL query for lineage
    context.add_output_metadata({
        "row_count": len(df),
        "database": "postgres",
        "schema": "warehouse",
        "table": "product_sales_metrics",
        "sql_query": MetadataValue.md(f"```sql\n{sql_query}\n```"),
        "source_tables": ["source.products", "source.order_items", "source.orders"],
        "destination_table": "warehouse.product_sales_metrics",
        "join_type": "LEFT JOIN (multiple)",
        "join_keys": ["product_id", "order_id"],
        "aggregations": ["COUNT", "SUM", "AVG"],
        "derived_columns": ["revenue_per_customer"],
        "lineage_type": "sql_transformation",
        "total_revenue": f"${df['total_revenue'].sum():.2f}",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df


@asset(
    group_name="sql_lineage",
    description="Daily order summary with INSERT...SELECT query for lineage tracking",
    metadata={
        "database": "postgres",
        "schema": "warehouse",
        "table": "daily_order_summary_sql",
        "query_type": "INSERT_SELECT"
    }
)
def daily_order_summary_sql(context: AssetExecutionContext, source_db: PostgresResource, warehouse_db: PostgresResource) -> pd.DataFrame:
    """
    Create daily order summary using INSERT...SELECT pattern for lineage.
    
    This asset demonstrates INSERT...SELECT query tracking for lineage.
    """
    context.log.info("Creating daily order summary with SQL lineage...")
    
    # Read data with explicit SELECT query
    select_query = """
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
        ORDER BY order_date DESC
    """
    
    with source_db.get_connection() as conn:
        df = pd.read_sql(select_query, conn)
    
    context.log.info(f"Retrieved {len(df)} daily summaries")
    
    # Create table with DDL
    with warehouse_db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS warehouse.daily_order_summary_sql CASCADE")
            create_table_query = """
                CREATE TABLE warehouse.daily_order_summary_sql (
                    order_date DATE PRIMARY KEY,
                    total_orders INTEGER,
                    unique_customers INTEGER,
                    total_sales DECIMAL(12, 2),
                    avg_order_value DECIMAL(10, 2),
                    min_order_value DECIMAL(10, 2),
                    max_order_value DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            cur.execute(create_table_query)
            cur.execute("CREATE INDEX idx_daily_order_summary_sql_date ON warehouse.daily_order_summary_sql(order_date);")
    
    # Insert data
    engine = warehouse_db.get_engine()
    try:
        df.to_sql('daily_order_summary_sql', engine, if_exists='append', index=False, chunksize=1000, schema='warehouse')
    finally:
        engine.dispose()
    
    # Construct the equivalent INSERT...SELECT for documentation
    insert_select_query = f"""
        INSERT INTO warehouse.daily_order_summary_sql 
            (order_date, total_orders, unique_customers, total_sales, avg_order_value, min_order_value, max_order_value)
        {select_query}
    """
    
    # Add comprehensive metadata including both queries for lineage
    context.add_output_metadata({
        "row_count": len(df),
        "database": "postgres",
        "schema": "warehouse",
        "table": "daily_order_summary_sql",
        "select_query": MetadataValue.md(f"```sql\n{select_query}\n```"),
        "insert_select_query": MetadataValue.md(f"```sql\n{insert_select_query}\n```"),
        "create_table_query": MetadataValue.md(f"```sql\n{create_table_query}\n```"),
        "source_tables": ["source.orders"],
        "destination_table": "warehouse.daily_order_summary_sql",
        "aggregations": ["COUNT", "SUM", "AVG", "MIN", "MAX"],
        "group_by": ["DATE(order_date)"],
        "lineage_type": "sql_insert_select",
        "date_range": f"{df['order_date'].min()} to {df['order_date'].max()}",
        "total_sales": f"${df['total_sales'].sum():.2f}",
        "preview": MetadataValue.md(df.head(5).to_markdown())
    })
    
    return df

