"""
Dagster Resources Configuration

Defines PostgreSQL resources for source and warehouse databases.
"""

import os
from dagster import EnvVar, ConfigurableResource
from contextlib import contextmanager
import psycopg2
from sqlalchemy import create_engine
from typing import Optional


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
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a query and return results."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:
                    return cur.fetchall()
                return None
    
    def get_engine(self):
        """Get SQLAlchemy engine for pandas to_sql()."""
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(connection_string)


def get_source_db_resource() -> PostgresResource:
    """Create source database resource from environment variables."""
    return PostgresResource(
        host=os.getenv("SOURCE_DB_HOST", "source_postgresql"),
        port=int(os.getenv("SOURCE_DB_PORT", "5432")),
        user=os.getenv("SOURCE_DB_USER", "source_user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "source_pass"),
        database=os.getenv("SOURCE_DB_NAME", "source_db")
    )


def get_warehouse_db_resource() -> PostgresResource:
    """Create warehouse database resource from environment variables."""
    return PostgresResource(
        host=os.getenv("WAREHOUSE_DB_HOST", "warehouse_postgresql"),
        port=int(os.getenv("WAREHOUSE_DB_PORT", "5432")),
        user=os.getenv("WAREHOUSE_DB_USER", "warehouse_user"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse_pass"),
        database=os.getenv("WAREHOUSE_DB_NAME", "warehouse_db")
    )
