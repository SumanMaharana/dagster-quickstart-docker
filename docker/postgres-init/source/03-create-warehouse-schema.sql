-- ============================================
-- Warehouse Schema Initialization
-- Creates a separate schema for warehouse tables
-- ============================================

-- Create warehouse schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Grant necessary permissions (postgres is the superuser in single-db setup)
GRANT ALL ON SCHEMA warehouse TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA warehouse TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT ALL ON TABLES TO postgres;

-- Add comment
COMMENT ON SCHEMA warehouse IS 'Data warehouse schema for transformed and enriched data';

