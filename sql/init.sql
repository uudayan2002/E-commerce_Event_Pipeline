-- ============================================================================
-- Initialization Script for PostgreSQL
-- Used by Docker Compose 'initdb' or manual setup.
-- ============================================================================

-- Create dedicated user and database if not using defaults
-- CREATE USER etl_user WITH PASSWORD 'etl_pass';
-- CREATE DATABASE ecommerce_events OWNER etl_user;
-- \c ecommerce_events etl_user;

-- Run the schema definition
\i /docker-entrypoint-initdb.d/schema.sql

-- Seeding (Optional: Metadata or small lookup tables)
-- INSERT INTO daily_metrics (metric_date) VALUES (CURRENT_DATE);
