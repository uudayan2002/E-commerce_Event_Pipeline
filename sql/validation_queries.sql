-- ============================================================================
-- Validation Queries for ETL Integrity
-- Purpose: Check data quality, counts, and anomalies after load.
-- ============================================================================

-- 1. Reconciliation: Check if total (clean + invalid) equals raw ingestion
-- SELECT 
--     (SELECT count(*) FROM raw_events) as raw_total,
--     (SELECT count(*) FROM clean_events) + (SELECT count(*) FROM invalid_events) as processed_total;

-- 2. Data Quality: Find events with null IP addresses (in clean layer)
-- SELECT event_id, user_id FROM clean_events WHERE ip_address IS NULL;

-- 3. Business Logic: Check for negative amounts in non-refund events
-- SELECT * FROM clean_events WHERE amount < 0 AND event_type != 'refund';

-- 4. Duplicate Check: Find if any event_id exists multiple times in raw_events
-- SELECT event_id, count(*) FROM raw_events GROUP BY event_id HAVING count(*) > 1;

-- 5. Reporting: Top 5 countries by revenue
-- SELECT country, sum(amount) as revenue 
-- FROM clean_events 
-- WHERE event_type = 'purchase' 
-- GROUP BY country 
-- ORDER BY revenue DESC 
-- LIMIT 5;
