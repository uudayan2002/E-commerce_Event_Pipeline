-- ============================================================================
-- Retail Transactional Intelligence Schema (PySpark Optimized)
-- ============================================================================

-- 1. STAGING LAYER
CREATE TABLE IF NOT EXISTS raw_transactions (
    ingestion_id BIGSERIAL PRIMARY KEY,
    invoice TEXT,
    stock_code TEXT,
    description TEXT,
    quantity TEXT,
    invoice_date TEXT,
    price TEXT,
    customer_id TEXT,
    country TEXT,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. CORE / CLEAN LAYER
CREATE TABLE IF NOT EXISTS clean_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    description TEXT,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    total_amount DECIMAL(15, 2), -- Populated by PySpark
    customer_id VARCHAR(20),
    country VARCHAR(100),
    transaction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    is_cancellation BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON clean_transactions (transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_txn_customer ON clean_transactions (customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_invoice ON clean_transactions (invoice_no);

-- 3. ERROR LAYER
CREATE TABLE IF NOT EXISTS invalid_transactions (
    invalid_id BIGSERIAL PRIMARY KEY,
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    original_data JSONB NOT NULL -- Full row stored as JSON for auditing
);

-- 4. ANALYTICS (GOLD) LAYER
CREATE TABLE IF NOT EXISTS daily_sales_summary (
    sales_date DATE PRIMARY KEY,
    total_revenue DECIMAL(18, 2),
    order_count INT,
    unique_customers INT,
    cancellation_count INT,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_metrics (
    stock_code VARCHAR(20) PRIMARY KEY,
    description TEXT,
    total_quantity_sold BIGINT DEFAULT 0,
    total_revenue DECIMAL(18, 2) DEFAULT 0.00,
    transaction_count INT DEFAULT 0,
    cancellation_rate DECIMAL(5, 2) DEFAULT 0.00,
    last_sold_at TIMESTAMP WITH TIME ZONE
);


