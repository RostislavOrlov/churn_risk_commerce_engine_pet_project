CREATE TABLE IF NOT EXISTS dw_marts.dwh_user_ltv (
    user_id VARCHAR(64) PRIMARY KEY,
    long_term_value_rub NUMERIC(12, 2) NOT NULL,
    customer_segment VARCHAR(32) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_ltv_segment 
ON dw_marts.dwh_user_ltv (user_id, customer_segment);

CREATE TABLE IF NOT EXISTS dw_marts.dwh_market_margins (
    order_id VARCHAR(64) PRIMARY KEY,
    order_margin_rub NUMERIC(12, 2) NOT NULL,
    is_express_grocery SMALLINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_margins_order_id 
ON dw_marts.dwh_market_margins (order_id);

CREATE TABLE IF NOT EXISTS dw_marts.ml_user_churn_scores (
    user_id VARCHAR(64) NOT NULL,
    churn_probability REAL NOT NULL,
    log_date DATE NOT NULL,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (log_date, user_id)
);


SELECT create_hypertable(
    'dw_marts.ml_user_churn_scores', 
    'log_date', 
    chunk_time_interval => INTERVAL '7 days', 
    if_not_exists => TRUE
);

ALTER TABLE dw_marts.ml_user_churn_scores SET (
    timescaledb.compress, 
    timescaledb.compress_segmentby = 'user_id',
    timescaledb.compress_orderby = 'log_date DESC'
);

SELECT add_compression_policy(
    'dw_marts.ml_user_churn_scores', 
    INTERVAL '7 days', 
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_dw_churn_history 
ON dw_marts.ml_user_churn_scores (user_id, log_date DESC);

CREATE TABLE IF NOT EXISTS dw_marts.incident_log (
    incident_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id VARCHAR(64) NOT NULL,
    failing_node_id VARCHAR(64) NOT NULL,
    logistic_stage_idx SMALLINT NOT NULL,          
    calculated_delay_minutes REAL NOT NULL,
    incident_status VARCHAR(32) NOT NULL,          
    assigned_team VARCHAR(64) NOT NULL,           
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_incidents_active_node 
ON dw_marts.incident_log (failing_node_id) 
WHERE incident_status IN ('RAISED', 'REROUTED');

CREATE TABLE IF NOT EXISTS dw_marts.preemptive_compensations (
    compensation_id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    churn_risk_score REAL NOT NULL,                
    voucher_code VARCHAR(32) NOT NULL,            
    discount_amount_rub NUMERIC(10, 2) NOT NULL,   
    is_sent_flag SMALLINT DEFAULT 0,               
    sent_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);


CREATE UNIQUE INDEX IF NOT EXISTS uidx_compensation_order 
ON dw_marts.preemptive_compensations (order_id);

CREATE TABLE IF NOT EXISTS dw_marts.node_dynamic_capacities (
    node_id VARCHAR(64) PRIMARY KEY,
    node_type VARCHAR(32) NOT NULL,                
    base_capacity_packages INT NOT NULL,           
    current_capacity_packages INT NOT NULL,       
    bottleneck_risk_factor REAL DEFAULT 0.0,       
    is_active_block SMALLINT DEFAULT 0,            
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'CREATED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_created 
    ON public.orders (user_id, created_at DESC);
