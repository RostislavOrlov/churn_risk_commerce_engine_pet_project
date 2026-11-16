CREATE TABLE IF NOT EXISTS stg_analytics.ux_features_buffer (
    log_date DATE NOT NULL,
    user_id TEXT NOT NULL,
    max_session_depth INT DEFAULT 0,
    p90_frustration REAL DEFAULT 0.0,
    total_rage_clicks INT DEFAULT 0,
    pogo_sticking_count INT DEFAULT 0,
    max_velocity_volatility REAL DEFAULT 0.0,
    max_unique_products INT DEFAULT 0,
    max_unique_categories INT DEFAULT 0,
    total_perf_events INT DEFAULT 0,
    api_errors_count INT DEFAULT 0,
    p95_interface_render_ms REAL DEFAULT 0.0,
    avg_network_ms REAL DEFAULT 0.0,
    total_searches INT DEFAULT 0,
    total_filter_resets INT DEFAULT 0,
    max_search_scroll INT DEFAULT 0,
    origin_node_id TEXT DEFAULT '',
    pickup_point_id TEXT DEFAULT '',
    promo_price REAL DEFAULT 0.0,
    has_bundles SMALLINT DEFAULT 0,
    category_depth INT DEFAULT 0,
    PRIMARY KEY (log_date, user_id)
);

SELECT create_hypertable(
    'stg_analytics.ux_features_buffer', 
    'log_date', 
    chunk_time_interval => INTERVAL '1 day', 
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS stg_analytics.logistics_features_buffer (
    log_date DATE NOT NULL,
    user_id TEXT NOT NULL,
    product_id TEXT DEFAULT '',
    order_id VARCHAR(64) DEFAULT '',
    origin_node_id TEXT DEFAULT '',
    pickup_point_id TEXT DEFAULT '',
    calculated_delay_minutes REAL DEFAULT 0.0,
    current_logistic_stage SMALLINT DEFAULT 1,
    is_bottleneck_flag SMALLINT DEFAULT 0,
    failing_node_id TEXT DEFAULT '',
    route_time REAL DEFAULT 0.0,
    route_cost REAL DEFAULT 0.0,
    route_jitter REAL DEFAULT 0.0,
    bottleneck_risk_factor REAL DEFAULT 0.0,
    graph_hops INT DEFAULT 0,
    isolation_score REAL DEFAULT 0.0,
    PRIMARY KEY (log_date, user_id)
);

SELECT create_hypertable(
    'stg_analytics.logistics_features_buffer', 
    'log_date', 
    chunk_time_interval => INTERVAL '1 day', 
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS stg_analytics.logistics_features_buffer_1 (
    log_date DATE NOT NULL,
    user_id TEXT NOT NULL,
    product_id TEXT DEFAULT '',
    order_id VARCHAR(64) DEFAULT '',
    origin_node_id TEXT DEFAULT '',
    pickup_point_id TEXT DEFAULT '',
    route_time REAL DEFAULT 0.0,
    route_cost REAL DEFAULT 0.0,
    route_jitter REAL DEFAULT 0.0,
    bottleneck_risk_factor REAL DEFAULT 0.0,
    graph_hops INT DEFAULT 0,
    isolation_score REAL DEFAULT 0.0,
    PRIMARY KEY (log_date, user_id)
);

CREATE TABLE IF NOT EXISTS stg_analytics.logistics_features_buffer_2 (
    log_date DATE NOT NULL,
    order_id VARCHAR(64) DEFAULT '',
    calculated_delay_minutes REAL DEFAULT 0.0,
    current_logistic_stage SMALLINT DEFAULT 1,
    is_bottleneck_flag SMALLINT DEFAULT 0,
    failing_node_id TEXT DEFAULT '',
    PRIMARY KEY (log_date, order_id)
);

CREATE TABLE IF NOT EXISTS stg_analytics.stg_ml_user_churn_scores (
    user_id TEXT NOT NULL,
    churn_probability FLOAT8 NOT NULL,
    log_date DATE NOT NULL
);

SELECT create_hypertable(
    'stg_analytics.stg_ml_user_churn_scores', 
    'log_date', 
    chunk_time_interval => INTERVAL '1 day', 
    if_not_exists => TRUE
);

ALTER TABLE stg_analytics.stg_ml_user_churn_scores SET (
    timescaledb.compress, 
    timescaledb.compress_segmentby = 'user_id',
    timescaledb.compress_orderby = 'log_date DESC'
);

SELECT add_compression_policy(
    'stg_analytics.stg_ml_user_churn_scores', 
    INTERVAL '2 days', 
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_stg_churn_user 
ON stg_analytics.stg_ml_user_churn_scores (user_id, log_date DESC);

CREATE TABLE IF NOT EXISTS stg_analytics.stg_incident_log (
    order_id VARCHAR(64) NOT NULL,
    failing_node_id VARCHAR(64) NOT NULL,
    logistic_stage_idx SMALLINT NOT NULL,
    calculated_delay_minutes REAL NOT NULL,
    incident_status VARCHAR(32) NOT NULL,
    assigned_team VARCHAR(64) NOT NULL
);


CREATE TABLE IF NOT EXISTS stg_analytics.stg_preemptive_compensations (
    user_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    churn_risk_score REAL NOT NULL,
    voucher_code VARCHAR(32) NOT NULL,
    discount_amount_rub NUMERIC(10, 2) NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);


CREATE TABLE IF NOT EXISTS stg_analytics.stg_node_dynamic_capacities (
    node_id VARCHAR(64) NOT NULL,
    node_type VARCHAR(32) NOT NULL,
    base_capacity_packages INT NOT NULL,
    current_capacity_packages INT NOT NULL,
    bottleneck_risk_factor REAL NOT NULL,
    is_active_block SMALLINT NOT NULL
);


CREATE OR REPLACE VIEW stg_analytics.behavioral_features_store AS
WITH blended_base AS (
    SELECT 
        COALESCE(ux.log_date, lg.log_date) AS log_date,
        COALESCE(ux.user_id, lg.user_id) AS user_id,
        COALESCE(ux.max_session_depth, 0) AS max_session_depth,
        COALESCE(ux.p90_frustration, 0.0) AS p90_frustration,
        COALESCE(ux.total_rage_clicks, 0) AS total_rage_clicks,
        COALESCE(ux.pogo_sticking_count, 0) AS pogo_sticking_count,
        COALESCE(ux.max_velocity_volatility, 0.0) AS max_velocity_volatility,
        COALESCE(ux.max_unique_products, 0) AS max_unique_products,
        COALESCE(ux.max_unique_categories, 0) AS max_unique_categories,
        COALESCE(ux.total_perf_events, 0) AS total_perf_events,
        COALESCE(ux.api_errors_count, 0) AS api_errors_count,
        COALESCE(ux.p95_interface_render_ms, 0.0) AS p95_interface_render_ms,
        COALESCE(ux.avg_network_ms, 0.0) AS avg_network_ms,
        COALESCE(ux.total_searches, 0) AS total_searches,
        COALESCE(ux.total_filter_resets, 0) AS total_filter_resets,
        COALESCE(ux.max_search_scroll, 0) AS max_search_scroll,
        COALESCE(NULLIF(ux.origin_node_id, ''), lg.origin_node_id, '') AS origin_node_id,
        COALESCE(NULLIF(ux.pickup_point_id, ''), lg.pickup_point_id, '') AS pickup_point_id,
        COALESCE(ux.promo_price, 0.0) AS promo_price,
        COALESCE(ux.has_bundles, 0::SMALLINT) AS has_bundles,
        COALESCE(ux.category_depth, 0) AS category_depth,
        COALESCE(lg.product_id, '') AS product_id,
        COALESCE(lg.order_id, '') AS order_id,
        COALESCE(lg.calculated_delay_minutes, 0.0) AS calculated_delay_minutes,
        COALESCE(lg.current_logistic_stage, 1::SMALLINT) AS current_logistic_stage,
        COALESCE(lg.is_bottleneck_flag, 0::SMALLINT) AS is_bottleneck_flag,
        COALESCE(lg.failing_node_id, '') AS failing_node_id,
        COALESCE(lg.route_time, 0.0) AS route_time,
        COALESCE(lg.route_cost, 0.0) AS route_cost,
        COALESCE(lg.route_jitter, 0.0) AS route_jitter,
        COALESCE(lg.bottleneck_risk_factor, 0.0) AS bottleneck_risk_factor,
        COALESCE(lg.graph_hops, 0) AS graph_hops,
        COALESCE(lg.isolation_score, 0.0) AS isolation_score
    FROM stg_analytics.ux_features_buffer ux
    FULL OUTER JOIN stg_analytics.logistics_features_buffer lg 
        ON ux.log_date = lg.log_date AND ux.user_id = lg.user_id
)
SELECT 
    *,
    (LN(1.0 + p90_frustration) * max_session_depth * (route_time / 12.0) * (1.0 + bottleneck_risk_factor) * (graph_hops + 1))::REAL AS frustration_index,
    (CASE WHEN api_errors_count = 0 THEN 0.0 ELSE (promo_price * api_errors_count) END)::REAL AS financial_error_gravity,
    (isolation_score * has_bundles)::REAL AS bundle_supply_vulnerability
FROM blended_base;
