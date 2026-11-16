INSERT INTO stg_analytics.logistics_features_buffer_1 (
    log_date,
    user_id,
    product_id,
    order_id,
    origin_node_id,
    pickup_point_id,
    route_time,
    route_cost,
    route_jitter,
    bottleneck_risk_factor,
    graph_hops,
    isolation_score
)
SELECT 
    src.log_date::DATE AS log_date,
    src.user_id::TEXT AS user_id,
    COALESCE(src.product_id, '')::TEXT AS product_id,
    COALESCE(src.order_id, '')::VARCHAR(64) AS order_id,
    COALESCE(src.origin_node_id, '')::TEXT AS origin_node_id,
    COALESCE(src.pickup_point_id, '')::TEXT AS pickup_point_id,
    COALESCE(src.route_time, 0.0)::REAL AS route_time,
    COALESCE(src.route_cost, 0.0)::REAL AS route_cost,
    COALESCE(src.route_jitter, 0.0)::REAL AS route_jitter,
    COALESCE(src.bottleneck_risk_factor, 0.0)::REAL AS bottleneck_risk_factor,
    COALESCE(src.graph_hops, 0)::INT AS graph_hops,
    COALESCE(src.isolation_score, 0.0)::REAL AS isolation_score
FROM (
    SELECT 
        log_date,
        user_id,
        product_id,
        order_id,
        origin_node_id,
        pickup_point_id,
        route_time,
        route_cost,
        route_jitter,
        bottleneck_risk_factor,
        graph_hops,
        isolation_score
    FROM stg_analytics.raw_neo4j_and_dwh_extract
    WHERE log_date = '2026-11-08'::DATE 
) AS src
ON CONFLICT (log_date, user_id) 
DO UPDATE SET 
    product_id = EXCLUDED.product_id,
    order_id = EXCLUDED.order_id,
    origin_node_id = EXCLUDED.origin_node_id,
    pickup_point_id = EXCLUDED.pickup_point_id,
    route_time = EXCLUDED.route_time,
    route_cost = EXCLUDED.route_cost,
    route_jitter = EXCLUDED.route_jitter,
    bottleneck_risk_factor = EXCLUDED.bottleneck_risk_factor,
    graph_hops = EXCLUDED.graph_hops,
    isolation_score = EXCLUDED.isolation_score;
