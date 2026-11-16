SELECT 
    b1.log_date,
    b1.user_id,
    COALESCE(b1.product_id, '') AS product_id,
    COALESCE(NULLIF(b1.order_id, ''), ord.order_id, '') AS order_id,
    COALESCE(b1.origin_node_id, '') AS origin_node_id,
    COALESCE(b1.pickup_point_id, '') AS pickup_point_id,
    COALESCE(b2.calculated_delay_minutes, 0.0)::REAL AS calculated_delay_minutes,
    COALESCE(b2.current_logistic_stage, 1)::SMALLINT AS current_logistic_stage,
    COALESCE(b2.is_bottleneck_flag, 0)::SMALLINT AS is_bottleneck_flag,
    COALESCE(b2.failing_node_id, '') AS failing_node_id,
    COALESCE(b1.route_time, 0.0)::REAL AS route_time,
    COALESCE(b1.route_cost, 0.0)::REAL AS route_cost,
    COALESCE(b1.route_jitter, 0.0)::REAL AS route_jitter,
    COALESCE(b1.bottleneck_risk_factor, 0.0)::REAL AS bottleneck_risk_factor,
    COALESCE(b1.graph_hops, 0)::INT AS graph_hops,
    COALESCE(b1.isolation_score, 0.0)::REAL AS isolation_score
FROM stg_analytics.logistics_features_buffer_1 b1
LEFT JOIN LATERAL (
    SELECT o.order_id
    FROM public.orders o
    WHERE o.user_id = b1.user_id
    ORDER BY o.created_at DESC
    LIMIT 1
) ord ON TRUE
LEFT JOIN stg_analytics.logistics_features_buffer_2 b2 
    ON b1.log_date = b2.log_date 
   AND COALESCE(NULLIF(b1.order_id, ''), ord.order_id) = b2.order_id
