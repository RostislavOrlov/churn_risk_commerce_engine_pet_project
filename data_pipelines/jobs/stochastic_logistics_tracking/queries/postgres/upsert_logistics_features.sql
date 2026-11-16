INSERT INTO stg_analytics.logistics_features_buffer_2 (
    log_date,
    user_id,
    product_id,
    calculated_delay_minutes,
    current_logistic_stage,
    is_bottleneck_flag,
    failing_node_id,
    route_time,
    route_cost,
    route_jitter
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (log_date, user_id) DO UPDATE SET
    product_id = EXCLUDED.product_id,
    calculated_delay_minutes = EXCLUDED.calculated_delay_minutes,
    current_logistic_stage = EXCLUDED.current_logistic_stage,
    is_bottleneck_flag = EXCLUDED.is_bottleneck_flag,
    failing_node_id = EXCLUDED.failing_node_id,
    route_time = EXCLUDED.route_time,
    route_cost = EXCLUDED.route_cost,
    route_jitter = EXCLUDED.route_jitter;
