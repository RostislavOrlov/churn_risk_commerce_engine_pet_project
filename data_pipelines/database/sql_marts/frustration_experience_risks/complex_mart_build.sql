INSERT INTO dw_marts.ml_user_churn_scores (
    user_id,
    churn_probability,
    log_date,
    calculated_at
)
SELECT 
    user_id,
    churn_probability,
    log_date,
    NOW() AS calculated_at
FROM stg_analytics.stg_ml_user_churn_scores
WHERE log_date = (SELECT MAX(log_date) FROM stg_analytics.stg_ml_user_churn_scores)
ON CONFLICT (log_date, user_id) DO UPDATE 
SET 
    churn_probability = EXCLUDED.churn_probability,
    calculated_at = NOW();

WITH 
fresh_ml_scores AS (
    SELECT 
        user_id,
        churn_probability,
        log_date
    FROM stg_analytics.stg_ml_user_churn_scores
    WHERE log_date = (SELECT MAX(log_date) FROM stg_analytics.stg_ml_user_churn_scores)
),

fresh_logistics_passports AS (
    SELECT 
        b2.order_id,
        b2.calculated_delay_minutes,
        b2.current_logistic_stage,
        b2.is_bottleneck_flag,
        b2.failing_node_id
    FROM stg_analytics.logistics_features_buffer_2 b2
    WHERE b2.log_date = (SELECT MAX(log_date) FROM stg_analytics.logistics_features_buffer_2)
      AND b2.calculated_delay_minutes > 15.0
),

enriched_customer_frustration_matrix AS (
    SELECT 
        fs.user_id,
        COALESCE(lp.order_id, 'ORD_' || fs.user_id) AS order_id,
        fs.churn_probability,
        COALESCE(lp.calculated_delay_minutes, 0.0) AS calculated_delay_minutes,
        COALESCE(lp.current_logistic_stage, 1) AS current_logistic_stage,
        COALESCE(lp.is_bottleneck_flag, 0) AS is_bottleneck_flag,
        COALESCE(NULLIF(lp.failing_node_id, ''), 'HUB_UNKNOWN') AS failing_node_id,
        COALESCE(ltv.long_term_value_rub, 0.0) AS user_ltv,
        COALESCE(ltv.customer_segment, 'REGULAR') AS customer_segment,
        COALESCE(margin.order_margin_rub, 0.0) AS order_margin,
        COALESCE(margin.is_express_grocery, 0) AS is_express_grocery
    FROM fresh_ml_scores fs
    LEFT JOIN public.orders o ON fs.user_id = o.user_id
    LEFT JOIN fresh_logistics_passports lp ON o.order_id = lp.order_id
    LEFT JOIN dw_marts.dwh_user_ltv ltv ON fs.user_id = ltv.user_id
    LEFT JOIN dw_marts.dwh_market_margins margin ON lp.order_id = margin.order_id
),

generated_incidents AS (
    INSERT INTO dw_marts.incident_log (
        order_id,
        failing_node_id,
        logistic_stage_idx,
        calculated_delay_minutes,
        incident_status,
        assigned_team
    )
    SELECT 
        order_id,
        failing_node_id,
        current_logistic_stage,
        calculated_delay_minutes,
        'RAISED' AS incident_status,
        CASE 
            WHEN current_logistic_stage IN (1, 2) THEN 'WAREHOUSE_OPERATIONS_TEAM'
            WHEN current_logistic_stage IN (3, 4, 5, 6) THEN 'LINEHAUL_DISPATCHERS_TEAM'
            ELSE 'LAST_MILE_COURIER_MANAGERS'
        END AS assigned_team
    FROM enriched_customer_frustration_matrix
    WHERE churn_probability > 0.25 OR is_bottleneck_flag = 1 OR calculated_delay_minutes > 15.0
    RETURNING failing_node_id, calculated_delay_minutes
),

generated_compensations AS (
    INSERT INTO dw_marts.preemptive_compensations (
        user_id,
        order_id,
        churn_risk_score,
        voucher_code,
        discount_amount_rub,
        expires_at
    )
    SELECT 
        user_id,
        order_id,
        churn_probability,
        'SORRY_' || current_logistic_stage || '_' || UPPER(SUBSTRING(user_id, 5, 6)),
        CASE 
            WHEN customer_segment = 'VIP' AND is_express_grocery = 1 THEN 500.00
            WHEN customer_segment = 'VIP' OR is_express_grocery = 1 THEN 300.00
            ELSE 150.00
        END AS discount_amount_rub,
        NOW() + INTERVAL '7 days' AS expires_at
    FROM enriched_customer_frustration_matrix
    WHERE churn_probability >= 0.35
    ON CONFLICT (order_id) DO NOTHING
)

INSERT INTO dw_marts.node_dynamic_capacities (
    node_id,
    node_type,
    base_capacity_packages,
    current_capacity_packages,
    bottleneck_risk_factor,
    is_active_block
)
SELECT 
    gi.failing_node_id AS node_id,
    CASE 
        WHEN gi.failing_node_id LIKE 'WH_%' THEN 'WAREHOUSE'
        WHEN gi.failing_node_id LIKE 'HUB_%' THEN 'SORTING_HUB'
        ELSE 'CROSSDOCK'
    END AS node_type,
    100000 AS base_capacity_packages,
    GREATEST(
        10000, 
        100000 - (COUNT(gi.*) * 250) - INT4(SUM(gi.calculated_delay_minutes))
    ) AS current_capacity_packages,
    ROUND(CAST(AVG(gi.calculated_delay_minutes) / 60.0 AS NUMERIC), 4) AS bottleneck_risk_factor,
    CASE WHEN COUNT(gi.*) > 150 THEN 1 ELSE 0 END AS is_active_block
FROM generated_incidents gi
GROUP BY gi.failing_node_id
ON CONFLICT (node_id) DO UPDATE 
SET 
    current_capacity_packages = EXCLUDED.current_capacity_packages,
    bottleneck_risk_factor = EXCLUDED.bottleneck_risk_factor,
    is_active_block = EXCLUDED.is_active_block,
    last_updated_at = NOW();
