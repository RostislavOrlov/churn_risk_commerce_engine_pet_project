{{ config(materialized='ephemeral') }}
SELECT user_id, timestamp, product_id, warehouse_id, pickup_point_id, page_type, category_id
FROM {{ source('s2_clickstream', 'user_clicks') }}
WHERE 1=1
{% if is_incremental() %}
  AND timestamp >= toDateTime('{{ var("target_date") }} 00:00:00')
  AND timestamp <  toDateTime('{{ var("target_date") }} 00:00:00') + INTERVAL 26 HOUR
{% else %}
  AND toDateTime(timestamp) >= subtractMonths(now(), 3)
{% endif %}
