{{ config(materialized='ephemeral') }}
SELECT user_id, action_type, scroll_depth_items
FROM {{ source('s2_analytics', 'search_funnel_logs') }}
WHERE 1=1
{% if is_incremental() %}
  AND toDate(timestamp) = toDate('{{ var("target_date") }}')
{% endif %}
