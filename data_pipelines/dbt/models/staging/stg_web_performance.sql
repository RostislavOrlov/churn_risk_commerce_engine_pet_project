{{ config(materialized='ephemeral') }}
SELECT user_id, status_code, page_load_ms, dns_resolution_ms
FROM {{ source('s2_telemetry', 'web_performance_logs') }}
WHERE 1=1
{% if is_incremental() %}
  AND toDate(timestamp) = toDate('{{ var("target_date") }}')
{% endif %}
