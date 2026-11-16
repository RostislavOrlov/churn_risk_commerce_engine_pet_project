{{
    config(
        materialized='ephemeral'
    )
}}

SELECT 
    toString(event_id) AS event_id,
    toString(package_id) AS package_id,
    toString(order_id) AS order_id,
    toUInt32(timestamp) AS timestamp,
    toString(status) AS status,
    toString(darkstore_id) AS darkstore_id,
    toString(executor_id) AS executor_id,
    toString(executor_type) AS executor_type
FROM {{ source('s2_shipping', 'clickstream_scan_events') }}
WHERE 1=1

{% if is_incremental() %}
    AND log_date = toDate('{{ var("target_date") }}')
{% else %}
    AND log_date >= subtractMonths(now(), 3)
{% endif %}
