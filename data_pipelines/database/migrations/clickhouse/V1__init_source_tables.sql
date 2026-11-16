CREATE DATABASE IF NOT EXISTS default;
CREATE DATABASE IF NOT EXISTS s2_telemetry;
CREATE DATABASE IF NOT EXISTS s2_analytics;
CREATE DATABASE IF NOT EXISTS s2_shipping;
CREATE TABLE default.user_clicks_queue (
    user_id String,
    timestamp DateTime,
    product_id String,
    warehouse_id String,
    pickup_point_id String,
    page_type String,
    category_id UInt32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092', 
    kafka_topic_list = 'commerce.user_clicks',
    kafka_group_name = 'ch-clickstream-ingestor',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2;
CREATE TABLE default.user_clicks (
    log_date Date,
    user_id String,
    timestamp DateTime,
    product_id String,
    warehouse_id String,
    pickup_point_id String,
    page_type String,
    category_id UInt32
) ENGINE = MergeTree
PARTITION BY log_date
PRIMARY KEY user_id
ORDER BY (user_id, timestamp)
SETTINGS index_granularity = 8192;
CREATE MATERIALIZED VIEW default.mv_user_clicks_transformer TO default.user_clicks AS
SELECT 
    toDate(timestamp) AS log_date,
    user_id,
    timestamp,
    product_id,
    warehouse_id,
    pickup_point_id,
    page_type,
    category_id
FROM default.user_clicks_queue;
CREATE TABLE s2_telemetry.web_performance_queue (
    user_id String,
    timestamp DateTime,
    status_code UInt16,
    page_load_ms UInt32,
    dns_resolution_ms UInt32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'telemetry.web_performance',
    kafka_group_name = 'ch-performance-ingestor',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2;
CREATE TABLE s2_telemetry.web_performance_logs (
    log_date Date,
    user_id String,
    timestamp DateTime,
    status_code UInt16,
    page_load_ms UInt32,
    dns_resolution_ms UInt32
) ENGINE = MergeTree
PARTITION BY log_date
PRIMARY KEY user_id
ORDER BY (user_id, timestamp)
SETTINGS index_granularity = 8192;
CREATE MATERIALIZED VIEW s2_telemetry.mv_web_performance_transformer TO s2_telemetry.web_performance_logs AS
SELECT 
    toDate(timestamp) AS log_date,
    user_id,
    timestamp,
    status_code,
    page_load_ms,
    dns_resolution_ms
FROM s2_telemetry.web_performance_queue;
CREATE TABLE s2_analytics.search_funnel_queue (
    user_id String,
    timestamp DateTime,
    action_type String,
    scroll_depth_items UInt16
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'commerce.search_funnel',
    kafka_group_name = 'ch-search-funnel-ingestor',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2;
CREATE TABLE s2_analytics.search_funnel_logs (
    log_date Date,
    user_id String,
    timestamp DateTime,
    action_type String,
    scroll_depth_items UInt16
) ENGINE = MergeTree
PARTITION BY log_date
PRIMARY KEY user_id
ORDER BY (user_id, timestamp)
SETTINGS index_granularity = 8192;
CREATE MATERIALIZED VIEW s2_analytics.mv_search_funnel_transformer TO s2_analytics.search_funnel_logs AS
SELECT 
    toDate(timestamp) AS log_date,
    user_id,
    timestamp,
    action_type,
    scroll_depth_items
FROM s2_analytics.search_funnel_queue;
CREATE TABLE IF NOT EXISTS s2_shipping.clickstream_scan_events_queue (
    event_id String,
    package_id String,
    order_id String,
    product_id String,
    timestamp UInt64, 
    status String,
    darkstore_id String,
    executor_id String,
    executor_type String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shipping.scan_events',
    kafka_group_name = 'ch-shipping-scan-ingestor',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    input_format_skip_unknown_fields = 1; 
CREATE TABLE IF NOT EXISTS s2_shipping.clickstream_scan_events (
    log_date Date,
    event_id String,
    package_id String,
    order_id String,
    product_id String,
    timestamp UInt64,
    status LowCardinality(String),
    darkstore_id String,
    executor_id String,
    executor_type LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_date)
PRIMARY KEY (log_date, order_id, event_id)
ORDER BY (log_date, order_id, event_id, timestamp)
SETTINGS index_granularity = 8192;
CREATE MATERIALIZED VIEW IF NOT EXISTS s2_shipping.mv_clickstream_scan_events_transformer 
TO s2_shipping.clickstream_scan_events AS
SELECT 
    toDate(toDateTime(timestamp)) AS log_date,
    event_id,
    package_id,
    order_id,
    product_id,
    timestamp,
    status,
    darkstore_id,
    executor_id,
    executor_type
FROM s2_shipping.clickstream_scan_events_queue;
