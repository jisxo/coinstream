CREATE DATABASE IF NOT EXISTS coinstream_cdc;

CREATE TABLE IF NOT EXISTS coinstream_cdc.cdc_raw_events
(
    event_id String,
    source_table LowCardinality(String),
    op_type LowCardinality(String),
    source_pk String,
    source_updated_at DateTime64(3, 'UTC'),
    source_tx_id Nullable(UInt64),
    source_offset UInt64,
    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    dedup_key String,
    before_json String,
    after_json String,
    is_duplicate UInt8 DEFAULT 0,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (kafka_topic, kafka_partition, source_offset, event_id);

CREATE TABLE IF NOT EXISTS coinstream_cdc.cdc_consumer_offsets
(
    consumer_name LowCardinality(String),
    topic LowCardinality(String),
    partition Int32,
    offset UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (consumer_name, topic, partition);

CREATE TABLE IF NOT EXISTS coinstream_cdc.order_current_state
(
    order_id UInt64,
    user_id UInt64,
    status LowCardinality(String),
    amount Decimal(18, 2),
    source_updated_at DateTime64(3, 'UTC'),
    is_deleted UInt8 DEFAULT 0,
    load_ts DateTime64(3, 'UTC') DEFAULT now64(3),
    source_offset UInt64 DEFAULT 0,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(source_updated_at)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS coinstream_cdc.mart_order_metrics_hourly
(
    bucket_start DateTime('UTC'),
    order_count UInt64,
    paid_order_count UInt64,
    canceled_order_count UInt64,
    total_amount Decimal(18, 2),
    load_ts DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(load_ts)
PARTITION BY toDate(bucket_start)
ORDER BY bucket_start;
