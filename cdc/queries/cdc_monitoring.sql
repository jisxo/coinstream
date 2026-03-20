-- 1) freshness: source_updated_at -> ingested_at 지연
SELECT
    round(max(dateDiff('millisecond', source_updated_at, ingested_at)) / 1000.0, 3) AS freshness_sec_max,
    round(avg(dateDiff('millisecond', source_updated_at, ingested_at)) / 1000.0, 3) AS freshness_sec_avg,
    max(ingested_at) AS last_ingested_at
FROM coinstream_cdc.cdc_raw_events
WHERE ingested_at >= now() - INTERVAL 1 HOUR;


-- 2) consumer lag: raw 최신 offset - state_applier consumed offset
WITH raw_latest AS (
    SELECT
        kafka_topic AS topic,
        kafka_partition AS partition,
        max(source_offset) AS latest_offset
    FROM coinstream_cdc.cdc_raw_events
    GROUP BY topic, partition
),
applied AS (
    SELECT
        topic,
        partition,
        max(offset) AS consumed_offset
    FROM coinstream_cdc.cdc_consumer_offsets FINAL
    WHERE consumer_name = 'cdc_state_applier'
    GROUP BY topic, partition
)
SELECT
    r.topic,
    r.partition,
    r.latest_offset,
    coalesce(a.consumed_offset, 0) AS consumed_offset,
    (r.latest_offset - coalesce(a.consumed_offset, 0)) AS consumer_lag
FROM raw_latest r
LEFT JOIN applied a
    ON r.topic = a.topic
   AND r.partition = a.partition
ORDER BY r.topic, r.partition;


-- 3) duplicate count/rate
WITH duplicate_agg AS (
    SELECT
        sum(greatest(cnt - 1, 0)) AS duplicate_count
    FROM (
        SELECT event_id, count() AS cnt
        FROM coinstream_cdc.cdc_raw_events
        WHERE ingested_at >= now() - INTERVAL 24 HOUR
        GROUP BY event_id
    )
),
total_agg AS (
    SELECT count() AS total_count
    FROM coinstream_cdc.cdc_raw_events
    WHERE ingested_at >= now() - INTERVAL 24 HOUR
)
SELECT
    t.total_count,
    d.duplicate_count,
    round(d.duplicate_count / nullIf(t.total_count, 0), 4) AS duplicate_rate
FROM total_agg t
CROSS JOIN duplicate_agg d;


-- 4) delete ratio
SELECT
    count() AS total_events,
    countIf(op_type = 'd') AS delete_events,
    round(countIf(op_type = 'd') / nullIf(count(), 0), 4) AS delete_ratio
FROM coinstream_cdc.cdc_raw_events
WHERE ingested_at >= now() - INTERVAL 24 HOUR;


-- 5) mart hourly trend
SELECT
    bucket_start,
    order_count,
    paid_order_count,
    canceled_order_count,
    total_amount,
    load_ts
FROM coinstream_cdc.mart_order_metrics_hourly FINAL
ORDER BY bucket_start DESC
LIMIT 48;


-- 6) current state row count / deleted count
SELECT
    count() AS total_state_rows,
    countIf(is_deleted = 1) AS deleted_state_rows,
    countIf(is_deleted = 0) AS active_state_rows
FROM coinstream_cdc.order_current_state FINAL;
