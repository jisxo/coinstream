-- Late event trend (최근 24시간)
SELECT
  window_start,
  late_event_count,
  throughput,
  freshness_seconds
FROM coinstream.mart_pipeline_health_1m
WHERE window_start >= now() - INTERVAL 24 HOUR
ORDER BY window_start;
