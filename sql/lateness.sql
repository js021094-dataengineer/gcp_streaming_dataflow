-- How late do trades reach Dataflow? Picks a value for ALLOWED_LATENESS_SECONDS (transforms.py).
-- delay = processing_time - trade_time, per trade. This is an upper bound on lateness: a delayed
-- trade only counts as "late" for a window if the watermark has already passed that window's end.
-- Run (since = first minute AFTER the startup backlog drained, UTC):
--   bq query --project_id=gcp-streaming-dataflow --use_legacy_sql=false --format=pretty \
--     --parameter=since:TIMESTAMP:"2026-10-07 10:10:00 UTC" < sql/lateness.sql
SELECT
  symbol,
  COUNT(*) AS trades,
  ROUND(APPROX_QUANTILES(delay_s, 1000)[OFFSET(500)], 2) AS p50_s,
  ROUND(APPROX_QUANTILES(delay_s, 1000)[OFFSET(990)], 2) AS p99_s,
  ROUND(APPROX_QUANTILES(delay_s, 1000)[OFFSET(999)], 2) AS p999_s,
  ROUND(MAX(delay_s), 2) AS max_s,
  COUNTIF(delay_s > 30) AS over_30s,
  COUNTIF(delay_s > 60) AS over_60s,
  COUNTIF(delay_s > 120) AS over_120s
FROM (
  SELECT
    symbol,
    TIMESTAMP_DIFF(processing_time, trade_time, MILLISECOND) / 1000 AS delay_s
  FROM crypto_streaming.trades_clean
  WHERE trade_time >= @since
)
GROUP BY symbol
ORDER BY symbol
