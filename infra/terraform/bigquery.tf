# Three datasets, one job each:
#   crypto_streaming  written by the Dataflow pipeline (bronze / silver / streamed gold) and the
#                     views that clean those tables up (trades_clean, trade_metrics_1m_latest)
#   crypto_history    written by the batch candle loader (backfill/load_klines.py)
#   crypto_analytics  views only: what dashboards and analysis read
resource "google_bigquery_dataset" "crypto" {
  dataset_id                 = "crypto_streaming"
  location                   = var.region
  description                = "Written by the streaming pipeline: Binance trades via Pub/Sub + Dataflow (bronze, silver, streamed gold) and the de-duplicating views over them."
  labels                     = local.labels
  delete_contents_on_destroy = true
  depends_on                 = [google_project_service.services]
}

resource "google_bigquery_dataset" "history" {
  dataset_id                 = "crypto_history"
  location                   = var.region
  description                = "Written by the batch backfill: historical Binance 1-minute candles (klines)."
  labels                     = local.labels
  delete_contents_on_destroy = true
  depends_on                 = [google_project_service.services]
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = "crypto_analytics"
  location                   = var.region
  description                = "Views only, for dashboards and analysis: streamed and historical data combined, range and time-of-day profiles."
  labels                     = local.labels
  delete_contents_on_destroy = true
  depends_on                 = [google_project_service.services]
}

locals {
  partition_expiration_ms = var.partition_expiration_days * 24 * 60 * 60 * 1000

  tables = {
    raw_events = {
      description     = "Bronze: every message exactly as received, for replay."
      partition_field = "ingest_ts"
      clustering      = ["event_type", "symbol"]
    }
    trades = {
      description     = "Silver: validated, typed Binance trades."
      partition_field = "trade_time"
      clustering      = ["symbol"]
    }
    dead_letter = {
      description     = "Messages that failed decoding, validation or the BigQuery write."
      partition_field = "processing_ts"
      clustering      = ["error_stage"]
    }
    trade_metrics_1m = {
      description     = "Gold: VWAP, OHLC and volume per symbol per 1-minute event-time window. Late data adds panes; read trade_metrics_1m_latest."
      partition_field = "window_start"
      clustering      = ["symbol"]
    }
  }
}

resource "google_bigquery_table" "tables" {
  for_each = local.tables

  dataset_id          = google_bigquery_dataset.crypto.dataset_id
  table_id            = each.key
  description         = each.value.description
  labels              = local.labels
  schema              = file("${local.schema_dir}/${each.key}.json")
  clustering          = each.value.clustering
  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = each.value.partition_field
    expiration_ms = local.partition_expiration_ms
  }
}

# Historical candles loaded by a batch job (docs/roadmap-kline-backfill.md). Tiny, and the whole point
# is old data, so unlike the streaming tables it has no partition expiration. It is not written by the
# pipeline, hence its own dataset.
resource "google_bigquery_table" "klines_1m" {
  dataset_id          = google_bigquery_dataset.history.dataset_id
  table_id            = "klines_1m"
  description         = "Gold, imported: Binance 1-minute candles in the trade_metrics_1m shape, loaded by the backfill job."
  labels              = local.labels
  schema              = file("${local.schema_dir}/klines_1m.json")
  clustering          = ["symbol"]
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }
}

# The pipeline writes at-least-once, so `trades` can hold rare duplicates (the same Pub/Sub
# message processed twice). Analytics should read this view: one row per (symbol, trade_id).
# trade_time is part of the PARTITION BY (duplicates share it) so filters on trade_time can
# still prune partitions.
resource "google_bigquery_table" "trades_clean" {
  dataset_id          = google_bigquery_dataset.crypto.dataset_id
  table_id            = "trades_clean"
  description         = "Silver, de-duplicated: one row per (symbol, trade_id) from trades. Query this, not trades."
  labels              = local.labels
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT *
      FROM `${var.project_id}.${google_bigquery_dataset.crypto.dataset_id}.trades`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, trade_id, trade_time
        ORDER BY processing_time, ingest_time
      ) = 1
    SQL
  }

  # The view is validated against the table at creation, so the table must exist first.
  depends_on = [google_bigquery_table.tables]
}

# The windowed KPIs can be emitted more than once per window (an on-time pane, then one more for
# every late trade within the allowed lateness; each later pane is a complete replacement), and
# the at-least-once sink can repeat a pane. This view keeps the latest pane per symbol and window.
resource "google_bigquery_table" "trade_metrics_1m_latest" {
  dataset_id          = google_bigquery_dataset.crypto.dataset_id
  table_id            = "trade_metrics_1m_latest"
  description         = "Gold, one row per (symbol, window): the latest pane of trade_metrics_1m. Query this, not the table."
  labels              = local.labels
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT *
      FROM `${var.project_id}.${google_bigquery_dataset.crypto.dataset_id}.trade_metrics_1m`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, window_start
        ORDER BY pane_index DESC, processing_ts DESC
      ) = 1
    SQL
  }

  depends_on = [google_bigquery_table.tables]
}

# One row per (symbol, 1-minute window) from both gold sources: the streamed windows and the imported
# Binance candles. The candle wins where both exist: validation showed the streamed row is never the
# more complete one (it is short of trades in the minutes where a pipeline session started or stopped).
# Streamed rows only fill the recent minutes that have not been backfilled yet. `source` says which.
resource "google_bigquery_table" "trade_metrics_1m_all" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "trade_metrics_1m_all"
  description         = "Gold, one row per (symbol, 1-minute window) from klines_1m and trade_metrics_1m_latest. The candle wins where both exist; source says which. Use this for history and time-of-day analysis."
  labels              = local.labels
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT * EXCEPT (priority)
      FROM (
        SELECT
          symbol, window_start, window_end, trade_count, volume, quote_volume, vwap,
          open, high, low, close, buy_volume, sell_volume,
          'binance_klines' AS source, 1 AS priority
        FROM `${var.project_id}.${google_bigquery_dataset.history.dataset_id}.klines_1m`
        UNION ALL
        SELECT
          symbol, window_start, window_end, trade_count, volume, quote_volume, vwap,
          open, high, low, close, buy_volume, sell_volume,
          'stream' AS source, 2 AS priority
        FROM `${var.project_id}.${google_bigquery_dataset.crypto.dataset_id}.trade_metrics_1m_latest`
      )
      QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, window_start ORDER BY priority) = 1
    SQL
  }

  depends_on = [
    google_bigquery_table.klines_1m,
    google_bigquery_table.trade_metrics_1m_latest,
  ]
}

# Price range per 1-minute window (high - low), derived in SQL so it applies to all existing
# windows. range_pct is relative to the window's VWAP, so it is comparable across symbols and price
# levels; range_per_musd is the range (in percent) per million quote-currency traded, a rough
# price-impact measure.
resource "google_bigquery_table" "trade_range_1m" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "trade_range_1m"
  description         = "Gold, one row per (symbol, window): high-low price range (absolute and as a share of VWAP) next to volume. Built from trade_metrics_1m_all, so it covers the imported candle history (2025 onwards) plus the streamed windows; source says which."
  labels              = local.labels
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT
        symbol,
        window_start,
        trade_count,
        volume,
        quote_volume,
        vwap,
        high,
        low,
        high - low AS range_abs,
        SAFE_DIVIDE(high - low, vwap) * 100 AS range_pct,
        SAFE_DIVIDE(SAFE_DIVIDE(high - low, vwap) * 100, quote_volume / 1e6) AS range_pct_per_musd,
        source
      FROM `${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}.trade_metrics_1m_all`
    SQL
  }

  depends_on = [google_bigquery_table.trade_metrics_1m_all]
}

# Time-of-day profiles for dashboards, computed in SQL over the whole combined history
# (trade_metrics_1m_all). Range = (high - low) / VWAP in percent. Rows are split by weekday/weekend
# (UTC day) and by US daylight saving time, because the US-driven spikes (8:30 and 9:30 US Eastern) move by
# one hour in UTC when the clocks change. Both views scan the whole history (about 165 MB per query),
# so keep the Data Studio freshness at 15 minutes or longer.
locals {
  profile_base_sql = <<-SQL
    SELECT
      symbol,
      IF(EXTRACT(DAYOFWEEK FROM window_start) IN (1, 7), 'weekend', 'weekday') AS day_type,
      DATETIME_DIFF(DATETIME(window_start, 'America/New_York'), DATETIME(window_start, 'UTC'), HOUR) = -4 AS us_dst,
      window_start,
      quote_volume,
      trade_count,
      SAFE_DIVIDE(high - low, vwap) * 100 AS range_pct
    FROM `${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}.trade_metrics_1m_all`
    WHERE quote_volume > 0
  SQL
}

resource "google_bigquery_table" "trade_profile_minute_of_day" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "trade_profile_minute_of_day"
  description         = "Gold profile: per symbol, weekday/weekend, US-DST flag and UTC minute of day (HH:MM), the average / median / 95th percentile 1-minute range in percent, average quote volume (millions) and trade count, over the whole history of trade_metrics_1m_all."
  labels              = local.labels
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      WITH m AS (
        ${local.profile_base_sql}
      )
      SELECT
        symbol, day_type, us_dst,
        FORMAT_TIMESTAMP('%H:%M', window_start) AS minute_of_day_utc,
        COUNT(*) AS minutes,
        AVG(range_pct) AS avg_range_pct,
        APPROX_QUANTILES(range_pct, 100)[OFFSET(50)] AS median_range_pct,
        APPROX_QUANTILES(range_pct, 100)[OFFSET(95)] AS p95_range_pct,
        CAST(AVG(quote_volume) AS FLOAT64) / 1e6 AS avg_musd,
        AVG(trade_count) AS avg_trade_count
      FROM m
      GROUP BY symbol, day_type, us_dst, minute_of_day_utc
    SQL
  }

  depends_on = [google_bigquery_table.trade_metrics_1m_all]
}

resource "google_bigquery_table" "trade_profile_hourly" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "trade_profile_hourly"
  description         = "Gold profile: per symbol, weekday/weekend, US-DST flag and UTC hour of day, the average / median / 95th percentile 1-minute range in percent, average quote volume (millions) and trade count, over the whole history of trade_metrics_1m_all."
  labels              = local.labels
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      WITH m AS (
        ${local.profile_base_sql}
      )
      SELECT
        symbol, day_type, us_dst,
        EXTRACT(HOUR FROM window_start) AS hour_utc,
        COUNT(*) AS minutes,
        AVG(range_pct) AS avg_range_pct,
        APPROX_QUANTILES(range_pct, 100)[OFFSET(50)] AS median_range_pct,
        APPROX_QUANTILES(range_pct, 100)[OFFSET(95)] AS p95_range_pct,
        CAST(AVG(quote_volume) AS FLOAT64) / 1e6 AS avg_musd,
        AVG(trade_count) AS avg_trade_count
      FROM m
      GROUP BY symbol, day_type, us_dst, hour_utc
    SQL
  }

  depends_on = [google_bigquery_table.trade_metrics_1m_all]
}
