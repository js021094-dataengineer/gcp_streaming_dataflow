resource "google_bigquery_dataset" "crypto" {
  dataset_id                 = "crypto_streaming"
  location                   = var.region
  description                = "Binance market data ingested via Pub/Sub + Dataflow."
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
    # Historical candles loaded by a batch job (docs/roadmap-kline-backfill.md). Tiny, and the
    # whole point is old data, so it is exempt from the partition expiration.
    klines_1m = {
      description     = "Gold, imported: Binance 1-minute candles in the trade_metrics_1m shape, loaded by the backfill job. Not written by the streaming pipeline."
      partition_field = "window_start"
      clustering      = ["symbol"]
      keep_forever    = true
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
    expiration_ms = try(each.value.keep_forever, false) ? null : local.partition_expiration_ms
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

# 5-minute roll-up of the 1-minute gold layer, computed in SQL (no extra pipeline). Built from
# trade_metrics_1m_latest so the extra panes are already gone. VWAP is re-derived from the sums
# (quote_volume / volume), never averaged from the per-minute VWAPs; open / close are the first /
# last 1-minute window's values. A bucket that is still filling holds fewer than 5 minutes
# (see minutes_in_bucket).
resource "google_bigquery_table" "trade_metrics_5m" {
  dataset_id          = google_bigquery_dataset.crypto.dataset_id
  table_id            = "trade_metrics_5m"
  description         = "Gold, one row per (symbol, 5-minute bucket): roll-up of trade_metrics_1m_latest. Good source for dashboards."
  labels              = local.labels
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT
        symbol,
        TIMESTAMP_BUCKET(window_start, INTERVAL 5 MINUTE) AS bucket_start,
        COUNT(*) AS minutes_in_bucket,
        SUM(trade_count) AS trade_count,
        SUM(volume) AS volume,
        SUM(quote_volume) AS quote_volume,
        SAFE_DIVIDE(SUM(quote_volume), SUM(volume)) AS vwap,
        ARRAY_AGG(open ORDER BY window_start ASC LIMIT 1)[OFFSET(0)] AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        ARRAY_AGG(close ORDER BY window_start DESC LIMIT 1)[OFFSET(0)] AS close,
        SUM(buy_volume) AS buy_volume,
        SUM(sell_volume) AS sell_volume
      FROM `${var.project_id}.${google_bigquery_dataset.crypto.dataset_id}.trade_metrics_1m_latest`
      GROUP BY symbol, bucket_start
    SQL
  }

  depends_on = [google_bigquery_table.trade_metrics_1m_latest]
}

# Price range per 1-minute window (high - low), derived in SQL so it applies to all existing
# windows. range_pct is relative to the window's VWAP, so it is comparable across symbols and price
# levels; range_per_musd is the range (in percent) per million quote-currency traded, a rough
# price-impact measure.
resource "google_bigquery_table" "trade_range_1m" {
  dataset_id          = google_bigquery_dataset.crypto.dataset_id
  table_id            = "trade_range_1m"
  description         = "Gold, one row per (symbol, window): high-low price range (absolute and as a share of VWAP) next to volume. Built from trade_metrics_1m_latest."
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
        SAFE_DIVIDE(SAFE_DIVIDE(high - low, vwap) * 100, quote_volume / 1e6) AS range_pct_per_musd
      FROM `${var.project_id}.${google_bigquery_dataset.crypto.dataset_id}.trade_metrics_1m_latest`
    SQL
  }

  depends_on = [google_bigquery_table.trade_metrics_1m_latest]
}
