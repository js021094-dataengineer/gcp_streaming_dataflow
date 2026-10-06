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
