# Roadmap: historical 1-minute candle (kline) backfill

Status: **idea / not implemented.** This note holds the design so it can be built later.
Statements about Binance's REST API and public data are from memory and are marked **(verify)** -
check the current Binance docs before relying on them.

## Problem

The streaming pipeline only has data for the hours it ran. Questions such as "which minutes of a
24 h day have the largest price range" or "how does range relate to volume across many days"
need many continuous days. Running Dataflow around the clock costs about $6 per day (about
$40-50 per week), and the live stream cannot be replayed.

## Goal

Load historical 1-minute candles for BTCUSDT and ETHUSDT into BigQuery with a small batch job
(no Dataflow), in the same shape as the gold layer, so the range / volume analysis and the
Data Studio dashboard can use days or months of history.

Non-goals:
- A second live stream (`@kline_1m`). Live data already arrives through the trade pipeline.
- Trade-level backfill of trade-id gaps (that is `docs/roadmap-rest-backfill.md`).
- Replacing the streaming gold layer. The streamed table stays the "real-time" source.

## What a candle gives us **(verify)**

`GET /api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=...&endTime=...&limit=1000` returns, per
candle: open time, open, high, low, close, base volume, close time, quote volume, number of trades,
taker-buy base volume, taker-buy quote volume. Up to 1000 candles per call (about 16.7 h of 1-minute
candles), so one symbol-month is about 45 calls. Public market data, no API key. Binance blocks US
IPs, so run it from the existing europe-west6 setup or the owner's machine, not a US host.
Daily/monthly zipped candle files at `data.binance.vision` are an alternative source for long
ranges **(verify)**.

Mapping to `trade_metrics_1m` columns:

| Gold column | From candle |
|---|---|
| `symbol` | request parameter |
| `window_start` | open time (ms -> TIMESTAMP) |
| `window_end` | `window_start + 1 min` |
| `trade_count` | number of trades |
| `volume` / `quote_volume` | base volume / quote volume |
| `vwap` | `quote_volume / volume` (same definition as the pipeline) |
| `open`, `high`, `low`, `close` | same |
| `buy_volume` | taker-buy base volume |
| `sell_volume` | `volume - buy_volume` |
| `pane_timing`, `pane_index` | not applicable (constant, or dropped) |
| `processing_ts` | load time |

Not available from candles: individual trade sizes and ids, and the exact trade-time ordering the
silver layer has.

## Design

1. **Own table, not the streamed table.** New table `klines_1m` (Terraform, schema in
   `pipeline/crypto_pipeline/schemas/klines_1m.json` as the single source of truth), with the gold
   column names above plus `source = 'binance_klines'`. Imported data stays distinguishable from
   data computed by our pipeline, and re-loading it can never touch the streaming table.
2. **Combined view.** `trade_metrics_1m_all`: union of `trade_metrics_1m_latest` and `klines_1m`,
   one row per `(symbol, window_start)`. Where both have a window, the streamed row wins (our own
   numbers), otherwise the candle row. A `source` column says which. `trade_range_1m` and the
   5-minute roll-up can be pointed at this view later.
3. **Batch loader.** A small Python script (e.g. `backfill/klines.py`, plus `make backfill-klines
   FROM=... TO=...`), run on demand:
   - Paginate by `startTime` in 1000-candle steps, respect rate limits (back off on HTTP 429/418),
     never request the still-open current minute.
   - Load with a BigQuery load job (or `MERGE` from a staging table) keyed on
     `(symbol, window_start)`, so re-runs and overlapping ranges are idempotent - no duplicates.
   - Prices and volumes parsed as `Decimal` and written as NUMERIC, like the pipeline
     (the NUMERIC scale constants live in `parsing.py`).
4. **Pure functions + tests.** Candle -> row mapping and the pagination logic are pure and unit
   tested with recorded sample responses, no network. Offline `make test` stays offline.

## Validation (do this before trusting the history)

Compare the candles with our streamed gold layer on the windows both have (about 127 per
symbol at the time of writing):
- `open`, `high`, `low`, `close`: should match exactly except in minutes where our producer lost
  trades (known gaps, listed in CLAUDE.md).
- `volume`, `quote_volume`, `trade_count`: should match, except for gaps (candles are complete,
  we can be short) and the ~0.006% duplicates the gold layer keeps.
- Report counts of exact matches vs. differences and inspect the differences before the view
  treats candles as equivalent.

## Cost

Candle download and BigQuery loading are free at this size (load jobs are free, a month of
1-minute candles for two symbols is about 90k rows, a few MB). No Dataflow, no VM time. The only
cost is the owner's time and, if run from the VM, nothing extra.

## Limits and open questions

- Candles give no trade-level detail, so the exact duplicate / gap analysis of the silver layer
  does not apply to this data.
- Candle boundaries and our event-time windows should both be UTC minutes **(verify)**; the
  validation step above confirms it.
- How far back to go. Retention is not an issue at this size, so a few months is easy; decide
  based on what the analysis needs (several weeks covers day-of-week patterns).
- Time-of-day analysis wants UTC and local-time views; keep stored timestamps in UTC.
- Whether to add a scheduled job (Cloud Run job / Scheduler) to keep the table topped up. Not
  needed for the first version; on-demand is enough.

## Suggested order

1. Schema + Terraform table + combined view (`make infra`, no billable resources).
2. Pure mapping/pagination code with tests (`make test`).
3. Loader script, run for a short range (e.g. one day) and run the validation queries.
4. Backfill the wanted history, then point `trade_range_1m` and the Data Studio charts at the
   combined view and run the time-of-day / range-vs-volume analysis.
