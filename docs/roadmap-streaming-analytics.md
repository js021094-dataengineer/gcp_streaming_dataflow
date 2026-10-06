# Roadmap: streaming analytics (windowed KPIs + dashboard)

Status: **deployed and verified live (session 4): the first Dataflow run stalled, and the
fixed-size accumulator fixed it.** On 2026-10-06 the windowed combine stage got stuck (`Stuck state:
workflow-msec-finish`, watermark frozen, no gold rows) while the silver path kept up. The first
accumulator held a dict of every trade; offline, on 178k real trades, that cost 93 s of coder
work for one 4,061-trade window when trades arrive one per bundle (a streaming runner re-reads
and re-writes the accumulator around every bundle). The accumulator is now fixed-size (0.4 KiB
instead of 674 KiB, 0.18 s for the same case). A second live run then worked: gold rows current
to within seconds, no stuck state, and 22 of 22 windows identical to a SQL recomputation from
`trades` (count, volume, VWAP, open, close, high, low). The dashboard (Looker Studio) is deliberately postponed. Statements
about Beam / Dataflow / BigQuery / Looker Studio behaviour are from memory and are marked
**(verify)** - check the current docs before relying on them.

Decisions taken: 1-minute windows only (a 5-minute roll-up can be a SQL view over
`trade_metrics_1m_latest`: sums add up, VWAP = total quote volume / total volume, open from the
first minute, close from the last); no early firings; allowed lateness 120 s.

## Goal

Show *streaming analytics* skills, not just ingestion: compute event-time windowed KPIs
inside the Beam pipeline, land them in a **gold** BigQuery table, and chart them.

Why in Beam and not only SQL: a `GROUP BY` minute query over `trades` is batch analytics on
streamed data (it recomputes at query time). Windows, watermarks, triggers, allowed lateness
and (later) de-duplication in Beam are the streaming skills this project is meant to demonstrate, and
the pipeline already carries what they need (event time from `event_ts`, deterministic `msg_id`).

Non-goals (for the first version): `bookTicker`/spread analytics, alerting, anything needing
more than the `trades` stream.

## KPIs

Per symbol, per 1-minute event-time window:

| Column | Definition |
|---|---|
| `trade_count` | number of trades (at-least-once duplicates, ~0.006%, are not removed) |
| `volume` | sum of `quantity` (base currency) |
| `quote_volume` | sum of `quote_quantity` |
| `vwap` | `quote_volume / volume` (the `trades.quote_quantity` column was added as the VWAP building block) |
| `open`, `high`, `low`, `close` | price of the first / max / min / last trade by `(trade_time, trade_id)` |
| `buy_volume`, `sell_volume` | volume by aggressor side; `is_buyer_maker = true` means the aggressor was a **seller** |

Average quantity per trade is `volume / trade_count` - derivable, no extra column needed.

Moving averages: derive them in SQL over the 1-minute table with window functions (cheap).
Beam `SlidingWindows(300, 60)` is an optional second step if the goal is to show sliding
windows in the pipeline itself.

## Design

```
ParseMessageFn --trades--> WriteTrades                       (unchanged, silver)
               \--trades--> FixedWindows(60 s) -> key by symbol -> combine (fixed-size accumulator) -> format
                            -> back to global window -> WriteTable(trade_metrics_1m) \-> rejects -> dead_letter
```

A new branch off `routed[TRADES_TAG]` in `pipeline/crypto_pipeline/pipeline.py`.

1. **Event time needs no re-stamping.** Elements read from Pub/Sub already carry `event_ts`
   (= trade time `T`) as their timestamp, and `ParDo` outputs keep it, so windows follow
   exchange time. Note that rows on `TRADES_TAG` already hold Beam `Timestamp` and `Decimal`
   values (see `_ms_to_beam_ts` in `transforms.py`) - the aggregation code must work with those.
2. **Duplicates: a known limitation for now.** The first full run showed at-least-once
   duplicates in `trades` (6 of ~105k rows, both `raw_events` and `trades` repeated 2-3 s
   apart); in the gold table a duplicate adds to the count and volume of its window (about
   0.006% of trades). History of this decision: (a) a `GroupByKey` on `(symbol, trade_id)`
   before the combine was rejected, because in ACCUMULATING mode a late pane of that first
   stage re-emits an element the second stage has already counted; (b) a dict keyed by
   `trade_id` inside the accumulator made de-duplication exact and idempotent, but its size
   grows with the window, and that stalled the first Dataflow run (see Status); (c) the current
   fixed-size accumulator cannot recognise a repeated trade. Exact de-duplication would need
   per-trade state that does not live in the combiner: a stateful `DoFn` keyed by
   `(symbol, trade_id)` with a "seen" flag and an event-time timer to clear it after window end
   plus allowed lateness, placed before the window so each trade is emitted once (which also
   removes the late-pane double-count worry). Do this only if exactness matters.
3. **Window and triggers.** `FixedWindows(60)`; `allowed_lateness` ~120 s;
   `AfterWatermark(late=AfterCount(1))`; `ACCUMULATING` mode. Start without early firings;
   add `early=AfterProcessingTime(10)` later if the dashboard should update inside the minute.
   Late firings re-emit the full window, so the output table gets several rows per window.
   Observed on 2026-10-06: every window also gets one extra `LATE` pane (pane_index 1), identical
   to its `ON_TIME` pane, about 150 s after the window end (window end + allowed lateness +
   watermark lag): our reading is that it is emitted when the window expires, not because a trade
   arrived late (a genuinely late trade would show a higher `trade_count`; none so far).
   Consumers should read the latest pane (`trade_metrics_1m_latest`).
4. **Aggregation.** One custom `CombineFn` (`TradeMetricsFn`, associative + commutative, as Beam
   requires). The accumulator is a **fixed-size** tuple: count, volume, quote volume, buy and
   sell volume, high, low, and the open and close trade keyed by `(trade_time, trade_id)`, so
   out-of-order arrival gives the same open/close. Fixed size matters because a streaming runner
   persists the accumulator and re-reads it around every bundle: the state cost per bundle must
   not grow with the window. Keep `Decimal` throughout; `vwap` is computed at extraction and
   quantized to NUMERIC scale 9. The arithmetic lives in the **pure module**
   `crypto_pipeline/metrics.py` (no Beam imports), like `parsing.py`; `transforms.py` only wraps it.
5. **Formatting.** A `DoFn` with `beam.DoFn.WindowParam` and `beam.DoFn.PaneInfoParam` that
   emits `window_start`, `window_end`, `pane_timing` (`EARLY`/`ON_TIME`/`LATE`), `pane_index`
   and `processing_ts`, converting timestamps with `_ms_to_beam_ts`.
6. **Sink.** Reuse `WriteTable` (Storage Write API, at-least-once) and route
   `failed_rows_with_errors` to `dead_letter` via `failed_write_to_dead_letter`, exactly like
   `raw_events` and `trades`. Because retries can repeat a pane, consumers read the *latest
   pane per window* (view below), which also covers late updates.
7. **Idle symbols.** A window with no trades produces no row. Charts should treat a missing
   minute as zero volume, not as an error.

## Schema and infrastructure

New schema file `pipeline/crypto_pipeline/schemas/trade_metrics_1m.json` (single source of
truth for Terraform and the pipeline):

`symbol` STRING REQUIRED, `window_start` TIMESTAMP REQUIRED (partition), `window_end`
TIMESTAMP REQUIRED, `trade_count` INTEGER, `volume`, `quote_volume`, `vwap`, `open`, `high`,
`low`, `close`, `buy_volume`, `sell_volume` NUMERIC, `pane_timing` STRING, `pane_index`
INTEGER, `processing_ts` TIMESTAMP.

| File | Change |
|---|---|
| `pipeline/crypto_pipeline/schemas/trade_metrics_1m.json` | new |
| `pipeline/crypto_pipeline/bq_schemas.py` | constant `TRADE_METRICS_1M` |
| `pipeline/crypto_pipeline/metrics.py` | new: pure aggregation logic |
| `pipeline/crypto_pipeline/transforms.py` | combine + format transforms |
| `pipeline/crypto_pipeline/pipeline.py` | `--metrics_table` option and the new branch |
| `infra/terraform/bigquery.tf` | add `trade_metrics_1m` to `local.tables` (partition `window_start`, cluster `symbol`) |
| `infra/terraform/outputs.tf`, `scripts/up.sh` | output and pass the new table parameter; check how `scripts/build.sh` / the Flex Template metadata declare parameters (verify) |
| `tests/` | see Testing |

### Views (Terraform `google_bigquery_table` with a `view` block)

- `trades_clean`: `SELECT * FROM trades QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, trade_id ORDER BY processing_time) = 1`.
  Gives silver the de-duplicated contract; ad-hoc analytics and the dashboard read this
  instead of repeating the `QUALIFY` clause (README currently asks every query to do that).
- `trade_metrics_1m_latest`: latest pane per `(symbol, window_start)`:
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, window_start ORDER BY pane_index DESC, processing_ts DESC) = 1`.

## Dashboard

- **Looker Studio** (free): connect to the `trade_metrics_1m_latest` view; time-series chart of
  VWAP per symbol, bar chart of volume, scorecards for trade count and last price. Set the
  data freshness to the shortest option. (verify current options)
- The pipeline only runs during sessions (cost), so the dashboard shows stored history.
  Capture screenshots or a short GIF from a live session into `docs/img/` and embed them in
  the README - that is what a reader sees when the pipeline is off.
- Access: the data source runs with the owner's credentials unless configured otherwise;
  note this if the report is shared. Keep queries on the partition column to keep scanned
  bytes (and cost) tiny.
- Alternatives needing hosting (not recommended first): Grafana with a BigQuery plugin, Streamlit.

## Scheduling

Nothing to schedule for the Beam path: each window is emitted when the watermark passes it.
The views are computed at query time. A scheduled query is only needed if a materialised
table is wanted (BigQuery scheduled queries have a minimum interval of a few minutes, verify);
Beam sliding windows cover the "moving average" need without one.

## Testing

Unit (no network, `make test`), using Beam `TestPipeline` + `TestStream` to control the
watermark:
- `metrics.py`: combine of known trades gives the expected count/volume/VWAP/OHLC; out-of-order
  input gives identical open/close; `merge` is order-independent; `Decimal` precision (9 dp).
- Duplicates (same `(symbol, trade_id)` twice) are counted again: the tests document this known
  limitation, and should be flipped if a stateful de-duplication stage is added.
- The accumulator stays small: its pickled size does not grow with the number of trades.
- Window assignment by trade time at the minute boundary (59.999 s vs 60.000 s).
- Late data: within allowed lateness -> `LATE` pane with updated totals; beyond it -> dropped.
- Empty window -> no row.
- Output rows match the schema file exactly (as `test_rows_match_bigquery_schemas_exactly` does for the others).

Live acceptance (a short session, ask before `make up`):
1. Run >= 10 min; `make down`, wait for `Drained` (the drain also fires the remaining windows).
2. Recompute the same windows in SQL from `trades_clean` (`TIMESTAMP_TRUNC(trade_time, MINUTE)`)
   and compare to `trade_metrics_1m_latest` for closed windows: counts and volumes must match.
3. `dead_letter` has no new `bq_write` rows.

## Deploy

Per `CLAUDE.md`: `make test`, then `make infra` (new table + views; ask first), `make build`
(pipeline changed), `make down` (wait for Drained), `make up`. Added cost is small: one more
sink and a light aggregation on the existing single worker.

## Open decisions

- Final-only (on-time + late) vs. early firings for a live-updating chart.
- Allowed lateness value, and whether the REST backfill note's late rows should be included.
- Whether to also add the Beam sliding-window variant or keep moving averages in SQL.
- Window size (1 min only, or also 5 min via a second table or a `window_size` column).

## Effort

Roughly 1-2 days including tests, the views and a first dashboard.
