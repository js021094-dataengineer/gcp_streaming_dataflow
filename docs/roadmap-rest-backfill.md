# Roadmap: REST backfill for trade gaps

Status: **idea / not implemented.** This note holds everything needed to build it later.
Statements about Binance's REST API are from memory and are marked **(verify)** - check the
current Binance docs before relying on them.

## Problem

The Binance WebSocket `@trade` stream is live-only: while the producer is disconnected
(reconnect, restart, network blip, VM stopped) the trades that happen are never delivered
and cannot be replayed through the stream.

Observed in the first full run (2026-10-02): the producer restarted ~09:03 and
`trades` had one gap per symbol - 64 missing BTCUSDT and 6 missing ETHUSDT trade ids over
~7 s. `raw_events` held nothing for that window, so the loss happened before Pub/Sub
(producer offline), not in Dataflow.

Trade ids are sequential per symbol, so a gap is exactly detectable: if the last seen id is
`N` and the next frame has id `M > N + 1`, trades `N+1 .. M-1` are missing.

## Goal

After any disconnect, fetch the missing trades from Binance's REST API and publish them to
the same Pub/Sub topic so they flow through the unchanged Beam pipeline into
`raw_events` / `trades`.

Non-goals: backfilling hours of downtime between sessions (see "Limits"), `bookTicker`
(no trade ids, no sequence to backfill by).

## Why it fits the existing design

- **Idempotent.** `build_message` in `producer/producer.py` already derives a deterministic
  `msg_id = binance:<SYMBOL>:trade:<trade id>`. A backfilled trade built the same way gets
  the identical id, so any overlap with live frames is dropped by Dataflow (`id_label`).
- **No pipeline change needed.** If the REST trade is wrapped into the same JSON envelope
  as a stream frame, `parsing.decode_payload` / `parse_trade` handle it as-is.
- **Event time stays correct.** `event_ts` = trade time `T`, so late rows carry their real
  exchange time. Fine today (no windowing). For the planned VWAP / moving averages this
  becomes the *allowed lateness* question: backfilled rows arrive seconds to minutes late.
- **Bronze keeps the audit trail.** Set the `source` attribute to `binance-rest` (the
  `raw_events.source` column already exists - no schema change) to tell backfilled rows
  from live ones.

## Endpoints (verify all)

| Endpoint | Gives | Notes |
|---|---|---|
| `GET /api/v3/trades?symbol=&limit=` | latest trades, up to 1000 | no `fromId`; only covers a short gap (1000 BTC trades is ~20-100 s) |
| `GET /api/v3/historicalTrades?symbol=&fromId=&limit=` | trades from an id onward, up to 1000 per call | needs an API key header (`X-MBX-APIKEY`, read-only market data, **no signature**) |
| `GET /api/v3/aggTrades` | aggregated trades | **do not use**: its ids (`a`) are aggregate ids, not the trade ids (`t`) our `trades.trade_id` and `msg_id` use |

`historicalTrades` with `fromId` is the primary mechanism. Also check whether the
market-data-only host `data-api.binance.vision` serves it without a key.

REST trade shape: `{"id", "price", "qty", "quoteQty", "time", "isBuyerMaker", "isBestMatch"}`.
Rate limits are weight-based per IP (a request is ~25 weight, limit in the thousands per
minute) - far above what a gap fill needs, but honour `429`/`418` + `Retry-After`.

## Mapping REST trade -> stream frame

Build exactly what the WebSocket would have sent, then pass it through the existing
`build_message()`:

```json
{"stream": "btcusdt@trade",
 "data": {"e": "trade", "s": "BTCUSDT", "t": <id>, "p": "<price>", "q": "<qty>",
          "T": <time>, "m": <isBuyerMaker>}}
```

- Omit `E` (event time): REST doesn't have it, `parse_trade` treats it as optional, so
  `trades.event_time` is NULL for backfilled rows (a second marker besides `source`).
- `p` / `q` are already decimal strings in REST - pass through untouched (NUMERIC safety
  in `_to_numeric` is unchanged).
- `ingest_ts` = backfill time (`clock_ms()`), so exchange->producer latency for these rows
  is large by design; exclude `source = 'binance-rest'` from latency dashboards.

## Design

Keep the conversion in a **pure function** (`rest_trade_to_frame`) next to
`build_message` so it stays unit-testable and the producer remains a forwarder of
Binance-shaped data.

1. **Track state.** `last_trade_id[symbol]` updated in `consume()` for every `trade` frame
   (parse `data.t` from the frame; `build_message` already does this parsing).
2. **Detect.** `detect_gap(last_id, new_id) -> Optional[(from_id, to_id)]`, called for every
   trade frame. Catches the reconnect gap *and* any mid-stream jump.
3. **Fill.** On a gap, spawn an `asyncio` task that pages `historicalTrades` from
   `fromId=last+1` (limit 1000) until it reaches `to_id`, converts each trade, and calls
   `sink.publish`. The live loop keeps running meanwhile; ordering between live and
   backfill messages doesn't matter (event-time + dedupe).
4. **Bound it.** Skip and log (`WARNING backfill skipped: gap=<n> exceeds MAX_BACKFILL_TRADES`)
   when the gap is larger than a cap (suggested default ~50,000 trades). Never block or
   delay the live stream for a backfill; use a small concurrency limit (1 task per symbol).
5. **Observe.** Add `gaps_detected`, `trades_backfilled`, `backfill_skipped`,
   `backfill_errors` to `Stats` and the 60 s stats line.
6. **Config (env vars).** `BACKFILL_ENABLED` (default `true`), `MAX_BACKFILL_TRADES`,
   `BINANCE_REST_BASE` (default `https://api.binance.com`), `BINANCE_API_KEY` (optional,
   see below).

### Variant B (alternative / complement): separate gap-filler job

Instead of the producer, a small batch job finds gaps with the README gap query on
`trades` and fills them from REST. Pros: producer stays 100 % dumb, works across VM
stops. Cons: another component to deploy and schedule, rows land minutes later. Reasonable
as a later add-on for gaps the in-producer fill skipped.

## Limits and decisions to make

- **Between sessions.** After `make down` / `make up` the producer process restarts and
  `last_trade_id` is lost; the gap spans the whole downtime (hours = millions of trades).
  Options: (a) accept it and document (recommended: backfilling downtime is not the goal),
  (b) persist a checkpoint (GCS object / metadata) and rely on the cap to skip huge gaps.
- **API key.** `historicalTrades` needs a read-only key header. If used, store it in
  Secret Manager (grant `crypto-producer` accessor, enable the API in Terraform) - never in
  `config.env` committed to git or in VM metadata in clear text. If `data-api.binance.vision`
  works without a key, skip all of this.
- **Region.** REST calls come from the same `europe-west6` VM; Binance blocks US IPs for
  REST as well, so nothing changes there.
- **Dedupe window.** Dataflow's `id_label` dedupe only holds for a limited window (minutes).
  That's fine: the filled range is exact (`last+1 .. new-1`), so it doesn't overlap live
  data. BigQuery at-least-once duplicates are still handled by the `(symbol, trade_id)`
  dedupe in analytics queries.

## Files to touch

| File | Change |
|---|---|
| `producer/producer.py` | `detect_gap`, `rest_trade_to_frame`, REST client + `backfill()` task, stats counters, config |
| `producer/requirements.txt` | HTTP client (`aiohttp` or stdlib `urllib` in a thread) |
| `tests/test_producer.py` | see Testing |
| `infra/terraform/*` | only if using Secret Manager for an API key |
| `README.md` | document the behaviour in "Design decisions" and the gap-query note |
| `pipeline/` | **no change** (hence no `make build`) |

Deploy: `make infra` (uploads producer code), then restart the VM (startup script runs
only on boot). Per `CLAUDE.md`: ask before `make infra`.

## Testing

Unit (no network):
- `detect_gap`: consecutive ids -> None; jump -> exact range; first frame (no state) -> None;
  id going backwards/duplicate -> None.
- `rest_trade_to_frame` -> `build_message` -> `parsing.process_message`: yields a valid
  `trades` row; `msg_id` equals the one a live frame with the same id produces; `event_time`
  is None; `source` is `binance-rest`.
- `backfill()` with a fake REST client: paging across multiple 1000-trade pages, exact
  `[from, to]` bounds, cap respected, `429` retry, error counted but live loop unaffected.

Live acceptance (costs a short session, ask first):
1. `make up`, let it run a few minutes.
2. On the VM force a disconnect (e.g. `sudo systemctl restart producer`).
3. Run the README gap query - expect **zero** gaps for the window after the restart
   (state is lost on a process restart, so test the *reconnect* path instead, e.g. block the
   network briefly or lower `STALE_AFTER_S`), and `raw_events` contains rows with
   `source = 'binance-rest'`.
4. `make down`; confirm VM `TERMINATED` and no Dataflow job.

## Effort / cost

Roughly a day of work including tests. No new billable infrastructure (REST calls are
free; Secret Manager free tier if a key is needed).
