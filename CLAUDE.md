# CLAUDE.md

Context for Claude Code sessions in this repo. Read this first, then `README.md`.
**Keep the "Progress log" and "Next steps" sections up to date at the end of every session.**

## Project

Portfolio project showing streaming data engineering on Google Cloud:
Binance WebSocket (BTCUSDT, ETHUSDT `@trade`) → producer on an e2-micro VM → Pub/Sub topic `crypto-raw`
→ Dataflow (Apache Beam, Python, Flex Template) → BigQuery dataset `crypto_streaming`
(`raw_events` bronze, `trades` silver, `trades_clean` view = silver de-duplicated - query this, `dead_letter`).
Current scope: **ingestion into BigQuery only**;
analytics (VWAP, moving averages) come later.

## Environment (important)

- Owner works on **Windows + WSL (Ubuntu)**. Repo path: Windows `C:\Users\jschu\Documents\Github\gcp_streaming_dataflow`,
  WSL `/mnt/c/Users/jschu/Documents/Github/gcp_streaming_dataflow`.
- **Run all `make`, `gcloud`, `terraform`, Python commands inside WSL.** `make`, the bash scripts and `.venv/bin/...`
  don't work in PowerShell. gcloud + terraform are installed and logged in inside WSL (user `jannis94102@gmail.com`).
- WSL mounts C: with `metadata` (set in `/etc/wsl.conf`) - needed so `python3 -m venv` works on `/mnt/c`.
- Local Python in WSL is 3.14; the cloud image uses 3.11. Code must work on both.
- Line endings: `.gitattributes` forces LF. Scripts/`startup.sh` break with CRLF - never introduce CRLF.
- Windows git in this repo has `core.filemode false` (otherwise every script shows as "modified" because NTFS has no
  executable bit). Scripts must stay executable (mode 755) in git - don't commit mode changes.
- Git push works from PowerShell (Windows credential manager). WSL git may not have credentials configured.

## GCP setup (already done)

| Item | Value |
|---|---|
| Project | `gcp-streaming-dataflow` (billing linked, budget alert 20 in billing currency) |
| Region / zone | `europe-west6` / `europe-west6-a` - keep everything here; Binance blocks US IPs |
| Terraform state | `gs://gcp-streaming-dataflow-tfstate` |
| Pipeline bucket | `gs://gcp-streaming-dataflow-pipeline` (Flex Template at `templates/crypto-pipeline.json`, producer code at `producer/`) |
| Image repo | `europe-west6-docker.pkg.dev/gcp-streaming-dataflow/dataflow` |
| Pub/Sub | topic `crypto-raw`, subscription `crypto-raw-dataflow` (1-day retention, no expiry) |
| Producer VM | `crypto-producer`, e2-micro, Debian 12, runs `producer.service` via systemd |
| Service accounts | `crypto-producer`, `crypto-dataflow`, `crypto-build` |

Config lives in `config.env` (git-ignored); template in `config.env.example`.

## Commands

```bash
make test            # unit tests (23, all passing)
make producer-local  # 20 live trades to stdout, no GCP
make infra           # terraform apply (also uploads producer code + startup script to VM metadata)
make build           # Cloud Build image + Flex Template (only needed after changes in pipeline/)
make up              # START billing: launch Dataflow job + start VM
make down            # STOP billing: stop VM, then drain Dataflow
make status          # VM / jobs / rows in last 15 min
make logs            # producer logs from Cloud Logging
```

Start/stop only the producer (no Dataflow):
`gcloud compute instances start|stop crypto-producer --zone europe-west6-a --project gcp-streaming-dataflow`

Startup-script progress on the VM:
`gcloud compute instances get-serial-port-output crypto-producer --zone europe-west6-a --project gcp-streaming-dataflow | grep -i startup | tail -15`

## Rules for Claude

- **Cost:** Dataflow is the expensive part (~$0.20-0.30/h). Ask before `make up`, `make infra` (apply), `make destroy`
  or anything that creates billable resources. Always end a working session with `make down` and confirm
  VM = TERMINATED and no active Dataflow job.
- Run `make test` before `make build` / committing pipeline changes.
- BigQuery schemas in `pipeline/crypto_pipeline/schemas/*.json` are the single source of truth for Terraform AND the pipeline.
- Producer stays a "dumb pipe" (forward frames unchanged + attributes). Business logic belongs in `parsing.py`.
- Changing `producer/startup.sh` or `producer/*.py` → `make infra`, then restart the VM (startup script runs only on boot).
- Changing `pipeline/` → `make build`, then `make down` (wait for Drained) and `make up`.

## Progress log

**2026-10-01 (session 1)**
- Project designed and built; pushed to GitHub (commits `ae87c6f`, `28079cb`, `1a3d711`).
- WSL Ubuntu set up with make, python3-venv, gcloud, terraform.
- `make producer-local` OK (live trades from Binance), `make test` 23/23 passed.
- `make bootstrap` OK, `make infra` OK (43 resources), `make build` OK (image + Flex Template in GCS).
- Bug fixed: `startup.sh` checked for venv wrongly → now checks `dpkg -s python3-venv` (commit `1a3d711`).
- Producer tested in the cloud **without Dataflow**: connected to Binance, ~35-75 trades/s, 0 publish errors,
  0 reconnects. ~17k messages left unacked in `crypto-raw-dataflow` (expire ~2026-10-02 20:30 CEST).
- End state: VM `TERMINATED`, **Dataflow never launched yet**.

**2026-10-02 (session 2) - first full run with Dataflow**
- `make up` OK: Flex Template launched without fixes, job ran on 1 x n1-standard-2 worker. Ran ~10 min, `make down` OK.
  End state verified: job `Drained`, VM `TERMINATED`, no worker VMs.
- Data landed in `raw_events` and `trades` (Storage Write API, rows visible in the streaming buffer).
- Backlog from the session-1 producer test (~60k messages) was consumed in ~2 min; event time from `event_ts` is
  honoured (rows with `trade_time` from the previous day were processed fine).
- Steady-state latency exchange -> BigQuery: p50 ~1-4 s, p99 ~4-9 s at ~90-100 trades/s. Exchange -> producer p50 ~125 ms.
  The first minutes show huge latency only because of the old backlog and the 3-5 min worker startup.
- Gap check: one gap per symbol (64 BTCUSDT, 6 ETHUSDT trade ids, ~7 s). `raw_events` is empty for that window, so the
  producer was offline: it received nothing from ~09:03:02 and logged a graceful stop + reconnect at 09:03:07-09.
  Likely cause (from code + timestamps, not verified on the VM): `producer.service` was `enable`d, so systemd started it at
  boot (first DNS lookup failed), then `startup.sh` ended with an unconditional `systemctl restart` that killed it; the
  stop took ~5 s (websocket close handshake, `close_timeout=5`) plus ~1.5 s to reconnect.
  Fix committed (`7e1ac1a`): `startup.sh` now runs `systemctl disable producer.service` (deployed in session 3).
  Binance's WebSocket cannot replay, so those trades are lost unless backfilled (see roadmap note).
- Data quality: 0 duplicate `(symbol, trade_id)` keys in `trades` (last 24 h); `dead_letter` empty.
- Observed: `make status` printed nothing under "BigQuery - last 15 minutes" although rows existed; first the Dataflow
  job list was also empty (fixed itself on the next run). Not yet investigated.
- Added `docs/roadmap-rest-backfill.md` (design only, nothing implemented) and a README roadmap bullet (commit `fe623f5`).

**2026-10-03 (session 3) - 1 h 10 min run (17:00 -> ~18:09 UTC)**
- `make infra` applied (plan: 0 add / 1 change / 0 destroy, only the VM's `startup-script` metadata). Tables `trades` and
  `raw_events` were emptied beforehand with `DELETE ... WHERE 1=1`. `make up` -> ~1 h 10 min -> `make down`.
  End state verified: job `Drained`, VM `TERMINATED`, no worker VMs.
- First boot after the deploy still double-started the producer (connect 17:01:31, flush 17:01:45, reconnect 17:01:46) -
  expected, because the disk still had the unit enabled; that boot's script ran `disable`. **A second boot is not verified yet.**
  Resulting gap: 5 ETHUSDT + 25 BTCUSDT trade ids (last trade 17:01:38-39, flush 6 s later - consistent with the ~5 s
  websocket close wait). No other gaps all session; 0 reconnects, 0 publish errors after boot.
- Market was quiet (Saturday): producer 3-80 msgs/s, ~29 trades/s in the busiest 10 min. First rows reached BigQuery ~6.5 min
  after the job launch (worker startup), not 3-5 min.
- Latency: exchange->producer p50 110 ms / p99 399 ms; producer->Pub/Sub p50 84 ms / p99 118 ms (max 197 ms, no clock skew);
  Pub/Sub->BigQuery via Dataflow p50 ~0.3-0.8 s, p99 up to ~3.4 s (last 10 min before stop).
- Data quality at 18:08: 91,268 `raw_events` / 91,269 `trades` (1-row difference = in-flight), `dead_letter` empty.
  **6 duplicate `(symbol, trade_id)` keys**, all present twice in BOTH `raw_events` and `trades` (`raw_copies = 2`), in two
  incidents (5 trades at 17:10:22 and 1 at 17:15:11), copies processed 2-3 s apart; none after 17:15. Likely cause (inference,
  not verified): a Dataflow bundle retry re-running the DoFn with the at-least-once sink - `id_label` dedupes at the read only.
  README already says analytics must dedupe; silver `trades` itself is not deduplicated.
- Fixed `scripts/logs.sh` (commit `a7da6fd`): `--order=asc` + `--limit` kept the OLDEST entries, so new lines were cut off.
- Added `docs/roadmap-streaming-analytics.md` (design only; commit `2a7da8f`).

**2026-10-06 (session 4) - second-boot check, `trades_clean` view, pipeline walk-through**
- Verified the `startup.sh` fix on a second boot (VM only, no Dataflow, 09:13-09:18 UTC): exactly one `connecting` ~45 s after
  start, no flush / double start, 0 errors. VM stopped again (`TERMINATED`, no jobs). Cost: a few cents. Its ~6.4k published
  messages wait in the subscription (1-day retention) for the next Dataflow session.
- Duplicate analysis: all 6 duplicate pairs share the same `msg_id` AND `pubsub_message_id`, `ingest_ts` and `publish_ts`; only
  `processing_ts` differs (2-3 s). So one Pub/Sub message was processed twice by Dataflow - not a producer double-publish.
  Pub/Sub redelivery vs bundle retry is still undetermined (ids 21917296661541752..58 are contiguous but two of them were not duplicated).
- Added the `trades_clean` view (`google_bigquery_table.trades_clean` in `bigquery.tf`, applied with `make infra`: 1 added).
  It keeps one row per `(symbol, trade_id, trade_time)`, earliest `processing_time` first. Checked: `trades` 104,983 rows vs
  `trades_clean` 104,977 (= the 6 duplicates), 0 duplicate keys left. README tables and queries now use the view.
- Walked through the pipeline code with the owner; points worth remembering: `id_label` is applied by Dataflow's native Pub/Sub source
  at read time only, so a retried bundle can still write twice; the Flex Template spec has no parameter metadata (parameters are
  passed through to `main.py` as flags); gRPC Storage Write API rows are queryable immediately and DML works on recent rows
  (the earlier "can't delete buffered rows" remark was the old streaming-insert rule).
- Looked up BigQuery ingestion pricing (search summary only, page fetch was truncated, unverified): Storage Write API ~$0.025/GiB with
  2 TiB/month free vs legacy streaming inserts ~$0.05/GiB - negligible at this volume either way.

## Next steps

1. Optional: find the cause of the duplicates - Dataflow worker logs (warnings/retries) around 17:10:20-30 and 17:15:09-15 UTC on 2026-10-03.
2. Fix `scripts/status.sh` so the BigQuery section prints rows (run its query by hand to see why it is empty); consider
   pointing it at `trades_clean`.
3. Consider ordering in `up.sh`: start the VM only once the worker is up (worker takes ~6.5 min), to avoid the startup backlog.
4. Make `consume()` react to `stop` immediately and shorten the websocket close wait, so any restart loses less.
5. Later: streaming analytics - windowed VWAP/OHLC in Beam + Looker Studio chart (`docs/roadmap-streaming-analytics.md`);
   REST backfill of trade-id gaps (`docs/roadmap-rest-backfill.md`); `bookTicker` stream; monitoring dashboard + alerts;
   CI (GitHub Actions). Optional: budget kill-switch.

## Known gotchas

- First VM boot installs packages (~2 min); later boots ~1 min.
- WSL clock can drift (local `ingest_ts` was ~1.8 s behind exchange time) - fix with `wsl --shutdown`. VM clock is fine.
- `make producer-local` `msgs_per_s` includes connect time - not a real rate.
- pytest warning `cannot collect test class 'TestPipeline'` is harmless.
- Dataflow job takes a few minutes to go `Draining` -> `Drained` after `make down`; the worker VM disappears first, the job state lags.
- `make logs` only shows the last 30 min (`--freshness=30m`) and can take a minute to return - run it in the background.
- Timestamps in `raw_events`: `event_ts` = Binance trade time, `ingest_ts` = producer received the frame, `publish_ts` = Pub/Sub accepted
  it (server-side), `processing_ts` = Dataflow processed it. In `trades` they are `trade_time`, `ingest_time`, `processing_time`.
- `make infra` (apply) needs an interactive `yes`; non-interactive runs stop at the prompt - run it in the user's own WSL terminal.
  While an apply waits at that prompt it already holds the Terraform state lock, so `make plan` fails until it finishes.
- `bq query` with a backtick-quoted `project.dataset.table` inside `wsl bash -lc "..."` loses the backticks (shell command
  substitution). Put the SQL in a file and redirect it (`< file.sql`), or use `dataset.table` without the project.
