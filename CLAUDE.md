# CLAUDE.md

Context for Claude Code sessions in this repo. Read this first, then `README.md`.
**Keep the "Progress log" and "Next steps" sections up to date at the end of every session.**

## Project

Portfolio project showing streaming data engineering on Google Cloud:
Binance WebSocket (BTCUSDT, ETHUSDT `@trade`) → producer on an e2-micro VM → Pub/Sub topic `crypto-raw`
→ Dataflow (Apache Beam, Python, Flex Template) → BigQuery dataset `crypto_streaming`
(`raw_events` bronze, `trades` silver, `dead_letter`). Current scope: **ingestion into BigQuery only**;
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

## Next steps

1. First full run: `make up` → after ~10 min `make status`; check Dataflow console (job graph, counters
   `crypto/trades_ok`, `crypto/dead_lettered`), and BigQuery `SELECT * FROM crypto_streaming.trades ORDER BY trade_time DESC LIMIT 10`.
   Most likely failure point: job launch (Flex Template, Java cross-language Storage Write API, IAM) → read job logs.
2. Verify data quality with README queries: duplicates on `(symbol, trade_id)`, trade_id gaps, latency, dead letters.
3. `make down`, confirm nothing running.
4. Later: event-time VWAP / moving averages (windows, allowed lateness), `bookTicker` stream,
   monitoring dashboard + alerts, CI (GitHub Actions). Optional: budget kill-switch.

## Known gotchas

- First VM boot installs packages (~2 min); later boots ~1 min.
- WSL clock can drift (local `ingest_ts` was ~1.8 s behind exchange time) - fix with `wsl --shutdown`. VM clock is fine.
- `make producer-local` `msgs_per_s` includes connect time - not a real rate.
- pytest warning `cannot collect test class 'TestPipeline'` is harmless.
