"""Backfill historical Binance 1-minute candles into BigQuery `klines_1m`.

    python backfill/load_klines.py --symbols BTCUSDT,ETHUSDT --from 2026-10-01 --to 2026-10-07
    python backfill/load_klines.py --from 2026-10-06 --dry-run        # fetch and count, write nothing

Fetches candles from Binance's public REST API (no API key), maps them with `klines.py`, loads them
into a temporary staging table and MERGEs into `klines_1m` keyed on (symbol, window_start), so
re-runs and overlapping ranges never create duplicates. See docs/roadmap-kline-backfill.md.

Needs BigQuery credentials (`gcloud auth application-default login`) unless --dry-run. Binance
blocks US IPs, so run it from Europe, like the rest of the project.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import klines
from crypto_pipeline import bq_schemas

BINANCE_URL = "https://api.binance.com/api/v3/klines"
DEFAULT_DATASET = "crypto_streaming"
TARGET_TABLE = bq_schemas.KLINES_1M
STAGE_TABLE = "klines_1m_stage"
REQUEST_PAUSE_S = 0.15  # stay far below Binance's request-weight limit (verify)
MAX_RETRIES = 6


# ---------------------------------------------------------------- pure helpers

def parse_utc(text: str) -> int:
    """'2026-10-01' or '2026-10-01T10:30' (UTC) -> epoch milliseconds."""
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"cannot parse {text!r}; use YYYY-MM-DD or YYYY-MM-DDTHH:MM (UTC)")


def build_merge_sql(project: str, dataset: str, columns: list[str], lo_iso: str, hi_iso: str) -> str:
    """MERGE staging -> target on (symbol, window_start). The window_start range prunes partitions."""
    target = f"`{project}.{dataset}.{TARGET_TABLE}`"
    stage = f"`{project}.{dataset}.{STAGE_TABLE}`"
    key = {"symbol", "window_start"}
    updates = ",\n    ".join(f"{c} = S.{c}" for c in columns if c not in key)
    cols = ", ".join(columns)
    vals = ", ".join(f"S.{c}" for c in columns)
    return f"""MERGE {target} T
USING {stage} S
ON T.symbol = S.symbol AND T.window_start = S.window_start
   AND T.window_start BETWEEN TIMESTAMP('{lo_iso}') AND TIMESTAMP('{hi_iso}')
WHEN MATCHED THEN UPDATE SET
    {updates}
WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"""


def fetch_json(
    get: Callable[..., Any], params: dict[str, Any], sleep: Callable[[float], None] = time.sleep,
    retries: int = MAX_RETRIES,
) -> Any:
    """GET with retries on HTTP 429 / 418 / 5xx and network errors (exponential backoff, honours Retry-After)."""
    delay = 1.0
    last = "no attempt"
    for _ in range(retries):
        try:
            resp = get(BINANCE_URL, params=params, timeout=20)
        except Exception as exc:  # network errors: retry
            last = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                return resp.json()
            last = f"HTTP {resp.status_code}: {resp.text[:200]}"
            if resp.status_code not in (418, 429) and resp.status_code < 500:
                raise RuntimeError(f"Binance rejected the request ({last})")
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = max(delay, float(retry_after))
        sleep(delay)
        delay = min(delay * 2, 60.0)
    raise RuntimeError(f"Binance request failed after {retries} attempts ({last})")


def fetch_symbol(
    get: Callable[..., Any], symbol: str, start_ms: int, end_ms: int, now_ms: int, loaded_ms: int,
    sleep: Callable[[float], None] = time.sleep, log: Callable[[str], None] = print,
) -> list[dict[str, Any]]:
    """All closed candles of `symbol` in [start_ms, end_ms) as rows (end is capped at the last closed minute)."""
    end_ms = min(end_ms, klines.last_closed_minute_start(now_ms) + klines.MINUTE_MS)
    rows: list[dict[str, Any]] = []
    windows = list(klines.request_windows(start_ms, end_ms))
    for i, (first, last) in enumerate(windows, 1):
        raw = fetch_json(get, {
            "symbol": symbol, "interval": "1m", "startTime": first, "endTime": last,
            "limit": klines.MAX_CANDLES_PER_REQUEST,  # the API default is only 500
        }, sleep=sleep)
        rows += klines.candles_to_rows(symbol, raw, loaded_ms, now_ms)
        if i % 10 == 0 or i == len(windows):
            log(f"  {symbol}: {i}/{len(windows)} requests, {len(rows)} candles")
        sleep(REQUEST_PAUSE_S)
    return rows


# ---------------------------------------------------------------- BigQuery

def load_rows(project: str, dataset: str, rows: list[dict[str, Any]], log: Callable[[str], None] = print) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    fields = bq_schemas.fields(TARGET_TABLE)
    schema = [bigquery.SchemaField(f["name"], f["type"], mode=f.get("mode", "NULLABLE")) for f in fields]
    stage_id = f"{project}.{dataset}.{STAGE_TABLE}"
    try:
        job = client.load_table_from_json(
            rows, stage_id,
            job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"),
        )
        job.result()
        log(f"  staged {len(rows)} rows")
        lo, hi = rows[0]["window_start"], rows[-1]["window_start"]
        sql = build_merge_sql(project, dataset, [f["name"] for f in fields], lo, hi)
        merged = client.query(sql)
        merged.result()
        log(f"  merged into {TARGET_TABLE}: {merged.num_dml_affected_rows} rows inserted/updated")
    finally:
        client.delete_table(stage_id, not_found_ok=True)


# ---------------------------------------------------------------- CLI

def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbols", default=os.environ.get("SYMBOLS", "btcusdt,ethusdt"),
                        help="comma separated (default: SYMBOLS from config.env)")
    parser.add_argument("--from", dest="start", required=True, help="start, UTC, inclusive (YYYY-MM-DD[THH:MM])")
    parser.add_argument("--to", dest="end", help="end, UTC, exclusive (default: now)")
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID"), help="default: PROJECT_ID from config.env")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--dry-run", action="store_true", help="fetch and count only, write nothing to BigQuery")
    args = parser.parse_args(argv)

    import requests

    now_ms = int(time.time() * 1000)
    start_ms = parse_utc(args.start)
    end_ms = parse_utc(args.end) if args.end else now_ms
    if end_ms <= start_ms:
        parser.error("--to must be after --from")
    if not args.dry_run and not args.project:
        parser.error("--project (or PROJECT_ID) is required unless --dry-run")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    session = requests.Session()
    for symbol in symbols:
        print(f"{symbol}: {datetime.fromtimestamp(start_ms / 1000, timezone.utc):%Y-%m-%d %H:%M} -> "
              f"{datetime.fromtimestamp(end_ms / 1000, timezone.utc):%Y-%m-%d %H:%M} UTC")
        rows = fetch_symbol(session.get, symbol, start_ms, end_ms, now_ms, loaded_ms=int(time.time() * 1000))
        if not rows:
            print("  no closed candles in the range")
            continue
        opens = [int(datetime.fromisoformat(r["window_start"].replace("Z", "+00:00")).timestamp() * 1000) for r in rows]
        gaps = klines.missing_minutes(opens, start_ms, min(end_ms, opens[-1] + klines.MINUTE_MS))
        print(f"  {len(rows)} candles, {len(gaps)} missing minutes")
        if args.dry_run:
            continue
        load_rows(args.project, args.dataset, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
