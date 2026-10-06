"""BigQuery schemas, shared with Terraform (single source of truth).

The JSON files in ./schemas are read by Terraform to create the tables and by
the pipeline to tell the Storage Write API how to encode rows.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

_SCHEMA_DIR = Path(__file__).parent / "schemas"

RAW_EVENTS = "raw_events"
TRADES = "trades"
DEAD_LETTER = "dead_letter"
TRADE_METRICS_1M = "trade_metrics_1m"


@lru_cache(maxsize=None)
def fields(table: str) -> tuple[dict[str, Any], ...]:
    with open(_SCHEMA_DIR / f"{table}.json", encoding="utf-8") as fh:
        return tuple(json.load(fh))


def table_schema(table: str) -> dict[str, Any]:
    """Schema in the {"fields": [...]} shape Beam's WriteToBigQuery expects."""
    return {
        "fields": [
            {"name": f["name"], "type": f["type"], "mode": f.get("mode", "NULLABLE")}
            for f in fields(table)
        ]
    }


def timestamp_fields(table: str) -> frozenset[str]:
    return frozenset(f["name"] for f in fields(table) if f["type"] == "TIMESTAMP")
