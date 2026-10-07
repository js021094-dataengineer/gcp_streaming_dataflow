"""Pure helpers for the historical 1-minute candle (kline) backfill.

No network, no BigQuery and no Apache Beam imports, so everything here is unit tested offline.
The loader that calls Binance and BigQuery (a later step) only wires these functions together.
See docs/roadmap-kline-backfill.md.

A Binance kline is a list (field order from the Binance docs, **verify** against the current docs)::

    [open_time_ms, open, high, low, close, volume, close_time_ms, quote_volume,
     trade_count, taker_buy_base_volume, taker_buy_quote_volume, ignored]

Prices and volumes are decimal strings. Output rows use the column names of
`schemas/klines_1m.json`: NUMERIC values as plain decimal strings, timestamps as UTC ISO strings.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterator, Optional, Sequence

from crypto_pipeline.parsing import NUMERIC_QUANTUM, ParseError, _to_numeric

MINUTE_MS = 60_000
MAX_CANDLES_PER_REQUEST = 1000  # Binance limit per klines call (verify)
SOURCE = "binance_klines"

_KLINE_FIELDS = 12


class KlineError(ValueError):
    """A candle that is malformed or fails a sanity check."""


def floor_minute(ts_ms: int) -> int:
    return ts_ms - ts_ms % MINUTE_MS


def last_closed_minute_start(now_ms: int) -> int:
    """Open time of the newest candle that is already complete (the running minute is excluded)."""
    return floor_minute(now_ms) - MINUTE_MS


def request_windows(
    start_ms: int, end_ms: int, limit: int = MAX_CANDLES_PER_REQUEST
) -> Iterator[tuple[int, int]]:
    """Split [start_ms, end_ms) into klines requests of at most `limit` candles.

    Yields (start_time, end_time) pairs of candle open times, both minute aligned and both
    included: pass them as startTime / endTime. `start_ms` is rounded down to the minute and
    `end_ms` is exclusive.
    """
    if limit < 1:
        raise ValueError("limit must be >= 1")
    cursor = floor_minute(start_ms)
    stop = floor_minute(end_ms)
    while cursor < stop:
        last_open = min(cursor + (limit - 1) * MINUTE_MS, stop - MINUTE_MS)
        yield cursor, last_open
        cursor = last_open + MINUTE_MS


def _iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _num(name: str, value: Any) -> Decimal:
    try:
        return _to_numeric(name, value)
    except ParseError as exc:
        raise KlineError(str(exc)) from exc


def _fmt(value: Decimal) -> str:
    return format(value, "f")


def candle_to_row(symbol: str, candle: Sequence[Any], loaded_ms: int) -> dict[str, Any]:
    """Map one raw kline to a `klines_1m` row, or raise KlineError."""
    if not isinstance(candle, (list, tuple)) or len(candle) < _KLINE_FIELDS - 1:
        raise KlineError(f"expected a kline list with {_KLINE_FIELDS} fields, got {candle!r}")

    open_time = candle[0]
    trade_count = candle[8]
    if not isinstance(open_time, int) or isinstance(open_time, bool) or open_time < 0:
        raise KlineError(f"bad open time: {open_time!r}")
    if open_time % MINUTE_MS:
        raise KlineError(f"open time is not on a minute boundary: {open_time}")
    if not isinstance(trade_count, int) or isinstance(trade_count, bool) or trade_count < 0:
        raise KlineError(f"bad trade count: {trade_count!r}")

    open_, high, low, close = (_num(n, candle[i]) for n, i in
                               (("open", 1), ("high", 2), ("low", 3), ("close", 4)))
    volume = _num("volume", candle[5])
    quote_volume = _num("quote_volume", candle[7])
    buy_volume = _num("buy_volume", candle[9])

    if low > high or not (low <= open_ <= high) or not (low <= close <= high):
        raise KlineError(f"OHLC out of order at {open_time}: o={open_} h={high} l={low} c={close}")
    if min(open_, high, low, close) <= 0:
        raise KlineError(f"non-positive price at {open_time}")
    if volume < 0 or quote_volume < 0 or buy_volume < 0:
        raise KlineError(f"negative volume at {open_time}")
    if buy_volume > volume:
        raise KlineError(f"buy volume {buy_volume} exceeds volume {volume} at {open_time}")
    if (volume == 0) != (trade_count == 0):
        raise KlineError(f"volume {volume} and trade count {trade_count} disagree at {open_time}")

    vwap: Optional[Decimal] = None
    if volume > 0:
        vwap = (quote_volume / volume).quantize(NUMERIC_QUANTUM)

    return {
        "symbol": symbol.upper(),
        "window_start": _iso(open_time),
        "window_end": _iso(open_time + MINUTE_MS),
        "trade_count": trade_count,
        "volume": _fmt(volume),
        "quote_volume": _fmt(quote_volume),
        "vwap": None if vwap is None else _fmt(vwap),
        "open": _fmt(open_),
        "high": _fmt(high),
        "low": _fmt(low),
        "close": _fmt(close),
        "buy_volume": _fmt(buy_volume),
        "sell_volume": _fmt(volume - buy_volume),
        "source": SOURCE,
        "loaded_ts": _iso(loaded_ms // 1000 * 1000),
    }


def candles_to_rows(
    symbol: str, candles: Sequence[Sequence[Any]], loaded_ms: int, now_ms: int
) -> list[dict[str, Any]]:
    """Map a klines response to rows: drop the still-running minute, sort, de-duplicate by open time."""
    cutoff = last_closed_minute_start(now_ms)
    by_open: dict[int, dict[str, Any]] = {}
    for candle in candles:
        row = candle_to_row(symbol, candle, loaded_ms)
        open_time = candle[0]
        if open_time <= cutoff:
            by_open[open_time] = row
    return [by_open[key] for key in sorted(by_open)]


def missing_minutes(open_times: Sequence[int], start_ms: int, end_ms: int) -> list[int]:
    """Minute open times in [start_ms, end_ms) that have no candle."""
    present = set(open_times)
    return [t for t in range(floor_minute(start_ms), floor_minute(end_ms), MINUTE_MS) if t not in present]
