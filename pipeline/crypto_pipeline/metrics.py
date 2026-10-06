"""Pure-Python aggregation of trades into per-window KPIs (VWAP, OHLC, volume).

Like `parsing.py` this module has no Apache Beam imports, so the arithmetic is unit tested
in milliseconds. The Beam `CombineFn` in `transforms.py` is a thin wrapper around it.

The accumulator holds one entry per trade, keyed by `trade_id` (it lives inside one symbol's
window, because the pipeline keys by symbol first). That makes de-duplication a property of
the data structure: adding the same trade twice, or merging two accumulators that both saw it,
collapses to one entry. This matters because late panes in ACCUMULATING mode re-deliver
elements, and the at-least-once pipeline can process the same Pub/Sub message twice.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable, Optional

NUMERIC_SCALE = 9
_NUMERIC_QUANTUM = Decimal(1).scaleb(-NUMERIC_SCALE)  # 0.000000001

# trade_id -> (trade_time_micros, price, quantity, quote_quantity, is_buyer_maker)
Accumulator = dict[int, tuple]


def _micros(value: Any) -> int:
    """Beam `Timestamp` objects carry `.micros`; plain ints are epoch milliseconds."""
    micros = getattr(value, "micros", None)
    if micros is not None:
        return int(micros)
    return int(value) * 1000


def _q(value: Decimal) -> Decimal:
    return value.quantize(_NUMERIC_QUANTUM)


def add_trade(acc: Accumulator, trade: dict[str, Any]) -> Accumulator:
    """Add one `trades` row to the accumulator (in place) and return it."""
    price = trade["price"]
    quantity = trade["quantity"]
    quote = trade.get("quote_quantity")
    if quote is None:
        quote = _q(price * quantity)
    entry = (_micros(trade["trade_time"]), price, quantity, quote, trade.get("is_buyer_maker"))
    # A repeated trade_id is the same trade; keep the first copy.
    acc.setdefault(trade["trade_id"], entry)
    return acc


def merge(accumulators: Iterable[Accumulator]) -> Accumulator:
    """Union of accumulators; duplicate trade ids collapse. Order independent."""
    merged: Accumulator = {}
    for acc in accumulators:
        for trade_id, entry in acc.items():
            merged.setdefault(trade_id, entry)
    return merged


def finalize(acc: Accumulator) -> Optional[dict[str, Any]]:
    """KPIs for one symbol and window, or None for an empty accumulator.

    open / close are the first / last trade by (trade time, trade id), so the result does
    not depend on arrival order. `buy_volume` is volume where the aggressor was the buyer
    (is_buyer_maker is False), `sell_volume` where the aggressor was the seller.
    """
    if not acc:
        return None

    ordered = sorted(acc.items(), key=lambda item: (item[1][0], item[0]))
    prices = [entry[1] for _, entry in ordered]
    volume = sum((entry[2] for _, entry in ordered), Decimal(0))
    quote_volume = sum((entry[3] for _, entry in ordered), Decimal(0))
    buy_volume = sum((entry[2] for _, entry in ordered if entry[4] is False), Decimal(0))
    sell_volume = sum((entry[2] for _, entry in ordered if entry[4] is True), Decimal(0))

    return {
        "trade_count": len(ordered),
        "volume": _q(volume),
        "quote_volume": _q(quote_volume),
        "vwap": _q(quote_volume / volume),
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "buy_volume": _q(buy_volume),
        "sell_volume": _q(sell_volume),
    }
