"""Pure-Python aggregation of trades into per-window KPIs (VWAP, OHLC, volume).

Like `parsing.py` this module has no Apache Beam imports, so the arithmetic is unit tested
in milliseconds. The Beam `CombineFn` in `transforms.py` is a thin wrapper around it.

The accumulator has a FIXED size: a tuple of counts, sums, high / low and the open / close
trade (keyed by trade time and id). That matters in a streaming runner: Dataflow persists
the accumulator between bundles and decodes / re-encodes it around every one, so an
accumulator that grows with the number of trades makes every bundle slower as the window
fills (a dict of every trade did exactly that: offline, one trade per bundle took 93 s of
coder work for a 4,061-trade window, and the first Dataflow run stalled in the combine).

Known limitation: trades are NOT de-duplicated here. The at-least-once pipeline can process
the same Pub/Sub message twice (6 of ~105k trades in the first long run, about 0.006%), and a
duplicate then adds to the count and volume of its window. Exact de-duplication needs per-trade
state, e.g. a stateful DoFn upstream of the window (see docs/roadmap-streaming-analytics.md).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable, Optional

NUMERIC_SCALE = 9
_NUMERIC_QUANTUM = Decimal(1).scaleb(-NUMERIC_SCALE)  # 0.000000001

# (count, volume, quote_volume, buy_volume, sell_volume, high, low,
#  open_key, open_price, close_key, close_price)  with key = (trade_time_micros, trade_id)
Accumulator = tuple

_ZERO = Decimal(0)
_EMPTY: Accumulator = (0, _ZERO, _ZERO, _ZERO, _ZERO, None, None, None, None, None, None)


def empty() -> Accumulator:
    return _EMPTY


def _micros(value: Any) -> int:
    """Beam `Timestamp` objects carry `.micros`; plain ints are epoch milliseconds."""
    micros = getattr(value, "micros", None)
    if micros is not None:
        return int(micros)
    return int(value) * 1000


def _q(value: Decimal) -> Decimal:
    return value.quantize(_NUMERIC_QUANTUM)


def add_trade(acc: Accumulator, trade: dict[str, Any]) -> Accumulator:
    """Return a new accumulator that also includes one `trades` row."""
    price = trade["price"]
    quantity = trade["quantity"]
    quote = trade.get("quote_quantity")
    if quote is None:
        quote = _q(price * quantity)
    maker = trade.get("is_buyer_maker")
    key = (_micros(trade["trade_time"]), trade["trade_id"])

    count, volume, quote_volume, buy, sell, high, low, open_key, open_price, close_key, close_price = acc
    if open_key is None or key < open_key:
        open_key, open_price = key, price
    if close_key is None or key > close_key:
        close_key, close_price = key, price
    return (
        count + 1,
        volume + quantity,
        quote_volume + quote,
        buy + quantity if maker is False else buy,
        sell + quantity if maker is True else sell,
        price if high is None or price > high else high,
        price if low is None or price < low else low,
        open_key,
        open_price,
        close_key,
        close_price,
    )


def _combine(a: Accumulator, b: Accumulator) -> Accumulator:
    if a[0] == 0:
        return b
    if b[0] == 0:
        return a
    open_key, open_price = (a[7], a[8]) if a[7] <= b[7] else (b[7], b[8])
    close_key, close_price = (a[9], a[10]) if a[9] >= b[9] else (b[9], b[10])
    return (
        a[0] + b[0],
        a[1] + b[1],
        a[2] + b[2],
        a[3] + b[3],
        a[4] + b[4],
        max(a[5], b[5]),
        min(a[6], b[6]),
        open_key,
        open_price,
        close_key,
        close_price,
    )


def merge(accumulators: Iterable[Accumulator]) -> Accumulator:
    """Combine accumulators (associative and commutative, as Beam requires)."""
    merged = _EMPTY
    for acc in accumulators:
        merged = _combine(merged, acc)
    return merged


def finalize(acc: Accumulator) -> Optional[dict[str, Any]]:
    """KPIs for one symbol and window, or None for an empty accumulator.

    open / close are the first / last trade by (trade time, trade id), so the result does
    not depend on arrival order. `buy_volume` is volume where the aggressor was the buyer
    (is_buyer_maker is False), `sell_volume` where the aggressor was the seller.
    """
    count, volume, quote_volume, buy, sell, high, low, _, open_price, _, close_price = acc
    if count == 0:
        return None
    return {
        "trade_count": count,
        "volume": _q(volume),
        "quote_volume": _q(quote_volume),
        "vwap": _q(quote_volume / volume),
        "open": open_price,
        "high": high,
        "low": low,
        "close": close_price,
        "buy_volume": _q(buy),
        "sell_volume": _q(sell),
    }
