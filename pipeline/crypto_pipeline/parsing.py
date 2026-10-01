"""Pure-Python parsing and validation of Binance messages.

Deliberately free of Apache Beam imports so the business rules can be unit
tested in milliseconds and reused elsewhere (e.g. a backfill job that replays
the bronze table).

All timestamps are handled as integer epoch **milliseconds** here; the Beam
layer converts them to `apache_beam.utils.timestamp.Timestamp` right before
writing to BigQuery.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

# BigQuery NUMERIC = precision 38, scale 9.
NUMERIC_SCALE = 9
_NUMERIC_QUANTUM = Decimal(1).scaleb(-NUMERIC_SCALE)  # 0.000000001
_NUMERIC_MAX_INTEGER_DIGITS = 29

# Sanity bounds for exchange timestamps (ms since epoch).
_MIN_PLAUSIBLE_TS_MS = 1_483_228_800_000  # 2017-01-01, before Binance existed
_MAX_FUTURE_SKEW_MS = 24 * 60 * 60 * 1000  # reject anything > 1 day ahead

SUPPORTED_EVENT_TYPES = frozenset({"trade"})


class ParseError(Exception):
    """A message that cannot be turned into a typed row.

    `stage` tells you *where* it failed ("decode", "route", "validate"),
    which ends up in the dead-letter table for easy triage.
    """

    def __init__(self, stage: str, message: str):
        super().__init__(message)
        self.stage = stage


@dataclass
class ParsedMessage:
    """Everything the pipeline needs to emit for one Pub/Sub message."""

    raw: dict[str, Any]
    trade: Optional[dict[str, Any]] = None
    error: Optional[dict[str, Any]] = None
    event_type: str = "unknown"
    extra: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _to_int_ms(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_numeric(name: str, value: Any) -> Decimal:
    """Parse a Binance decimal string into a BigQuery-NUMERIC-safe Decimal."""
    if not isinstance(value, (str, int)):
        raise ParseError("validate", f"{name} must be a decimal string, got {type(value).__name__}")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ParseError("validate", f"{name} is not a number: {value!r}") from exc
    if not number.is_finite():
        raise ParseError("validate", f"{name} is not finite: {value!r}")
    exponent = number.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -NUMERIC_SCALE:
        # More than 9 decimals would be silently rounded by BigQuery - refuse.
        stripped = number.normalize()
        if stripped.as_tuple().exponent < -NUMERIC_SCALE:
            raise ParseError("validate", f"{name} has more than {NUMERIC_SCALE} decimals: {value!r}")
        number = stripped
    if number.adjusted() >= _NUMERIC_MAX_INTEGER_DIGITS:
        raise ParseError("validate", f"{name} is too large for NUMERIC: {value!r}")
    return number


def _check_ts(name: str, ts_ms: Optional[int], now_ms: int, required: bool) -> Optional[int]:
    if ts_ms is None:
        if required:
            raise ParseError("validate", f"{name} is missing")
        return None
    if ts_ms < _MIN_PLAUSIBLE_TS_MS or ts_ms > now_ms + _MAX_FUTURE_SKEW_MS:
        raise ParseError("validate", f"{name} out of plausible range: {ts_ms}")
    return ts_ms


def decode_payload(data: bytes) -> tuple[str, dict[str, Any], Optional[str]]:
    """Decode raw bytes into (text, inner_event, stream_name).

    Binance combined streams wrap events as {"stream": "...", "data": {...}};
    raw streams send the event itself. Both are accepted.
    """
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ParseError("decode", f"payload is not valid UTF-8: {exc}") from exc
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ParseError("decode", f"payload is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise ParseError("decode", f"payload must be a JSON object, got {type(obj).__name__}")

    if "data" in obj and isinstance(obj.get("data"), dict):
        return text, obj["data"], obj.get("stream")
    return text, obj, None


# --------------------------------------------------------------------------- #
# Typed rows
# --------------------------------------------------------------------------- #
def parse_trade(event: dict[str, Any], now_ms: int) -> dict[str, Any]:
    """Validate a Binance `<symbol>@trade` event and map it to the trades schema.

    Binance fields: e=event type, E=event time, s=symbol, t=trade id,
    p=price, q=quantity, T=trade time, m=is buyer the market maker.
    """
    if event.get("e") != "trade":
        raise ParseError("validate", f"expected event type 'trade', got {event.get('e')!r}")

    symbol = event.get("s")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ParseError("validate", "symbol (s) is missing")

    trade_id = event.get("t")
    if isinstance(trade_id, bool) or not isinstance(trade_id, int) or trade_id < 0:
        raise ParseError("validate", f"trade id (t) must be a non-negative integer, got {trade_id!r}")

    price = _to_numeric("price (p)", event.get("p"))
    quantity = _to_numeric("quantity (q)", event.get("q"))
    if price <= 0:
        raise ParseError("validate", f"price must be > 0, got {price}")
    if quantity <= 0:
        raise ParseError("validate", f"quantity must be > 0, got {quantity}")

    is_buyer_maker = event.get("m")
    if not isinstance(is_buyer_maker, bool):
        raise ParseError("validate", f"buyer-is-maker (m) must be boolean, got {is_buyer_maker!r}")

    trade_time_ms = _check_ts("trade time (T)", _to_int_ms(event.get("T")), now_ms, required=True)
    event_time_ms = _check_ts("event time (E)", _to_int_ms(event.get("E")), now_ms, required=False)

    # price * qty can have up to 16 decimals; round to NUMERIC scale.
    quote_quantity = (price * quantity).quantize(_NUMERIC_QUANTUM)

    return {
        "symbol": symbol.upper(),
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "quote_quantity": quote_quantity,
        "is_buyer_maker": is_buyer_maker,
        "trade_time": trade_time_ms,
        "event_time": event_time_ms,
    }


def _publish_ms(publish_time: Any) -> Optional[int]:
    """Pub/Sub publish_time arrives as a tz-aware datetime (or None)."""
    if publish_time is None:
        return None
    if hasattr(publish_time, "timestamp"):
        return int(publish_time.timestamp() * 1000)
    return _to_int_ms(publish_time)


def process_message(
    data: Optional[bytes],
    attributes: Optional[dict[str, str]],
    pubsub_message_id: Optional[str],
    publish_time: Any,
    now_ms: int,
) -> ParsedMessage:
    """Turn one Pub/Sub message into bronze row + (trade row | dead-letter row).

    Never raises: every failure becomes a dead-letter row so the streaming job
    keeps running. The bronze row is always produced (replay safety net).
    """
    attributes = dict(attributes or {})
    data = data or b""
    publish_ms = _publish_ms(publish_time)
    ingest_ms = _to_int_ms(attributes.get("ingest_ts"))
    event_ms = _to_int_ms(attributes.get("event_ts"))
    msg_id = attributes.get("msg_id") or (f"pubsub:{pubsub_message_id}" if pubsub_message_id else None)
    if msg_id is None:
        msg_id = f"unknown:{now_ms}"

    raw = {
        "msg_id": msg_id,
        "pubsub_message_id": pubsub_message_id,
        "source": attributes.get("source"),
        "stream": attributes.get("stream"),
        "event_type": attributes.get("event_type"),
        "symbol": attributes.get("symbol"),
        "event_ts": event_ms,
        "ingest_ts": ingest_ms if ingest_ms is not None else (publish_ms if publish_ms is not None else now_ms),
        "publish_ts": publish_ms,
        "processing_ts": now_ms,
        "payload": data.decode("utf-8", errors="replace"),
    }
    result = ParsedMessage(raw=raw)

    try:
        _, event, stream = decode_payload(data)
        event_type = event.get("e")
        result.event_type = event_type or "unknown"
        if event_type not in SUPPORTED_EVENT_TYPES:
            raise ParseError("route", f"unsupported event type {event_type!r} (stream={stream!r})")

        trade = parse_trade(event, now_ms)
        trade.update(
            {
                "msg_id": msg_id,
                "ingest_time": raw["ingest_ts"],
                "processing_time": now_ms,
            }
        )
        result.trade = trade
    except ParseError as err:
        result.error = make_dead_letter_row(
            stage=err.stage,
            error_type=type(err).__name__,
            error_message=str(err),
            payload=raw["payload"],
            attributes=attributes,
            msg_id=msg_id,
            pubsub_message_id=pubsub_message_id,
            publish_ms=publish_ms,
            now_ms=now_ms,
        )
    except Exception as err:  # noqa: BLE001 - last line of defence, never crash the job
        result.error = make_dead_letter_row(
            stage="unexpected",
            error_type=type(err).__name__,
            error_message=str(err),
            payload=raw["payload"],
            attributes=attributes,
            msg_id=msg_id,
            pubsub_message_id=pubsub_message_id,
            publish_ms=publish_ms,
            now_ms=now_ms,
        )
    return result


def make_dead_letter_row(
    *,
    stage: str,
    error_type: str,
    error_message: str,
    payload: Optional[str],
    attributes: Optional[dict[str, Any]],
    msg_id: Optional[str],
    pubsub_message_id: Optional[str],
    publish_ms: Optional[int],
    now_ms: int,
) -> dict[str, Any]:
    return {
        "msg_id": msg_id,
        "pubsub_message_id": pubsub_message_id,
        "error_stage": stage,
        "error_type": error_type,
        "error_message": (error_message or "")[:4000],
        "payload": payload,
        "attributes": json.dumps(attributes or {}, sort_keys=True, default=str),
        "publish_ts": publish_ms,
        "processing_ts": now_ms,
    }
