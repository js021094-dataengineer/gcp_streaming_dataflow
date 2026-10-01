"""Business rules for turning Binance frames into BigQuery rows (no Beam needed)."""

import json
from datetime import datetime, timezone
from decimal import Decimal

from crypto_pipeline import bq_schemas, parsing

NOW_MS = 1_790_000_000_000  # 2026-09-21


def trade_frame(**overrides):
    event = {
        "e": "trade", "E": NOW_MS - 5, "s": "BTCUSDT", "t": 123456789,
        "p": "64123.45000000", "q": "0.00150000", "T": NOW_MS - 10, "m": True, "M": True,
    }
    event.update(overrides)
    return json.dumps({"stream": "btcusdt@trade", "data": event}).encode()


ATTRS = {
    "source": "binance", "stream": "btcusdt@trade", "event_type": "trade", "symbol": "BTCUSDT",
    "event_ts": str(NOW_MS - 10), "ingest_ts": str(NOW_MS - 3), "msg_id": "binance:BTCUSDT:trade:123456789",
}


def process(data, attrs=ATTRS):
    return parsing.process_message(
        data, attrs, "pubsub-1", datetime.fromtimestamp((NOW_MS - 2) / 1000, tz=timezone.utc), NOW_MS
    )


def test_valid_trade_produces_raw_and_trade_rows():
    result = process(trade_frame())
    assert result.error is None
    trade = result.trade
    assert trade["symbol"] == "BTCUSDT"
    assert trade["trade_id"] == 123456789
    assert trade["price"] == Decimal("64123.45")
    assert trade["quantity"] == Decimal("0.0015")
    assert trade["quote_quantity"] == Decimal("96.185175000")
    assert trade["is_buyer_maker"] is True
    assert trade["trade_time"] == NOW_MS - 10
    assert trade["event_time"] == NOW_MS - 5
    assert trade["ingest_time"] == NOW_MS - 3
    assert trade["msg_id"] == ATTRS["msg_id"]

    raw = result.raw
    assert raw["msg_id"] == ATTRS["msg_id"]
    assert raw["payload"] == trade_frame().decode()
    assert raw["publish_ts"] == NOW_MS - 2
    assert raw["processing_ts"] == NOW_MS


def test_rows_match_bigquery_schemas_exactly():
    result = process(trade_frame())
    assert set(result.trade) == {f["name"] for f in bq_schemas.fields(bq_schemas.TRADES)}
    assert set(result.raw) == {f["name"] for f in bq_schemas.fields(bq_schemas.RAW_EVENTS)}
    bad = process(b"not json")
    assert set(bad.error) == {f["name"] for f in bq_schemas.fields(bq_schemas.DEAD_LETTER)}


def test_required_fields_are_never_null():
    result = process(trade_frame())
    for table, row in [(bq_schemas.TRADES, result.trade), (bq_schemas.RAW_EVENTS, result.raw)]:
        for f in bq_schemas.fields(table):
            if f["mode"] == "REQUIRED":
                assert row[f["name"]] is not None, (table, f["name"])


def test_unwrapped_raw_stream_format_is_accepted():
    event = json.loads(trade_frame())["data"]
    result = process(json.dumps(event).encode())
    assert result.trade is not None and result.error is None


def test_invalid_json_goes_to_dead_letter_but_is_kept_in_raw():
    result = process(b"{not json")
    assert result.trade is None
    assert result.error["error_stage"] == "decode"
    assert result.raw["payload"] == "{not json"


def test_non_utf8_payload_goes_to_dead_letter():
    result = process(b"\xff\xfe\x00")
    assert result.error["error_stage"] == "decode"


def test_unsupported_event_type_is_routed_to_dead_letter():
    data = json.dumps({"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}}).encode()
    result = process(data)
    assert result.error["error_stage"] == "route"
    assert "aggTrade" in result.error["error_message"]


def test_validation_failures():
    cases = {
        "negative price": {"p": "-1"},
        "zero quantity": {"q": "0"},
        "price not a number": {"p": "abc"},
        "price as float": {"p": 1.5},
        "missing symbol": {"s": None},
        "trade id string": {"t": "123"},
        "trade id bool": {"t": True},
        "missing trade time": {"T": None},
        "trade time in far future": {"T": NOW_MS + 3 * 86_400_000},
        "trade time before 2017": {"T": 1_000_000_000_000},
        "m not bool": {"m": "true"},
        "too many decimals": {"q": "0.0000000001"},
        "infinite": {"p": "Infinity"},
    }
    for name, override in cases.items():
        result = process(trade_frame(**override))
        assert result.trade is None, name
        assert result.error["error_stage"] == "validate", name


def test_trailing_zero_decimals_beyond_scale_are_accepted():
    # 10 decimals but only trailing zeros -> representable as NUMERIC
    result = process(trade_frame(q="0.0015000000"))
    assert result.trade["quantity"] == Decimal("0.0015")


def test_missing_attributes_fall_back_gracefully():
    result = process(trade_frame(), attrs={})
    assert result.trade is not None
    assert result.raw["msg_id"] == "pubsub:pubsub-1"
    assert result.raw["ingest_ts"] == NOW_MS - 2  # falls back to publish time


def test_dead_letter_row_keeps_attributes_as_json():
    result = process(b"[]")
    assert result.error["error_stage"] == "decode"
    assert json.loads(result.error["attributes"])["symbol"] == "BTCUSDT"


def test_process_message_never_raises_on_garbage():
    for garbage in [None, b"", b"null", b"123", b'{"data": 5}', b'{"stream": "x", "data": {"e": "trade"}}']:
        result = parsing.process_message(garbage, None, None, None, NOW_MS)
        assert result.error is not None
        assert result.raw["msg_id"]
