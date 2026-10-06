"""Beam-level tests of the parse/route DoFn (requires apache-beam)."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

beam = pytest.importorskip("apache_beam")

from apache_beam.io.gcp.pubsub import PubsubMessage  # noqa: E402
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions  # noqa: E402
from apache_beam.testing.test_pipeline import TestPipeline  # noqa: E402
from apache_beam.testing.test_stream import TestStream  # noqa: E402
from apache_beam.testing.util import assert_that, equal_to  # noqa: E402
from apache_beam.transforms.window import TimestampedValue  # noqa: E402
from apache_beam.utils.timestamp import Timestamp  # noqa: E402

from crypto_pipeline.transforms import (  # noqa: E402
    ALLOWED_LATENESS_SECONDS,
    DEAD_LETTER_TAG,
    RAW_TAG,
    TRADES_TAG,
    ParseMessageFn,
    WindowedTradeMetrics,
    failed_write_to_dead_letter,
)


def _msg(data: bytes, msg_id: str) -> PubsubMessage:
    return PubsubMessage(
        data,
        {"msg_id": msg_id, "ingest_ts": "1790000000000", "event_ts": "1790000000000"},
        message_id=msg_id,
        publish_time=datetime(2026, 9, 21, tzinfo=timezone.utc),
    )


GOOD = json.dumps({"stream": "btcusdt@trade", "data": {
    "e": "trade", "E": 1790000000000, "s": "BTCUSDT", "t": 1, "p": "100.5", "q": "2",
    "T": 1790000000000, "m": False, "M": True}}).encode()


def test_routing_counts():
    with TestPipeline() as p:
        out = (
            p
            | beam.Create([_msg(GOOD, "a"), _msg(b"oops", "b"), _msg(GOOD.replace(b'"100.5"', b'"-1"'), "c")])
            | beam.ParDo(ParseMessageFn()).with_outputs(RAW_TAG, TRADES_TAG, DEAD_LETTER_TAG)
        )
        assert_that(out[RAW_TAG] | "rawIds" >> beam.Map(lambda r: r["msg_id"]), equal_to(["a", "b", "c"]), label="raw")
        assert_that(out[TRADES_TAG] | "tradeIds" >> beam.Map(lambda r: r["msg_id"]), equal_to(["a"]), label="trades")
        assert_that(
            out[DEAD_LETTER_TAG] | "dlq" >> beam.Map(lambda r: (r["msg_id"], r["error_stage"])),
            equal_to([("b", "decode"), ("c", "validate")]),
            label="dlq",
        )


def test_timestamps_are_converted_for_storage_write_api():
    with TestPipeline() as p:
        out = (
            p
            | beam.Create([_msg(GOOD, "a")])
            | beam.ParDo(ParseMessageFn()).with_outputs(RAW_TAG, TRADES_TAG, DEAD_LETTER_TAG)
        )
        assert_that(
            out[TRADES_TAG] | beam.Map(lambda r: (type(r["trade_time"]), r["trade_time"])),
            equal_to([(Timestamp, Timestamp(micros=1790000000000 * 1000))]),
        )


# --------------------------------------------------------------------------- #
# Windowed KPIs. TestStream lets the test control the watermark, so on-time and
# late panes are deterministic. Windows are [0, 60), [60, 120), ... seconds.
# --------------------------------------------------------------------------- #
def _streaming_pipeline():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return TestPipeline(options=options)


def _row(trade_id, price, qty, t_s, symbol="BTCUSDT", maker=False):
    """A `trades` row as ParseMessageFn emits it, stamped with its trade time (seconds)."""
    ts = Timestamp(t_s)
    price, qty = Decimal(price), Decimal(qty)
    row = {
        "symbol": symbol,
        "trade_id": trade_id,
        "price": price,
        "quantity": qty,
        "quote_quantity": price * qty,
        "is_buyer_maker": maker,
        "trade_time": ts,
    }
    return TimestampedValue(row, ts)


def _summary(r):
    return (r["symbol"], r["trade_count"], r["volume"], r["vwap"], r["open"], r["close"], r["pane_timing"], r["pane_index"])


def test_on_time_pane_has_vwap_ohlc_and_window_bounds():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_row(1, "100", "2", 10), _row(2, "110", "1", 20), _row(3, "90", "1", 30)])
        .advance_watermark_to(61)
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = p | stream | WindowedTradeMetrics()
        assert_that(
            out | "summary" >> beam.Map(_summary),
            equal_to([("BTCUSDT", 3, Decimal("4"), Decimal("100"), Decimal("100"), Decimal("90"), "ON_TIME", 0)]),
            label="summary",
        )
        assert_that(
            out | "bounds" >> beam.Map(lambda r: (r["window_start"], r["window_end"], r["high"], r["low"])),
            equal_to([(Timestamp(0), Timestamp(60), Decimal("110"), Decimal("90"))]),
            label="bounds",
        )


def test_trades_in_different_minutes_and_symbols_are_separate_rows():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([
            _row(1, "100", "1", 10),
            _row(2, "102", "1", 70),                     # next minute
            _row(1, "10", "5", 15, symbol="ETHUSDT"),    # other symbol, same minute as the first
        ])
        .advance_watermark_to(130)
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = p | stream | WindowedTradeMetrics()
        assert_that(
            out | beam.Map(lambda r: (r["symbol"], r["window_start"], r["trade_count"])),
            equal_to([
                ("BTCUSDT", Timestamp(0), 1),
                ("BTCUSDT", Timestamp(60), 1),
                ("ETHUSDT", Timestamp(0), 1),
            ]),
        )


def test_arrival_order_does_not_change_open_and_close():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_row(3, "90", "1", 30), _row(1, "100", "1", 10), _row(2, "110", "1", 20)])
        .advance_watermark_to(61)
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = p | stream | WindowedTradeMetrics()
        assert_that(out | beam.Map(lambda r: (r["open"], r["close"])), equal_to([(Decimal("100"), Decimal("90"))]))


def test_duplicate_trades_are_not_removed_known_limitation():
    # Fixed-size accumulator: a repeated trade (at-least-once processing, ~0.006% of trades) is
    # counted again. Exact de-duplication would need per-trade state (see metrics.py).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_row(1, "100", "2", 10), _row(1, "100", "2", 10), _row(2, "110", "1", 20)])
        .advance_watermark_to(61)
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = p | stream | WindowedTradeMetrics()
        assert_that(out | beam.Map(lambda r: (r["trade_count"], r["volume"])), equal_to([(3, Decimal("5"))]))


def test_late_trade_within_allowed_lateness_updates_the_window():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_row(1, "100", "2", 10)])
        .advance_watermark_to(61)                  # window [0, 60) closes: ON_TIME pane
        .add_elements([_row(2, "110", "1", 20)])   # late, but within the allowed lateness
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = p | stream | WindowedTradeMetrics()
        assert_that(
            out | beam.Map(lambda r: (r["pane_timing"], r["pane_index"], r["trade_count"], r["volume"])),
            equal_to([("ON_TIME", 0, 1, Decimal("2")), ("LATE", 1, 2, Decimal("3"))]),
        )


def test_late_redelivered_duplicate_is_counted_again_known_limitation():
    # Same limitation for a late duplicate: the LATE pane is a complete replacement and includes it twice.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_row(1, "100", "2", 10)])
        .advance_watermark_to(61)
        .add_elements([_row(1, "100", "2", 10)])   # the same trade again, late
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = p | stream | WindowedTradeMetrics()
        assert_that(
            out | beam.Map(lambda r: (r["pane_timing"], r["trade_count"], r["volume"])),
            equal_to([("ON_TIME", 1, Decimal("2")), ("LATE", 2, Decimal("4"))]),
        )


def test_trade_later_than_allowed_lateness_is_dropped():
    too_late = 60 + ALLOWED_LATENESS_SECONDS + 1
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_row(1, "100", "2", 10)])
        .advance_watermark_to(too_late)
        .add_elements([_row(2, "110", "1", 20)])   # window [0, 60) is already garbage collected
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = p | stream | WindowedTradeMetrics()
        assert_that(
            out | beam.Map(lambda r: (r["pane_timing"], r["trade_count"])),
            equal_to([("ON_TIME", 1)]),
        )


def test_metrics_table_option_is_optional():
    from crypto_pipeline.pipeline import StreamingOptions  # also checks that pipeline.py imports cleanly

    base = [
        "--input_subscription=projects/p/subscriptions/s",
        "--raw_table=p:d.raw_events",
        "--trades_table=p:d.trades",
        "--dead_letter_table=p:d.dead_letter",
    ]
    assert PipelineOptions(base).view_as(StreamingOptions).metrics_table == ""
    with_metrics = PipelineOptions(base + ["--metrics_table=p:d.trade_metrics_1m"])
    assert with_metrics.view_as(StreamingOptions).metrics_table == "p:d.trade_metrics_1m"


def test_bq_rejection_becomes_dead_letter_row():
    row = failed_write_to_dead_letter(
        {"error_message": "boom", "failed_row": {"msg_id": "x", "trade_time": Timestamp(1)}}, "trades"
    )
    assert row["error_stage"] == "bq_write"
    assert row["msg_id"] == "x"
    assert isinstance(row["processing_ts"], Timestamp)
    assert "1970-01-01" in row["payload"]
