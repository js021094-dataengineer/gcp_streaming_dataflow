"""Beam-level tests of the parse/route DoFn (requires apache-beam)."""

import json
from datetime import datetime, timezone

import pytest

beam = pytest.importorskip("apache_beam")

from apache_beam.io.gcp.pubsub import PubsubMessage  # noqa: E402
from apache_beam.testing.test_pipeline import TestPipeline  # noqa: E402
from apache_beam.testing.util import assert_that, equal_to  # noqa: E402
from apache_beam.utils.timestamp import Timestamp  # noqa: E402

from crypto_pipeline.transforms import (  # noqa: E402
    DEAD_LETTER_TAG,
    RAW_TAG,
    TRADES_TAG,
    ParseMessageFn,
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


def test_bq_rejection_becomes_dead_letter_row():
    row = failed_write_to_dead_letter(
        {"error_message": "boom", "failed_row": {"msg_id": "x", "trade_time": Timestamp(1)}}, "trades"
    )
    assert row["error_stage"] == "bq_write"
    assert row["msg_id"] == "x"
    assert isinstance(row["processing_ts"], Timestamp)
    assert "1970-01-01" in row["payload"]
