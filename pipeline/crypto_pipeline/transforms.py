"""Beam transforms: Pub/Sub message -> bronze / silver / dead-letter rows."""

from __future__ import annotations

import json
import logging
import time
from decimal import Decimal
from typing import Any, Iterable

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import trigger, window
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import PaneInfoTiming

from crypto_pipeline import bq_schemas, metrics, parsing

LOG = logging.getLogger(__name__)

WINDOW_SECONDS = 60
# Trades arriving up to this long after the watermark passed a window still update it.
ALLOWED_LATENESS_SECONDS = 120

RAW_TAG = "raw"
TRADES_TAG = "trades"
DEAD_LETTER_TAG = "dead_letter"

_TS_FIELDS = {
    RAW_TAG: bq_schemas.timestamp_fields(bq_schemas.RAW_EVENTS),
    TRADES_TAG: bq_schemas.timestamp_fields(bq_schemas.TRADES),
    DEAD_LETTER_TAG: bq_schemas.timestamp_fields(bq_schemas.DEAD_LETTER),
}


def _ms_to_beam_ts(row: dict[str, Any], ts_fields: Iterable[str]) -> dict[str, Any]:
    """The Storage Write API expects Beam `Timestamp` objects for TIMESTAMP."""
    out = dict(row)
    for name in ts_fields:
        value = out.get(name)
        if isinstance(value, int):
            out[name] = Timestamp(micros=value * 1000)
    return out


class ParseMessageFn(beam.DoFn):
    """Fan one Pub/Sub message out to raw (always) + trades or dead-letter."""

    def __init__(self) -> None:
        self.messages = Metrics.counter("crypto", "messages_in")
        self.trades = Metrics.counter("crypto", "trades_ok")
        self.dead = Metrics.counter("crypto", "dead_lettered")
        # Producer receive -> Dataflow processing, in ms. Visible in the Dataflow UI.
        self.ingest_latency = Metrics.distribution("crypto", "ingest_to_processing_ms")
        # Exchange trade time -> Dataflow processing, in ms.
        self.e2e_latency = Metrics.distribution("crypto", "trade_to_processing_ms")

    def process(self, message):  # message: apache_beam.io.gcp.pubsub.PubsubMessage
        now_ms = int(time.time() * 1000)
        self.messages.inc()

        result = parsing.process_message(
            data=message.data,
            attributes=message.attributes,
            pubsub_message_id=getattr(message, "message_id", None),
            publish_time=getattr(message, "publish_time", None),
            now_ms=now_ms,
        )

        yield TaggedOutput(RAW_TAG, _ms_to_beam_ts(result.raw, _TS_FIELDS[RAW_TAG]))

        if result.raw.get("ingest_ts") is not None:
            self.ingest_latency.update(max(0, now_ms - result.raw["ingest_ts"]))

        if result.trade is not None:
            self.trades.inc()
            self.e2e_latency.update(max(0, now_ms - result.trade["trade_time"]))
            yield TaggedOutput(TRADES_TAG, _ms_to_beam_ts(result.trade, _TS_FIELDS[TRADES_TAG]))

        if result.error is not None:
            self.dead.inc()
            Metrics.counter("crypto", f"dead_lettered_{result.error['error_stage']}").inc()
            yield TaggedOutput(DEAD_LETTER_TAG, _ms_to_beam_ts(result.error, _TS_FIELDS[DEAD_LETTER_TAG]))


def _json_default(value: Any) -> Any:
    if isinstance(value, Timestamp):
        return value.to_rfc3339()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def failed_write_to_dead_letter(failed: dict[str, Any], table: str) -> dict[str, Any]:
    """Map a Storage Write API rejection to a dead-letter row."""
    failed_row = failed.get("failed_row") or {}
    row = parsing.make_dead_letter_row(
        stage="bq_write",
        error_type=f"bq_write:{table}",
        error_message=str(failed.get("error_message")),
        payload=json.dumps(failed_row, default=_json_default, sort_keys=True),
        attributes={},
        msg_id=failed_row.get("msg_id"),
        pubsub_message_id=failed_row.get("pubsub_message_id"),
        publish_ms=None,
        now_ms=int(time.time() * 1000),
    )
    return _ms_to_beam_ts(row, _TS_FIELDS[DEAD_LETTER_TAG])


class WriteTable(beam.PTransform):
    """Storage Write API (at-least-once) into a pre-created table."""

    def __init__(self, table_spec: str, schema_name: str) -> None:
        super().__init__()
        self.table_spec = table_spec
        self.schema_name = schema_name

    def expand(self, rows):
        return rows | WriteToBigQuery(
            table=self.table_spec,
            schema=bq_schemas.table_schema(self.schema_name),
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            use_at_least_once=True,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )


class _LogDroppedFn(beam.DoFn):
    """Last resort for rows that even the dead-letter table rejected."""

    def __init__(self) -> None:
        self.dropped = Metrics.counter("crypto", "dead_letter_write_failed")

    def process(self, failed):
        self.dropped.inc()
        LOG.error("Dead-letter write failed: %s", str(failed)[:2000])


# --------------------------------------------------------------------------- #
# Windowed KPIs (gold): VWAP, OHLC, volume per symbol per event-time minute
# --------------------------------------------------------------------------- #
_METRICS_TS_FIELDS = bq_schemas.timestamp_fields(bq_schemas.TRADE_METRICS_1M)


class TradeMetricsFn(beam.CombineFn):
    """Aggregate the `trades` rows of one symbol and window into KPIs.

    The arithmetic lives in `metrics.py`. The accumulator is keyed by trade id, so duplicate
    trades (at-least-once processing, re-delivered late panes) are counted once.
    """

    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, trade):
        return metrics.add_trade(accumulator, trade)

    def merge_accumulators(self, accumulators):
        return metrics.merge(accumulators)

    def extract_output(self, accumulator):
        return metrics.finalize(accumulator)


class FormatMetricsFn(beam.DoFn):
    """(symbol, KPIs) + window + pane info -> a `trade_metrics_1m` row."""

    def process(self, element, win=beam.DoFn.WindowParam, pane_info=beam.DoFn.PaneInfoParam):
        symbol, kpis = element
        if kpis is None:
            return
        row = {
            "symbol": symbol,
            "window_start": win.start,
            "window_end": win.end,
            **kpis,
            "pane_timing": PaneInfoTiming.to_string(pane_info.timing),
            "pane_index": pane_info.index,
            "processing_ts": int(time.time() * 1000),
        }
        yield _ms_to_beam_ts(row, _METRICS_TS_FIELDS)


class WindowedTradeMetrics(beam.PTransform):
    """`trades` rows -> one KPI row per symbol, 1-minute event-time window and pane.

    Fixed windows on the element timestamp (the Pub/Sub `event_ts` attribute = trade time).
    One on-time pane when the watermark passes the window end, then one more pane for every
    late element arriving within the allowed lateness. ACCUMULATING mode makes each late pane
    a complete, replacement result for its window; read the latest pane per window.
    """

    def expand(self, trades):
        return (
            trades
            | "WindowTrades"
            >> beam.WindowInto(
                window.FixedWindows(WINDOW_SECONDS),
                trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
            )
            | "KeyBySymbol" >> beam.Map(lambda trade: (trade["symbol"], trade))
            | "AggregateWindow" >> beam.CombinePerKey(TradeMetricsFn())
            | "FormatMetrics" >> beam.ParDo(FormatMetricsFn())
        )
