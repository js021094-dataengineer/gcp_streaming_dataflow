"""Streaming pipeline: Pub/Sub (raw Binance frames) -> BigQuery bronze/silver/DLQ.

    Pub/Sub ──► ReadFromPubSub (event-time from `event_ts`, dedupe on `msg_id`)
            ──► ParseMessageFn ──┬─► raw_events   (every message, unchanged payload)
                                 ├─► trades       (validated, typed)
                                 └─► dead_letter  (decode / route / validate errors)
    BigQuery write rejections from raw_events and trades are routed to dead_letter too.

If --metrics_table is set, a gold branch is added:
    trades ──► 1-minute event-time windows per symbol ──► VWAP / OHLC / volume ──► trade_metrics_1m
"""

from __future__ import annotations

import logging

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window

from crypto_pipeline import bq_schemas
from crypto_pipeline.transforms import (
    DEAD_LETTER_TAG,
    RAW_TAG,
    TRADES_TAG,
    ParseMessageFn,
    WindowedTradeMetrics,
    WriteTable,
    _LogDroppedFn,
    failed_write_to_dead_letter,
)


class StreamingOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            required=True,
            help="projects/<project>/subscriptions/<name>",
        )
        parser.add_argument("--raw_table", required=True, help="<project>:<dataset>.raw_events")
        parser.add_argument("--trades_table", required=True, help="<project>:<dataset>.trades")
        parser.add_argument("--dead_letter_table", required=True, help="<project>:<dataset>.dead_letter")
        parser.add_argument(
            "--metrics_table",
            default="",
            help="<project>:<dataset>.trade_metrics_1m. Optional: when empty the windowed KPI branch is skipped.",
        )
        parser.add_argument(
            "--event_time_attribute",
            default="event_ts",
            help="Pub/Sub attribute (epoch ms) used as Beam event time.",
        )
        parser.add_argument(
            "--dedupe_id_attribute",
            default="msg_id",
            help="Pub/Sub attribute Dataflow uses to drop duplicate deliveries. "
            "Set to an empty string for runners that do not support it (DirectRunner).",
        )


def build_pipeline(pipeline: beam.Pipeline, opts: StreamingOptions) -> None:
    messages = pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        subscription=opts.input_subscription,
        with_attributes=True,
        timestamp_attribute=opts.event_time_attribute or None,
        id_label=opts.dedupe_id_attribute or None,
    )

    routed = messages | "ParseAndValidate" >> beam.ParDo(ParseMessageFn()).with_outputs(
        RAW_TAG, TRADES_TAG, DEAD_LETTER_TAG
    )

    raw_result = routed[RAW_TAG] | "WriteRawEvents" >> WriteTable(opts.raw_table, bq_schemas.RAW_EVENTS)
    trades_result = routed[TRADES_TAG] | "WriteTrades" >> WriteTable(opts.trades_table, bq_schemas.TRADES)

    raw_rejects = raw_result.failed_rows_with_errors | "RawRejectsToDLQ" >> beam.Map(
        failed_write_to_dead_letter, table=bq_schemas.RAW_EVENTS
    )
    trade_rejects = trades_result.failed_rows_with_errors | "TradeRejectsToDLQ" >> beam.Map(
        failed_write_to_dead_letter, table=bq_schemas.TRADES
    )

    dead_letter_sources = [routed[DEAD_LETTER_TAG], raw_rejects, trade_rejects]
    if opts.metrics_table:
        dead_letter_sources.append(_add_metrics_branch(routed[TRADES_TAG], opts.metrics_table))

    dlq_result = (
        tuple(dead_letter_sources)
        | "MergeDeadLetters" >> beam.Flatten()
        | "WriteDeadLetters" >> WriteTable(opts.dead_letter_table, bq_schemas.DEAD_LETTER)
    )
    _ = dlq_result.failed_rows_with_errors | "LogDroppedDeadLetters" >> beam.ParDo(_LogDroppedFn())


def _add_metrics_branch(trades, metrics_table: str):
    """Gold branch: windowed VWAP / OHLC / volume per symbol and minute -> BigQuery.

    Returns the rows BigQuery rejected, already shaped as dead-letter rows.
    """
    kpis = trades | "TradeMetrics" >> WindowedTradeMetrics()
    result = (
        kpis
        # The sink and the dead-letter Flatten work on globally windowed data.
        | "RewindowForSink" >> beam.WindowInto(window.GlobalWindows())
        | "WriteTradeMetrics" >> WriteTable(metrics_table, bq_schemas.TRADE_METRICS_1M)
    )
    return result.failed_rows_with_errors | "MetricRejectsToDLQ" >> beam.Map(
        failed_write_to_dead_letter, table=bq_schemas.TRADE_METRICS_1M
    )


def run(argv=None) -> None:
    options = PipelineOptions(argv)
    options.view_as(StandardOptions).streaming = True
    opts = options.view_as(StreamingOptions)

    pipeline = beam.Pipeline(options=options)
    build_pipeline(pipeline, opts)
    pipeline.run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
