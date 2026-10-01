"""Binance WebSocket -> Pub/Sub producer.

Design goals
------------
* Dumb pipe: forward every WebSocket frame to Pub/Sub byte-for-byte. All
  business logic (validation, typing, routing) lives in the Beam pipeline,
  where it is testable and replayable from the bronze table.
* Light metadata as Pub/Sub attributes:
    source, stream, event_type, symbol   -> routing / filtering
    event_ts   (epoch ms)                -> Beam event time (timestamp_attribute)
    ingest_ts  (epoch ms)                -> producer receive time (latency metrics)
    msg_id     (deterministic)           -> Dataflow de-duplication (id_label)
* Resilient: exponential backoff with jitter, proactive reconnect before
  Binance's 24h connection limit, staleness watchdog, graceful shutdown that
  flushes pending publishes.

Configuration (environment variables)
-------------------------------------
GCP_PROJECT        GCP project id                         (required unless --stdout)
PUBSUB_TOPIC       topic id, e.g. crypto-raw              (required unless --stdout)
SYMBOLS            comma separated, default "btcusdt,ethusdt"
STREAMS            comma separated stream types, default "trade"
BINANCE_WS_BASE    default "wss://stream.binance.com:9443"
STALE_AFTER_S      reconnect if no frame for this long, default 60
LOG_TO_CLOUD       "true" to send logs to Cloud Logging (set on the VM)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

LOG = logging.getLogger("producer")

DEFAULT_WS_BASE = "wss://stream.binance.com:9443"
# Binance closes connections after 24h; reconnect a bit earlier on our terms.
MAX_CONNECTION_AGE_S = 23 * 60 * 60
BACKOFF_INITIAL_S = 1.0
BACKOFF_MAX_S = 60.0
STATS_INTERVAL_S = 60.0


# --------------------------------------------------------------------------- #
# Pure functions (unit tested, no network / GCP imports)
# --------------------------------------------------------------------------- #
def build_stream_url(base: str, symbols: list[str], streams: list[str]) -> str:
    names = [f"{sym.strip().lower()}@{st.strip()}" for sym in symbols for st in streams if sym.strip() and st.strip()]
    if not names:
        raise ValueError("at least one symbol and one stream type are required")
    return f"{base.rstrip('/')}/stream?streams={'/'.join(names)}"


def build_message(frame: str | bytes, ingest_ms: int) -> tuple[bytes, dict[str, str]]:
    """Wrap a raw WebSocket frame into (Pub/Sub data, attributes).

    Never raises: frames we can't understand are still forwarded with
    event_type="unknown" so the pipeline's dead-letter path records them.
    """
    data = frame.encode("utf-8") if isinstance(frame, str) else bytes(frame)
    attrs: dict[str, str] = {
        "source": "binance",
        "ingest_ts": str(ingest_ms),
        "event_type": "unknown",
    }
    event_ms: Optional[int] = None
    msg_id: Optional[str] = None

    try:
        obj = json.loads(data)
        stream = obj.get("stream") if isinstance(obj, dict) else None
        event = obj.get("data") if isinstance(obj, dict) and isinstance(obj.get("data"), dict) else obj
        if isinstance(event, dict):
            event_type = event.get("e")
            symbol = event.get("s")
            if stream:
                attrs["stream"] = str(stream)
            if event_type:
                attrs["event_type"] = str(event_type)
            if symbol:
                attrs["symbol"] = str(symbol).upper()

            # Event time: trade time for trades, else the event time, else receive time.
            for key in ("T", "E"):
                value = event.get(key)
                if isinstance(value, int) and not isinstance(value, bool) and value > 0:
                    event_ms = value
                    break

            # Deterministic id => duplicates after reconnects / retries collapse in Dataflow.
            if event_type == "trade" and symbol and event.get("t") is not None:
                msg_id = f"binance:{str(symbol).upper()}:trade:{event['t']}"
    except (ValueError, TypeError):
        pass

    attrs["event_ts"] = str(event_ms if event_ms is not None else ingest_ms)
    attrs["msg_id"] = msg_id or f"binance:{attrs['event_type']}:{uuid.uuid4().hex}"
    return data, attrs


@dataclass
class Stats:
    received: int = 0
    published: int = 0
    publish_errors: int = 0
    reconnects: int = 0
    last_frame_monotonic: float = field(default_factory=time.monotonic)
    _window_start: float = field(default_factory=time.monotonic)
    _window_received: int = 0

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        elapsed = max(now - self._window_start, 1e-9)
        rate = (self.received - self._window_received) / elapsed
        self._window_start, self._window_received = now, self.received
        return {
            "received": self.received,
            "published": self.published,
            "publish_errors": self.publish_errors,
            "reconnects": self.reconnects,
            "msgs_per_s": round(rate, 2),
        }


def next_backoff(current: float) -> float:
    """Exponential backoff with full jitter."""
    return min(BACKOFF_MAX_S, current * 2) * random.uniform(0.5, 1.0)


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #
class StdoutSink:
    """Local dry-run: print what would be published."""

    def __init__(self, stats: Stats) -> None:
        self.stats = stats

    def publish(self, data: bytes, attrs: dict[str, str]) -> None:
        print(json.dumps({"attributes": attrs, "data": data.decode("utf-8", "replace")}), flush=True)
        self.stats.published += 1

    def close(self) -> None:
        pass


class PubSubSink:
    """Batched, flow-controlled Pub/Sub publisher.

    Flow control with BLOCK behaviour is the backpressure valve: if Pub/Sub
    can't keep up, publish() blocks, we stop reading the socket, and the
    TCP window pushes back on Binance instead of growing memory unbounded.
    """

    def __init__(self, project: str, topic: str, stats: Stats) -> None:
        from google.cloud import pubsub_v1
        from google.cloud.pubsub_v1.types import (
            BatchSettings,
            LimitExceededBehavior,
            PublisherOptions,
            PublishFlowControl,
        )

        self.stats = stats
        self.client = pubsub_v1.PublisherClient(
            batch_settings=BatchSettings(max_messages=500, max_bytes=1_000_000, max_latency=0.05),
            publisher_options=PublisherOptions(
                flow_control=PublishFlowControl(
                    message_limit=20_000,
                    byte_limit=50 * 1024 * 1024,
                    limit_exceeded_behavior=LimitExceededBehavior.BLOCK,
                )
            ),
        )
        self.topic_path = self.client.topic_path(project, topic)

    def _on_done(self, future) -> None:
        try:
            future.result()
            self.stats.published += 1
        except Exception as exc:  # noqa: BLE001
            self.stats.publish_errors += 1
            LOG.error("publish failed: %s", exc)

    def publish(self, data: bytes, attrs: dict[str, str]) -> None:
        future = self.client.publish(self.topic_path, data, **attrs)
        future.add_done_callback(self._on_done)

    def close(self) -> None:
        LOG.info("flushing pending publishes...")
        self.client.stop()  # blocks until all batches are sent


# --------------------------------------------------------------------------- #
# WebSocket loop
# --------------------------------------------------------------------------- #
async def consume(url: str, sink, stats: Stats, stop: asyncio.Event, stale_after_s: float,
                  max_messages: Optional[int] = None,
                  clock_ms: Callable[[], int] = lambda: int(time.time() * 1000)) -> None:
    import websockets  # imported lazily so unit tests don't need it

    backoff = BACKOFF_INITIAL_S
    while not stop.is_set():
        connected_at = time.monotonic()
        try:
            LOG.info("connecting to %s", url)
            async with websockets.connect(url, open_timeout=15, ping_interval=20, ping_timeout=20,
                                          max_size=2**20, close_timeout=5) as ws:
                LOG.info("connected")
                while not stop.is_set():
                    if time.monotonic() - connected_at > MAX_CONNECTION_AGE_S:
                        LOG.info("connection age limit reached, reconnecting proactively")
                        break
                    try:
                        frame = await asyncio.wait_for(ws.recv(), timeout=stale_after_s)
                    except asyncio.TimeoutError:
                        LOG.warning("no data for %ss, reconnecting", stale_after_s)
                        break
                    stats.received += 1
                    stats.last_frame_monotonic = time.monotonic()
                    data, attrs = build_message(frame, clock_ms())
                    sink.publish(data, attrs)
                    if max_messages and stats.received >= max_messages:
                        stop.set()
                    # A healthy connection resets the backoff.
                    if time.monotonic() - connected_at > 60:
                        backoff = BACKOFF_INITIAL_S
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - network errors of all kinds
            LOG.warning("websocket error: %s: %s", type(exc).__name__, exc)

        if stop.is_set():
            break
        stats.reconnects += 1
        delay = backoff
        backoff = next_backoff(backoff)
        LOG.info("reconnecting in %.1fs", delay)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


async def report_stats(stats: Stats, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=STATS_INTERVAL_S)
        except asyncio.TimeoutError:
            LOG.info("stats %s", json.dumps(stats.snapshot()))


async def main_async(args: argparse.Namespace) -> int:
    symbols = [s for s in os.environ.get("SYMBOLS", "btcusdt,ethusdt").split(",") if s.strip()]
    streams = [s for s in os.environ.get("STREAMS", "trade").split(",") if s.strip()]
    url = build_stream_url(os.environ.get("BINANCE_WS_BASE", DEFAULT_WS_BASE), symbols, streams)
    stale_after_s = float(os.environ.get("STALE_AFTER_S", "60"))

    stats = Stats()
    if args.stdout:
        sink = StdoutSink(stats)
    else:
        project, topic = os.environ.get("GCP_PROJECT"), os.environ.get("PUBSUB_TOPIC")
        if not project or not topic:
            LOG.error("GCP_PROJECT and PUBSUB_TOPIC must be set (or use --stdout)")
            return 2
        sink = PubSubSink(project, topic, stats)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    reporter = asyncio.create_task(report_stats(stats, stop))
    try:
        await consume(url, sink, stats, stop, stale_after_s, max_messages=args.max_messages)
    finally:
        stop.set()
        await reporter
        sink.close()
        LOG.info("final stats %s", json.dumps(stats.snapshot()))
    return 0


def setup_logging() -> None:
    if os.environ.get("LOG_TO_CLOUD", "").lower() == "true":
        try:
            import google.cloud.logging

            google.cloud.logging.Client().setup_logging(log_level=logging.INFO)
            logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
            return
        except Exception as exc:  # noqa: BLE001
            print(f"Cloud Logging unavailable, falling back to stderr: {exc}", file=sys.stderr)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stdout", action="store_true", help="print messages instead of publishing")
    parser.add_argument("--max-messages", type=int, default=None, help="stop after N frames (smoke tests)")
    args = parser.parse_args()
    setup_logging()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
