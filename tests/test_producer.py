"""Producer helpers: URL building and Pub/Sub message construction."""

import json

import producer


def test_build_stream_url_combines_symbols_and_streams():
    url = producer.build_stream_url("wss://stream.binance.com:9443/", ["BTCUSDT", " ethusdt "], ["trade"])
    assert url == "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"


def test_build_stream_url_rejects_empty():
    try:
        producer.build_stream_url("wss://x", [], ["trade"])
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_trade_frame_gets_deterministic_id_and_event_time():
    frame = json.dumps({"stream": "btcusdt@trade", "data": {
        "e": "trade", "E": 1_700_000_000_100, "s": "BTCUSDT", "t": 42,
        "p": "1", "q": "1", "T": 1_700_000_000_000, "m": False, "M": True}})
    data, attrs = producer.build_message(frame, ingest_ms=1_700_000_000_500)
    assert data == frame.encode()  # forwarded byte-for-byte
    assert attrs == {
        "source": "binance",
        "stream": "btcusdt@trade",
        "event_type": "trade",
        "symbol": "BTCUSDT",
        "event_ts": "1700000000000",  # trade time T wins over event time E
        "ingest_ts": "1700000000500",
        "msg_id": "binance:BTCUSDT:trade:42",
    }


def test_same_trade_twice_gets_same_id():
    frame = json.dumps({"data": {"e": "trade", "s": "ETHUSDT", "t": 7, "T": 1_700_000_000_000}})
    _, a = producer.build_message(frame, 1)
    _, b = producer.build_message(frame, 2)
    assert a["msg_id"] == b["msg_id"] == "binance:ETHUSDT:trade:7"


def test_event_without_timestamp_falls_back_to_ingest_time():
    # e.g. bookTicker events carry no time field
    frame = json.dumps({"stream": "btcusdt@bookTicker", "data": {"u": 1, "s": "BTCUSDT", "b": "1", "a": "2"}})
    _, attrs = producer.build_message(frame, ingest_ms=1_700_000_000_999)
    assert attrs["event_ts"] == "1700000000999"
    assert attrs["msg_id"].startswith("binance:unknown:")


def test_garbage_is_still_forwarded():
    data, attrs = producer.build_message(b"\x00garbage", ingest_ms=5)
    assert data == b"\x00garbage"
    assert attrs["event_type"] == "unknown"
    assert attrs["event_ts"] == "5"
    assert all(isinstance(v, str) for v in attrs.values())  # Pub/Sub requires str attributes


def test_backoff_is_bounded():
    delay = producer.BACKOFF_INITIAL_S
    for _ in range(50):
        delay = producer.next_backoff(delay)
        assert 0 < delay <= producer.BACKOFF_MAX_S


class _FakeWS:
    def __init__(self, frames):
        self.frames = list(frames)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def recv(self):
        if not self.frames:
            raise ConnectionError("server closed the connection")
        return self.frames.pop(0)


class _ListSink:
    def __init__(self, stats):
        self.stats, self.items = stats, []

    def publish(self, data, attrs):
        self.items.append((data, attrs))
        self.stats.published += 1


def test_consume_reconnects_after_disconnect_and_stops(monkeypatch):
    """Two connections: the first drops after 2 frames, the second delivers the rest."""
    import asyncio
    import sys
    import types

    frame = json.dumps({"data": {"e": "trade", "s": "BTCUSDT", "t": 1, "T": 1_700_000_000_000}})
    connections = [_FakeWS([frame, frame]), _FakeWS([frame, frame, frame])]
    fake = types.SimpleNamespace(connect=lambda *a, **k: connections.pop(0))
    monkeypatch.setitem(sys.modules, "websockets", fake)
    monkeypatch.setattr(producer, "BACKOFF_INITIAL_S", 0.01)

    stats = producer.Stats()
    sink = _ListSink(stats)

    async def run():
        stop = asyncio.Event()
        await asyncio.wait_for(producer.consume("wss://fake", sink, stats, stop, stale_after_s=1, max_messages=4), 5)

    asyncio.run(run())
    assert stats.received == 4
    assert stats.reconnects == 1
    assert len(sink.items) == 4
