"""Kline loader: date parsing, MERGE SQL, HTTP retries and paging, all with a fake HTTP client."""

import pytest

import load_klines
from crypto_pipeline import bq_schemas

MIN = 60_000
T0 = 1_791_365_220_000  # 2026-10-07 09:27:00 UTC


def candle(open_time):
    return [open_time, "100", "101", "99", "100.5", "2", open_time + MIN - 1, "201", 3, "1", "100", "0"]


class FakeResponse:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload if payload is not None else []
        self.headers = headers or {}
        self.text = "body"

    def json(self):
        return self._payload


class FakeGet:
    """Serves queued responses (or exceptions) and records every call's params."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class RangeGet:
    """Behaves like Binance: returns the candles between startTime and endTime, at most `limit`."""

    def __init__(self):
        self.calls = []

    def __call__(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        first, last, limit = params["startTime"], params["endTime"], params["limit"]
        times = range(first, last + MIN, MIN)
        return FakeResponse(payload=[candle(t) for t in list(times)[:limit]])


# ---------------------------------------------------------------- dates

def test_parse_utc_formats():
    assert load_klines.parse_utc("2026-10-07") == 1_791_331_200_000
    assert load_klines.parse_utc("2026-10-07T09:27") == T0
    assert load_klines.parse_utc("2026-10-07 09:27") == T0


def test_parse_utc_rejects_garbage():
    with pytest.raises(ValueError):
        load_klines.parse_utc("yesterday")


# ---------------------------------------------------------------- MERGE sql

def test_merge_sql_is_keyed_and_covers_all_columns():
    columns = [f["name"] for f in bq_schemas.fields(bq_schemas.KLINES_1M)]
    sql = load_klines.build_merge_sql("proj", "ds", columns, "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")
    assert "MERGE `proj.ds.klines_1m` T" in sql
    assert "USING `proj.ds.klines_1m_stage` S" in sql
    assert "T.symbol = S.symbol AND T.window_start = S.window_start" in sql
    assert "BETWEEN TIMESTAMP('2026-10-01T00:00:00Z') AND TIMESTAMP('2026-10-02T00:00:00Z')" in sql
    for name in columns:
        assert f"S.{name}" in sql
    # the key columns are never updated
    update_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "symbol =" not in update_part and "window_start =" not in update_part
    assert "close = S.close" in update_part


# ---------------------------------------------------------------- retries

def test_fetch_json_retries_rate_limits_then_succeeds():
    get = FakeGet([FakeResponse(429, headers={"Retry-After": "3"}), FakeResponse(503), FakeResponse(200, [1])])
    sleeps = []
    assert load_klines.fetch_json(get, {"a": 1}, sleep=sleeps.append) == [1]
    assert len(get.calls) == 3
    assert sleeps[0] == 3.0 and sleeps[1] > sleeps[0] - 1  # Retry-After honoured, then backoff grows


def test_fetch_json_retries_network_errors():
    get = FakeGet([ConnectionError("boom"), FakeResponse(200, [2])])
    assert load_klines.fetch_json(get, {}, sleep=lambda s: None) == [2]


def test_fetch_json_gives_up_after_the_retry_budget():
    get = FakeGet([FakeResponse(500)] * 3)
    with pytest.raises(RuntimeError, match="after 3 attempts"):
        load_klines.fetch_json(get, {}, sleep=lambda s: None, retries=3)


def test_fetch_json_does_not_retry_client_errors():
    get = FakeGet([FakeResponse(400)])
    with pytest.raises(RuntimeError, match="rejected"):
        load_klines.fetch_json(get, {}, sleep=lambda s: None)
    assert len(get.calls) == 1


# ---------------------------------------------------------------- paging

def test_fetch_symbol_pages_with_an_explicit_limit_and_returns_every_minute_once():
    get = RangeGet()
    n = 2300
    rows = load_klines.fetch_symbol(
        get, "BTCUSDT", T0, T0 + n * MIN, now_ms=T0 + 10_000 * MIN, loaded_ms=T0 + 10_000 * MIN,
        sleep=lambda s: None, log=lambda s: None)
    assert len(get.calls) == 3
    assert all(c["limit"] == 1000 and c["interval"] == "1m" and c["symbol"] == "BTCUSDT" for c in get.calls)
    assert len(rows) == n
    starts = [r["window_start"] for r in rows]
    assert starts == sorted(set(starts))


def test_fetch_symbol_never_requests_the_running_minute():
    get = RangeGet()
    now = T0 + 5 * MIN + 30_000  # in the middle of minute T0 + 5
    rows = load_klines.fetch_symbol(
        get, "ETHUSDT", T0, T0 + 100 * MIN, now_ms=now, loaded_ms=now, sleep=lambda s: None, log=lambda s: None)
    assert [r["window_start"] for r in rows][-1] == "2026-10-07T09:31:00Z"  # T0 + 4 min is the last closed
    assert get.calls[-1]["endTime"] == T0 + 4 * MIN


def test_fetch_symbol_with_nothing_to_fetch_makes_no_requests():
    get = FakeGet([])
    rows = load_klines.fetch_symbol(
        get, "BTCUSDT", T0 + 5 * MIN, T0 + 5 * MIN, now_ms=T0 + 100 * MIN, loaded_ms=0,
        sleep=lambda s: None, log=lambda s: None)
    assert rows == [] and get.calls == []
