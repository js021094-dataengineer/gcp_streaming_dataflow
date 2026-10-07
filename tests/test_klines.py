"""Kline backfill helpers: candle -> row mapping, request windows, closed-minute handling."""

import pytest

import klines
from crypto_pipeline import bq_schemas

MIN = 60_000
T0 = 1_791_365_220_000  # 2026-10-07 09:27:00 UTC, minute aligned
LOADED = T0 + 90 * MIN + 123


def make_candle(open_time=T0, o="100.00", h="101.50", l="99.50", c="101.00",
                vol="10.5", quote="1050.75", trades=42, buy="6.0"):
    return [open_time, o, h, l, c, vol, open_time + MIN - 1, quote, trades, buy, "600.0", "0"]


# ---------------------------------------------------------------- mapping

def test_candle_to_row_maps_every_column():
    row = klines.candle_to_row("btcusdt", make_candle(), LOADED)
    assert row == {
        "symbol": "BTCUSDT",
        "window_start": "2026-10-07T09:27:00Z",
        "window_end": "2026-10-07T09:28:00Z",
        "trade_count": 42,
        "volume": "10.5",
        "quote_volume": "1050.75",
        "vwap": "100.071428571",
        "open": "100.00",
        "high": "101.50",
        "low": "99.50",
        "close": "101.00",
        "buy_volume": "6.0",
        "sell_volume": "4.5",
        "source": "binance_klines",
        "loaded_ts": "2026-10-07T10:57:00Z",
    }


def test_row_columns_match_the_table_schema():
    row = klines.candle_to_row("BTCUSDT", make_candle(), LOADED)
    assert set(row) == {f["name"] for f in bq_schemas.fields(bq_schemas.KLINES_1M)}


def test_vwap_is_quote_volume_over_volume():
    row = klines.candle_to_row("BTCUSDT", make_candle(vol="3", quote="300.3", buy="1"), LOADED)
    assert row["vwap"] == "100.100000000"


def test_zero_volume_candle_has_no_vwap():
    candle = make_candle(o="100", h="100", l="100", c="100", vol="0", quote="0", trades=0, buy="0")
    row = klines.candle_to_row("BTCUSDT", candle, LOADED)
    assert row["vwap"] is None
    assert row["volume"] == "0" and row["sell_volume"] == "0"


def test_small_numbers_are_not_written_in_scientific_notation():
    candle = make_candle(o="0.00000012", h="0.00000013", l="0.00000011", c="0.00000012",
                         vol="1000000", quote="0.12", trades=5, buy="500000")
    row = klines.candle_to_row("SHIBUSDT", candle, LOADED)
    assert row["open"] == "0.00000012"
    assert "e" not in row["vwap"].lower()


@pytest.mark.parametrize("candle", [
    "not a list",
    [1, 2, 3],
    make_candle(open_time=T0 + 1),                    # not on a minute boundary
    make_candle(open_time=-MIN),
    make_candle(trades=-1),
    make_candle(trades="42"),
    make_candle(h="99.00"),                           # high below low
    make_candle(o="102.00"),                          # open above high
    make_candle(c="99.00"),                           # close below low
    make_candle(l="0", o="1", h="2", c="1"),          # non-positive price
    make_candle(vol="-1"),
    make_candle(buy="11"),                            # buy volume above volume
    make_candle(vol="0", quote="0", buy="0", trades=5),  # no volume but trades
    make_candle(vol="1", quote="1", buy="0", trades=0),  # volume but no trades
    make_candle(o="abc"),
    make_candle(vol="NaN"),
    make_candle(vol=10.5),                            # numbers must be strings
])
def test_bad_candles_are_rejected(candle):
    with pytest.raises(klines.KlineError):
        klines.candle_to_row("BTCUSDT", candle, LOADED)


# ---------------------------------------------------------------- response handling

def test_running_minute_is_dropped():
    now = T0 + 3 * MIN + 20_000  # 20 s into the minute that opens at T0 + 3 min
    candles = [make_candle(T0 + i * MIN) for i in range(4)]
    rows = klines.candles_to_rows("BTCUSDT", candles, LOADED, now)
    assert [r["window_start"] for r in rows] == [
        "2026-10-07T09:27:00Z", "2026-10-07T09:28:00Z", "2026-10-07T09:29:00Z"]


def test_candle_closing_exactly_now_is_kept():
    now = T0 + MIN  # minute T0 just closed, the minute T0 + 1 min just opened
    rows = klines.candles_to_rows("BTCUSDT", [make_candle(T0), make_candle(T0 + MIN)], LOADED, now)
    assert len(rows) == 1 and rows[0]["window_start"] == "2026-10-07T09:27:00Z"


def test_rows_are_sorted_and_deduplicated_by_open_time():
    now = T0 + 10 * MIN
    a, b = make_candle(T0 + MIN, c="101.00"), make_candle(T0, c="100.50")
    again = make_candle(T0 + MIN, c="101.20")  # same minute twice: the later one wins
    rows = klines.candles_to_rows("BTCUSDT", [a, b, again], LOADED, now)
    assert [r["window_start"] for r in rows] == ["2026-10-07T09:27:00Z", "2026-10-07T09:28:00Z"]
    assert rows[1]["close"] == "101.20"


def test_one_bad_candle_fails_the_batch():
    with pytest.raises(klines.KlineError):
        klines.candles_to_rows("BTCUSDT", [make_candle(T0), make_candle(T0 + MIN, h="1")],
                               LOADED, T0 + 10 * MIN)


# ---------------------------------------------------------------- request windows

def test_request_windows_split_at_the_limit():
    windows = list(klines.request_windows(T0, T0 + 2500 * MIN))
    assert windows == [
        (T0, T0 + 999 * MIN),
        (T0 + 1000 * MIN, T0 + 1999 * MIN),
        (T0 + 2000 * MIN, T0 + 2499 * MIN),
    ]


def test_request_windows_cover_the_range_without_overlap():
    windows = list(klines.request_windows(T0 + 17, T0 + 3333 * MIN + 5, limit=700))
    assert windows[0][0] == T0                      # start rounded down to the minute
    covered = []
    for first, last in windows:
        assert (last - first) // MIN + 1 <= 700
        covered += list(range(first, last + MIN, MIN))
    assert covered == list(range(T0, T0 + 3333 * MIN, MIN))  # end is exclusive, also rounded down


def test_request_windows_empty_and_single():
    assert list(klines.request_windows(T0, T0)) == []
    assert list(klines.request_windows(T0, T0 - MIN)) == []
    assert list(klines.request_windows(T0, T0 + MIN)) == [(T0, T0)]


def test_request_windows_rejects_bad_limit():
    with pytest.raises(ValueError):
        list(klines.request_windows(T0, T0 + MIN, limit=0))


# ---------------------------------------------------------------- time helpers

def test_last_closed_minute_start():
    assert klines.last_closed_minute_start(T0 + 20_000) == T0 - MIN
    assert klines.last_closed_minute_start(T0) == T0 - MIN


def test_missing_minutes():
    present = [T0, T0 + MIN, T0 + 3 * MIN]
    assert klines.missing_minutes(present, T0, T0 + 5 * MIN) == [T0 + 2 * MIN, T0 + 4 * MIN]
    assert klines.missing_minutes(present, T0, T0) == []
