"""Unit tests for the pure KPI aggregation (no Beam)."""

from decimal import Decimal

from crypto_pipeline import metrics


def _trade(trade_id, price, qty, t_ms, maker=False):
    price, qty = Decimal(price), Decimal(qty)
    return {
        "symbol": "BTCUSDT",
        "trade_id": trade_id,
        "price": price,
        "quantity": qty,
        "quote_quantity": price * qty,
        "is_buyer_maker": maker,
        "trade_time": t_ms,
    }


def _acc(*trades):
    acc = {}
    for trade in trades:
        metrics.add_trade(acc, trade)
    return acc


def test_vwap_ohlc_and_volume():
    out = metrics.finalize(_acc(_trade(1, "100", "2", 1000), _trade(2, "110", "1", 2000), _trade(3, "90", "1", 3000)))
    assert out["trade_count"] == 3
    assert out["volume"] == Decimal("4")
    assert out["quote_volume"] == Decimal("400")
    assert out["vwap"] == Decimal("100")
    assert (out["open"], out["high"], out["low"], out["close"]) == (Decimal("100"), Decimal("110"), Decimal("90"), Decimal("90"))


def test_vwap_is_rounded_to_numeric_scale():
    out = metrics.finalize(_acc(_trade(1, "100", "1", 1000), _trade(2, "101", "2", 2000)))
    assert out["vwap"] == Decimal("100.666666667")  # 302 / 3, 9 decimals


def test_open_and_close_do_not_depend_on_arrival_order():
    forward = [_trade(1, "100", "1", 1000), _trade(2, "105", "1", 2000), _trade(3, "95", "1", 3000)]
    assert metrics.finalize(_acc(*forward)) == metrics.finalize(_acc(*reversed(forward)))
    out = metrics.finalize(_acc(*reversed(forward)))
    assert out["open"] == Decimal("100")
    assert out["close"] == Decimal("95")


def test_equal_trade_times_are_ordered_by_trade_id():
    out = metrics.finalize(_acc(_trade(5, "200", "1", 1000), _trade(4, "100", "1", 1000)))
    assert out["open"] == Decimal("100")  # trade id 4 came first
    assert out["close"] == Decimal("200")


def test_duplicate_trades_are_counted_once():
    trade = _trade(1, "100", "2", 1000)
    out = metrics.finalize(_acc(trade, trade, dict(trade)))
    assert out["trade_count"] == 1
    assert out["volume"] == Decimal("2")


def test_merge_is_a_union_and_idempotent():
    a = _acc(_trade(1, "100", "1", 1000), _trade(2, "101", "1", 2000))
    b = _acc(_trade(2, "101", "1", 2000), _trade(3, "102", "1", 3000))
    merged = metrics.merge([a, b])
    assert sorted(merged) == [1, 2, 3]
    assert metrics.finalize(metrics.merge([a, a])) == metrics.finalize(a)
    assert metrics.finalize(metrics.merge([a, b])) == metrics.finalize(metrics.merge([b, a]))


def test_buy_and_sell_volume_follow_the_aggressor():
    out = metrics.finalize(_acc(
        _trade(1, "100", "3", 1000, maker=False),  # aggressor was the buyer
        _trade(2, "100", "2", 2000, maker=True),   # aggressor was the seller
    ))
    assert out["buy_volume"] == Decimal("3")
    assert out["sell_volume"] == Decimal("2")
    assert out["volume"] == Decimal("5")


def test_missing_quote_quantity_is_computed():
    trade = _trade(1, "100.5", "2", 1000)
    trade["quote_quantity"] = None
    assert metrics.finalize(_acc(trade))["quote_volume"] == Decimal("201")


def test_empty_accumulator_gives_no_output():
    assert metrics.finalize({}) is None


def test_beam_style_timestamps_are_read_by_micros():
    class FakeTimestamp:
        micros = 5_000_000

    assert metrics._micros(FakeTimestamp()) == 5_000_000
    assert metrics._micros(5000) == 5_000_000  # epoch milliseconds
