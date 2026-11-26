"""Numbers from Hyperliquid are untrusted: NaN, infinity, zero or a negative price/size never get
through; the value is refused as if it were unparseable."""
import json

import pytest

from hltoolkit.models import parse_fill
from hltoolkit.numbers import finite, positive, whole
from hltoolkit.ws import FillsFeed

BAD = ["NaN", "nan", "inf", "-inf", "Infinity", "1e400"]
FILL = {"coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": 1, "startPosition": "0", "tid": 7, "fee": "0.01"}


class Clock:
    def now(self):
        return 0.0

    async def sleep(self, s):
        pass


@pytest.mark.parametrize("bad", BAD + [float("nan"), float("inf")])
def test_finite_refuses_nan_and_infinity(bad):
    with pytest.raises(ValueError):
        finite(bad)


def test_finite_and_positive_accept_good_numbers():
    assert finite("-1.5") == -1.5 and finite(0) == 0.0
    assert positive("0.001") == 0.001
    for bad in ("0", "-2", 0.0):
        with pytest.raises(ValueError):
            positive(bad)


def test_whole_refuses_an_infinite_float_with_value_error():
    assert whole("12") == 12 and whole(3.0) == 3
    for bad in (float("inf"), float("-inf"), float("nan")):
        with pytest.raises(ValueError):  # not OverflowError, which a ValueError handler would miss
            whole(bad)
    with pytest.raises(ValueError):
        whole("x")


@pytest.mark.parametrize("field", ["px", "sz", "startPosition", "fee"])
@pytest.mark.parametrize("bad", BAD)
def test_a_fill_with_a_broken_number_is_unparseable(field, bad):
    with pytest.raises(ValueError):
        parse_fill("T1", {**FILL, field: bad})


@pytest.mark.parametrize("field,bad", [("px", "0"), ("px", "-5"), ("sz", "0"), ("sz", "-1")])
def test_a_fill_needs_a_positive_price_and_size(field, bad):
    with pytest.raises(ValueError):
        parse_fill("T1", {**FILL, field: bad})


def test_a_fill_needs_a_known_side():
    with pytest.raises(ValueError):
        parse_fill("T1", {**FILL, "side": "X"})


def test_the_websocket_skips_a_broken_fill_and_keeps_the_good_one():
    feed = FillsFeed({"T1": "0xT1"}, "wss://unused", clock=Clock())
    msg = {"channel": "userFills", "data": {"user": "0xT1", "fills": [
        {**FILL, "sz": "NaN", "tid": 1}, {**FILL, "px": "Infinity", "tid": 2}, {**FILL, "tid": 3}]}}
    raw = json.dumps(msg).replace('"Infinity"', "Infinity")  # also a bare JSON literal
    assert [f.tid for f in feed._handle(raw)] == [3]
