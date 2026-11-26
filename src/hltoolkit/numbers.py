"""Numbers from an exchange API are untrusted.

`float()` accepts "NaN", "inf" and "1e400", and `json.loads` accepts bare NaN/Infinity, so a broken
answer would otherwise slip past every `x > 0` check (NaN compares False both ways) and into whatever
uses the number next. These helpers raise ValueError, the same error a non-number raises, so code that
already handles "unparseable" handles these too.
"""

import math

__all__ = ["finite", "positive", "whole"]


def finite(x) -> float:
    """x as a float, refusing NaN and ±infinity."""
    v = float(x)
    if not math.isfinite(v):
        raise ValueError(f"not a finite number: {x!r}")
    return v


def positive(x) -> float:
    """x as a finite float above 0 (a price, a size)."""
    v = finite(x)
    if v <= 0:
        raise ValueError(f"not above 0: {x!r}")
    return v


def whole(x) -> int:
    """x as an int (a time, a trade id). A float must be finite: int(Infinity) raises OverflowError,
    which a ValueError handler would not catch."""
    if isinstance(x, float):
        finite(x)
    return int(x)
