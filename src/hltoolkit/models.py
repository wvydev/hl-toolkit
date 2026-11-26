"""What the clients hand back."""

from dataclasses import dataclass, field
from typing import Any, Literal

from hltoolkit.numbers import finite, positive, whole

__all__ = ["Fill", "parse_fill"]


@dataclass(frozen=True)
class Fill:
    """One fill of one account. `user` is the name you gave the account (its address by default).
    The checked fields are parsed; everything Hyperliquid sent is kept in `raw`."""

    user: str
    coin: str
    px: float
    sz: float
    side: Literal["B", "A"]  # B = bought, A = sold
    time_ms: int
    start_position: float
    tid: int
    fee: float  # negative is a rebate
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)


def parse_fill(user: str, f: dict) -> Fill:
    """A Fill from one raw fill dict. Raises ValueError (or KeyError/TypeError) on a broken one:
    a missing field, a bad side, NaN or infinity anywhere, a price or size not above 0."""
    side = f["side"]
    if side not in ("B", "A"):
        raise ValueError(f"bad side {side!r}")
    return Fill(
        user=user, coin=str(f["coin"]), px=positive(f["px"]), sz=positive(f["sz"]), side=side,
        time_ms=whole(f["time"]), start_position=finite(f["startPosition"]), tid=whole(f["tid"]),
        fee=finite(f["fee"]), raw=dict(f),
    )
