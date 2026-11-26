"""The clock the clients read time from and sleep on. Pass your own to drive them in tests."""

import asyncio
import time
from typing import Protocol

__all__ = ["Clock", "SystemClock"]


class Clock(Protocol):
    def now(self) -> float:
        """Seconds since the epoch."""
        ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    """Wall-clock time and asyncio.sleep."""

    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
