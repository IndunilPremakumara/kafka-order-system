"""
Running Average Aggregator
--------------------------
Thread-safe accumulator that maintains a live running average of order prices.

The consumer imports this module and calls update() once per successfully
processed order.  Any thread can call get() at any time to read the current
state without blocking the polling loop.
"""
import threading


class RunningAverage:
    """Incremental, thread-safe running average."""

    def __init__(self):
        self._lock = threading.Lock()
        self._count = 0
        self._total = 0.0

    def update(self, value: float) -> None:
        """Add *value* to the running total."""
        with self._lock:
            self._count += 1
            self._total += value

    def get(self) -> tuple[int, float]:
        """Return (count, average).  average is 0.0 when count is 0."""
        with self._lock:
            if self._count == 0:
                return 0, 0.0
            return self._count, self._total / self._count

    def reset(self) -> None:
        """Reset all state back to zero."""
        with self._lock:
            self._count = 0
            self._total = 0.0

    def __repr__(self) -> str:
        count, avg = self.get()
        return f"RunningAverage(count={count}, average={avg:.2f})"


# Module-level singleton used by the consumer
price_aggregator = RunningAverage()
