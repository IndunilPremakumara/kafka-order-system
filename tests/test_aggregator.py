"""
Tests for src/aggregator.py — no Kafka broker required.
"""
import threading
import sys
import os

# Allow importing from src/ without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from aggregator import RunningAverage


class TestRunningAverageEmpty:
    def test_initial_count_is_zero(self):
        ra = RunningAverage()
        count, _ = ra.get()
        assert count == 0

    def test_initial_average_is_zero(self):
        ra = RunningAverage()
        _, avg = ra.get()
        assert avg == 0.0


class TestRunningAverageSingleUpdate:
    def test_count_becomes_one(self):
        ra = RunningAverage()
        ra.update(42.0)
        count, _ = ra.get()
        assert count == 1

    def test_average_equals_value(self):
        ra = RunningAverage()
        ra.update(42.0)
        _, avg = ra.get()
        assert abs(avg - 42.0) < 1e-9


class TestRunningAverageMultipleUpdates:
    def test_count_increments(self):
        ra = RunningAverage()
        for price in [10.0, 20.0, 30.0]:
            ra.update(price)
        count, _ = ra.get()
        assert count == 3

    def test_average_is_correct(self):
        ra = RunningAverage()
        prices = [10.0, 20.0, 30.0]
        for p in prices:
            ra.update(p)
        _, avg = ra.get()
        assert abs(avg - 20.0) < 1e-6

    def test_average_updates_incrementally(self):
        ra = RunningAverage()
        ra.update(100.0)
        _, avg1 = ra.get()
        assert abs(avg1 - 100.0) < 1e-9

        ra.update(200.0)
        _, avg2 = ra.get()
        assert abs(avg2 - 150.0) < 1e-9

    def test_large_number_of_updates(self):
        ra = RunningAverage()
        n = 10_000
        for i in range(1, n + 1):
            ra.update(float(i))
        count, avg = ra.get()
        assert count == n
        expected = (n + 1) / 2.0  # average of 1..n
        assert abs(avg - expected) < 0.01


class TestRunningAverageReset:
    def test_reset_clears_count(self):
        ra = RunningAverage()
        ra.update(50.0)
        ra.reset()
        count, _ = ra.get()
        assert count == 0

    def test_reset_clears_average(self):
        ra = RunningAverage()
        ra.update(50.0)
        ra.reset()
        _, avg = ra.get()
        assert avg == 0.0

    def test_update_after_reset(self):
        ra = RunningAverage()
        ra.update(100.0)
        ra.reset()
        ra.update(25.0)
        count, avg = ra.get()
        assert count == 1
        assert abs(avg - 25.0) < 1e-9


class TestRunningAverageThreadSafety:
    def test_concurrent_updates_count(self):
        """Spawn many threads all calling update(); final count must be exact."""
        ra = RunningAverage()
        n_threads = 50
        updates_per_thread = 100

        def worker():
            for _ in range(updates_per_thread):
                ra.update(1.0)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        count, avg = ra.get()
        assert count == n_threads * updates_per_thread
        assert abs(avg - 1.0) < 1e-9


class TestRunningAverageRepr:
    def test_repr_contains_count_and_average(self):
        ra = RunningAverage()
        ra.update(10.0)
        ra.update(20.0)
        r = repr(ra)
        assert "count=2" in r
        assert "average=15.00" in r
