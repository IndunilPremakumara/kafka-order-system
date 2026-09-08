"""
Tests for retry logic and DLQ routing in src/consumer.py.

All Kafka I/O is mocked — no broker required.
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Reset the module-level singleton between tests
import aggregator
from aggregator import RunningAverage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_mock_msg(order_id="1001", product="Item1", price=50.0):
    """Return a mock Kafka message whose .value() is a real Avro blob."""
    from avro_codec import encode_order
    msg = MagicMock()
    msg.key.return_value = order_id.encode("utf-8")
    msg.value.return_value = encode_order(
        {"orderId": order_id, "product": product, "price": price}
    )
    msg.topic.return_value = "orders"
    msg.partition.return_value = 0
    msg.offset.return_value = 42
    msg.headers.return_value = []
    return msg


def make_dlq_producer():
    """Return a mock DLQ producer."""
    p = MagicMock()
    p.produce = MagicMock()
    p.poll = MagicMock()
    return p


# ---------------------------------------------------------------------------
# Import function under test after path is set up
# ---------------------------------------------------------------------------

from consumer import handle_with_retry, TransientError, PermanentError


# ---------------------------------------------------------------------------
# Test: successful processing
# ---------------------------------------------------------------------------

class TestSuccessfulProcessing:
    def setup_method(self):
        aggregator.price_aggregator.reset()

    def test_returns_true_on_success(self):
        msg = make_mock_msg(price=100.0)
        dlq = make_dlq_producer()
        result = handle_with_retry(msg, dlq, fail_rate=0.0, perm_fail_rate=0.0)
        assert result is True

    def test_dlq_not_called_on_success(self):
        msg = make_mock_msg(price=50.0)
        dlq = make_dlq_producer()
        handle_with_retry(msg, dlq, fail_rate=0.0, perm_fail_rate=0.0)
        dlq.produce.assert_not_called()

    def test_aggregator_updated_on_success(self):
        aggregator.price_aggregator.reset()
        msg = make_mock_msg(price=200.0)
        dlq = make_dlq_producer()
        handle_with_retry(msg, dlq, fail_rate=0.0, perm_fail_rate=0.0)
        count, avg = aggregator.price_aggregator.get()
        assert count == 1
        assert abs(avg - 200.0) < 0.01


# ---------------------------------------------------------------------------
# Test: transient failures with eventual success
# ---------------------------------------------------------------------------

class TestTransientRetry:
    def setup_method(self):
        aggregator.price_aggregator.reset()

    def test_succeeds_after_one_transient_failure(self):
        """process_order raises TransientError once then succeeds."""
        msg = make_mock_msg(price=75.0)
        dlq = make_dlq_producer()

        call_count = {"n": 0}

        def patched_process(m, fail_rate, perm_fail_rate):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise TransientError("first attempt fails")
            from avro_codec import decode_order
            return decode_order(m.value())

        with patch("consumer.process_order", side_effect=patched_process), \
             patch("consumer.time.sleep"):
            result = handle_with_retry(msg, dlq, max_retries=3)

        assert result is True
        assert call_count["n"] == 2
        dlq.produce.assert_not_called()

    def test_succeeds_after_two_transient_failures(self):
        msg = make_mock_msg(price=75.0)
        dlq = make_dlq_producer()

        call_count = {"n": 0}

        def patched_process(m, fail_rate, perm_fail_rate):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise TransientError(f"attempt {call_count['n']} fails")
            from avro_codec import decode_order
            return decode_order(m.value())

        with patch("consumer.process_order", side_effect=patched_process), \
             patch("consumer.time.sleep"):
            result = handle_with_retry(msg, dlq, max_retries=3)

        assert result is True
        assert call_count["n"] == 3
        dlq.produce.assert_not_called()


# ---------------------------------------------------------------------------
# Test: retry exhaustion → DLQ
# ---------------------------------------------------------------------------

class TestRetryExhaustion:
    def setup_method(self):
        aggregator.price_aggregator.reset()

    def test_returns_false_when_retries_exhausted(self):
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        with patch("consumer.process_order",
                   side_effect=TransientError("always fails")), \
             patch("consumer.time.sleep"), \
             patch("consumer.send_to_dlq"):
            result = handle_with_retry(msg, dlq, max_retries=3)

        assert result is False

    def test_dlq_called_once_when_retries_exhausted(self):
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        with patch("consumer.process_order",
                   side_effect=TransientError("always fails")), \
             patch("consumer.time.sleep") as mock_sleep, \
             patch("consumer.send_to_dlq") as mock_dlq:
            handle_with_retry(msg, dlq, max_retries=3)

        mock_dlq.assert_called_once()

    def test_backoff_is_called_on_each_transient_failure(self):
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        with patch("consumer.process_order",
                   side_effect=TransientError("fail")), \
             patch("consumer.time.sleep") as mock_sleep, \
             patch("consumer.send_to_dlq"):
            handle_with_retry(msg, dlq, max_retries=3, base_backoff=0.5)

        # 3 retries → 2 sleeps (fail on attempt 1→sleep, attempt 2→sleep, attempt 3→DLQ)
        assert mock_sleep.call_count == 2

    def test_aggregator_not_updated_when_retries_exhausted(self):
        msg = make_mock_msg(price=999.0)
        dlq = make_dlq_producer()

        before_count, _ = aggregator.price_aggregator.get()

        with patch("consumer.process_order",
                   side_effect=TransientError("fail")), \
             patch("consumer.time.sleep"), \
             patch("consumer.send_to_dlq"):
            handle_with_retry(msg, dlq, max_retries=3)

        after_count, _ = aggregator.price_aggregator.get()
        assert after_count == before_count  # unchanged


# ---------------------------------------------------------------------------
# Test: permanent failure → immediate DLQ, no retries
# ---------------------------------------------------------------------------

class TestPermanentFailure:
    def setup_method(self):
        aggregator.price_aggregator.reset()

    def test_returns_false_on_permanent_error(self):
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        with patch("consumer.process_order",
                   side_effect=PermanentError("bad data")), \
             patch("consumer.send_to_dlq"):
            result = handle_with_retry(msg, dlq, max_retries=3)

        assert result is False

    def test_no_sleep_on_permanent_error(self):
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        with patch("consumer.process_order",
                   side_effect=PermanentError("bad data")), \
             patch("consumer.time.sleep") as mock_sleep, \
             patch("consumer.send_to_dlq"):
            handle_with_retry(msg, dlq, max_retries=3)

        mock_sleep.assert_not_called()

    def test_dlq_called_immediately_on_permanent_error(self):
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        with patch("consumer.process_order",
                   side_effect=PermanentError("bad data")), \
             patch("consumer.time.sleep"), \
             patch("consumer.send_to_dlq") as mock_dlq:
            handle_with_retry(msg, dlq, max_retries=3)

        # Called exactly once, on the very first attempt
        mock_dlq.assert_called_once()
        _, kwargs = mock_dlq.call_args_list[0][0], mock_dlq.call_args_list[0]
        # retry_count arg should be 1 (first attempt)
        args = mock_dlq.call_args[0]
        assert args[3] == 1  # retry_count positional arg

    def test_process_order_called_only_once_on_permanent_error(self):
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        with patch("consumer.process_order",
                   side_effect=PermanentError("bad data")) as mock_proc, \
             patch("consumer.time.sleep"), \
             patch("consumer.send_to_dlq"):
            handle_with_retry(msg, dlq, max_retries=3)

        # No retries for permanent errors
        assert mock_proc.call_count == 1


# ---------------------------------------------------------------------------
# Test: send_to_dlq header construction
# ---------------------------------------------------------------------------

class TestSendToDlq:
    def test_dlq_message_includes_error_reason(self):
        from consumer import send_to_dlq
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        send_to_dlq(dlq, msg, reason="test reason", retry_count=2)

        dlq.produce.assert_called_once()
        call_kwargs = dlq.produce.call_args[1]
        headers = dict(call_kwargs["headers"])
        assert b"test reason" in headers.get("X-Error-Reason", b"")

    def test_dlq_message_includes_retry_count(self):
        from consumer import send_to_dlq
        msg = make_mock_msg()
        dlq = make_dlq_producer()

        send_to_dlq(dlq, msg, reason="err", retry_count=3)

        call_kwargs = dlq.produce.call_args[1]
        headers = dict(call_kwargs["headers"])
        assert headers.get("X-Retry-Count") == b"3"

    def test_dlq_message_preserves_original_value(self):
        from consumer import send_to_dlq
        msg = make_mock_msg()
        dlq = make_dlq_producer()
        original_bytes = msg.value()

        send_to_dlq(dlq, msg, reason="err", retry_count=1)

        call_kwargs = dlq.produce.call_args[1]
        assert call_kwargs["value"] == original_bytes
