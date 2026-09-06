"""
Order Consumer
--------------
Consumes Avro-encoded order messages from the 'orders' topic.

Features
~~~~~~~~
* **Real-time aggregation** — prints a running average of order prices after
  every successfully processed message.
* **Retry logic** — transient processing errors are retried up to --max-retries
  times with exponential back-off (base 0.5 s, doubles each attempt).
* **Dead Letter Queue (DLQ)** — when all retries are exhausted, or a
  PermanentError is raised, the original raw bytes are forwarded to the
  'orders-dlq' topic with diagnostic headers so nothing is silently dropped.
* **Failure simulation** — use --fail-rate / --perm-fail-rate during a live
  demo to inject artificial transient / permanent failures without changing
  production code.

Usage
~~~~~
    python consumer.py
    python consumer.py --idle-exit 10
    python consumer.py --fail-rate 0.3 --perm-fail-rate 0.1
"""
import argparse
import random
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

import config
from aggregator import price_aggregator
from avro_codec import decode_order


# ---------------------------------------------------------------------------
# Custom exception types
# ---------------------------------------------------------------------------

class TransientError(Exception):
    """Raised to signal a temporary failure — the consumer will retry."""


class PermanentError(Exception):
    """Raised to signal an unrecoverable failure — goes straight to DLQ."""


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(msg, stream=sys.stdout):
    print(f"[consumer] {msg}", file=stream, flush=True)


def log_err(msg):
    log(msg, stream=sys.stderr)


# ---------------------------------------------------------------------------
# DLQ helper
# ---------------------------------------------------------------------------

def send_to_dlq(dlq_producer: Producer, original_msg, reason: str,
                retry_count: int) -> None:
    """Forward the raw message bytes to the DLQ topic with error metadata."""
    headers = [
        ("X-Error-Reason", reason.encode("utf-8")),
        ("X-Retry-Count", str(retry_count).encode("utf-8")),
        ("X-Original-Topic", (original_msg.topic() or "unknown").encode("utf-8")),
        ("X-Original-Partition", str(original_msg.partition()).encode("utf-8")),
        ("X-Original-Offset", str(original_msg.offset()).encode("utf-8")),
    ]
    dlq_producer.produce(
        config.DLQ_TOPIC,
        key=original_msg.key(),
        value=original_msg.value(),  # raw bytes — preserve original encoding
        headers=headers,
        callback=lambda err, m: (
            log_err(f"DLQ delivery FAILED: {err}") if err else
            log(f"→ DLQ  orderId={m.key().decode() if m.key() else '?'} "
                f"reason={reason!r}")
        ),
    )
    dlq_producer.poll(0)


# ---------------------------------------------------------------------------
# Message processing with retry + DLQ
# ---------------------------------------------------------------------------

def process_order(msg, *, fail_rate: float = 0.0,
                  perm_fail_rate: float = 0.0) -> dict:
    """
    Decode the Avro message and simulate optional failures.

    Raises TransientError or PermanentError based on the configured rates so
    that retry / DLQ behaviour can be demonstrated without touching the broker.
    """
    order = decode_order(msg.value())

    # --- Simulated failures (demo only) ------------------------------------
    roll = random.random()
    if roll < perm_fail_rate:
        raise PermanentError(
            f"simulated permanent failure for orderId={order['orderId']}"
        )
    if roll < perm_fail_rate + fail_rate:
        raise TransientError(
            f"simulated transient failure for orderId={order['orderId']}"
        )
    # -----------------------------------------------------------------------

    return order


def handle_with_retry(msg, dlq_producer: Producer, *, max_retries: int = 3,
                      base_backoff: float = 0.5, fail_rate: float = 0.0,
                      perm_fail_rate: float = 0.0) -> bool:
    """
    Try to process *msg*, retrying on TransientError and routing to the DLQ on
    PermanentError or when retries are exhausted.

    Returns True if the message was processed successfully, False otherwise.
    """
    attempt = 0
    while True:
        try:
            order = process_order(msg, fail_rate=fail_rate,
                                  perm_fail_rate=perm_fail_rate)

            # --- Success ---
            price_aggregator.update(order["price"])
            count, avg = price_aggregator.get()
            log(
                f"OK  orderId={order['orderId']:>6}  "
                f"product={order['product']:<8}  "
                f"price={order['price']:>7.2f}  │  "
                f"n={count}  avg={avg:.2f}"
            )
            return True

        except PermanentError as exc:
            log_err(f"PERMANENT FAIL (attempt {attempt + 1}): {exc}")
            send_to_dlq(dlq_producer, msg, str(exc), attempt + 1)
            return False

        except (TransientError, Exception) as exc:
            attempt += 1
            if attempt >= max_retries:
                reason = f"exhausted {max_retries} retries: {exc}"
                log_err(f"RETRY EXHAUSTED: {reason}")
                send_to_dlq(dlq_producer, msg, reason, attempt)
                return False

            backoff = base_backoff * (2 ** (attempt - 1))
            log_err(
                f"TRANSIENT FAIL (attempt {attempt}/{max_retries}), "
                f"retrying in {backoff:.1f}s: {exc}"
            )
            time.sleep(backoff)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Avro order consumer")
    parser.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.ORDERS_TOPIC)
    parser.add_argument("--group-id", default=config.CONSUMER_GROUP)
    parser.add_argument("--idle-exit", type=float, default=0,
                        help="Exit after N seconds with no messages (0 = run forever)")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Max processing attempts before routing to DLQ")
    parser.add_argument("--fail-rate", type=float, default=0.0,
                        help="Fraction [0,1] of messages that simulate a transient error")
    parser.add_argument("--perm-fail-rate", type=float, default=0.0,
                        help="Fraction [0,1] of messages that simulate a permanent error")
    args = parser.parse_args()

    # --- Kafka consumer ----------------------------------------------------
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # committed manually, once handled
    })

    # --- DLQ producer (lightweight, shared) --------------------------------
    dlq_producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "acks": "all",
    })

    consumer.subscribe([args.topic])
    log(
        f"listening on '{args.topic}' (group={args.group_id})  "
        f"fail_rate={args.fail_rate:.0%}  "
        f"perm_fail_rate={args.perm_fail_rate:.0%}  "
        f"max_retries={args.max_retries}"
    )

    seen = success = dlq_count = 0
    last_message_at = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if args.idle_exit and time.time() - last_message_at > args.idle_exit:
                    log(f"idle for {args.idle_exit}s, exiting")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log_err(f"kafka error: {msg.error()}")
                continue

            last_message_at = time.time()
            seen += 1

            ok = handle_with_retry(
                msg,
                dlq_producer,
                max_retries=args.max_retries,
                fail_rate=args.fail_rate,
                perm_fail_rate=args.perm_fail_rate,
            )

            if ok:
                success += 1
            else:
                dlq_count += 1

            # Commit offset regardless — message has been handled (or DLQ'd)
            consumer.commit(msg, asynchronous=False)

    except KeyboardInterrupt:
        log("interrupted")
    except KafkaException as exc:
        log_err(f"fatal kafka exception: {exc}")
    finally:
        dlq_producer.flush(10)
        count, avg = price_aggregator.get()
        log(
            f"done — seen={seen}  success={success}  dlq={dlq_count}  "
            f"avg_price={avg:.2f}  (n={count})"
        )
        consumer.close()


if __name__ == "__main__":
    main()
