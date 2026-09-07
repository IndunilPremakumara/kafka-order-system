"""
DLQ Consumer
------------
Reads and displays messages from the 'orders-dlq' Dead Letter Queue topic.

Use this during a live demo to inspect every order that permanently failed
processing, along with the reason it was routed to the DLQ.

Usage:
    python dlq_consumer.py
    python dlq_consumer.py --idle-exit 10
"""
import argparse
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

import config


def log(msg, stream=sys.stdout):
    print(f"[dlq-consumer] {msg}", file=stream, flush=True)


def decode_header(headers, key: str, fallback: str = "?") -> str:
    """Extract a named header value from the confluent-kafka headers list."""
    if not headers:
        return fallback
    for k, v in headers:
        if k == key:
            return v.decode("utf-8") if isinstance(v, bytes) else str(v)
    return fallback


def display_dlq_message(msg) -> None:
    """Pretty-print one DLQ message with all diagnostic headers."""
    headers = msg.headers() or []
    key = msg.key().decode("utf-8") if msg.key() else "?"

    reason = decode_header(headers, "X-Error-Reason")
    retries = decode_header(headers, "X-Retry-Count")
    original_topic = decode_header(headers, "X-Original-Topic")
    original_partition = decode_header(headers, "X-Original-Partition")
    original_offset = decode_header(headers, "X-Original-Offset")

    log(
        f"★ DLQ message ──────────────────────────────\n"
        f"    orderId          : {key}\n"
        f"    original topic   : {original_topic} "
        f"[partition={original_partition}, offset={original_offset}]\n"
        f"    attempts made    : {retries}\n"
        f"    failure reason   : {reason}\n"
        f"────────────────────────────────────────────"
    )


def main():
    parser = argparse.ArgumentParser(description="Dead Letter Queue consumer")
    parser.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.DLQ_TOPIC)
    parser.add_argument("--group-id", default=f"{config.CONSUMER_GROUP}-dlq")
    parser.add_argument("--idle-exit", type=float, default=0,
                        help="Exit after N seconds with no new DLQ messages (0 = wait forever)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([args.topic])
    log(f"watching DLQ topic '{args.topic}' (group={args.group_id})")

    seen = 0
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
                log(f"kafka error: {msg.error()}", sys.stderr)
                continue

            last_message_at = time.time()
            display_dlq_message(msg)
            seen += 1
            consumer.commit(msg, asynchronous=False)

    except KeyboardInterrupt:
        log("interrupted")
    except KafkaException as exc:
        log(f"fatal kafka exception: {exc}", sys.stderr)
    finally:
        log(f"total DLQ messages seen: {seen}")
        consumer.close()


if __name__ == "__main__":
    main()
