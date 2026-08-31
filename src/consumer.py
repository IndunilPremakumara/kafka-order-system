"""
Order Consumer
--------------
Consumes Avro-encoded order messages from the 'orders' topic and prints them.

Offsets are committed manually rather than automatically, so an offset only
moves forward once the message before it has actually been handled. That
matters more as processing gets added in later, but it is easier to build on
than to retrofit.

Usage:
    python consumer.py
    python consumer.py --idle-exit 10
"""
import argparse
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

import config
from avro_codec import decode_order


def log(msg, stream=sys.stdout):
    print(f"[consumer] {msg}", file=stream, flush=True)


def handle_message(msg):
    """Decode one Kafka message and report it."""
    order = decode_order(msg.value())
    log(f"orderId={order['orderId']} product={order['product']} "
        f"price={order['price']:.2f}")
    return order


def main():
    parser = argparse.ArgumentParser(description="Avro order consumer")
    parser.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.ORDERS_TOPIC)
    parser.add_argument("--group-id", default=config.CONSUMER_GROUP)
    parser.add_argument("--idle-exit", type=float, default=0,
                        help="Exit after N seconds with no messages (0 = run forever)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # committed manually, once handled
    })

    consumer.subscribe([args.topic])
    log(f"listening on '{args.topic}' (group={args.group_id})")

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
            handle_message(msg)
            seen += 1
            consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        log("interrupted")
    except KafkaException as exc:
        log(f"fatal kafka exception: {exc}", sys.stderr)
    finally:
        log(f"consumed {seen} orders")
        consumer.close()


if __name__ == "__main__":
    main()
