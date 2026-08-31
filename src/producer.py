"""
Order Producer
--------------
Generates randomized order messages, serializes them with Avro (schema in
schema/order.avsc) and publishes them to the 'orders' Kafka topic.

The orderId is used as the message key, so all events for one order always
land on the same partition and are processed in order.

Usage:
    python producer.py                        # 1 order/sec, forever
    python producer.py --count 50 --rate 5    # 50 orders at 5/sec
"""
import argparse
import random
import sys
import time

from confluent_kafka import Producer

import config
from avro_codec import encode_order

PRODUCTS = [f"Item{i}" for i in range(1, 11)]


def log(msg, stream=sys.stdout):
    print(f"[producer] {msg}", file=stream, flush=True)


def delivery_report(err, msg):
    """Called once per message when the broker acks it (or gives up)."""
    if err is not None:
        log(f"DELIVERY FAILED key={msg.key()}: {err}", sys.stderr)
    else:
        log(f"delivered orderId={msg.key().decode()} "
            f"partition={msg.partition()} offset={msg.offset()}")


def make_order(order_id):
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Avro order producer")
    parser.add_argument("--count", type=int, default=0,
                        help="Number of messages to send (0 = run forever)")
    parser.add_argument("--rate", type=float, default=1.0, help="Messages per second")
    parser.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.ORDERS_TOPIC)
    parser.add_argument("--start-id", type=int, default=1001)
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "acks": "all",              # wait for the full ISR -> durability
        "enable.idempotence": True,  # no duplicates on internal retries
        "retries": 5,                # librdkafka retries transient send errors
        "retry.backoff.ms": 300,
    })

    order_id = args.start_id - 1
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            order_id += 1
            order = make_order(order_id)

            producer.produce(args.topic,
                             key=order["orderId"].encode("utf-8"),
                             value=encode_order(order),
                             callback=delivery_report)
            producer.poll(0)   # serve delivery callbacks
            sent += 1
            if args.rate > 0:
                time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        log("interrupted, flushing...")
    except BufferError:
        log("local queue full, flushing...", sys.stderr)
        producer.flush(15)
    finally:
        remaining = producer.flush(15)
        if remaining:
            log(f"WARNING: {remaining} messages still undelivered after flush",
                sys.stderr)
        log(f"done. sent={sent}")


if __name__ == "__main__":
    main()
