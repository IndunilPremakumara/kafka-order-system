"""
Central configuration for the Kafka order system.

Every value can be overridden with an environment variable, which keeps the
scripts runnable unchanged against a local Docker broker or a remote cluster.
"""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "orders-dlq")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "order-consumer-group")
