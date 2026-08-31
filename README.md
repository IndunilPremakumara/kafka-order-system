# Kafka Order Processing System

A Kafka producer/consumer pipeline for order messages, using Avro
serialization, with real-time price aggregation, retry logic for transient
failures, and a dead letter queue for permanently failed messages.

Work in progress — see the checklist below.

## The order message

`schema/order.avsc` is the single source of truth for the message shape:

| Field     | Type     | Description                                   |
|-----------|----------|-----------------------------------------------|
| `orderId` | `string` | Unique identifier for the order (e.g. "1001") |
| `product` | `string` | Name of the purchased item (e.g. "Item1")     |
| `price`   | `float`  | Price of the product (randomized)             |

Producer and consumer will both load that one file through
`src/avro_codec.py`, so they can never drift out of sync.

## Why no Schema Registry

Avro is normally paired with a Confluent Schema Registry. This project embeds
the `.avsc` file instead and uses `fastavro`'s schemaless writer, because with
a single shared schema file the two sides physically cannot disagree, and it
keeps the infrastructure down to one Kafka container. A registry earns its
keep once schemas need to *evolve* across independently deployed services,
which is not the case here.

## Running Kafka locally

```bash
docker compose up -d
docker compose ps          # wait for kafka to report "healthy"
```

Single broker in KRaft mode, so there is no Zookeeper to run. Kafka UI is
exposed at <http://localhost:8081>.

## Python setup

```bash
python -m venv venv
venv\Scripts\activate                  # macOS/Linux: source venv/bin/activate
pip install -r requirements.txt
```

## Checklist

- [x] Avro schema and codec
- [x] Producer
- [ ] Consumer
- [ ] Real-time running average
- [ ] Retry logic
- [ ] Dead letter queue
- [ ] Tests
