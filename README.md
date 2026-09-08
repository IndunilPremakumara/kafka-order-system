# Kafka Order Processing System

A Kafka producer/consumer pipeline for order messages, using **Avro serialization**,
with **real-time price aggregation**, **retry logic** for transient failures,
and a **Dead Letter Queue (DLQ)** for permanently failed messages.

## Architecture

```
┌──────────────┐   Avro bytes   ┌──────────────────┐
│   producer   │ ─────────────► │  orders  (topic) │
└──────────────┘                └────────┬─────────┘
                                         │ poll
                                ┌────────▼─────────┐
                                │     consumer     │
                                │  ┌─────────────┐ │
                                │  │  aggregator │ │  running avg price
                                │  └─────────────┘ │
                                │  ┌─────────────┐ │
                                │  │ retry loop  │ │  up to 3 attempts
                                │  └──────┬──────┘ │
                                └─────────┼────────┘
                                          │ on failure
                                ┌─────────▼────────┐
                                │  orders-dlq      │ ◄── dlq_consumer
                                └──────────────────┘
```

## Order Message Schema

`schema/order.avsc` is the single source of truth for the message shape:

| Field      | Type     | Description                                    |
|------------|----------|------------------------------------------------|
| `orderId`  | `string` | Unique identifier for the order (e.g. "1001")  |
| `product`  | `string` | Name of the purchased item (e.g. "Item1")      |
| `price`    | `float`  | Price of the product (randomized)              |

Both producer and consumer load the schema through `src/avro_codec.py`, so
they can never drift out of sync.

## Why No Schema Registry

Avro is normally paired with a Confluent Schema Registry. This project embeds
the `.avsc` file instead and uses `fastavro`'s schemaless writer, because with
a single shared schema file the two sides physically cannot disagree, and it
keeps the infrastructure down to one Kafka container.

---

## Quick Start

### 1 — Start Kafka

```bash
docker compose up -d
docker compose ps          # wait for kafka to report "healthy"
```

Single broker in KRaft mode — no Zookeeper needed.
**Kafka UI** is available at <http://localhost:8081>.

### 2 — Python setup

```bash
python -m venv venv
venv\Scripts\activate          # macOS / Linux: source venv/bin/activate
pip install -r requirements.txt
```

### 3 — Run the producer

```bash
# stream 30 orders at 2 per second
python src/producer.py --count 30 --rate 2

# run forever at 1 per second (default)
python src/producer.py
```

### 4 — Run the consumer (in a second terminal)

```bash
# normal run — no injected failures
python src/consumer.py

# demo: 30 % transient failures + 10 % permanent failures
python src/consumer.py --fail-rate 0.3 --perm-fail-rate 0.1

# stop after 10 s of silence
python src/consumer.py --idle-exit 10
```

The consumer prints a running average after every successfully processed order:

```
[consumer] OK  orderId=  1001  product=Item3     price=  142.50  │  n=1  avg=142.50
[consumer] OK  orderId=  1002  product=Item7     price=   88.20  │  n=2  avg=115.35
[consumer] TRANSIENT FAIL (attempt 1/3), retrying in 0.5s: simulated transient failure …
[consumer] OK  orderId=  1003  product=Item1     price=  310.00  │  n=3  avg=180.23
[consumer] → DLQ  orderId=1004  reason='simulated permanent failure …'
```

### 5 — Inspect the DLQ (in a third terminal)

```bash
python src/dlq_consumer.py

# exit automatically after 10 s of no new DLQ messages
python src/dlq_consumer.py --idle-exit 10
```

Example output:

```
[dlq-consumer] ★ DLQ message ──────────────────────────────
    orderId          : 1004
    original topic   : orders [partition=0, offset=3]
    attempts made    : 1
    failure reason   : simulated permanent failure for orderId=1004
────────────────────────────────────────────
```

---

## Feature Details

### Real-Time Price Aggregation

`src/aggregator.py` — thread-safe `RunningAverage` class.
The consumer calls `price_aggregator.update(price)` after every successful
message and prints the live count and average alongside each order.

### Retry Logic

Wrapped inside `handle_with_retry()` in `src/consumer.py`:

| Attempt | Delay before next attempt |
|---------|--------------------------|
| 1       | 0.5 s                    |
| 2       | 1.0 s                    |
| 3       | 2.0 s → DLQ              |

Only `TransientError` (and generic exceptions) trigger a retry.
`PermanentError` skips directly to DLQ on the first attempt.

Configure with `--max-retries N` (default: 3).

### Dead Letter Queue

Failed messages are forwarded to the `orders-dlq` topic with these headers:

| Header                | Content                              |
|-----------------------|--------------------------------------|
| `X-Error-Reason`      | Human-readable failure message       |
| `X-Retry-Count`       | Number of processing attempts made   |
| `X-Original-Topic`    | Source topic (`orders`)              |
| `X-Original-Partition`| Source partition number              |
| `X-Original-Offset`   | Source offset                        |

The original Avro bytes are preserved in the DLQ payload.

---

## Tests

```bash
pytest tests/ -v
```

All tests run without a live Kafka broker (Kafka I/O is mocked).

| Test file                       | Covers                                   |
|---------------------------------|------------------------------------------|
| `tests/test_aggregator.py`      | `RunningAverage` — correctness & thread safety |
| `tests/test_avro_codec.py`      | Avro encode/decode round-trips & error cases |
| `tests/test_consumer_retry.py`  | Retry logic, DLQ routing, backoff, headers |

---

## Project Layout

```
kafka-order-system/
├── docker-compose.yml        # Kafka (KRaft) + Kafka UI
├── requirements.txt
├── schema/
│   └── order.avsc            # Avro schema — single source of truth
├── scripts/
│   ├── setup-topics.sh
│   └── setup-topics.ps1
├── src/
│   ├── config.py             # Env-var-driven configuration
│   ├── avro_codec.py         # encode_order / decode_order helpers
│   ├── aggregator.py         # Thread-safe RunningAverage
│   ├── producer.py           # Order producer
│   ├── consumer.py           # Order consumer (retry + DLQ)
│   └── dlq_consumer.py       # DLQ inspector
└── tests/
    ├── test_aggregator.py
    ├── test_avro_codec.py
    └── test_consumer_retry.py
```

---

## Checklist

- [x] Avro schema and codec
- [x] Producer
- [x] Consumer
- [x] Real-time running average of prices
- [x] Retry logic (exponential back-off, configurable max retries)
- [x] Dead Letter Queue
- [x] Tests
