"""
Shared Avro encode/decode helpers.

We avoid depending on a Confluent Schema Registry service (extra infra to run)
and instead embed the schema at build time, writing/reading raw Avro binary
(single-record, schemaless encoding) via fastavro. Producer and consumer both
load the same order.avsc file, so they always agree on the schema.
"""
import io
import os
import fastavro

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema", "order.avsc")

_schema = fastavro.schema.load_schema(SCHEMA_PATH)


def get_schema():
    return _schema


def encode_order(order: dict) -> bytes:
    """Serialize a dict matching order.avsc into Avro binary bytes."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, _schema, order)
    return buf.getvalue()


def decode_order(data: bytes) -> dict:
    """Deserialize Avro binary bytes back into a dict."""
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, _schema)
