"""
Tests for src/avro_codec.py — no Kafka broker required.
"""
import io
import sys
import os

import fastavro
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from avro_codec import encode_order, decode_order, get_schema


class TestRoundTrip:
    """encode_order → decode_order must be identity for any valid order."""

    def _roundtrip(self, order: dict) -> dict:
        return decode_order(encode_order(order))

    def test_basic_order(self):
        order = {"orderId": "1001", "product": "Item1", "price": 99.99}
        result = self._roundtrip(order)
        assert result["orderId"] == order["orderId"]
        assert result["product"] == order["product"]
        assert abs(result["price"] - order["price"]) < 0.01  # float tolerance

    def test_order_id_as_string(self):
        order = {"orderId": "9999", "product": "ItemX", "price": 1.0}
        result = self._roundtrip(order)
        assert isinstance(result["orderId"], str)
        assert result["orderId"] == "9999"

    def test_product_field_preserved(self):
        for product in ["Item1", "Item5", "Item10", "SpecialItem-αβγ"]:
            order = {"orderId": "0001", "product": product, "price": 10.0}
            result = self._roundtrip(order)
            assert result["product"] == product

    def test_price_zero(self):
        order = {"orderId": "0", "product": "Free", "price": 0.0}
        result = self._roundtrip(order)
        assert abs(result["price"] - 0.0) < 1e-6

    def test_price_large(self):
        order = {"orderId": "X", "product": "Luxury", "price": 99999.99}
        result = self._roundtrip(order)
        assert abs(result["price"] - 99999.99) < 1.0  # float32 precision

    def test_encode_returns_bytes(self):
        order = {"orderId": "1", "product": "A", "price": 5.0}
        result = encode_order(order)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_decode_returns_dict(self):
        order = {"orderId": "2", "product": "B", "price": 7.5}
        result = decode_order(encode_order(order))
        assert isinstance(result, dict)

    def test_all_fields_present_after_roundtrip(self):
        order = {"orderId": "42", "product": "Item3", "price": 123.45}
        result = self._roundtrip(order)
        assert set(result.keys()) == {"orderId", "product", "price"}


class TestSchemaLoading:
    def test_get_schema_returns_parsed_schema(self):
        schema = get_schema()
        assert schema is not None

    def test_schema_has_correct_fields(self):
        schema = get_schema()
        field_names = {f["name"] for f in schema["fields"]}
        assert field_names == {"orderId", "product", "price"}


class TestMalformedInput:
    def test_decode_empty_bytes_raises(self):
        with pytest.raises(Exception):
            decode_order(b"")

    def test_decode_random_bytes_raises(self):
        with pytest.raises(Exception):
            decode_order(b"\x00\x01\x02\x03\x04\x05garbage")

    def test_encode_missing_field_raises(self):
        with pytest.raises(Exception):
            encode_order({"orderId": "1", "product": "X"})  # missing price

    def test_encode_wrong_type_raises(self):
        with pytest.raises(Exception):
            encode_order({"orderId": 123, "product": "X", "price": "not_a_float"})
