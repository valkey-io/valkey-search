import json
import struct
import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def _vec(values):
    return struct.pack(f"<{len(values)}f", *values)


class TestAggregateLoadTextVector(ValkeySearchTestCaseBase):
    """
    Regression tests for FT.AGGREGATE LOAD against a TEXT or VECTOR field.

    Loading a TEXT field used to crash the server (CHECK(false) on the
    indexer type during reply). Loading a VECTOR field is not a supported
    operation and should be reported as an error rather than crashing or
    returning raw vector bytes.
    """

    def _create_index(self, client: Valkey, key_type: str):
        if key_type == "HASH":
            assert client.execute_command(
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA",
                "price", "NUMERIC",
                "title", "TEXT",
                "embedding", "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32",
                "DIM", "3",
                "DISTANCE_METRIC", "L2",
            ) == b"OK"

            for i in range(3):
                assert client.execute_command(
                    "HSET", f"doc:{i}",
                    "price", str(10 + i),
                    "title", f"hello world {i}",
                    "embedding", _vec([float(i), float(i + 1), float(i + 2)]),
                ) == 3
        else:
            assert key_type == "JSON"
            assert client.execute_command(
                "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "doc:",
                "SCHEMA",
                "$.price", "AS", "price", "NUMERIC",
                "$.title", "AS", "title", "TEXT",
                "$.embedding", "AS", "embedding",
                "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32",
                "DIM", "3",
                "DISTANCE_METRIC", "L2",
            ) == b"OK"

            for i in range(3):
                doc = {
                    "price": 10 + i,
                    "title": f"hello world {i}",
                    "embedding": [float(i), float(i + 1), float(i + 2)],
                }
                assert client.execute_command(
                    "JSON.SET", f"doc:{i}", "$", json.dumps(doc),
                ) == b"OK"

    @pytest.mark.parametrize("key_type", ["HASH", "JSON"])
    def test_load_text_field_does_not_crash(self, key_type):
        client: Valkey = self.server.get_new_client()
        self._create_index(client, key_type)

        result = client.execute_command(
            "FT.AGGREGATE", "idx", "@price:[-inf +inf]",
            "LOAD", "1", "@title",
        )

        expected_field = b"title" if key_type == "HASH" else b"$.title"

        assert result[0] == 3
        for row in result[1:]:
            fields = dict(zip(row[::2], row[1::2]))
            assert expected_field in fields
            assert b"hello world" in fields[expected_field]

        assert client.ping() is True

    @pytest.mark.parametrize("key_type", ["HASH", "JSON"])
    def test_load_vector_field_returns_error(self, key_type):
        client: Valkey = self.server.get_new_client()
        self._create_index(client, key_type)

        with pytest.raises(ResponseError, match=r"(?i)vector"):
            client.execute_command(
                "FT.AGGREGATE", "idx", "@price:[-inf +inf]",
                "LOAD", "1", "@embedding",
            )

        assert client.ping() is True
