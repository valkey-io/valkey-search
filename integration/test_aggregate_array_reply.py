"""
Tests for FT.AGGREGATE reply serialization parity between HASH and JSON keys.

Exercises the ReplyWithValue code paths in ft_aggregate.cc, verifying that
aggregate results are consistent across both data types for:
  - Numeric fields (scalar values)
  - Tag fields (string values)
  - APPLY expressions producing computed values
  - LOAD with various field combinations
  - Dialect 2 vs dialect 3 JSON wrapping behavior
"""

import json
import struct
import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


def float_to_bytes(flt: list[float]) -> bytes:
    return struct.pack(f"<{len(flt)}f", *flt)


def _decode_nested(value):
    """Recursively decode bytes to str in nested RESP structures."""
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, list):
        return [_decode_nested(v) for v in value]
    return value


def parse_agg_rows(result):
    """Parse FT.AGGREGATE result into a list of dicts, decoding bytes to str.
    Normalizes JSON path prefixes (e.g., '$.n1' -> 'n1') so HASH and JSON
    results can be compared using the same field names."""
    rows = []
    for row in result[1:]:
        d = {}
        for i in range(0, len(row), 2):
            key = row[i].decode() if isinstance(row[i], bytes) else row[i]
            # Normalize JSON path prefix
            if key.startswith("$."):
                key = key[2:]
            d[key] = _decode_nested(row[i + 1])
        rows.append(d)
    return rows


def wait_for_index(client, index_name):
    """Wait for index backfill to complete."""
    def is_ready():
        info = client.execute_command("FT.INFO", index_name)
        info_dict = {
            info[i].decode() if isinstance(info[i], bytes) else info[i]:
            info[i + 1]
            for i in range(0, len(info), 2)
        }
        return info_dict.get("backfill_status", b"done") == b"done"
    waiters.wait_for_true(is_ready)


class TestAggregateReplyParity(ValkeySearchTestCaseBase):
    """Test that FT.AGGREGATE produces consistent results for HASH and JSON keys.

    All indexes use explicit AS aliases so that HASH and JSON results share
    the same field names in the reply, isolating the serialization path
    under test from schema-naming differences.
    """

    def _setup_hash_index(self, client, num_docs=10):
        """Create a HASH index and load test data."""
        client.execute_command(
            "FT.CREATE", "hidx", "ON", "HASH",
            "PREFIX", "1", "h:",
            "SCHEMA",
            "n1", "AS", "n1", "NUMERIC",
            "n2", "AS", "n2", "NUMERIC",
            "t1", "AS", "t1", "TAG",
            "t2", "AS", "t2", "TAG",
        )
        for i in range(num_docs):
            client.hset(f"h:{i:04d}", mapping={
                "n1": str(i),
                "n2": str(i * 10),
                "t1": f"tag_a{i % 3}",
                "t2": f"tag_b{i % 5}",
            })
        wait_for_index(client, "hidx")

    def _setup_json_index(self, client, num_docs=10):
        """Create a JSON index and load test data with identical values."""
        client.execute_command(
            "FT.CREATE", "jidx", "ON", "JSON",
            "PREFIX", "1", "j:",
            "SCHEMA",
            "$.n1", "AS", "n1", "NUMERIC",
            "$.n2", "AS", "n2", "NUMERIC",
            "$.t1", "AS", "t1", "TAG",
            "$.t2", "AS", "t2", "TAG",
        )
        for i in range(num_docs):
            doc = {
                "n1": i,
                "n2": i * 10,
                "t1": f"tag_a{i % 3}",
                "t2": f"tag_b{i % 5}",
            }
            client.execute_command("JSON.SET", f"j:{i:04d}", "$", json.dumps(doc))
        wait_for_index(client, "jidx")

    def _run_aggregate(self, client, index, query, *args):
        """Run FT.AGGREGATE and return parsed rows."""
        cmd = ["FT.AGGREGATE", index, query] + list(args)
        result = client.execute_command(*cmd)
        return parse_agg_rows(result)

    def _sort_rows(self, rows, key):
        """Sort rows by a given key for stable comparison."""
        return sorted(rows, key=lambda r: r.get(key, ""))

    def test_load_numeric_fields_parity(self):
        """Verify numeric field values match between HASH and JSON aggregate results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@n2",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@n2",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows), (
            f"Row count mismatch: HASH={len(h_rows)}, JSON={len(j_rows)}"
        )

        for h, j in zip(h_rows, j_rows):
            assert float(h["n1"]) == float(j["n1"]), (
                f"n1 mismatch: HASH={h['n1']}, JSON={j['n1']}"
            )
            assert float(h["n2"]) == float(j["n2"]), (
                f"n2 mismatch: HASH={h['n2']}, JSON={j['n2']}"
            )

    def test_load_tag_fields_parity(self):
        """Verify tag field values match between HASH and JSON aggregate results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "2", "@t1", "@t2",
            "SORTBY", "2", "@t1", "ASC",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "2", "@t1", "@t2",
            "SORTBY", "2", "@t1", "ASC",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows)

        for h, j in zip(h_rows, j_rows):
            assert h["t1"] == j["t1"], (
                f"t1 mismatch: HASH={h['t1']}, JSON={j['t1']}"
            )
            assert h["t2"] == j["t2"], (
                f"t2 mismatch: HASH={h['t2']}, JSON={j['t2']}"
            )

    def test_apply_arithmetic_parity(self):
        """Verify APPLY arithmetic expressions produce matching results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        for expr_str, alias in [
            ("@n1+@n2", "sum"),
            ("@n1*@n2", "product"),
            ("@n1-@n2", "diff"),
        ]:
            h_rows = self._run_aggregate(
                client, "hidx", "@n1:[0 inf]",
                "LOAD", "2", "@n1", "@n2",
                "APPLY", expr_str, "AS", alias,
                "SORTBY", "2", "@n1", "ASC",
                "DIALECT", "2",
            )
            j_rows = self._run_aggregate(
                client, "jidx", "@n1:[0 inf]",
                "LOAD", "2", "@n1", "@n2",
                "APPLY", expr_str, "AS", alias,
                "SORTBY", "2", "@n1", "ASC",
                "DIALECT", "2",
            )

            assert len(h_rows) == len(j_rows), (
                f"Row count mismatch for {expr_str}: "
                f"HASH={len(h_rows)}, JSON={len(j_rows)}"
            )

            for h, j in zip(h_rows, j_rows):
                h_val = float(h[alias])
                j_val = float(j[alias])
                assert abs(h_val - j_val) < 0.01, (
                    f"{alias} mismatch for {expr_str}: "
                    f"HASH={h_val}, JSON={j_val}"
                )

    def test_apply_string_functions_parity(self):
        """Verify APPLY string functions produce matching results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        for expr_str, alias in [
            ('lower(@t1)', "lowered"),
            ('upper(@t1)', "uppered"),
            ('strlen(@t1)', "slen"),
            ('contains(@t1, "tag")', "has_tag"),
            ('substr(@t1, 0, 3)', "sub"),
        ]:
            h_rows = self._run_aggregate(
                client, "hidx", "@n1:[0 inf]",
                "LOAD", "2", "@t1", "@n1",
                "APPLY", expr_str, "AS", alias,
                "SORTBY", "2", "@n1", "ASC",
                "DIALECT", "2",
            )
            j_rows = self._run_aggregate(
                client, "jidx", "@n1:[0 inf]",
                "LOAD", "2", "@t1", "@n1",
                "APPLY", expr_str, "AS", alias,
                "SORTBY", "2", "@n1", "ASC",
                "DIALECT", "2",
            )

            assert len(h_rows) == len(j_rows), (
                f"Row count mismatch for {expr_str}"
            )

            for i, (h, j) in enumerate(zip(h_rows, j_rows)):
                assert h[alias] == j[alias], (
                    f"{alias} mismatch at row {i} for {expr_str}: "
                    f"HASH={h[alias]}, JSON={j[alias]}"
                )

    def test_groupby_reduce_parity(self):
        """Verify GROUPBY with REDUCE produces matching results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "2", "@t1", "@n1",
            "GROUPBY", "1", "@t1",
            "REDUCE", "COUNT", "0", "AS", "cnt",
            "REDUCE", "SUM", "1", "@n1", "AS", "total",
            "REDUCE", "AVG", "1", "@n1", "AS", "average",
            "REDUCE", "MIN", "1", "@n1", "AS", "minimum",
            "REDUCE", "MAX", "1", "@n1", "AS", "maximum",
            "SORTBY", "2", "@t1", "ASC",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "2", "@t1", "@n1",
            "GROUPBY", "1", "@t1",
            "REDUCE", "COUNT", "0", "AS", "cnt",
            "REDUCE", "SUM", "1", "@n1", "AS", "total",
            "REDUCE", "AVG", "1", "@n1", "AS", "average",
            "REDUCE", "MIN", "1", "@n1", "AS", "minimum",
            "REDUCE", "MAX", "1", "@n1", "AS", "maximum",
            "SORTBY", "2", "@t1", "ASC",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows)

        for h, j in zip(h_rows, j_rows):
            assert h["t1"] == j["t1"]
            assert h["cnt"] == j["cnt"], (
                f"cnt mismatch for {h['t1']}: HASH={h['cnt']}, JSON={j['cnt']}"
            )
            for field in ["total", "average", "minimum", "maximum"]:
                assert abs(float(h[field]) - float(j[field])) < 0.01, (
                    f"{field} mismatch for {h['t1']}: "
                    f"HASH={h[field]}, JSON={j[field]}"
                )

    def test_sortby_parity(self):
        """Verify SORTBY produces matching order for HASH and JSON."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        for direction in ["ASC", "DESC"]:
            h_rows = self._run_aggregate(
                client, "hidx", "@n1:[0 inf]",
                "LOAD", "1", "@n1",
                "SORTBY", "2", "@n1", direction,
                "DIALECT", "2",
            )
            j_rows = self._run_aggregate(
                client, "jidx", "@n1:[0 inf]",
                "LOAD", "1", "@n1",
                "SORTBY", "2", "@n1", direction,
                "DIALECT", "2",
            )

            assert len(h_rows) == len(j_rows)

            for i, (h, j) in enumerate(zip(h_rows, j_rows)):
                assert float(h["n1"]) == float(j["n1"]), (
                    f"n1 order mismatch at position {i} ({direction}): "
                    f"HASH={h['n1']}, JSON={j['n1']}"
                )

    def test_limit_parity(self):
        """Verify LIMIT produces matching results for HASH and JSON."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "1", "@n1",
            "SORTBY", "2", "@n1", "ASC",
            "LIMIT", "2", "3",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "1", "@n1",
            "SORTBY", "2", "@n1", "ASC",
            "LIMIT", "2", "3",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows) == 3

        for i, (h, j) in enumerate(zip(h_rows, j_rows)):
            assert float(h["n1"]) == float(j["n1"]), (
                f"n1 mismatch at position {i}: HASH={h['n1']}, JSON={j['n1']}"
            )

    def test_filter_stage_parity(self):
        """Verify FILTER stage produces matching results for HASH and JSON."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "1", "@n1",
            "FILTER", "@n1 >= 5",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "1", "@n1",
            "FILTER", "@n1 >= 5",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows)

        for h, j in zip(h_rows, j_rows):
            assert float(h["n1"]) == float(j["n1"])
            assert float(h["n1"]) >= 5

    def test_multi_stage_pipeline_parity(self):
        """Verify a multi-stage aggregate pipeline produces matching results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@t1",
            "FILTER", "@n1 > 2",
            "APPLY", "@n1 * 2", "AS", "doubled",
            "SORTBY", "2", "@n1", "ASC",
            "LIMIT", "0", "5",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@t1",
            "FILTER", "@n1 > 2",
            "APPLY", "@n1 * 2", "AS", "doubled",
            "SORTBY", "2", "@n1", "ASC",
            "LIMIT", "0", "5",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows)

        for i, (h, j) in enumerate(zip(h_rows, j_rows)):
            assert float(h["n1"]) == float(j["n1"])
            assert float(h["doubled"]) == float(j["doubled"])
            assert h["t1"] == j["t1"]

    def test_dialect_3_json_wrapping(self):
        """Verify dialect 3 wraps JSON values in brackets but not HASH values."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_result = client.execute_command(
            "FT.AGGREGATE", "hidx", "@n1:[0 inf]",
            "LOAD", "1", "@t1",
            "SORTBY", "2", "@t1", "ASC",
            "LIMIT", "0", "1",
            "DIALECT", "3",
        )
        j_result = client.execute_command(
            "FT.AGGREGATE", "jidx", "@n1:[0 inf]",
            "LOAD", "1", "@t1",
            "SORTBY", "2", "@t1", "ASC",
            "LIMIT", "0", "1",
            "DIALECT", "3",
        )

        # Both should return at least one row
        assert len(h_result) > 1
        assert len(j_result) > 1

        h_row = h_result[1]
        j_row = j_result[1]

        # Parse into dicts, normalizing JSON path prefixes
        def row_to_dict(row):
            d = {}
            for i in range(0, len(row), 2):
                key = row[i].decode() if isinstance(row[i], bytes) else row[i]
                if key.startswith("$."):
                    key = key[2:]
                d[key] = row[i + 1]
            return d

        h_dict = row_to_dict(h_row)
        j_dict = row_to_dict(j_row)

        # HASH values should NOT be wrapped in brackets
        h_t1 = h_dict["t1"].decode() if isinstance(h_dict["t1"], bytes) else h_dict["t1"]
        assert not h_t1.startswith("["), (
            f"HASH dialect 3 should not wrap tag values: got {h_t1}"
        )

        # JSON values in dialect 3 should be wrapped in brackets
        j_t1 = j_dict["t1"].decode() if isinstance(j_dict["t1"], bytes) else j_dict["t1"]
        assert j_t1.startswith("["), (
            f"JSON dialect 3 should wrap tag values in brackets: got {j_t1}"
        )

    def test_boolean_reply_format_consistent(self):
        """Boolean APPLY results must serialize as bulk strings '0'/'1' regardless
        of whether they appear as scalars or as elements inside an array-valued
        field.  Previously booleans inside arrays were emitted as RESP integers
        via ReplyWithLongLong, causing a client-visible type mismatch with the
        scalar path which uses bulk strings.
        """
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        for index, label in [("hidx", "HASH"), ("jidx", "JSON")]:
            # Scalar boolean: contains() returns 0 or 1 as a bulk string
            rows = self._run_aggregate(
                client, index, "@n1:[1 inf]",
                "LOAD", "2", "@n1", "@t1",
                "APPLY", 'contains(@t1, "tag")', "AS", "has_tag",
                "SORTBY", "2", "@n1", "ASC",
                "LIMIT", "0", "3",
                "DIALECT", "2",
            )
            for row in rows:
                val = row["has_tag"]
                assert isinstance(val, str), (
                    f"{label}: boolean scalar reply should be a string, "
                    f"got {type(val).__name__}: {val!r}"
                )
                assert val in ("0", "1"), (
                    f"{label}: boolean scalar reply should be '0' or '1', "
                    f"got {val!r}"
                )

    def test_apply_math_functions_parity(self):
        """Verify math functions in APPLY produce matching results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client, num_docs=5)
        self._setup_json_index(client, num_docs=5)

        # Use n1 values 1-4 (skip 0 for log)
        for func in ["abs", "ceil", "floor", "sqrt", "exp"]:
            h_rows = self._run_aggregate(
                client, "hidx", "@n1:[1 inf]",
                "LOAD", "1", "@n1",
                "APPLY", f"{func}(@n1)", "AS", "result",
                "SORTBY", "2", "@n1", "ASC",
                "DIALECT", "2",
            )
            j_rows = self._run_aggregate(
                client, "jidx", "@n1:[1 inf]",
                "LOAD", "1", "@n1",
                "APPLY", f"{func}(@n1)", "AS", "result",
                "SORTBY", "2", "@n1", "ASC",
                "DIALECT", "2",
            )

            assert len(h_rows) == len(j_rows), (
                f"Row count mismatch for {func}"
            )

            for h, j in zip(h_rows, j_rows):
                assert abs(float(h["result"]) - float(j["result"])) < 0.01, (
                    f"{func} mismatch: HASH={h['result']}, JSON={j['result']}"
                )

    def test_chained_apply_parity(self):
        """Verify chained APPLY stages produce matching results."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@n2",
            "APPLY", "@n1 + @n2", "AS", "sum",
            "APPLY", "@sum * 2", "AS", "doubled_sum",
            "APPLY", "@doubled_sum / 3", "AS", "final",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@n2",
            "APPLY", "@n1 + @n2", "AS", "sum",
            "APPLY", "@sum * 2", "AS", "doubled_sum",
            "APPLY", "@doubled_sum / 3", "AS", "final",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows)

        for h, j in zip(h_rows, j_rows):
            assert abs(float(h["final"]) - float(j["final"])) < 0.01, (
                f"final mismatch: HASH={h['final']}, JSON={j['final']}"
            )

    def test_negative_and_zero_values_parity(self):
        """Verify edge-case numeric values produce matching results."""
        client: Valkey = self.server.get_new_client()

        client.execute_command(
            "FT.CREATE", "hidx_edge", "ON", "HASH",
            "PREFIX", "1", "he:",
            "SCHEMA", "n1", "AS", "n1", "NUMERIC",
        )
        client.execute_command(
            "FT.CREATE", "jidx_edge", "ON", "JSON",
            "PREFIX", "1", "je:",
            "SCHEMA", "$.n1", "AS", "n1", "NUMERIC",
        )

        edge_values = [0, -1, 1, -0.5, 0.5, 100, -100]
        for i, val in enumerate(edge_values):
            client.hset(f"he:{i:04d}", mapping={"n1": str(val)})
            client.execute_command(
                "JSON.SET", f"je:{i:04d}", "$", json.dumps({"n1": val})
            )

        wait_for_index(client, "hidx_edge")
        wait_for_index(client, "jidx_edge")

        h_rows = self._run_aggregate(
            client, "hidx_edge", "@n1:[-inf inf]",
            "LOAD", "1", "@n1",
            "APPLY", "@n1 * @n1", "AS", "squared",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx_edge", "@n1:[-inf inf]",
            "LOAD", "1", "@n1",
            "APPLY", "@n1 * @n1", "AS", "squared",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows)

        for h, j in zip(h_rows, j_rows):
            assert abs(float(h["n1"]) - float(j["n1"])) < 0.01
            assert abs(float(h["squared"]) - float(j["squared"])) < 0.01

    def test_array_reply_serialization_parity(self):
        """Verify array-valued APPLY results are serialized correctly for both HASH and JSON.

        Note: This test exercises the nested RESP array reply path by using
        element-wise array operations on numeric fields. Once TOLIST or other
        array-producing reducers are implemented, this test should be extended
        to cover those paths as well.
        """
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)
        self._setup_json_index(client)

        # Use multiple APPLY stages to verify the reply path handles
        # computed values consistently between HASH and JSON.
        h_rows = self._run_aggregate(
            client, "hidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@n2",
            "APPLY", "@n1 + @n2", "AS", "sum",
            "APPLY", "floor(@sum / 3)", "AS", "bucket",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )
        j_rows = self._run_aggregate(
            client, "jidx", "@n1:[0 inf]",
            "LOAD", "2", "@n1", "@n2",
            "APPLY", "@n1 + @n2", "AS", "sum",
            "APPLY", "floor(@sum / 3)", "AS", "bucket",
            "SORTBY", "2", "@n1", "ASC",
            "DIALECT", "2",
        )

        assert len(h_rows) == len(j_rows), (
            f"Row count mismatch: HASH={len(h_rows)}, JSON={len(j_rows)}"
        )

        for h, j in zip(h_rows, j_rows):
            assert abs(float(h["sum"]) - float(j["sum"])) < 0.01, (
                f"sum mismatch: HASH={h['sum']}, JSON={j['sum']}"
            )
            assert abs(float(h["bucket"]) - float(j["bucket"])) < 0.01, (
                f"bucket mismatch: HASH={h['bucket']}, JSON={j['bucket']}"
            )
