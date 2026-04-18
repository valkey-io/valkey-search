"""Integration tests for FT.SEARCH INKEYS option."""

import struct
import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def float_to_bytes(values: list) -> bytes:
    return struct.pack(f"<{len(values)}f", *values)


class TestFTSearchInkeys(ValkeySearchTestCaseBase):

    def _setup_hash_index(self, client: Valkey):
        client.execute_command(
            "FT.CREATE", "idx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "score", "NUMERIC",
            "category", "TAG",
        )
        for i in range(5):
            client.execute_command(
                "HSET", f"doc:{i}",
                "score", str(i * 10),
                "category", f"cat{i % 2}",
            )

    def _setup_vector_index(self, client: Valkey):
        client.execute_command(
            "FT.CREATE", "vidx",
            "ON", "HASH",
            "PREFIX", "1", "vdoc:",
            "SCHEMA",
            "label", "TAG",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32",
            "DIM", "3",
            "DISTANCE_METRIC", "L2",
        )
        for i in range(5):
            vec = float_to_bytes([float(i), float(i), float(i)])
            client.execute_command(
                "HSET", f"vdoc:{i}",
                "label", f"lbl{i}",
                "vec", vec,
            )

    def test_inkeys_with_nocontent(self):
        """AC1: INKEYS filters to specified keys; NOCONTENT returns only key names."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "3", "doc:0", "doc:1", "doc:2",
            "NOCONTENT",
            "DIALECT", "2",
        )

        assert result[0] == 3
        assert len(result) == 4  # count + 3 key names
        keys = {result[i].decode() for i in range(1, len(result))}
        assert keys == {"doc:0", "doc:1", "doc:2"}

    def test_inkeys_all_nonexistent(self):
        """AC3: All non-existent keys returns empty result."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "2", "ghost:99", "ghost:100",
            "NOCONTENT",
            "DIALECT", "2",
        )

        assert result[0] == 0
        assert len(result) == 1

    def test_inkeys_mixed_existing_nonexistent(self):
        """AC2: Non-existent keys silently ignored; correct content for survivors."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # With content — verify field values for surviving keys
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "4", "doc:2", "ghost:1", "ghost:2", "doc:4",
            "DIALECT", "2",
        )

        assert result[0] == 2
        docs = {}
        for i in range(1, len(result), 2):
            key = result[i].decode()
            fields = result[i + 1]
            field_dict = {}
            for j in range(0, len(fields), 2):
                field_dict[fields[j].decode()] = fields[j + 1].decode()
            docs[key] = field_dict

        assert set(docs.keys()) == {"doc:2", "doc:4"}
        assert docs["doc:2"]["score"] == "20"
        assert docs["doc:4"]["score"] == "40"

    def test_inkeys_with_limit(self):
        """AC1: INKEYS combined with LIMIT truncation and offset."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # LIMIT 0 2: total is 4, but only 2 returned
        result_trunc = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "4", "doc:0", "doc:1", "doc:2", "doc:3",
            "LIMIT", "0", "2",
            "NOCONTENT",
            "DIALECT", "2",
        )
        assert result_trunc[0] == 4
        assert len(result_trunc) == 3

        # LIMIT 1 2: offset into the result set
        result_all = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "3", "doc:0", "doc:1", "doc:2",
            "NOCONTENT",
            "DIALECT", "2",
        )
        result_offset = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "3", "doc:0", "doc:1", "doc:2",
            "LIMIT", "1", "2",
            "NOCONTENT",
            "DIALECT", "2",
        )
        assert result_all[0] == 3
        assert result_offset[0] == 3
        assert len(result_offset) == 3

    def test_inkeys_with_sortby(self):
        """AC1: INKEYS combined with SORTBY preserves ordering."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        result_asc = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "3", "doc:0", "doc:2", "doc:4",
            "SORTBY", "score", "ASC",
            "DIALECT", "2",
        )
        result_desc = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "3", "doc:0", "doc:2", "doc:4",
            "SORTBY", "score", "DESC",
            "DIALECT", "2",
        )

        assert result_asc[0] == 3
        assert result_desc[0] == 3

        keys_asc = [result_asc[i].decode() for i in range(1, len(result_asc), 2)]
        keys_desc = [result_desc[i].decode() for i in range(1, len(result_desc), 2)]

        assert keys_asc == list(reversed(keys_desc))
        assert keys_asc == ["doc:0", "doc:2", "doc:4"]

    def test_inkeys_with_content_and_return(self):
        """AC1: INKEYS returns correct content in default and RETURN modes."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # Default content mode — all fields returned
        result_default = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "2", "doc:1", "doc:3",
            "DIALECT", "2",
        )
        assert result_default[0] == 2
        docs = {}
        for i in range(1, len(result_default), 2):
            key = result_default[i].decode()
            fields = result_default[i + 1]
            field_dict = {}
            for j in range(0, len(fields), 2):
                field_dict[fields[j].decode()] = fields[j + 1].decode()
            docs[key] = field_dict
        assert set(docs.keys()) == {"doc:1", "doc:3"}
        assert docs["doc:1"]["score"] == "10"
        assert docs["doc:3"]["score"] == "30"

        # RETURN projection — only requested fields
        result_return = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "2", "doc:1", "doc:3",
            "RETURN", "1", "score",
            "DIALECT", "2",
        )
        assert result_return[0] == 2
        for i in range(1, len(result_return), 2):
            fields = result_return[i + 1]
            field_names = [fields[j].decode() for j in range(0, len(fields), 2)]
            assert field_names == ["score"]

    def test_inkeys_with_vector_knn(self):
        """AC1: INKEYS filters vector KNN results to specified keys."""
        client: Valkey = self.server.get_new_client()
        self._setup_vector_index(client)

        query_vec = float_to_bytes([0.1, 0.1, 0.1])

        result_all = client.execute_command(
            "FT.SEARCH", "vidx",
            "*=>[KNN 5 @vec $qvec]",
            "PARAMS", "2", "qvec", query_vec,
            "NOCONTENT",
            "DIALECT", "2",
        )
        assert result_all[0] == 5

        result_inkeys = client.execute_command(
            "FT.SEARCH", "vidx",
            "*=>[KNN 5 @vec $qvec]",
            "PARAMS", "2", "qvec", query_vec,
            "INKEYS", "2", "vdoc:0", "vdoc:2",
            "NOCONTENT",
            "DIALECT", "2",
        )

        assert result_inkeys[0] == 2
        keys = {result_inkeys[i].decode() for i in range(1, len(result_inkeys))}
        assert keys == {"vdoc:0", "vdoc:2"}

    def test_inkeys_vector_knn_all_filtered_out(self):
        """AC3: All INKEYS non-existent in vector search returns empty."""
        client: Valkey = self.server.get_new_client()
        self._setup_vector_index(client)

        query_vec = float_to_bytes([0.0, 0.0, 0.0])

        result = client.execute_command(
            "FT.SEARCH", "vidx",
            "*=>[KNN 5 @vec $qvec]",
            "PARAMS", "2", "qvec", query_vec,
            "INKEYS", "1", "ghost:99",
            "NOCONTENT",
            "DIALECT", "2",
        )

        assert result[0] == 0

    def test_inkeys_with_sortby_and_limit(self):
        """AC1: INKEYS combined with both SORTBY and LIMIT."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "4", "doc:0", "doc:1", "doc:3", "doc:4",
            "SORTBY", "score", "DESC",
            "LIMIT", "0", "2",
            "DIALECT", "2",
        )

        assert result[0] == 4
        # With content: [count, key, fields, key, fields, ...]
        keys = [result[i].decode() for i in range(1, len(result), 2)]
        assert keys == ["doc:4", "doc:3"]

    def test_inkeys_with_numeric_filter(self):
        """AC1: INKEYS combined with a numeric range filter."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # score: doc:0=0, doc:1=10, doc:2=20, doc:3=30, doc:4=40
        # Filter: score >= 15, INKEYS: doc:0..doc:3
        # Expected: doc:2 and doc:3 (doc:0 and doc:1 excluded by query)
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[15 +inf]",
            "INKEYS", "4", "doc:0", "doc:1", "doc:2", "doc:3",
            "NOCONTENT",
            "DIALECT", "2",
        )

        assert result[0] == 2
        keys = {result[i].decode() for i in range(1, len(result))}
        assert keys == {"doc:2", "doc:3"}

    def test_inkeys_with_tag_filter(self):
        """AC1: INKEYS combined with tag filter; zero-overlap case."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # cat0 docs: doc:0, doc:2, doc:4. INKEYS: doc:0..doc:3 → doc:0, doc:2
        result = client.execute_command(
            "FT.SEARCH", "idx", "@category:{cat0}",
            "INKEYS", "4", "doc:0", "doc:1", "doc:2", "doc:3",
            "NOCONTENT",
            "DIALECT", "2",
        )
        assert result[0] == 2
        keys = {result[i].decode() for i in range(1, len(result))}
        assert keys == {"doc:0", "doc:2"}

        # Zero overlap: cat0 docs are even, INKEYS are odd → empty
        result_empty = client.execute_command(
            "FT.SEARCH", "idx", "@category:{cat0}",
            "INKEYS", "2", "doc:1", "doc:3",
            "NOCONTENT",
            "DIALECT", "2",
        )
        assert result_empty[0] == 0

    def test_inkeys_invalid_count_errors(self):
        """AC4: Zero, negative, non-integer, and mismatched counts return errors."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # Zero count — returns empty result (matches Redis behavior)
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "0",
            "DIALECT", "2",
        )
        assert result[0] == 0

        # Negative count
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "-1", "doc:0",
                "DIALECT", "2",
            )

        # Non-integer count
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "abc",
                "DIALECT", "2",
            )

        # Count exceeds provided keys — parser consumes "DIALECT" and "2"
        # as key arguments, then fails on the missing 5th key.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "5", "doc:0", "doc:1", "doc:2",
            )


