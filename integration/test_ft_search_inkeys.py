"""Integration tests for FT.SEARCH INKEYS option."""

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from indexes import float_to_bytes


# KNN docs use vectors [i, i, i, i]; the query vector is [0, 0, 0, 0], so L2
# distance grows monotonically with i. vdoc:0 is nearest, vdoc:{N-1} farthest,
# giving a deterministic nearest-neighbor ranking to place in-set keys inside
# or outside the global top-K on demand.
KNN_NUM_DOCS = 50


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

    def test_inkeys_with_nocontent(self):
        """NOCONTENT: reply is count + keys only, no field arrays."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "3", "doc:0", "doc:1", "doc:2",
            "NOCONTENT",
            "DIALECT", "2",
        )

        assert result[0] == 3
        assert len(result) == 4
        keys = {result[i].decode() for i in range(1, len(result))}
        assert keys == {"doc:0", "doc:1", "doc:2"}

    def test_inkeys_with_sortby_and_limit(self):
        """INKEYS + SORTBY + LIMIT: ordering survives truncation."""
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
        keys = [result[i].decode() for i in range(1, len(result), 2)]
        assert keys == ["doc:4", "doc:3"]

    def test_inkeys_invalid_count_errors(self):
        """Malformed INKEYS counts reject at parse time."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # Zero count — empty result.
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "0",
            "DIALECT", "2",
        )
        assert result[0] == 0

        # Negative count.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "-1", "doc:0",
                "DIALECT", "2",
            )

        # Non-integer count.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "abc",
                "DIALECT", "2",
            )

        # Count exceeds provided keys.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "5", "doc:0", "doc:1", "doc:2",
            )


    def _setup_hnsw_index(self, client: Valkey, num_docs: int = KNN_NUM_DOCS):
        """HNSW vector index plus a numeric @score field. The numeric field
        lets a broad predicate steer the planner into the inline-filter branch."""
        client.execute_command(
            "FT.CREATE", "vidx",
            "ON", "HASH",
            "PREFIX", "1", "vdoc:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4",
            "DISTANCE_METRIC", "L2",
            "score", "NUMERIC",
        )
        for i in range(num_docs):
            client.execute_command(
                "HSET", f"vdoc:{i}",
                "vec", float_to_bytes([float(i)] * 4),
                "score", str(i),
            )

    def _returned_keys(self, result) -> set:
        return {result[i].decode() for i in range(1, len(result), 2)}

    def test_inkeys_knn_pure_outside_topk(self):
        """Pure KNN restricted to the three farthest docs, well outside the
        global top-3. The fix runs an exact search over the in-set keys, so
        all three come back ("k nearest within the set")."""
        client: Valkey = self.server.get_new_client()
        self._setup_hnsw_index(client)

        far_keys = [f"vdoc:{KNN_NUM_DOCS - 1}", f"vdoc:{KNN_NUM_DOCS - 2}",
                    f"vdoc:{KNN_NUM_DOCS - 3}"]
        result = client.execute_command(
            "FT.SEARCH", "vidx", "*=>[KNN 3 @vec $BLOB]",
            "INKEYS", str(len(far_keys)), *far_keys,
            "PARAMS", "2", "BLOB", float_to_bytes([0.0] * 4),
            "DIALECT", "2",
        )
        assert result[0] == 3, (
            f"KNN+INKEYS must return all 3 in-set matches, got {result[0]} "
            f"keys={self._returned_keys(result)}"
        )
        assert self._returned_keys(result) == set(far_keys)

    def test_inkeys_knn_predicate_intersection(self):
        """INKEYS names near+far keys; the predicate excludes the near ones.
        Result is the exact intersection (far pair only), proving both INKEYS
        and the predicate drive candidate selection, not just post-filtering."""
        client: Valkey = self.server.get_new_client()
        self._setup_hnsw_index(client)

        inkeys = ["vdoc:1", "vdoc:2", f"vdoc:{KNN_NUM_DOCS - 2}",
                  f"vdoc:{KNN_NUM_DOCS - 1}"]
        result = client.execute_command(
            "FT.SEARCH", "vidx", "@score:[3 +inf]=>[KNN 4 @vec $BLOB]",
            "INKEYS", str(len(inkeys)), *inkeys,
            "PARAMS", "2", "BLOB", float_to_bytes([0.0] * 4),
            "DIALECT", "2",
        )
        expected = {f"vdoc:{KNN_NUM_DOCS - 2}", f"vdoc:{KNN_NUM_DOCS - 1}"}
        assert result[0] == 2, (
            f"expected the 2 in-set keys passing the predicate, got "
            f"{result[0]} keys={self._returned_keys(result)}"
        )
        assert self._returned_keys(result) == expected

    def test_inkeys_knn_nonexistent_keys(self):
        """A real far key mixed with nonexistent keys: only the real key is
        returned. Confirms absent keys are skipped, not counted."""
        client: Valkey = self.server.get_new_client()
        self._setup_hnsw_index(client)

        keys = [f"vdoc:{KNN_NUM_DOCS - 1}", "vdoc:9999", "nope:1"]
        result = client.execute_command(
            "FT.SEARCH", "vidx", "*=>[KNN 3 @vec $BLOB]",
            "INKEYS", str(len(keys)), *keys,
            "PARAMS", "2", "BLOB", float_to_bytes([0.0] * 4),
            "DIALECT", "2",
        )
        assert result[0] == 1, f"expected 1 real key, got {result[0]}"
        assert self._returned_keys(result) == {f"vdoc:{KNN_NUM_DOCS - 1}"}
