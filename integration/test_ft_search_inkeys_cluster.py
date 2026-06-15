"""Integration tests for FT.SEARCH INKEYS in cluster mode.

Validates correct total_count semantics across shards, including the
engaged-empty (INKEYS 0) case and partial key sets distributed across
multiple shards.
"""

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Numeric, Tag, Vector, float_to_bytes


class TestFTSearchInkeysCluster(ValkeySearchClusterTestCase):

    def _setup_numeric_index(self, client: ValkeyCluster, num_docs: int = 50):
        """Create a HASH index with numeric+tag fields and load data spread
        across shards."""
        client.execute_command(
            "FT.CREATE", "idx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "score", "NUMERIC",
            "category", "TAG",
        )
        for i in range(num_docs):
            client.execute_command(
                "HSET", f"doc:{i}",
                "score", str(i),
                "category", f"cat{i % 3}",
            )

    def _setup_vector_index(self, client: ValkeyCluster, num_docs: int = 30):
        """Create a HASH index with a FLAT vector field and load data."""
        client.execute_command(
            "FT.CREATE", "vidx",
            "ON", "HASH",
            "PREFIX", "1", "vdoc:",
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "4",
            "DISTANCE_METRIC", "L2",
        )
        for i in range(num_docs):
            vec = float_to_bytes([float(i)] * 4)
            client.execute_command(
                "HSET", f"vdoc:{i}",
                "vec", vec,
            )

    def test_inkeys_zero_returns_zero_total_count(self):
        """INKEYS 0 in cluster mode must return total_count == 0, not a stale
        shard count.  This is the primary regression test for the presence-bit
        bug where empty INKEYS was indistinguishable from unset on the wire."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_numeric_index(client, num_docs=100)

        # INKEYS 0 — should return 0 rows AND total_count == 0
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "0",
            "DIALECT", "2",
        )
        assert result[0] == 0, (
            f"INKEYS 0 should report total_count=0, got {result[0]}"
        )
        # Only the count element should be present
        assert len(result) == 1

    def test_inkeys_zero_nocontent_returns_zero(self):
        """INKEYS 0 + NOCONTENT in cluster mode — same expectation."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_numeric_index(client, num_docs=50)

        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "0",
            "NOCONTENT",
            "DIALECT", "2",
        )
        assert result[0] == 0
        assert len(result) == 1

    def test_inkeys_partial_set_correct_total_count(self):
        """Non-empty INKEYS spread across shards: total_count must equal the
        number of keys that actually exist and match the query, not the raw
        shard-level match count."""
        client: ValkeyCluster = self.new_cluster_client()
        num_docs = 100
        self._setup_numeric_index(client, num_docs=num_docs)

        # Pick a subset of keys that exist
        target_keys = [f"doc:{i}" for i in range(0, num_docs, 5)]  # 20 keys
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", str(len(target_keys)), *target_keys,
            "LIMIT", "0", "100",
            "DIALECT", "2",
        )

        # total_count should be exactly the number of matching keys
        assert result[0] == len(target_keys), (
            f"Expected total_count={len(target_keys)}, got {result[0]}"
        )
        # Verify we got all the keys back
        returned_keys = {result[i].decode() for i in range(1, len(result), 2)}
        assert returned_keys == set(target_keys)

    def test_inkeys_partial_with_limit_correct_total_count(self):
        """INKEYS with LIMIT: total_count reflects all in-set matches,
        not just the returned page."""
        client: ValkeyCluster = self.new_cluster_client()
        num_docs = 100
        self._setup_numeric_index(client, num_docs=num_docs)

        target_keys = [f"doc:{i}" for i in range(20)]  # 20 keys
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", str(len(target_keys)), *target_keys,
            "LIMIT", "0", "5",
            "DIALECT", "2",
        )

        # total_count should be 20 (all matching keys), even though LIMIT is 5
        assert result[0] == 20, (
            f"Expected total_count=20, got {result[0]}"
        )
        # Should return exactly 5 rows
        returned_count = (len(result) - 1) // 2
        assert returned_count == 5

    def test_inkeys_nonexistent_keys_excluded(self):
        """Keys in INKEYS that don't exist in the dataset are not counted."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_numeric_index(client, num_docs=10)

        # Mix of existing and non-existing keys
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "5", "doc:0", "doc:1", "fake:99", "fake:100", "fake:101",
            "DIALECT", "2",
        )
        # Only doc:0 and doc:1 exist
        assert result[0] == 2
        returned_keys = {result[i].decode() for i in range(1, len(result), 2)}
        assert returned_keys == {"doc:0", "doc:1"}

    def test_inkeys_with_filter_predicate(self):
        """INKEYS combined with a filter predicate — both must be satisfied."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_numeric_index(client, num_docs=50)

        # INKEYS limits to doc:0..doc:19, filter further narrows to score < 10
        target_keys = [f"doc:{i}" for i in range(20)]
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[0 9]",
            "INKEYS", str(len(target_keys)), *target_keys,
            "LIMIT", "0", "100",
            "DIALECT", "2",
        )

        # Only doc:0 through doc:9 match both conditions
        assert result[0] == 10, (
            f"Expected total_count=10, got {result[0]}"
        )

    def test_inkeys_knn_vector_search(self):
        """KNN + INKEYS: total_count equals the number of returned neighbors
        that pass the INKEYS filter. The post-filter in content_resolution
        is the only corrector for vector queries."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_vector_index(client, num_docs=30)

        # Query vector near vdoc:0, but restrict to a subset of keys
        query_vec = float_to_bytes([0.0] * 4)
        target_keys = ["vdoc:0", "vdoc:1", "vdoc:2"]
        result = client.execute_command(
            "FT.SEARCH", "vidx",
            "*=>[KNN 10 @vec $BLOB]",
            "INKEYS", "3", *target_keys,
            "PARAMS", "2", "BLOB", query_vec,
            "LIMIT", "0", "10",
            "DIALECT", "2",
        )

        # total_count should be <= 3 (only keys in INKEYS that match)
        assert result[0] <= 3, (
            f"Expected total_count<=3, got {result[0]}"
        )
        # All returned keys must be in the target set
        returned_keys = {result[i].decode() for i in range(1, len(result), 2)}
        assert returned_keys.issubset(set(target_keys))
        assert result[0] == len(returned_keys)

    def test_inkeys_knn_zero_returns_zero(self):
        """KNN + INKEYS 0: no results and total_count == 0."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_vector_index(client, num_docs=30)

        query_vec = float_to_bytes([0.0] * 4)
        result = client.execute_command(
            "FT.SEARCH", "vidx",
            "*=>[KNN 10 @vec $BLOB]",
            "INKEYS", "0",
            "PARAMS", "2", "BLOB", query_vec,
            "DIALECT", "2",
        )

        assert result[0] == 0, (
            f"KNN + INKEYS 0 should report total_count=0, got {result[0]}"
        )
        assert len(result) == 1

    def test_inkeys_with_sortby(self):
        """INKEYS + SORTBY in cluster mode — ordering is correct and
        total_count reflects the in-set match count."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_numeric_index(client, num_docs=50)

        target_keys = [f"doc:{i}" for i in range(10)]
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", str(len(target_keys)), *target_keys,
            "SORTBY", "score", "ASC",
            "LIMIT", "0", "10",
            "DIALECT", "2",
        )

        assert result[0] == 10
        # Verify sort order by extracting scores
        keys = [result[i].decode() for i in range(1, len(result), 2)]
        # doc:0 should come first (score=0), doc:9 last (score=9)
        assert keys[0] == "doc:0"
        assert keys[-1] == "doc:9"
