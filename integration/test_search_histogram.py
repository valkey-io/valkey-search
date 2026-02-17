"""
Integration test for search execution time histogram.
Verifies that the worker_search_execution_latency histogram is updated after searches.
"""

import struct
import time
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker


class TestSearchHistogram(ValkeySearchTestCaseBase):
    """Test that search operations update the execution time histogram."""

    def _create_index_and_add_docs(self, index_name, prefix, vector_dim, num_docs=10):
        """Helper to create index and add documents.

        Args:
            index_name: Name of the index to create.
            prefix: Key prefix for documents.
            vector_dim: Dimension of vectors.
            num_docs: Number of documents to create.

        Returns:
            Tuple of (query_vector, query_bytes) for searching.
        """
        self.client.execute_command(
            "FT.CREATE", index_name,
            "PREFIX", "1", f"{prefix}:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", str(vector_dim),
            "DISTANCE_METRIC", "L2"
        )

        for i in range(num_docs):
            vector = [float(i) / 10.0] * vector_dim
            vector_bytes = b''.join(struct.pack('f', x) for x in vector)
            self.client.hset(f"{prefix}:{i}", "vec", vector_bytes)

        # Return query vector for searching
        query_vector = [0.5] * vector_dim
        query_bytes = b''.join(struct.pack('f', x) for x in query_vector)
        return query_vector, query_bytes

    def _verify_histogram_updated(self, mode_name):
        """Helper to verify histogram was updated with meaningful values.
        
        Args:
            mode_name: Description of the execution mode (for error messages).
        """
        info = self.client.execute_command("INFO", "search")
        
        # Verify histogram field exists
        assert "search_worker_search_execution_latency_usec" in info, \
            "Histogram field should be present in INFO"
        
        # Verify histogram has data
        histogram_value = info["search_worker_search_execution_latency_usec"]
        assert histogram_value, f"Histogram should have data after {mode_name}"
        assert isinstance(histogram_value, dict), \
            f"Histogram should be a dict, got: {type(histogram_value)}"
        assert "p50" in histogram_value, \
            f"Histogram should contain p50, got: {histogram_value}"
        
        # Verify p50 > 0 (meaningful latency)
        p50 = float(histogram_value["p50"])
        assert p50 > 0, \
            f"{mode_name} p50 latency should be > 0 microseconds, got: {p50}"
        
        # Check successful request counter was incremented
        success_count = int(info.get("search_successful_requests_count", 0))
        assert success_count > 0, \
            f"Successful requests count should be > 0 after {mode_name}"

    def test_histogram_updated_after_async_search(self):
        """Verify histogram is updated after an async (normal) vector search."""
        index_name = "vec_hist_idx"
        vector_dim = 128

        # Setup index and documents
        query_vector, query_bytes = self._create_index_and_add_docs(
            index_name, "vdoc", vector_dim)

        # Perform async search
        result = self.client.execute_command(
            "FT.SEARCH", index_name,
            "*=>[KNN 5 @vec $query]",
            "PARAMS", "2", "query", query_bytes,
            "DIALECT", "2"
        )

        # Verify search succeeded
        assert result[0] > 0, "Async search should return results"

        # Verify histogram was updated
        self._verify_histogram_updated("async search")

    def test_histogram_updated_after_sync_search(self):
        """Verify histogram is updated after a synchronous (MULTI/EXEC) search."""
        index_name = "sync_hist_idx"
        vector_dim = 64
        
        # Setup index and documents
        query_vector, query_bytes = self._create_index_and_add_docs(
            index_name, "sdoc", vector_dim)
        
        # Perform synchronous search using MULTI/EXEC
        self.client.execute_command("MULTI")
        self.client.execute_command(
            "FT.SEARCH", index_name,
            "*=>[KNN 3 @vec $query]",
            "PARAMS", "2", "query", query_bytes,
            "DIALECT", "2"
        )
        results = self.client.execute_command("EXEC")
        
        # Verify search succeeded
        assert results is not None
        assert len(results) == 1
        assert results[0][0] > 0, "Sync search should return results"
        
        # Verify histogram was updated
        self._verify_histogram_updated("sync search")


class TestSearchHistogramCluster(ValkeySearchClusterTestCase):
    """Test histogram updates in cluster/fanout mode."""

    def _verify_histogram_updated(self, client, mode_name):
        """Helper to verify histogram was updated with meaningful values.

        Args:
            client: The client to use for INFO command.
            mode_name: Description of the execution mode (for error messages).
        """
        info = client.execute_command("INFO", "search")

        # Verify histogram field exists and has data
        histogram_value = info["search_worker_search_execution_latency_usec"]
        assert histogram_value, f"Histogram should have data after {mode_name}"
        assert isinstance(histogram_value, dict), f"Histogram should be a dict"
        assert "p50" in histogram_value, f"Histogram should contain p50"
        
        # Verify p50 > 0 (meaningful latency)
        p50 = float(histogram_value["p50"])
        assert p50 > 0, \
            f"{mode_name} p50 latency should be > 0 microseconds, got: {p50}"
        
        # Check successful request counter was incremented
        success_count = int(info.get("search_successful_requests_count", 0))
        assert success_count > 0, \
            f"Successful requests count should be > 0 after {mode_name}"
    
    def test_histogram_updated_after_fanout_search(self):
        """Verify histogram is updated after a fanout (cluster) search."""
        # Create a vector index on primary node 0
        index_name = "fanout_hist_idx"
        vector_dim = 32
        
        primary_0 = self.new_client_for_primary(0)
        primary_0.execute_command(
            "FT.CREATE", index_name,
            "PREFIX", "1", "fdoc:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", str(vector_dim),
            "DISTANCE_METRIC", "L2"
        )
        
        # Add many documents to ensure all shards have data
        cluster_client = self.new_cluster_client()
        for i in range(100):
            vector = [float(i % 10) / 10.0] * vector_dim
            vector_bytes = b''.join(struct.pack('f', x) for x in vector)
            cluster_client.hset(f"fdoc:{i}", "vec", vector_bytes)
        
        # Perform fanout search
        query_vector = [0.6] * vector_dim
        query_bytes = b''.join(struct.pack('f', x) for x in query_vector)
        
        result = cluster_client.execute_command(
            "FT.SEARCH", index_name,
            "*=>[KNN 5 @vec $query]",
            "PARAMS", "2", "query", query_bytes,
            "DIALECT", "2"
        )
        
        # Verify search succeeded
        assert result[0] > 0, "Fanout search should return results"
        
        # Verify histogram was updated on shards that have data
        # Hash-based distribution means not all shards may have documents
        num_primaries = len(self.replication_groups)
        shards_with_histogram = 0
        for i in range(num_primaries):
            primary_client = self.new_client_for_primary(i)
            info = primary_client.execute_command("INFO", "search")

            if "search_worker_search_execution_latency_usec" in info:
                histogram_value = info["search_worker_search_execution_latency_usec"]
                if histogram_value:
                    shards_with_histogram += 1
                    self._verify_histogram_updated(
                        primary_client,
                        f"fanout search on primary {i}"
                    )

        # At least one shard must have executed the search
        assert shards_with_histogram > 0, \
            f"At least one shard should have histogram data, found {shards_with_histogram}"


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, "-v"])
