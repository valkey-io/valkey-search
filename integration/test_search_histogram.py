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

    def test_histogram_updated_after_async_search(self):
        """Verify histogram is updated after an async (normal) vector search."""
        # Create a vector index
        index_name = "vec_hist_idx"
        vector_dim = 128
        
        self.client.execute_command(
            "FT.CREATE", index_name,
            "PREFIX", "1", "vdoc:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", str(vector_dim),
            "DISTANCE_METRIC", "L2"
        )
        
        # Add some documents
        for i in range(10):
            vector = [float(i) / 10.0] * vector_dim
            vector_bytes = b''.join(struct.pack('f', x) for x in vector)
            self.client.hset(f"vdoc:{i}", "vec", vector_bytes)
        
        # Wait for indexing
        time.sleep(0.5)
        
        # Perform a search
        query_vector = [0.5] * vector_dim
        query_bytes = b''.join(struct.pack('f', x) for x in query_vector)
        
        result = self.client.execute_command(
            "FT.SEARCH", index_name,
            "*=>[KNN 5 @vec $query]",
            "PARAMS", "2", "query", query_bytes,
            "DIALECT", "2"
        )
        
        # Verify search succeeded
        assert result[0] > 0, "Search should return results"
        
        # Check INFO search for histogram
        info = self.client.execute_command("INFO", "search")
        
        # The histogram field should exist
        assert "search_worker_search_execution_latency_usec" in info, \
            "Histogram field should be present in INFO"
        
        # Check if histogram has data (returned as dict with percentiles)
        histogram_value = info["search_worker_search_execution_latency_usec"]
        assert histogram_value, f"Histogram should have data, got: '{histogram_value}'"
        
        # Verify it contains histogram data (dict with percentiles)
        assert isinstance(histogram_value, dict), \
            f"Histogram should be a dict, got: {type(histogram_value)}"
        assert "p50" in histogram_value, \
            f"Histogram should contain p50, got: {histogram_value}"
        
        # Verify histogram values are meaningful (> 0 microseconds)
        p50 = float(histogram_value["p50"])
        assert p50 > 0, \
            f"p50 latency should be > 0 microseconds, got: {p50}"
        
        # Check successful request counter was incremented
        success_count = int(info.get("search_successful_requests_count", 0))
        assert success_count > 0, \
            f"Successful requests count should be > 0, got: {success_count}"
    
    def test_histogram_updated_after_sync_search(self):
        """Verify histogram is updated after a synchronous (MULTI/EXEC) search."""
        # Create a vector index
        index_name = "sync_hist_idx"
        vector_dim = 64
        
        self.client.execute_command(
            "FT.CREATE", index_name,
            "PREFIX", "1", "sdoc:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", str(vector_dim),
            "DISTANCE_METRIC", "L2"
        )
        
        # Add some documents
        for i in range(10):
            vector = [float(i) / 10.0] * vector_dim
            vector_bytes = b''.join(struct.pack('f', x) for x in vector)
            self.client.hset(f"sdoc:{i}", "vec", vector_bytes)
        
        # Wait for indexing
        time.sleep(0.5)
        
        # Perform a synchronous search using MULTI/EXEC
        query_vector = [0.5] * vector_dim
        query_bytes = b''.join(struct.pack('f', x) for x in query_vector)
        
        # Start MULTI
        self.client.execute_command("MULTI")
        
        # Queue FT.SEARCH
        self.client.execute_command(
            "FT.SEARCH", index_name,
            "*=>[KNN 3 @vec $query]",
            "PARAMS", "2", "query", query_bytes,
            "DIALECT", "2"
        )
        
        # Execute
        results = self.client.execute_command("EXEC")
        
        # Verify search succeeded
        assert results is not None
        assert len(results) == 1
        assert results[0][0] > 0, "Search should return results"
        
        # Check INFO search for histogram
        info = self.client.execute_command("INFO", "search")
        
        # Verify histogram was updated
        histogram_value = info["search_worker_search_execution_latency_usec"]
        assert histogram_value, f"Histogram should have data after sync search"
        assert isinstance(histogram_value, dict), f"Histogram should be a dict"
        assert "p50" in histogram_value, f"Histogram should contain p50"
        
        # Verify histogram values are meaningful (> 0 microseconds)
        p50 = float(histogram_value["p50"])
        assert p50 > 0, \
            f"Sync search p50 latency should be > 0 microseconds, got: {p50}"
        
        # Check successful request counter
        success_count = int(info.get("search_successful_requests_count", 0))
        assert success_count > 0, \
            f"Successful requests count should be > 0 after sync search"


class TestSearchHistogramCluster(ValkeySearchClusterTestCase):
    """Test histogram updates in cluster/fanout mode."""
    
    def test_histogram_updated_after_fanout_search(self):
        """Verify histogram is updated after a fanout (cluster) search."""
        # Create a vector index on primary node 0
        index_name = "fanout_hist_idx"
        vector_dim = 32
        
        client = self.new_client_for_primary(0)
        client.execute_command(
            "FT.CREATE", index_name,
            "PREFIX", "1", "fdoc:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", str(vector_dim),
            "DISTANCE_METRIC", "L2"
        )
        
        # Add documents across shards
        cluster_client = self.new_cluster_client()
        for i in range(20):
            vector = [float(i % 10) / 10.0] * vector_dim
            vector_bytes = b''.join(struct.pack('f', x) for x in vector)
            cluster_client.hset(f"fdoc:{i}", "vec", vector_bytes)
        
        # Wait for indexing
        time.sleep(1.0)
        
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
        
        # Check INFO on primary node 0
        info = client.execute_command("INFO", "search")
        
        # Verify histogram was updated on this shard
        histogram_value = info["search_worker_search_execution_latency_usec"]
        assert histogram_value, f"Histogram should have data after fanout search"
        assert isinstance(histogram_value, dict), f"Histogram should be a dict"
        assert "p50" in histogram_value, f"Histogram should contain p50"
        
        # Verify histogram values are meaningful (> 0 microseconds)
        p50 = float(histogram_value["p50"])
        assert p50 > 0, \
            f"Fanout search p50 latency should be > 0 microseconds, got: {p50}"
        
        # Check successful request counter
        success_count = int(info.get("search_successful_requests_count", 0))
        assert success_count > 0, \
            f"Successful requests count should be > 0 after fanout search"


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, "-v"])
