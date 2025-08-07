import struct
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestReclaimableMemory(ValkeySearchTestCaseBase):
    """
    Test suite for the reclaimable_memory info field.
    
    This field should track memory that can be reclaimed after vector deletions,
    but is currently not implemented (always returns 0).
    """

    def test_reclaimable_memory_with_vector_operations(self):
        """
        Test reclaimable_memory behavior with vector index operations.
        
        This test demonstrates the expected behavior:
        1. Create a vector index
        2. Insert vectors
        3. Delete some vectors
        4. Check if reclaimable_memory increases
        5. Drop the index and mark sure reclaimable memory is 0.
        """

        client: Valkey = self.server.get_new_client()
        
        # Create a vector index
        index_name = "myIndex"
        client.execute_command("FT.CREATE", index_name, "SCHEMA", "vector",
                                "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM",
                                  "3", "DISTANCE_METRIC", "COSINE"
        )
        
        # Get initial reclaimable memory
        info_data = client.info("SEARCH")
        initial_reclaimable = int(info_data["search_index_reclaimable_memory"])
        assert initial_reclaimable == 0
        
        # Insert some vectors (3D vectors to match the index dimension)
        vectors = [
            ("vec:1", [1.0, 2.0, 3.0]),
            ("vec:2", [2.0, 3.0, 4.0]),
            ("vec:3", [3.0, 4.0, 5.0]),
            ("vec:4", [4.0, 5.0, 6.0]),
            ("vec:5", [5.0, 6.0, 7.0])
        ]
        
        for key, vector in vectors:
            # Convert vector to proper binary format (little-endian float32)
            vector_bytes = struct.pack('<3f', *vector)
            client.hset(key, "vector", vector_bytes)
        
        
        # Create a query vector in the same format
        query_vector = struct.pack('<3f', 1.0, 2.0, 3.0)
        
        # Verify vectors were indexed
        result = client.execute_command(
            "FT.SEARCH", index_name, "*=>[KNN 5 @vector $query_vector]", "PARAMS", "2", "query_vector", query_vector
        )
        assert result[0] == 5  # Should have 5 documents
        
        # Get memory usage after insertion
        info_data = client.info("SEARCH")
        after_insert_reclaimable = int(info_data["search_index_reclaimable_memory"])
        # Currently should still be 0
        assert after_insert_reclaimable == 0
        
        # Delete some vectors
        client.delete("vec:1")
        client.delete("vec:2")
        client.delete("vec:3")
        
        # Verify vectors were deleted from index
        result = client.execute_command(
            "FT.SEARCH", index_name, "*=>[KNN 10 @vector $query_vector]", "PARAMS", "2", "query_vector", query_vector
        )
        assert result[0] == 2  # Should have 2 documents remaining
        
        # Check reclaimable memory after deletions
        info_data = client.info("SEARCH")
        after_delete_reclaimable = int(info_data["search_index_reclaimable_memory"])
        
        # Each vector is 3 float32 values = 3 * 4 = 12 bytes
        # We deleted 3 vectors, so reclaimable memory should increase by 3 * 12 = 36 bytes
        expected_reclaimable = after_insert_reclaimable + (3 * 3 * 4)  # 3 vectors * 3 dimensions * 4 bytes per float32
        assert after_delete_reclaimable == expected_reclaimable
        
        # Drop the index
        client.execute_command("FT.DROPINDEX", index_name)
        
        # Check that reclaimable memory is 0 after dropping the index
        info_data = client.info("SEARCH")
        after_drop_reclaimable = int(info_data["search_index_reclaimable_memory"])
        assert after_drop_reclaimable == 0


    def test_reclaimable_memory_multiple_indexes(self):
        """Test reclaimable_memory behavior with multiple vector indexes."""
        client: Valkey = self.server.get_new_client()
        
        # Create multiple indexes
        indexes = ["multi_idx_1", "multi_idx_2"]
        
        for idx_name in indexes:
            client.execute_command(
                "FT.CREATE", idx_name,
                "ON", "HASH",
                "PREFIX", "1", f"{idx_name}:",
                "SCHEMA",
                "embedding", "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32",
                "DIM", "2",
                "DISTANCE_METRIC", "IP"
            )
        
        # Get initial reclaimable memory
        info_data = client.info("SEARCH")
        initial_reclaimable = int(info_data["search_index_reclaimable_memory"])
        assert initial_reclaimable == 0

        # Add vectors to both indexes
        for idx_name in indexes:
            for i in range(5):
                key = f"{idx_name}:{i}"
                vector = [float(i), float(i*2)]
                # Convert vector to proper binary format (little-endian float32)
                vector_bytes = struct.pack('<2f', *vector)
                client.hset(key, "embedding", vector_bytes)
        
        # Get memory usage after insertion
        info_data = client.info("SEARCH")
        after_insert_reclaimable = int(info_data["search_index_reclaimable_memory"])
        assert after_insert_reclaimable == 0
        
        # Delete vectors from first index only
        for i in range(3):
            client.delete(f"multi_idx_1:{i}")
        
        # Check reclaimable memory (should be global across all indexes)
        info_data = client.info("SEARCH")
        
        # The vectors are still in use
        assert int(info_data["search_index_reclaimable_memory"]) == 0

        # Delete vectors from second index
        for i in range(3):
            client.delete(f"multi_idx_2:{i}")

        # Check reclaimable memory 
        info_data = client.info("SEARCH")
        # The vectors are no longer in use
        assert int(info_data["search_index_reclaimable_memory"]) == 24
