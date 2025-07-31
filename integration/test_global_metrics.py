import struct
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestGlobalMetrics(ValkeySearchTestCaseBase):
    """    
    This test suite validates the fields within the search_global_metrics section.
    """

    def test_global_metrics(self):
        """
        Test search_global_metrics section fields only.
        
        This test validates only the fields within the search_global_metrics section
        from INFO SEARCH output, including:
        """

        client: Valkey = self.server.get_new_client()
        
        # Create an index with vector, tag, and numeric fields
        index_name = "test_idx"
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vector", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2",
            "price", "NUMERIC",
            "category", "TAG"
        )
        
        # Get initial global metrics
        info_data = client.info("SEARCH")
        
        # Validate initial search_global_metrics fields
        assert int(info_data["search_flat_nodes"]) == 0
        assert int(info_data["search_hnsw_edges"]) == 0
        assert int(info_data["search_hnsw_edges_marked_deleted"]) == 0
        assert int(info_data["search_hnsw_nodes"]) == 0
        assert int(info_data["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data["search_interned_strings"]) >= 0
        assert int(info_data["search_interned_strings_marked_deleted"]) == 0
        assert int(info_data["search_interned_strings_memory"]) >= 0
        assert int(info_data["search_keys_memory"]) >= 0
        assert int(info_data["search_numeric_records"]) == 0
        assert int(info_data["search_tags"]) >= 0
        assert int(info_data["search_tags_memory"]) >= 0
        assert int(info_data["search_vectors_memory"]) == 0
        assert int(info_data["search_vectors_memory_marked_deleted"]) == 0
        
        # Insert a document with all field types
        doc_key = "doc:1"
        embedding_vector = [0.1, 0.2, 0.3, 0.4]
        vector_bytes = struct.pack('<4f', *embedding_vector)
        
        client.hset(doc_key, mapping={
            "vector": vector_bytes,
            "category": "electronics",
            "price": 999
        })
        
        # Get metrics after insertion
        info_data = client.info("SEARCH")
        
        # Validate search_global_metrics fields after document insertion
        assert int(info_data["search_flat_nodes"]) == 0
        assert int(info_data["search_hnsw_edges"]) >= 0  # May vary based on HNSW implementation
        assert int(info_data["search_hnsw_edges_marked_deleted"]) == 0
        assert int(info_data["search_hnsw_nodes"]) == 1
        assert int(info_data["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data["search_interned_strings"]) >= 1
        assert int(info_data["search_interned_strings_marked_deleted"]) == 0
        assert int(info_data["search_interned_strings_memory"]) > 0
        assert int(info_data["search_keys_memory"]) > 0
        assert int(info_data["search_numeric_records"]) == 1
        assert int(info_data["search_tags"]) >= 1  # "electronics"
        assert int(info_data["search_tags_memory"]) > 0
        assert int(info_data["search_vectors_memory"]) == 16  # 4 floats * 4 bytes = 16 bytes
        assert int(info_data["search_vectors_memory_marked_deleted"]) == 0
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_deleted_key_memory_marked_as_deleted(self):
        """
        Test that after deleting a key, memory is still allocated but marked as deleted.
        
        This test validates that when a document is deleted, the memory is not immediately
        freed but instead marked as deleted, as shown in the search_global_metrics.
        """
        
        client: Valkey = self.server.get_new_client()

        # Create an index with vector, tag, and numeric fields
        index_name = "test_idx"
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vector", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2",
            "price", "NUMERIC",
            "category", "TAG", "SEPARATOR", "|"
        )
        
        # Insert a document with all field types
        doc_key = "doc:1"
        embedding_vector = [0.1, 0.2, 0.3, 0.4]
        vector_bytes = struct.pack('<4f', *embedding_vector)
        
        client.hset(doc_key, mapping={
            "vector": vector_bytes,
            "category": "aaaa|bbbb",
            "price": 999
        })
        
        # Get metrics after insertion
        info_data_after_insert = client.info("SEARCH")
        
        # Validate that data is present and not marked as deleted
        assert int(info_data_after_insert["search_hnsw_nodes"]) == 1
        assert int(info_data_after_insert["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_vectors_memory"]) == 16  # 4 floats * 4 bytes
        assert int(info_data_after_insert["search_vectors_memory_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_interned_strings"]) >= 1
        assert int(info_data_after_insert["search_interned_strings_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_interned_strings_memory"]) > 0
        # Store values before deletion for comparison
        hnsw_edges_before = int(info_data_after_insert["search_hnsw_edges"])
        hnsw_nodes_before = int(info_data_after_insert["search_hnsw_nodes"])
        vectors_memory_before = int(info_data_after_insert["search_vectors_memory"])

        # Delete the document
        result = client.delete(doc_key)
        assert result == 1  # Confirm deletion was successful
        
        # Get metrics after deletion
        info_data_after_delete = client.info("SEARCH")
        
        # Validate that memory is still allocated but marked as deleted
        # HNSW edges should be marked as deleted but memory still allocated
        assert int(info_data_after_delete["search_hnsw_edges"]) == hnsw_edges_before
        assert int(info_data_after_delete["search_hnsw_edges_marked_deleted"]) == hnsw_edges_before
        
        # HNSW nodes should be marked as deleted but still counted
        assert int(info_data_after_delete["search_hnsw_nodes"]) == hnsw_nodes_before
        assert int(info_data_after_delete["search_hnsw_nodes_marked_deleted"]) == hnsw_nodes_before
        
        # Interned strings only the vector string remains
        assert int(info_data_after_delete["search_interned_strings"]) == 1 
        assert int(info_data_after_delete["search_interned_strings_marked_deleted"]) == 1
        assert int(info_data_after_delete["search_interned_strings_memory"]) == 16
        
        # Vector memory should be marked as deleted but still allocated
        assert int(info_data_after_delete["search_vectors_memory"]) == vectors_memory_before
        assert int(info_data_after_delete["search_vectors_memory_marked_deleted"]) == vectors_memory_before
        
        # Keys memory should now be 0 since the key is deleted
        assert int(info_data_after_delete["search_keys_memory"]) == 0
        
        # Numeric and tag data should also be marked as deleted
        assert int(info_data_after_delete["search_numeric_records"]) == 0
        assert int(info_data_after_delete["search_tags"]) == 0
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_shared_vector_across_indices_drop_one_index(self):
        """
        Test that when the same vector is added to 2 indices tracking the same keys,
        after dropping one index the string is not marked as deleted since it is 
        alive in the second index.
        
        This test validates that interned strings are properly reference counted
        across multiple indices and are deleted when no index references them.
        """
        client: Valkey = self.server.get_new_client()
        # Create first index
        index_name1 = "test_idx1"
        client.execute_command(
            "FT.CREATE", index_name1,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vector", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2",
            "category", "TAG"
        )
        
        # Create second index with same prefix (tracking same keys)
        index_name2 = "test_idx2"
        client.execute_command(
            "FT.CREATE", index_name2,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vector", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2",
            "price", "NUMERIC"
        )
        
        # Insert a document that will be indexed by both indices
        doc_key = "doc:1"
        embedding_vector = [0.1, 0.2, 0.3, 0.4]
        vector_bytes = struct.pack('<4f', *embedding_vector)
        
        client.hset(doc_key, mapping={
            "vector": vector_bytes,
            "category": "electronics",
            "price": 999
        })
        
        # Get metrics after insertion (both indices should reference the same vector)
        info_data_after_insert = client.info("SEARCH")

        # Validate that data is present and not marked as deleted
        # Both indices should reference the same interned string for the vector
        assert int(info_data_after_insert["search_hnsw_nodes"]) == 2  # One node per index
        assert int(info_data_after_insert["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_vectors_memory"]) == 16  # Both indices using the same vector string
        assert int(info_data_after_insert["search_vectors_memory_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_interned_strings"]) >= 1  # At least the vector string
        assert int(info_data_after_insert["search_interned_strings_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_interned_strings_memory"]) > 16

        # Drop the first index
        client.execute_command("FT.DROPINDEX", index_name1)
        
        # Get metrics after dropping first index
        info_data_after_drop = client.info("SEARCH")
        
        # Validate that the interned strings are NOT marked as deleted
        # because they are still referenced by the second index
        assert int(info_data_after_drop["search_hnsw_nodes"]) == 1  # Only second index remains
        assert int(info_data_after_drop["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_after_drop["search_vectors_memory"]) == 16
        assert int(info_data_after_drop["search_vectors_memory_marked_deleted"]) == 0
        assert int(info_data_after_drop["search_interned_strings"]) >= 1
        assert int(info_data_after_drop["search_interned_strings_marked_deleted"]) == 0
        assert int(info_data_after_drop["search_interned_strings_memory"]) >= 16

        # Now drop the second index - this should make the strings to be deleted
        client.execute_command("FT.DROPINDEX", index_name2)
        info_data_final = client.info("SEARCH")
        
        # Now the interned strings should be clean
        assert int(info_data_final["search_hnsw_nodes"]) == 0
        assert int(info_data_final["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_final["search_vectors_memory"]) == 0
        assert int(info_data_final["search_vectors_memory_marked_deleted"]) == 0
        assert int(info_data_final["search_interned_strings"]) == 0
        assert int(info_data_final["search_interned_strings_marked_deleted"]) == 0
        assert int(info_data_final["search_interned_strings_memory"]) == 0

    def test_two_keys_same_vector_string_reference_counting(self):
        """
        Test that when 2 keys have the same vector string, validate using vector memory
        that only one string was created. After deleting one key, the vector string 
        is not marked as deleted. After deleting the second key, the vector string 
        is marked as deleted.
        
        This test validates vector string deduplication by tracking vector memory.
        """
        
        client: Valkey = self.server.get_new_client()
        # Create an index
        index_name = "test_idx"
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vector", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2"
        )
        
        # Create the same vector for both keys
        embedding_vector = [0.1, 0.2, 0.3, 0.4]
        vector_bytes = struct.pack('<4f', *embedding_vector)
        
        # Insert first document with the vector
        doc_key1 = "doc:1"
        client.hset(doc_key1, mapping={
            "vector": vector_bytes
        })
        
        # Get metrics after first insertion
        info_data_after_first = client.info("SEARCH")

        # Validate initial state - one vector
        assert int(info_data_after_first["search_hnsw_nodes"]) == 1
        assert int(info_data_after_first["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_after_first["search_vectors_memory"]) == 16  # 4 floats * 4 bytes
        assert int(info_data_after_first["search_vectors_memory_marked_deleted"]) == 0
        
        # Insert second document with the SAME vector
        doc_key2 = "doc:2"
        client.hset(doc_key2, mapping={
            "vector": vector_bytes  # Same vector bytes
        })
        
        # Get metrics after second insertion
        info_data_after_second = client.info("SEARCH")
        
        # Key validation: Track only vector memory to validate deduplication
        # We should have 2 HNSW nodes but vector memory should show deduplication
        assert int(info_data_after_second["search_hnsw_nodes"]) == 2
        assert int(info_data_after_second["search_hnsw_nodes_marked_deleted"]) == 0
        
        # Validate only one string was created using vector memory
        assert int(info_data_after_second["search_vectors_memory"]) == 16
        assert int(info_data_after_second["search_vectors_memory_marked_deleted"]) == 0
        
        # Delete the first key
        assert client.delete(doc_key1) == 1
        
        # Get metrics after deleting first key
        info_data_after_first_delete = client.info("SEARCH")
        
        # Validate using vector memory that the string is NOT marked as deleted
        # because the second key still references it
        assert int(info_data_after_first_delete["search_hnsw_nodes"]) == 2  # Both nodes remains
        assert int(info_data_after_first_delete["search_hnsw_nodes_marked_deleted"]) == 1  # One is marked as deleted
        assert int(info_data_after_first_delete["search_vectors_memory"]) == 16  # Memory still allocated
        assert int(info_data_after_first_delete["search_vectors_memory_marked_deleted"]) == 0  # Vector is still in use at one index
        # Delete the second key
        assert client.delete(doc_key2) == 1
        
        # Get metrics after deleting second key
        info_data_after_second_delete = client.info("SEARCH")
        
        assert int(info_data_after_second_delete["search_hnsw_nodes"]) == 2  # Nodes are still active
        assert int(info_data_after_second_delete["search_hnsw_nodes_marked_deleted"]) == 2  # Nodes are marked as deleted
        assert int(info_data_after_second_delete["search_vectors_memory"]) == 16  # Memory still allocated
        assert int(info_data_after_second_delete["search_vectors_memory_marked_deleted"]) == 16  # All marked as deleted
        
        # Drop index - assert memory cleanup
        client.execute_command("FT.DROPINDEX", index_name)

        info_data_after_drop = client.info("SEARCH")
        assert int(info_data_after_drop["search_hnsw_nodes"]) == 0  # Nodes are still active
        assert int(info_data_after_drop["search_hnsw_nodes_marked_deleted"]) == 0  # Nodes are marked as deleted
        assert int(info_data_after_drop["search_vectors_memory"]) == 0  # Memory still allocated
        assert int(info_data_after_drop["search_vectors_memory_marked_deleted"]) == 0  # All marked as deleted
