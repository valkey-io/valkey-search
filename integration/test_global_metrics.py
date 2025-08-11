import struct
import time
import threading
import random
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

    def test_rdb_fill_metrics_persistence(self):
        """
        Test that global metrics are correctly restored after RDB save/load cycle.
        
        This test validates that when data is saved to RDB and the server is restarted,
        the search global metrics are properly restored to their original values.
        """
        
        client: Valkey = self.server.get_new_client()
        
        # Create an HNSW index with vector, tag, and numeric fields
        hnsw_index_name = "hnsw_idx"
        client.execute_command(
            "FT.CREATE", hnsw_index_name,
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
        
        # Create a FLAT index with vector field to test flat nodes persistence
        flat_index_name = "flat_idx"
        client.execute_command(
            "FT.CREATE", flat_index_name,
            "ON", "HASH",
            "PREFIX", "1", "flat:",
            "SCHEMA",
            "vector", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2",
            "category", "TAG"
        )
        
        # Insert multiple documents for HNSW index
        doc_keys = ["doc:1", "doc:2", "doc:3", "doc:4"]
        embedding_vectors = [
            [0.1, 0.2, 0.3, 0.4],
            [0.5, 0.6, 0.7, 0.8], 
            [0.9, 1.0, 1.1, 1.2],
            [0.9, 1.0, 1.1, 1.2]
        ]
        
        for i, doc_key in enumerate(doc_keys):
            vector_bytes = struct.pack('<4f', *embedding_vectors[i])
            client.hset(doc_key, mapping={
                "vector": vector_bytes,
                "category": f"category{i}|subcategory{i}",
                "price": 100 * (i + 1)
            })
        
        # Insert documents for FLAT index
        flat_doc_keys = ["flat:1", "flat:2", "flat:3"]
        flat_embedding_vectors = [
            [1.1, 1.2, 1.3, 1.4],
            [1.5, 1.6, 1.7, 1.8],
            [1.9, 2.0, 2.1, 2.2]
        ]
        
        for i, flat_doc_key in enumerate(flat_doc_keys):
            vector_bytes = struct.pack('<4f', *flat_embedding_vectors[i])
            client.hset(flat_doc_key, mapping={
                "vector": vector_bytes,
                "category": f"flat_category{i}"
            })
        
        # Delete documents 4 and 2 from HNSW index to test marked_deleted functionality
        client.delete("doc:4")
        client.delete("doc:2")
        
        # Delete one document from FLAT index
        client.delete("flat:2")
        
        # Get metrics after deletions (before save)
        info_data_before_save = client.info("SEARCH")
        
        # Store the metrics we want to validate after RDB load
        expected_metrics = {
            "search_flat_nodes": int(info_data_before_save["search_flat_nodes"]),
            "search_hnsw_edges": int(info_data_before_save["search_hnsw_edges"]),
            "search_hnsw_edges_marked_deleted": int(info_data_before_save["search_hnsw_edges_marked_deleted"]),
            "search_hnsw_nodes": int(info_data_before_save["search_hnsw_nodes"]),
            "search_hnsw_nodes_marked_deleted": int(info_data_before_save["search_hnsw_nodes_marked_deleted"]),
            "search_interned_strings": int(info_data_before_save["search_interned_strings"]),
            "search_interned_strings_memory": int(info_data_before_save["search_interned_strings_memory"]),
            "search_keys_memory": int(info_data_before_save["search_keys_memory"]),
            "search_numeric_records": int(info_data_before_save["search_numeric_records"]),
            "search_tags": int(info_data_before_save["search_tags"]),
            "search_tags_memory": int(info_data_before_save["search_tags_memory"]),
            "search_vectors_memory": int(info_data_before_save["search_vectors_memory"]),
            "search_vectors_memory_marked_deleted": int(info_data_before_save["search_vectors_memory_marked_deleted"])
        }
        
        # Validate expected state before save (after deletions)
        # HNSW index: doc 3 and doc 4 have identical vectors, so deleting doc 4 doesn't affect vector memory
        assert expected_metrics["search_hnsw_nodes"] == 4  # 4 documents (nodes still exist)
        assert expected_metrics["search_hnsw_nodes_marked_deleted"] == 2  # 2 deleted documents (doc:2, doc:4)
        assert expected_metrics["search_numeric_records"] == 2  # 2 remaining price fields (doc:1, doc:3)
        assert expected_metrics["search_tags"] == 6  # 2 tags per remaining HNSW document * 2 documents + 2 remaining flat documents
        
        # FLAT index: 3 documents inserted, 1 deleted (flat:2), so 2 remaining
        assert expected_metrics["search_flat_nodes"] == 2 
        
        # Vector memory: 3 unique HNSW vectors + 2 unique FLAT vectors = 5 * 16 bytes = 80 bytes
        assert expected_metrics["search_vectors_memory"] == 80 
        assert expected_metrics["search_vectors_memory_marked_deleted"] == 16  # 1 deleted vectors (doc:2)
        
        # Keys memory: "doc:1" + "doc:3" + "flat:1" + "flat:3" = 5 + 5 + 6 + 6 = 22 bytes
        assert expected_metrics["search_keys_memory"] == 22
        assert expected_metrics["search_interned_strings"] >= 5  # At least 6 unique vectors still exist

        # Save the current state to RDB
        client.shutdown("save")
        # Restart the server to load the RDB file
        # This will automatically load the RDB file on startup
        # Get a new client after restart
        self.server.restart(False, False, False)
        
        # Get metrics after RDB load
        time.sleep(1)
        client = self.server.get_new_client()
        info_data_after_load = client.info("SEARCH")

        # Validate that all metrics are restored to their original values
        for metric_name, expected_value in expected_metrics.items():
            actual_value = int(info_data_after_load[metric_name])
            assert actual_value == expected_value, f"Metric {metric_name}: expected {expected_value}, got {actual_value}"

    def test_same_string_for_key_and_tag_metrics(self):
        """
        Test that when the same string is used for both a key and a tag value,
        the metrics correctly handle string deduplication and reference counting.
        """
        
        client: Valkey = self.server.get_new_client()

        # Use the same string for both key suffix and tag value
        shared_string = f"doc:1"  # Key contains the shared string

        index_name = "test_idx"
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", shared_string,
            "SCHEMA",
            "vector", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2",
            "category", "TAG"
        )

        embedding_vector = [0.1, 0.2, 0.3, 0.4]
        vector_bytes = struct.pack('<4f', *embedding_vector)

        client.hset(shared_string, mapping={
            "vector": vector_bytes,
            "category": shared_string  # Same string as in key
        })
        
        # Get metrics after insertion
        info_data_after_insert = client.info("SEARCH")
        
        # Validate that metrics correctly handle the shared string
        # The string should be deduplicated internally
        assert int(info_data_after_insert["search_interned_strings"]) >= 1
        assert int(info_data_after_insert["search_interned_strings_memory"]) == len(shared_string) * 2 + len(vector_bytes)
        assert int(info_data_after_insert["search_tags"]) == 1  # One tag value
        assert int(info_data_after_insert["search_tags_memory"]) == len(shared_string)
        assert int(info_data_after_insert["search_keys_memory"]) == len(shared_string)

    def test_key_delete_and_re_add_marked_deleted_reset(self):
        """
        Test that when a key is deleted and then re-added, the marked_deleted 
        counters are properly reset to 0.
        """
        client: Valkey = self.server.get_new_client()
        
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
        
        doc_key = "doc:1"
        embedding_vector = [0.1, 0.2, 0.3, 0.4]
        vector_bytes = struct.pack('<4f', *embedding_vector)
        
        client.hset(doc_key, mapping={
            "vector": vector_bytes,
            "category": "electronics",
            "price": 999
        })
        
        # Get metrics after initial insertion
        info_data_after_insert = client.info("SEARCH")
        
        # Validate initial state - nothing marked as deleted
        assert int(info_data_after_insert["search_hnsw_nodes"]) == 1
        assert int(info_data_after_insert["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_vectors_memory"]) == 16  # 4 floats * 4 bytes
        assert int(info_data_after_insert["search_vectors_memory_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_interned_strings"]) >= 1
        assert int(info_data_after_insert["search_numeric_records"]) == 1
        assert int(info_data_after_insert["search_tags"]) >= 1

        # Delete the document
        result = client.delete(doc_key)
        assert result == 1  # Confirm deletion was successful
        
        # Get metrics after deletion
        info = client.info("SEARCH")
        # Validate that resources are marked as deleted
        assert info["search_hnsw_edges"] == info_data_after_insert["search_hnsw_edges"]
        assert info["search_hnsw_edges_marked_deleted"] == info_data_after_insert["search_hnsw_edges"]
        assert int(info["search_hnsw_nodes"]) == 1
        assert int(info["search_hnsw_nodes_marked_deleted"]) == 1
        assert int(info["search_vectors_memory"]) == 16
        assert int(info["search_vectors_memory_marked_deleted"]) == 16
        assert int(info["search_interned_strings"]) ==  1 # only the vector string remained
        assert int(info["search_numeric_records"]) == 0
        assert int(info["search_tags"]) == 0
        assert int(info["search_keys_memory"]) == 0

        # Re-add the same document with the same data
        client.hset(doc_key, mapping={
            "vector": vector_bytes,
            "category": "electronics", 
            "price": 999
        })
        
        info = client.info("SEARCH")
        assert int(info["search_hnsw_edges_marked_deleted"]) == 16  # HNSW edges remain marked deleted
        assert int(info["search_hnsw_nodes_marked_deleted"]) == 1   # HNSW nodes remain marked deleted
        assert int(info["search_vectors_memory_marked_deleted"]) == 0  # Vector memory reused
        
        assert int(info["search_hnsw_nodes"]) == 2
        assert int(info["search_vectors_memory"]) == 16
        assert int(info["search_interned_strings"]) >= 1
        assert int(info["search_numeric_records"]) == 1
        assert int(info["search_tags"]) >= 1
        assert int(info["search_keys_memory"]) > 0
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_flat_and_hnsw_indexes_drop_hnsw_metrics_preserved(self):
         """
         Test that when creating 2 indexes (flat and HNSW) with the same 10 keys,
         after dropping the HNSW index, the metrics show that strings still exist
         and are properly maintained by the remaining flat index.
         """
         client: Valkey = self.server.get_new_client()

         # Create flat index
         flat_index_name = "flat_idx"
         client.execute_command(
             "FT.CREATE", flat_index_name,
             "ON", "HASH",
             "PREFIX", "1", "doc:",
             "SCHEMA",
             "vector", "VECTOR", "FLAT", "6",
             "TYPE", "FLOAT32",
             "DIM", "4",
             "DISTANCE_METRIC", "L2",
             "category", "TAG")

         # Create HNSW index with same prefix (tracking same keys)
         hnsw_index_name = "hnsw_idx"
         client.execute_command(
             "FT.CREATE", hnsw_index_name,
             "ON", "HASH",
             "PREFIX", "1", "doc:",
             "SCHEMA",
             "vector", "VECTOR", "HNSW", "6",
             "TYPE", "FLOAT32",
             "DIM", "4",
             "DISTANCE_METRIC", "L2",
             "price", "NUMERIC")

         # Insert 10 documents that will be indexed by both indices
         doc_keys = [f"doc:{i}" for i in range(1, 11)]
         embedding_vectors = [
             [0.1 * i, 0.2 * i, 0.3 * i, 0.4 * i] for i in range(1, 11)]

         for i, doc_key in enumerate(doc_keys):
             vector_bytes = struct.pack('<4f', *embedding_vectors[i])
             client.hset(doc_key, mapping={
                 "vector": vector_bytes,
                 "category": f"category_{i}",
                 "price": 100 * (i + 1) })

         # Get metrics after insertion (both indices should reference the same vectors)
         info_data_after_insert = client.info("SEARCH")
        
         # Validate that data is present and not marked as deleted
         # Both indices should reference the same interned strings for the vectors
         assert int(info_data_after_insert["search_flat_nodes"]) == 10  # 10 nodes in flat index
         assert int(info_data_after_insert["search_hnsw_nodes"]) == 10  # 10 nodes in HNSW index
         assert int(info_data_after_insert["search_hnsw_nodes_marked_deleted"]) == 0
         assert int(info_data_after_insert["search_vectors_memory"]) == 160  # 10 vectors * 16 bytes each
         assert int(info_data_after_insert["search_vectors_memory_marked_deleted"]) == 0
         assert int(info_data_after_insert["search_interned_strings"]) >= 10  # At least the 10 vector strings
         assert int(info_data_after_insert["search_numeric_records"]) == 10  # 10 price fields
         assert int(info_data_after_insert["search_tags"]) == 10  # 10 category tags
         assert int(info_data_after_insert["search_keys_memory"]) == 51  
         assert (int(info_data_after_insert["search_interned_strings_memory"]) == 
                 int(info_data_after_insert["search_vectors_memory"]) + 
                 int(info_data_after_insert["search_keys_memory"]) + 
                 int(info_data_after_insert["search_tags_memory"]))

         # Store values before dropping HNSW index for comparison
         flat_nodes_before = int(info_data_after_insert["search_flat_nodes"])
         vectors_memory_before = int(info_data_after_insert["search_vectors_memory"])
         tags_before = int(info_data_after_insert["search_tags"])
         keys_memory_before = int(info_data_after_insert["search_keys_memory"])

         # Drop the HNSW index
         client.execute_command("FT.DROPINDEX", hnsw_index_name)

         # Get metrics after dropping HNSW index
         info_data_after_drop = client.info("SEARCH")

         # Validate that the interned strings are NOT marked as deleted
         # because they are still referenced by the flat index
         assert int(info_data_after_drop["search_flat_nodes"]) == flat_nodes_before  # Flat index unchanged
         assert int(info_data_after_drop["search_hnsw_nodes"]) == 0  # HNSW nodes removed
         assert int(info_data_after_drop["search_hnsw_nodes_marked_deleted"]) == 0  # No marked deleted nodes
         assert int(info_data_after_drop["search_vectors_memory"]) == vectors_memory_before  # Vector memory preserved
         assert int(info_data_after_drop["search_vectors_memory_marked_deleted"]) == 0  # No marked deleted vector memory

         # Interned strings should still exist because flat index references them
         assert int(info_data_after_drop["search_interned_strings"]) >= 10  # At least the vector strings remain
         assert int(info_data_after_drop["search_interned_strings_memory"]) >= 160  # Vector strings memory preserved

         # Tags should be reduced since HNSW index had no tag fields, only flat index has tags
         assert int(info_data_after_drop["search_tags"]) == tags_before  # Tags from flat index remain

         # Keys memory should remain the same since flat index still tracks the keys
         assert int(info_data_after_drop["search_keys_memory"]) == keys_memory_before

         # Numeric records should be 0 since only HNSW index had numeric fields
         assert int(info_data_after_drop["search_numeric_records"]) == 0

         # Clean up - drop the remaining flat index
         client.execute_command("FT.DROPINDEX", flat_index_name)

         # Verify complete cleanup
         info_data_final = client.info("SEARCH")
         assert int(info_data_final["search_flat_nodes"]) == 0
         assert int(info_data_final["search_hnsw_nodes"]) == 0
         assert int(info_data_final["search_vectors_memory"]) == 0
         assert int(info_data_final["search_vectors_memory_marked_deleted"]) == 0
         assert int(info_data_final["search_interned_strings"]) == 0
         assert int(info_data_final["search_interned_strings_memory"]) == 0
         assert int(info_data_final["search_tags"]) == 0
         assert int(info_data_final["search_keys_memory"]) == 0
         assert int(info_data_final["search_numeric_records"]) == 0

    def test_multithreaded_operations_with_metrics_validation(self):
        """
        Test with 10 threads running 100 commands each
        After joining all threads, run FLUSHALL and validate all metrics are 0.
        """
        client: Valkey = self.server.get_new_client()
        
        # Create 10 indexes that all threads will access
        index_names = [f"test_idx_{i}" for i in range(10)]
        for index_name in index_names:
            client.execute_command(
                "FT.CREATE", index_name,
                "ON", "HASH", 
                "PREFIX", "1", "shared:",
                "SCHEMA",
                "vector", "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32",
                "DIM", "4", 
                "DISTANCE_METRIC", "L2",
                "price", "NUMERIC",
                "category", "TAG"
            )
        
        # Shared data structures for thread operations
        key_prefix = "shared:"
        num_keys = 50
        key_names = [f"{key_prefix}{i}" for i in range(num_keys)]
        
        def worker_thread(thread_id):
            """Worker function for each thread"""
            # Get a new client for this thread
            thread_client = self.server.get_new_client()
            
            for command_num in range(400):
                try:
                    operation = random.choice(['set_key', 'delete_key', 'index_create', 'index_drop'])
                    
                    if operation == 'set_key':
                        # Set a random key with vector, price, and category
                        key_name = random.choice(key_names)
                        embedding_vector = [random.random() for _ in range(4)]
                        vector_bytes = struct.pack('<4f', *embedding_vector)
                        
                        thread_client.hset(key_name, mapping={
                            "vector": vector_bytes,
                            "category": f"category_{random.randint(1, 10)}",
                            "price": random.randint(100, 1000)
                        })
                        
                    elif operation == 'delete_key':
                        # Delete a random key
                        key_name = random.choice(key_names)
                        thread_client.delete(key_name)
                        
                    elif operation == 'index_create':
                        # Create a new index
                        new_index_name = f"dynamic_idx_{thread_id}_{command_num}_{random.randint(1, 1000)}"
                        try:
                            thread_client.execute_command(
                                "FT.CREATE", new_index_name,
                                "ON", "HASH",
                                "PREFIX", "1", "shared:",
                                "SCHEMA",
                                "vector", "VECTOR", "HNSW", "6",
                                "TYPE", "FLOAT32", 
                                "DIM", "4",
                                "DISTANCE_METRIC", "L2",
                                "price", "NUMERIC"
                            )
                        except Exception:
                            # Index might already exist, ignore
                            pass
                            
                    elif operation == 'index_drop':
                        # Delete a random existing index
                        try:
                            # Try to delete one of the original indexes
                            index_to_delete = random.choice(index_names)
                            thread_client.execute_command("FT.DROPINDEX", index_to_delete)
                        except Exception:
                            # Index might not exist, ignore
                            pass
                                    
                except Exception as e:
                    # Ignore errors from concurrent operations
                    pass
            
            thread_client.close()
        
        # Create and start 10 threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        

        
        # Run FLUSHALL to clear all data
        client.flushall()
        
        # Get metrics after FLUSHALL
        info_after_flush = client.info("SEARCH")
        
        # Validate that all metrics are 0 after FLUSHALL
        metrics_to_check = [
            "search_flat_nodes",
            "search_hnsw_edges", 
            "search_hnsw_edges_marked_deleted",
            "search_hnsw_nodes",
            "search_hnsw_nodes_marked_deleted",
            "search_interned_strings",
            "search_interned_strings_memory",
            "search_keys_memory",
            "search_numeric_records",
            "search_tags",
            "search_tags_memory", 
            "search_vectors_memory",
            "search_vectors_memory_marked_deleted"
        ]
        
        for metric in metrics_to_check:
            metric_value = int(info_after_flush.get(metric, 0))
            assert metric_value == 0, f"Expected {metric} to be 0 after FLUSHALL, but got {metric_value}"

    def test_vector_marked_deleted_with_two_indexes_and_key_recreation(self):
        """
        Test vector marked as deleted behavior with 2 indexes:
        1. Create 2 indexes
        2. Create a key with a vector
        3. Delete the key, validate vector marked as deleted is 1
        4. Create the same key again with a new vector (different from before)
        5. Validate vector marked as deleted is still 1
        6. Drop one index and validate again
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
        
        # Create a key with a vector
        doc_key = "doc:1"
        embedding_vector1 = [0.1, 0.2, 0.3, 0.4]
        vector_bytes1 = struct.pack('<4f', *embedding_vector1)
        
        client.hset(doc_key, mapping={
            "vector": vector_bytes1,
            "category": "electronics",
            "price": 999
        })
        
        # Get metrics after insertion
        info_data_after_insert = client.info("SEARCH")
        
        # Validate initial state - both indexes should have the vector
        assert int(info_data_after_insert["search_hnsw_nodes"]) == 2  # One node per index
        assert int(info_data_after_insert["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_after_insert["search_vectors_memory"]) == 16  # One unique vector string
        assert int(info_data_after_insert["search_vectors_memory_marked_deleted"]) == 0
        
        # Delete the key
        result = client.delete(doc_key)
        assert result == 1  # Confirm deletion was successful
        
        # Get metrics after deletion
        info_data_after_delete = client.info("SEARCH")
        
        # Validate that vector is marked as deleted
        assert int(info_data_after_delete["search_hnsw_nodes"]) == 2  # Nodes still exist
        assert int(info_data_after_delete["search_hnsw_nodes_marked_deleted"]) == 2  # Both nodes marked as deleted
        assert int(info_data_after_delete["search_vectors_memory"]) == 16  # Vector memory still allocated
        assert int(info_data_after_delete["search_vectors_memory_marked_deleted"]) == 16  # Vector marked as deleted
        
        # Create the same key again with a NEW vector (different from before)
        embedding_vector2 = [0.5, 0.6, 0.7, 0.8]  # Different vector
        vector_bytes2 = struct.pack('<4f', *embedding_vector2)
        
        client.hset(doc_key, mapping={
            "vector": vector_bytes2,
            "category": "electronics",
            "price": 999
        })
        
        # Get metrics after re-creation with new vector
        info_data_after_recreate = client.info("SEARCH")
        
        # Validate that we now have both the old (marked deleted) and new vectors
        assert int(info_data_after_recreate["search_hnsw_nodes"]) == 4  # 2 old (marked deleted) + 2 new nodes
        assert int(info_data_after_recreate["search_hnsw_nodes_marked_deleted"]) == 2  # Old nodes still marked as deleted
        assert int(info_data_after_recreate["search_vectors_memory"]) == 32  # 2 unique vectors * 16 bytes each
        assert int(info_data_after_recreate["search_vectors_memory_marked_deleted"]) == 16  # Old vector still marked as deleted
        
        # Drop one index
        client.execute_command("FT.DROPINDEX", index_name1)
        
        # Get metrics after dropping one index
        info_data_after_drop = client.info("SEARCH")
        
        # Validate that metrics are updated correctly after dropping one index
        # We should have 1 active node and 1 marked deleted node from the remaining index
        assert int(info_data_after_drop["search_hnsw_nodes"]) == 2  # 1 old (marked deleted) + 1 new node from remaining index
        assert int(info_data_after_drop["search_hnsw_nodes_marked_deleted"]) == 1  # 1 old node still marked as deleted
        assert int(info_data_after_drop["search_vectors_memory"]) == 32  # Both vectors still in memory
        assert int(info_data_after_drop["search_vectors_memory_marked_deleted"]) == 16  # Old vector still marked as deleted
        
        # Clean up - drop the remaining index
        client.execute_command("FT.DROPINDEX", index_name2)
        
        # Verify complete cleanup
        info_data_final = client.info("SEARCH")
        assert int(info_data_final["search_hnsw_nodes"]) == 0
        assert int(info_data_final["search_hnsw_nodes_marked_deleted"]) == 0
        assert int(info_data_final["search_vectors_memory"]) == 0
        assert int(info_data_final["search_vectors_memory_marked_deleted"]) == 0
