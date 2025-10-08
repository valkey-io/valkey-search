import pytest
from valkey import ResponseError
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser
from typing import List, Dict, Any
from utils import IndexingTestHelper


class TestFTMetadataClusterValidation(ValkeySearchClusterTestCase):
    """
    Integration test for validating that metadata has been properly transferred
    across different nodes of the cluster. This test creates various text indexes
    with different parameters and validates that FT._LIST and FT.INFO commands
    return consistent results across all cluster nodes.
    """

    def wait_for_indexing_complete_on_all_nodes(self, index_name: str, timeout: int = 10):
        """Wait for indexing to complete on all cluster nodes."""
        nodes = [self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)]
        success = IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(nodes, index_name, timeout)
        if not success:
            raise TimeoutError(f"Indexing did not complete on all nodes within {timeout} seconds for index '{index_name}'")

    def get_ft_list_from_all_nodes(self) -> List[List[bytes]]:
        """Get FT._LIST results from all cluster nodes."""
        results = []
        for i in range(self.CLUSTER_SIZE):
            node = self.new_client_for_primary(i)
            result = node.execute_command("FT._LIST")
            results.append(result)
        return results

    def get_ft_info_from_all_nodes(self, index_name: str) -> List[FTInfoParser]:
        """Get FT.INFO results from all cluster nodes."""
        results = []
        for i in range(self.CLUSTER_SIZE):
            node = self.new_client_for_primary(i)
            parser = IndexingTestHelper.get_ft_info(node, index_name)
            results.append(parser)
        return results

    def validate_ft_list_consistency(self, expected_indexes: List[str]):
        """Validate that FT._LIST returns consistent results across all nodes."""
        ft_list_results = self.get_ft_list_from_all_nodes()
        
        # Convert all results to sets of strings for comparison
        normalized_results = []
        for result in ft_list_results:
            normalized = set()
            for item in result:
                if isinstance(item, bytes):
                    normalized.add(item.decode('utf-8'))
                else:
                    normalized.add(str(item))
            normalized_results.append(normalized)
        
        # All nodes should have the same set of indexes
        first_result = normalized_results[0]
        for i, result in enumerate(normalized_results[1:], 1):
            assert result == first_result, f"Node {i} FT._LIST result differs from node 0: {result} vs {first_result}"
        
        # Verify expected indexes are present
        expected_set = set(expected_indexes)
        assert expected_set.issubset(first_result), f"Expected indexes {expected_set} not found in {first_result}"

    def _normalize_dict_for_comparison(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize dictionary for comparison by sorting arrays and recursively normalizing nested structures.
        This ensures consistent comparison regardless of array ordering.
        """
        if not isinstance(data, dict):
            return data
            
        normalized = {}
        for key, value in data.items():
            if isinstance(value, list):
                # Sort lists that contain comparable elements (strings, numbers)
                try:
                    if value and all(isinstance(item, (str, int, float)) for item in value):
                        normalized[key] = sorted(value)
                    elif value and all(isinstance(item, dict) for item in value):
                        # For lists of dicts, normalize each dict and sort by a stable key if possible
                        normalized_items = [self._normalize_dict_for_comparison(item) for item in value]
                        # Try to sort by 'identifier' if present, otherwise keep original order
                        if all('identifier' in item for item in normalized_items):
                            normalized[key] = sorted(normalized_items, key=lambda x: x.get('identifier', ''))
                        else:
                            normalized[key] = normalized_items
                    else:
                        normalized[key] = value
                except (TypeError, AttributeError):
                    # If sorting fails, keep original order
                    normalized[key] = value
            elif isinstance(value, dict):
                normalized[key] = self._normalize_dict_for_comparison(value)
            else:
                normalized[key] = value
        return normalized

    def validate_ft_info_consistency(self, index_name: str):
        """Validate that FT.INFO returns consistent results across all nodes."""
        ft_info_results = self.get_ft_info_from_all_nodes(index_name)
        
        # All nodes should have the index
        for i, parser in enumerate(ft_info_results):
            assert parser is not None, f"Index '{index_name}' not found on node {i}"
        
        first_parser = ft_info_results[0]
        first_dict = self._normalize_dict_for_comparison(first_parser.to_dict())
        
        # Compare all nodes against the first using normalized dictionary comparison
        for i, parser in enumerate(ft_info_results[1:], 1):
            node_dict = self._normalize_dict_for_comparison(parser.to_dict())
            assert node_dict == first_dict, f"Complete metadata mismatch on node {i}"

    def test_complex_text_index_metadata_validation(self):
        """Test complex text index with multiple parameters and options."""
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        
        index_name = "complex_text_idx"
        
        # Create complex text index with various options including vector field
        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "2", "product:", "item:",
            "PUNCTUATION", ".,!?",
            "WITHOFFSETS",
            "NOSTEM",
            "STOPWORDS", "3", "the", "and", "or",
            "SCHEMA",
            "title", "TEXT", "NOSTEM",
            "description", "TEXT",
            "price", "NUMERIC",
            "category", "TAG", "SEPARATOR", "|",
            "subcategory", "TAG", "CASESENSITIVE",
            "embedding", "VECTOR", "HNSW", "10", "TYPE", "FLOAT32", "DIM", "20", "DISTANCE_METRIC", "COSINE", "M", "4", "EF_CONSTRUCTION", "100"
        ) == b"OK"
        
        # Wait for indexing to complete on all nodes
        self.wait_for_indexing_complete_on_all_nodes(index_name)
        
        # Validate FT._LIST consistency
        self.validate_ft_list_consistency([index_name])
        
        # Validate FT.INFO consistency across all nodes
        self.validate_ft_info_consistency(index_name)

    def test_multiple_indexes_metadata_validation(self):
        """Test multiple indexes with different configurations."""
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        
        indexes = [
            {
                "name": "products_idx",
                "command": [
                    "FT.CREATE", "products_idx",
                    "ON", "HASH",
                    "PREFIX", "1", "product:",
                    "SCHEMA", "name", "TEXT", "price", "NUMERIC"
                ]
            },
            {
                "name": "users_idx",
                "command": [
                    "FT.CREATE", "users_idx",
                    "ON", "HASH",
                    "PREFIX", "1", "user:",
                    "PUNCTUATION", ".-",
                    "SCHEMA", "email", "TEXT", "age", "NUMERIC", "tags", "TAG"
                ]
            },
            {
                "name": "articles_idx",
                "command": [
                    "FT.CREATE", "articles_idx",
                    "ON", "HASH",
                    "PREFIX", "1", "article:",
                    "WITHOFFSETS",
                    "STOPWORDS", "2", "a", "an",
                    "SCHEMA", "title", "TEXT", "content", "TEXT", "NOSTEM"
                ]
            }
        ]
        
        # Create all indexes
        for index_config in indexes:
            assert node0.execute_command(*index_config["command"]) == b"OK"
        
        # Wait for all indexes to complete on all nodes
        for index_config in indexes:
            self.wait_for_indexing_complete_on_all_nodes(index_config["name"])
        
        # Validate FT._LIST shows all indexes on all nodes
        expected_index_names = [idx["name"] for idx in indexes]
        self.validate_ft_list_consistency(expected_index_names)
        
        # Validate each index's metadata consistency
        for index_config in indexes:
            self.validate_ft_info_consistency(index_config["name"])
