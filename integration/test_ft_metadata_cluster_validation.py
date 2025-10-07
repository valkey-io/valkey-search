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

    def validate_ft_info_consistency(self, index_name: str, expected_attributes: Dict[str, Any], expected_schema_params: Dict[str, Any] = None):
        """Validate that FT.INFO returns consistent results across all nodes."""
        ft_info_results = self.get_ft_info_from_all_nodes(index_name)
        
        # All nodes should have the index
        for i, parser in enumerate(ft_info_results):
            assert parser is not None, f"Index '{index_name}' not found on node {i}"
        
        # Compare key metadata fields across all nodes
        first_parser = ft_info_results[0]
        
        # Validate global settings are consistent across nodes if expected values are provided
        if expected_schema_params:
            if 'punctuation' in expected_schema_params:
                assert first_parser.punctuation == expected_schema_params['punctuation'], f"Punctuation setting incorrect: {first_parser.punctuation}"
            if 'with_offsets' in expected_schema_params:
                assert first_parser.with_offsets == expected_schema_params['with_offsets'], f"WithOffsets setting incorrect: {first_parser.with_offsets}"
            if 'stop_words' in expected_schema_params:
                assert set(first_parser.stop_words) == set(expected_schema_params['stop_words']), f"Stop words incorrect: {first_parser.stop_words}"
        
        # Validate these settings are consistent across all nodes
        for i, parser in enumerate(ft_info_results[1:], 1):
            if expected_schema_params:
                if 'punctuation' in expected_schema_params:
                    assert parser.punctuation == first_parser.punctuation, f"Punctuation mismatch on node {i}"
                if 'with_offsets' in expected_schema_params:
                    assert parser.with_offsets == first_parser.with_offsets, f"WithOffsets mismatch on node {i}"
                if 'stop_words' in expected_schema_params:
                    assert set(parser.stop_words) == set(first_parser.stop_words), f"Stop words mismatch on node {i}"
        
        for i, parser in enumerate(ft_info_results[1:], 1):
            # Validate basic index information
            assert parser.index_name == first_parser.index_name, f"Index name mismatch on node {i}"
            assert len(parser.attributes) == len(first_parser.attributes), f"Attribute count mismatch on node {i}"
            
            # Validate index definition consistency
            assert parser.index_definition == first_parser.index_definition, f"Index definition mismatch on node {i}"
            
            # Validate attributes consistency - compare by identifier rather than position
            first_attr_names = set()
            for attr in first_parser.attributes:
                if isinstance(attr, dict):
                    first_attr_names.add(attr.get('identifier'))
                elif isinstance(attr, list):
                    parsed_attr = first_parser._parse_key_value_list(attr)
                    if isinstance(parsed_attr, dict):
                        first_attr_names.add(parsed_attr.get('identifier'))
            
            node_attr_names = set()
            for attr in parser.attributes:
                if isinstance(attr, dict):
                    node_attr_names.add(attr.get('identifier'))
                elif isinstance(attr, list):
                    parsed_attr = parser._parse_key_value_list(attr)
                    if isinstance(parsed_attr, dict):
                        node_attr_names.add(parsed_attr.get('identifier'))
            
            assert first_attr_names == node_attr_names, f"Attribute names mismatch on node {i}: {first_attr_names} vs {node_attr_names}"
            
            # Validate attribute types match across nodes
            for attr_name in first_attr_names:
                first_attr = first_parser.get_attribute_by_name(attr_name)
                node_attr = parser.get_attribute_by_name(attr_name)
                assert first_attr is not None and node_attr is not None, f"Attribute '{attr_name}' parsing failed"
                assert node_attr.get('type') == first_attr.get('type'), f"Attribute '{attr_name}' type mismatch on node {i}"
        
        # Validate specific expected attributes
        for attr_name, expected_config in expected_attributes.items():
            attr = first_parser.get_attribute_by_name(attr_name)
            assert attr is not None, f"Expected attribute '{attr_name}' not found"
            
            for key, expected_value in expected_config.items():
                actual_value = attr.get(key)
                assert actual_value == expected_value, f"Attribute '{attr_name}' {key} mismatch: expected {expected_value}, got {actual_value}"

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
        
        # Validate FT.INFO consistency with detailed attribute checking
        # Note: Based on server response, subcategory TAG field with CASESENSITIVE uses default separator ','
        expected_attributes = {
            "title": {
                "type": "TEXT",
                "identifier": "title",
                "NO_STEM": 1
            },
            "description": {
                "type": "TEXT",
                "identifier": "description"
            },
            "price": {
                "type": "NUMERIC",
                "identifier": "price"
            },
            "category": {
                "type": "TAG",
                "identifier": "category",
                "SEPARATOR": "|"
            },
            "subcategory": {
                "type": "TAG",
                "identifier": "subcategory",
                "SEPARATOR": ",",
                "CASESENSITIVE": 1
            },
            "embedding": {
                "type": "VECTOR",
                "identifier": "embedding",
                "algorithm": "HNSW",
                "data_type": "FLOAT32",
                "dim": 20,
                "distance_metric": "COSINE",
                "M": 4,
                "ef_construction": 100,
                "ef_runtime": 10
            }
        }
        
        # Define expected schema-level parameters
        expected_schema_params = {
            'punctuation': ".,!?",
            'with_offsets': 1,
            'stop_words': ["the", "and", "or"]
        }
        
        self.validate_ft_info_consistency(index_name, expected_attributes, expected_schema_params)

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
                ],
                "attributes": {
                    "name": {"type": "TEXT", "identifier": "name"},
                    "price": {"type": "NUMERIC", "identifier": "price"}
                }
            },
            {
                "name": "users_idx",
                "command": [
                    "FT.CREATE", "users_idx",
                    "ON", "HASH",
                    "PREFIX", "1", "user:",
                    "PUNCTUATION", ".-",
                    "SCHEMA", "email", "TEXT", "age", "NUMERIC", "tags", "TAG"
                ],
                "attributes": {
                    "email": {"type": "TEXT", "identifier": "email"},
                    "age": {"type": "NUMERIC", "identifier": "age"},
                    "tags": {"type": "TAG", "identifier": "tags"}
                }
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
                ],
                "attributes": {
                    "title": {"type": "TEXT", "identifier": "title"},
                    "content": {"type": "TEXT", "identifier": "content", "NO_STEM": 1}
                }
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
            self.validate_ft_info_consistency(index_config["name"], index_config["attributes"])
