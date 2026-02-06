from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import json
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
import time

"""
This file contains tests for non vector (numeric and tag) queries on Hash/JSON documents in Valkey Search - in CME / CMD.
"""
# Constants for numeric and tag queries on Hash/JSON documents.
numeric_tag_index_on_hash = "FT.CREATE products ON HASH PREFIX 1 product: SCHEMA price NUMERIC rating NUMERIC category TAG"
hash_docs = [
    ["HSET", "product:1", "category", "electronics", "name", "Laptop", "price", "999.99", "rating", "4.5", "desc", "Great"],
    ["HSET", "product:2", "category", "electronics", "name", "Tablet", "price", "499.00", "rating", "4.0", "desc", "Good"],
    ["HSET", "product:3", "category", "electronics", "name", "Phone", "price", "299.00", "rating", "3.8", "desc", "Ok"],
    ["HSET", "product:4", "category", "books", "name", "Book", "price", "19.99", "rating", "4.8", "desc", "Excellent"]
]
numeric_query = ["FT.SEARCH", "products", "@price:[300 1000] @rating:[4.4 +inf]"]
expected_hash_key = b'product:1'
expected_hash_value = {
    b'name': b"Laptop",
    b'price': b'999.99',
    b'rating': b'4.5',
    b'desc': b"Great",
    b'category': b"electronics"
}

numeric_tag_index_on_json = "FT.CREATE jsonproducts ON JSON PREFIX 1 jsonproduct: SCHEMA $.category as category TAG $.price as price NUMERIC $.rating as rating NUMERIC"
json_docs = [
    ['JSON.SET', 'jsonproduct:1', '$',
            '{"category":"electronics","name":"Laptop","price":999.99,"rating":4.5,"desc":"Great"}'],
    ['JSON.SET', 'jsonproduct:2', '$',
            '{"category":"electronics","name":"Tablet","price":499.00,"rating":4.0,"desc":"Good"}'],
    ['JSON.SET', 'jsonproduct:3', '$',
            '{"category":"electronics","name":"Phone","price":299.00,"rating":3.8,"desc":"Ok"}'],
    ['JSON.SET', 'jsonproduct:4', '$',
            '{"category":"books","name":"Book","price":19.99,"rating":4.8,"desc":"Excellent"}']
]
numeric_query_on_json = [
    "FT.SEARCH", "jsonproducts",
    "@price:[300 2000] @rating:[4.4 +inf]"
]
expected_numeric_json_key = b'jsonproduct:1'
expected_numeric_json_value = {
    "category": "electronics",
    "name": "Laptop",
    "price": 999.99,
    "rating": 4.5,
    "desc": "Great"
}
numeric_tag_query_on_json = [
    "FT.SEARCH", "jsonproducts",
    "@category:{books} @price:[10 30] @rating:[4.7 +inf]"
]
expected_numeric_tag_json_key = b'jsonproduct:4'
expected_numeric_tag_json_value = {
    "category": "books",
    "name": "Book",
    "price": 19.99,
    "rating": 4.8,
    "desc": "Excellent"
}

def create_indexes(client: Valkey):
    """
        Create the necessary indexes for numeric and tag queries on Hash/JSON documents.
    """
    assert client.execute_command(numeric_tag_index_on_hash) == b"OK"
    assert client.execute_command(numeric_tag_index_on_json) == b"OK"

def validate_non_vector_queries(client: Valkey):
    """
        Common validation for numeric and tag queries on Hash/JSON documents.
    """
    # Validate a numeric query on Hash documents.
    result = client.execute_command(*numeric_query)
    assert len(result) == 3
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_hash_key
    document = result[2]
    doc_fields = dict(zip(document[::2], document[1::2]))
    assert doc_fields == expected_hash_value
    # Test NOCONTENT on Hash documents
    result = client.execute_command(*(numeric_query + ["NOCONTENT"]))
    assert len(result) == 2
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_hash_key  # Only key, no content
    # Validate a numeric query on JSON documents.
    result = client.execute_command(*numeric_query_on_json)
    assert len(result) == 3
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_numeric_json_key
    json_data = result[2]
    assert json_data[0] == b'$'  # Check JSON path
    doc = json.loads(json_data[1].decode('utf-8'))
    for key, value in expected_numeric_json_value.items():
        assert key in doc, f"Key '{key}' not found in the document"
        assert doc[key] == value, f"Expected {key}={value}, got {key}={doc[key]}"
    assert set(doc.keys()) == set(expected_numeric_json_value.keys()), "Document contains unexpected fields"
    # Test NOCONTENT on JSON documents
    result = client.execute_command(*(numeric_query_on_json + ["NOCONTENT"]))
    assert len(result) == 2
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_numeric_json_key  # Only key, no content
    # Validate that a tag + numeric query on JSON document works.
    result = client.execute_command(*numeric_tag_query_on_json)
    assert len(result) == 3
    assert result[0] == 1  # Number of documents found
    assert result[1] == expected_numeric_tag_json_key
    json_data = result[2]
    assert json_data[0] == b'$'  # Check JSON path
    doc = json.loads(json_data[1].decode('utf-8'))
    for key, value in expected_numeric_tag_json_value.items():
        assert key in doc, f"Key '{key}' not found in the document"
        assert doc[key] == value, f"Expected {key}={value}, got {key}={doc[key]}"
    assert set(doc.keys()) == set(expected_numeric_tag_json_value.keys()), "Document contains unexpected fields"

def validate_limit_queries(client: Valkey):
    """
        Test LIMIT functionality on non-vector queries.
    """
    # Test LIMIT 0 2 - get first 2 results
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "0", "2")
    assert result[0] == 4  # Total count
    assert len(result) == 5  # 1 count + 2 docs (key + content each)
    # Test LIMIT 1 1 - skip first, get next 1
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "1", "1")
    assert result[0] == 4  # Total count
    assert len(result) == 3  # 1 count + 1 doc (key + content)
    # Test LIMIT with NOCONTENT
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "0", "2", "NOCONTENT")
    assert result[0] == 4  # Total count
    assert len(result) == 3  # 1 count + 2 keys only
    # Test LIMIT 0 0 - no results
    result = client.execute_command("FT.SEARCH", "products", "@price:[0 +inf]", "LIMIT", "0", "0")
    assert result[0] == 4  # Total count only
    assert len(result) == 1

def create_bulk_data_standalone(client: Valkey):
    """
        Create bulk data for standalone testing.
    """
    bulk_index = "FT.CREATE bulk_products ON HASH PREFIX 1 bulk_product: SCHEMA price NUMERIC category TAG rating NUMERIC"
    assert client.execute_command(bulk_index) == b"OK"
    # Insert 2500 documents with varying prices and categories
    for i in range(2500):
        price = 10 + (i * 2)  # Prices from 10 to 5008
        category = "cat" + str(i % 10)  # 10 different categories
        rating = 3.0 + (i % 3)  # Ratings 3.0, 4.0, 5.0
        client.execute_command("HSET", f"bulk_product:{i}", "price", str(price), "category", category, "rating", str(rating))

def create_bulk_data_cluster(index_client: Valkey, data_client):
    """
        Create bulk data for cluster testing.
    """
    bulk_index = "FT.CREATE bulk_products ON HASH PREFIX 1 bulk_product: SCHEMA price NUMERIC category TAG rating NUMERIC"
    assert index_client.execute_command(bulk_index) == b"OK"
    # Insert 2500 documents with varying prices and categories
    for i in range(2500):
        price = 10 + (i * 2)  # Prices from 10 to 5008
        category = "cat" + str(i % 10)  # 10 different categories
        rating = 3.0 + (i % 3)  # Ratings 3.0, 4.0, 5.0
        data_client.execute_command("HSET", f"bulk_product:{i}", "price", str(price), "category", category, "rating", str(rating))

def validate_buffer_multiplier_config(client: Valkey):
    """
        Test search result buffer multiplier configuration validation.
    """
    import pytest
    # Test valid positive values
    assert client.execute_command("CONFIG SET search.search-result-buffer-multiplier 2.5") == b"OK"
    assert client.execute_command("CONFIG SET search.search-result-buffer-multiplier 1.2") == b"OK"
    # Test that values outside range are rejected
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be between 1.0 and 1000.0"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier -1.0")
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be between 1.0 and 1000.0"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier 0.5")
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be between 1.0 and 1000.0"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier 1001.0")
    # Test that invalid strings are rejected
    with pytest.raises(ResponseError, match=r"Buffer multiplier must be a valid number"):
        client.execute_command("CONFIG SET search.search-result-buffer-multiplier invalid")

def validate_bulk_limit_queries(client: Valkey):
    """
        Test bulk operations with various LIMIT and OFFSET combinations to validate background limit changes.
    """
    validate_buffer_multiplier_config(client)
    assert client.execute_command("CONFIG SET search.search-result-buffer-multiplier 1.2") == b"OK"
    # Test various limit/offset combinations
    test_cases = [
        (0, 100),    # First 100 results
        (500, 50),   # 50 results starting from position 500
        (1000, 100), # 100 results starting from position 1000
        (2400, 200), # Last 100 results (should only return 100)
        (0, 1000),   # Large batch
        (2500, 10),  # Offset beyond available data
    ]
    for offset, limit in test_cases:
        # Test with content
        result = client.execute_command("FT.SEARCH", "bulk_products", "@price:[0 +inf]", "LIMIT", str(offset), str(limit))
        total_count = result[0]
        assert total_count == 2500  # Always should report total count
        expected_results = min(limit, max(0, 2500 - offset))
        actual_results = (len(result) - 1) // 2  # Subtract count, divide by 2 for key+content pairs
        assert actual_results == expected_results, f"Offset {offset}, Limit {limit}: expected {expected_results}, got {actual_results}"
        # Test with NOCONTENT
        result_nocontent = client.execute_command("FT.SEARCH", "bulk_products", "@price:[0 +inf]", "LIMIT", str(offset), str(limit), "NOCONTENT")
        assert result_nocontent[0] == 2500  # Total count
        actual_keys = len(result_nocontent) - 1  # Subtract count
        assert actual_keys == expected_results, f"NOCONTENT Offset {offset}, Limit {limit}: expected {expected_results}, got {actual_keys}"
    
    # Test filtered queries with limits
    result = client.execute_command("FT.SEARCH", "bulk_products", "@category:{cat0}", "LIMIT", "0", "50")
    assert result[0] == 250  # Should find 250 documents in cat0 (2500/10)
    assert (len(result) - 1) // 2 == 50  # Should return 50 results
    
    # Test with complex filter and offset
    result = client.execute_command("FT.SEARCH", "bulk_products", "@price:[100 500] @rating:[4.0 +inf]", "LIMIT", "2", "3")
    total_count = result[0]
    actual_results = (len(result) - 1) // 2
    assert actual_results <= 3  # Should return at most 3 results
    assert actual_results == min(3, max(0, total_count - 2))  # Respect offset of 2

def validate_aggregate_queries(client: Valkey):
    """
        Test FT.AGGREGATE with numeric and tag queries.
    """
    # Tag filter
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@category:{electronics}",
        "LOAD", "1", "price",
        "APPLY", "@price*2", "AS", "double_price"
    )
    assert result[0] == 3
    # Numeric filter
    result = client.execute_command(
        "FT.AGGREGATE", "products", "@price:[100 500]",
        "LOAD", "1", "category"
    )
    assert result[0] == 2

class TestNonVector(ValkeySearchTestCaseBase):

    def test_basic(self):
        """
            Test a numeric query and tag + numeric query on Hash/JSON docs in Valkey Search CMD.
        """
        # Create indexes:
        client: Valkey = self.server.get_new_client()
        create_indexes(client)
        # Data population:
        for doc in hash_docs:
            assert client.execute_command(*doc) == 5
        for doc in json_docs:
            assert client.execute_command(*doc) == b"OK"
        # Validation of numeric and tag queries.
        validate_non_vector_queries(client)
        # Test LIMIT functionality
        validate_limit_queries(client)
        # Test AGGREGATE functionality
        validate_aggregate_queries(client)

    def test_uningested_multi_field(self):
        """
            Test out the case where some index fields are not ingested. But other numeric and tag fields are.
        """
        client: Valkey = self.server.get_new_client()
        # Create multi-field index with TEXT, NUMERIC, and TAG fields
        multi_field_index = "FT.CREATE multifield_products ON HASH PREFIX 1 multifield_product: SCHEMA price NUMERIC rating NUMERIC new_field1 NUMERIC category TAG new_field2 TAG"
        assert client.execute_command(multi_field_index) == b"OK"
        # Data population with multifield_ prefix
        for doc in hash_docs:
            assert client.execute_command(*["HSET", "multifield_" + doc[1]] + doc[2:]) == 5
        # Test numeric query
        result = client.execute_command("FT.SEARCH", "multifield_products", "@price:[300 1000] @rating:[4.4 +inf]")
        assert result[0] == 1
        assert result[1] == b'multifield_product:1'
        # Test tag + numeric query
        result = client.execute_command("FT.SEARCH", "multifield_products", "@category:{books} @price:[10 30] @rating:[4.7 +inf]")
        assert result[0] == 1
        assert result[1] == b'multifield_product:4'

    def test_bulk_limit_background_changes(self):
        """
            Test bulk operations with various LIMIT and OFFSET combinations to validate background limit changes.
        """
        client: Valkey = self.server.get_new_client()
        create_bulk_data_standalone(client)
        validate_bulk_limit_queries(client)

class TestNonVectorCluster(ValkeySearchClusterTestCase):

    def test_non_vector_cluster(self):
        """
            Test a numeric query and tag + numeric query on Hash/JSON docs in Valkey Search CME.
        """
        # Create indexes:
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        create_indexes(client)
        # Data population:
        for doc in hash_docs:
            assert cluster_client.execute_command(*doc) == 5
        for doc in json_docs:
            assert cluster_client.execute_command(*doc) == b"OK"
        create_bulk_data_cluster(client, cluster_client)
        time.sleep(1)
        # Validation of numeric and tag queries.
        validate_non_vector_queries(client)
        # Test LIMIT functionality
        validate_limit_queries(client)
        # Test bulk limit functionality
        validate_bulk_limit_queries(client)
