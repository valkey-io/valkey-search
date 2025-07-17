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
numeric_index_on_hash = "FT.CREATE products ON HASH PREFIX 1 product: SCHEMA price NUMERIC rating NUMERIC"
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

numeric_tag_index_on_json = "FT.CREATE jsonproducts ON JSON PREFIX 1 jsonproduct: SCHEMA $.category TAG $.price NUMERIC $.rating NUMERIC"
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
    "@$.price:[300 2000] @$.rating:[4.4 +inf]"
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
    "@$.category:{books} @$.price:[10 30] @$.rating:[4.7 +inf]"
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
    assert client.execute_command(numeric_index_on_hash) == b"OK"
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
        time.sleep(1)
        # Validation of numeric and tag queries.
        validate_non_vector_queries(client)
