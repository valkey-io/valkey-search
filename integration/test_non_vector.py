from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import json

class TestNonVector(ValkeySearchTestCaseBase):

    def test_basic(self):
        """
            Test the a numeric query and tag + numeric query on Hash/JSON docs in Valkey Search.
        """
        client: Valkey = self.server.get_new_client()
        # Validate a numeric query on Hash documents.
        assert client.execute_command("FT.CREATE products ON HASH PREFIX 1 product: SCHEMA price NUMERIC rating NUMERIC") == b"OK"
        assert client.execute_command("HSET product:1 category 'electronics' name 'Laptop' price 999.99 rating 4.5 desc 'Great'") == 5
        assert client.execute_command("HSET product:2 category 'electronics' name 'Tablet' price 499.00 rating 4.0 desc 'Good'") == 5
        assert client.execute_command("HSET product:3 category 'electronics' name 'Phone' price 299.00 rating 3.8 desc 'Ok'") == 5
        assert client.execute_command("HSET product:4 category 'books' name 'Book' price 19.99 rating 4.8 desc 'Excellent'") == 5
        result = client.execute_command("FT.SEARCH", "products", "@price:[300 1000] @rating:[4.4 +inf]")
        assert len(result) == 3
        assert result[0] == 1  # Number of documents found
        assert result[1] == b'product:1'
        document = result[2]
        doc_fields = dict(zip(document[::2], document[1::2]))
        assert doc_fields == {
            b'name': b"'Laptop'",
            b'price': b'999.99',
            b'rating': b'4.5',
            b'desc': b"'Great'",
            b'category': b"'electronics'"
        }
        # Validate a numeric query on JSON documents.
        assert client.execute_command(
            "FT.CREATE jsonproducts ON JSON PREFIX 1 jsonproduct: SCHEMA $.category AS category TAG $.price AS price NUMERIC $.rating AS rating NUMERIC"
        ) == b"OK"
        assert client.execute_command(
            'JSON.SET', 'jsonproduct:1', '$', 
            '{"category":"electronics","name":"Laptop","price":999.99,"rating":4.5,"desc":"Great"}'
        ) == b"OK"
        assert client.execute_command(
            'JSON.SET', 'jsonproduct:2', '$',
            '{"category":"electronics","name":"Tablet","price":499.00,"rating":4.0,"desc":"Good"}'
        ) == b"OK"
        assert client.execute_command(
            'JSON.SET', 'jsonproduct:3', '$',
            '{"category":"electronics","name":"Phone","price":299.00,"rating":3.8,"desc":"Ok"}'
        ) == b"OK"
        assert client.execute_command(
            'JSON.SET', 'jsonproduct:4', '$',
            '{"category":"books","name":"Book","price":19.99,"rating":4.8,"desc":"Excellent"}'
        ) == b"OK"
        result = client.execute_command("FT.SEARCH", "jsonproducts", "@price:[300 1000] @rating:[4.4 +inf]")
        assert len(result) == 3
        assert result[0] == 1  # Number of documents found
        assert result[1] == b'jsonproduct:1'
        json_data = result[2]
        assert json_data[0] == b'$'  # Check JSON path
        doc = json.loads(json_data[1].decode('utf-8'))
        expected_laptop_doc = {
            "category": "electronics",
            "name": "Laptop",
            "price": 999.99,
            "rating": 4.5,
            "desc": "Great"
        }
        for key, value in expected_laptop_doc.items():
            assert key in doc, f"Key '{key}' not found in the document"
            assert doc[key] == value, f"Expected {key}={value}, got {key}={doc[key]}"
        assert set(doc.keys()) == set(expected_laptop_doc.keys()), "Document contains unexpected fields"
        # Validate that a tag + numeric query on JSON document works.
        expected_book_doc = {
            "category": "books",
            "name": "Book",
            "price": 19.99,
            "rating": 4.8,
            "desc": "Excellent"
        }
        result = client.execute_command(
            "FT.SEARCH", "jsonproducts",
            "@category:{books} @price:[10 30] @rating:[4.7 +inf]"
        )
        assert len(result) == 3
        assert result[0] == 1  # Number of documents found
        assert result[1] == b'jsonproduct:4'
        json_data = result[2]
        assert json_data[0] == b'$'  # Check JSON path
        doc = json.loads(json_data[1].decode('utf-8'))
        for key, value in expected_book_doc.items():
            assert key in doc, f"Key '{key}' not found in the document"
            assert doc[key] == value, f"Expected {key}={value}, got {key}={doc[key]}"
        assert set(doc.keys()) == set(expected_book_doc.keys()), "Document contains unexpected fields"
