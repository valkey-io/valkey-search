import pytest
import struct
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFilterExpressions(ValkeySearchTestCaseBase):
    """
    Comprehensive tests for filter expressions in FT.SEARCH queries.
    
    Test organization by complexity level:
    
    LEVEL 1 - Basic Filter Syntax:
        - Tag OR syntax: @field:{tag1|tag2|tag3}
        - Numeric ranges: All 9 variants (inclusive/exclusive, inf, equality)
        - Single filter operations
    
    LEVEL 2 - Simple Combinations:
        - Tag filters with AND
        - Tag filters with OR
        - Negation on single filters
        - Hybrid queries (filters + vector search)
    
    LEVEL 3 - Operator Precedence & Complex Logic:
        - Multiple operators with precedence rules
        - Parentheses to override precedence
        - Negation with other operators
        - Multiple filters combined
    
    LEVEL 4 - Advanced Combinations (COMMANDS.md examples):
        - Tag OR + AND: @genre:{comedy|horror} @year:[2015 2024]
        - Logical OR: @genre:{comedy|horror} | @year:[2015 2024]
        - Negation + AND: -@genre:{comedy} @year:[2015 2024]
        - Complex hierarchical expressions
    
    Coverage matches the "Filter Expression" section of COMMANDS.md.
    """

    def test_tag_or_syntax_basic(self):
        """
        Test basic tag OR syntax without vector search.
        Validates that @field:{tag1|tag2|tag3} works correctly.
        """
        client: Valkey = self.server.get_new_client()
        
        # Create index with tag field
        assert client.execute_command(
            "FT.CREATE", "countries_idx", 
            "ON", "HASH", 
            "PREFIX", "1", "country:",
            "SCHEMA", "country", "TAG"
        ) == b"OK"
        
        # Add test data
        assert client.execute_command("HSET", "country:1", "country", "USA") == 1
        assert client.execute_command("HSET", "country:2", "country", "GBR") == 1
        assert client.execute_command("HSET", "country:3", "country", "CAN") == 1
        assert client.execute_command("HSET", "country:4", "country", "FRA") == 1
        assert client.execute_command("HSET", "country:5", "country", "DEU") == 1
        
        # Test tag OR syntax: @country:{USA|GBR|CAN}
        result = client.execute_command("FT.SEARCH", "countries_idx", "@country:{USA|GBR|CAN}")
        assert result[0] == 3  # Should find 3 countries
        
        # Verify the correct countries were found
        found_countries = set()
        for i in range(1, len(result), 2):
            key = result[i].decode('utf-8')
            found_countries.add(key)
        
        assert found_countries == {"country:1", "country:2", "country:3"}
        
        # Test with single tag (should still work)
        result = client.execute_command("FT.SEARCH", "countries_idx", "@country:{FRA}")
        assert result[0] == 1
        assert result[1] == b"country:4"
        
        # Test with all tags using OR
        result = client.execute_command("FT.SEARCH", "countries_idx", "@country:{USA|GBR|CAN|FRA|DEU}")
        assert result[0] == 5  # Should find all 5 countries

    # =====================================================================
    # LEVEL 2 - Simple Combinations
    # =====================================================================

    def test_tag_or_syntax_in_hybrid_query(self):
        """
        LEVEL 2: Test tag OR syntax in hybrid queries with vector search.
        This is the main bug fix: @country:{USA|GBR|CAN}=>[KNN...] should work.
        Also validates equivalence with verbose syntax: (@country:{USA} | @country:{GBR} | @country:{CAN})
        """
        client: Valkey = self.server.get_new_client()
        
        # Create index with tag and vector fields
        assert client.execute_command(
            "FT.CREATE", "hybrid_idx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "country", "TAG",
            "embedding", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32",
            "DIM", "3",
            "DISTANCE_METRIC", "COSINE"
        ) == b"OK"
        
        # Create sample 3D vectors
        vec1 = struct.pack('3f', 1.0, 0.0, 0.0)
        vec2 = struct.pack('3f', 0.0, 1.0, 0.0)
        vec3 = struct.pack('3f', 0.0, 0.0, 1.0)
        vec4 = struct.pack('3f', 0.5, 0.5, 0.0)
        vec5 = struct.pack('3f', 0.5, 0.0, 0.5)
        
        # Add test data with vectors
        assert client.execute_command("HSET", "doc:1", "country", "USA", "embedding", vec1) == 2
        assert client.execute_command("HSET", "doc:2", "country", "GBR", "embedding", vec2) == 2
        assert client.execute_command("HSET", "doc:3", "country", "CAN", "embedding", vec3) == 2
        assert client.execute_command("HSET", "doc:4", "country", "FRA", "embedding", vec4) == 2
        assert client.execute_command("HSET", "doc:5", "country", "DEU", "embedding", vec5) == 2
        
        # Query vector (close to vec1)
        query_vec = struct.pack('3f', 0.9, 0.1, 0.0)
        
        # Test hybrid query with compact tag OR syntax
        result1 = client.execute_command(
            "FT.SEARCH", "hybrid_idx",
            "@country:{USA|GBR|CAN}=>[KNN 5 @embedding $vec]",
            "PARAMS", "2", "vec", query_vec,
            "NOCONTENT"
        )
        
        # Test hybrid query with verbose OR syntax (should be equivalent)
        result2 = client.execute_command(
            "FT.SEARCH", "hybrid_idx",
            "(@country:{USA} | @country:{GBR} | @country:{CAN})=>[KNN 5 @embedding $vec]",
            "PARAMS", "2", "vec", query_vec,
            "NOCONTENT"
        )
        
        # Both syntaxes should return the same results
        assert result1[0] == result2[0] and result1[0] >= 1 and result1[0] <= 3
        keys1 = set(result1[i].decode('utf-8') for i in range(1, len(result1)))
        keys2 = set(result2[i].decode('utf-8') for i in range(1, len(result2)))
        assert keys1 == keys2
        
        # Verify all results match the country filter
        for key in keys1:
            assert key in ["doc:1", "doc:2", "doc:3"], f"Unexpected key {key}"
        
        # Test that documents not matching the tag are excluded
        result = client.execute_command(
            "FT.SEARCH", "hybrid_idx",
            "@country:{FRA|DEU}=>[KNN 5 @embedding $vec]",
            "PARAMS", "2", "vec", query_vec,
            "NOCONTENT"
        )
        
        assert result[0] >= 1 and result[0] <= 2
        for i in range(1, len(result)):
            key = result[i].decode('utf-8')
            assert key in ["doc:4", "doc:5"], f"Unexpected key {key}"

    def test_tag_or_with_custom_separator_index(self):
        """
        LEVEL 2: Test that tag OR syntax uses '|' in queries even when index
        is created with a different separator (e.g., comma).
        """
        client: Valkey = self.server.get_new_client()
        
        # Create index with comma separator for data storage
        assert client.execute_command(
            "FT.CREATE", "custom_sep_idx",
            "ON", "HASH",
            "PREFIX", "1", "prod:",
            "SCHEMA",
            "tags", "TAG", "SEPARATOR", ","  # Index uses comma separator
        ) == b"OK"
        
        # Add data with comma-separated tags
        assert client.execute_command("HSET", "prod:1", "tags", "red,large") == 1
        assert client.execute_command("HSET", "prod:2", "tags", "blue,small") == 1
        assert client.execute_command("HSET", "prod:3", "tags", "red,small") == 1
        assert client.execute_command("HSET", "prod:4", "tags", "green,large") == 1
        
        # Query using '|' syntax (not comma) - this should work with the fix
        result = client.execute_command("FT.SEARCH", "custom_sep_idx", "@tags:{red|blue}")
        assert result[0] == 3  # Should find prod:1, prod:2, prod:3
        
        # Query for multiple values
        result = client.execute_command("FT.SEARCH", "custom_sep_idx", "@tags:{large|small}")
        assert result[0] == 4  # Should find all products

    def test_complex_hybrid_query_with_multiple_filters(self):
        """
        LEVEL 2: Test hybrid queries combining tag OR, numeric ranges, and vector search.
        Query: @category:{electronics|books} @price:[0 600] @rating:[4.0 5.0]=>[KNN...]
        """
        client: Valkey = self.server.get_new_client()
        
        # Create index
        assert client.execute_command(
            "FT.CREATE", "complex_idx",
            "ON", "HASH",
            "PREFIX", "1", "product:",
            "SCHEMA",
            "category", "TAG",
            "price", "NUMERIC",
            "rating", "NUMERIC",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32",
            "DIM", "2",
            "DISTANCE_METRIC", "COSINE"
        ) == b"OK"
        
        # Add test data
        vec1 = struct.pack('2f', 1.0, 0.0)
        vec2 = struct.pack('2f', 0.9, 0.1)
        vec3 = struct.pack('2f', 0.1, 0.9)
        vec4 = struct.pack('2f', 0.0, 1.0)
        
        assert client.execute_command("HSET", "product:1", "category", "electronics", "price", "500", "rating", "4.5", "vec", vec1) == 4
        assert client.execute_command("HSET", "product:2", "category", "books", "price", "20", "rating", "4.8", "vec", vec2) == 4
        assert client.execute_command("HSET", "product:3", "category", "electronics", "price", "800", "rating", "3.9", "vec", vec3) == 4
        assert client.execute_command("HSET", "product:4", "category", "clothing", "price", "100", "rating", "4.2", "vec", vec4) == 4
        
        query_vec = struct.pack('2f', 1.0, 0.0)
        
        # Complex query: tag OR + numeric ranges + vector search
        result = client.execute_command(
            "FT.SEARCH", "complex_idx",
            "@category:{electronics|books} @price:[0 600] @rating:[4.0 5.0]=>[KNN 5 @vec $qvec]",
            "PARAMS", "2", "qvec", query_vec,
            "RETURN", "3", "category", "price", "rating"
        )
        
        # Should return products matching all criteria
        assert result[0] >= 1
        
        # Verify results match the filters
        for i in range(1, len(result), 2):
            key = result[i].decode('utf-8')
            fields = result[i + 1]
            field_dict = dict(zip(fields[::2], fields[1::2]))
            
            category = field_dict[b'category'].decode('utf-8')
            price = float(field_dict[b'price'])
            rating = float(field_dict[b'rating'])
            
            # Verify filters
            assert category in ["electronics", "books"], f"Category {category} not in filter"
            assert 0 <= price <= 600, f"Price {price} out of range"
            assert 4.0 <= rating <= 5.0, f"Rating {rating} out of range"

    def test_numeric_only_hybrid_query(self):
        """
        LEVEL 2: Test hybrid queries with numeric filters only (no tag filters).
        Ensures the fix doesn't break numeric-only queries.
        Query: @price:[0 100] @stock:[50 250]=>[KNN...]
        """
        client: Valkey = self.server.get_new_client()
        
        # Create index
        assert client.execute_command(
            "FT.CREATE", "numeric_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA",
            "price", "NUMERIC",
            "stock", "NUMERIC",
            "embedding", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32",
            "DIM", "2",
            "DISTANCE_METRIC", "L2"
        ) == b"OK"
        
        # Add test data
        vec1 = struct.pack('2f', 1.0, 0.0)
        vec2 = struct.pack('2f', 0.8, 0.2)
        vec3 = struct.pack('2f', 0.2, 0.8)
        
        assert client.execute_command("HSET", "item:1", "price", "50", "stock", "100", "embedding", vec1) == 3
        assert client.execute_command("HSET", "item:2", "price", "150", "stock", "50", "embedding", vec2) == 3
        assert client.execute_command("HSET", "item:3", "price", "75", "stock", "200", "embedding", vec3) == 3
        
        query_vec = struct.pack('2f', 1.0, 0.0)
        
        # Numeric-only hybrid query
        result = client.execute_command(
            "FT.SEARCH", "numeric_idx",
            "@price:[0 100] @stock:[50 250]=>[KNN 5 @embedding $vec]",
            "PARAMS", "2", "vec", query_vec,
            "RETURN", "2", "price", "stock"
        )
        
        # Should return items matching numeric criteria
        assert result[0] >= 1
        
        # Verify all results match the numeric filters
        for i in range(1, len(result), 2):
            fields = result[i + 1]
            field_dict = dict(zip(fields[::2], fields[1::2]))
            
            price = float(field_dict[b'price'])
            stock = float(field_dict[b'stock'])
            
            assert 0 <= price <= 100, f"Price {price} out of range"
            assert 50 <= stock <= 250, f"Stock {stock} out of range"

    # =====================================================================
    # LEVEL 3 - Operator Precedence & Complex Logic
    # =====================================================================

    def test_negation_tag_filter(self):
        """
        LEVEL 3: Test negation on tag filter: -@field:{value}
        Returns all documents NOT matching the tag.
        Note: Documents without the field are not indexed and won't be returned.
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "neg_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "category", "TAG"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "category", "electronics") == 1
        assert client.execute_command("HSET", "item:2", "category", "books") == 1
        assert client.execute_command("HSET", "item:3", "category", "clothing") == 1
        
        # Test -@category:{books} - should return electronics and clothing
        result = client.execute_command("FT.SEARCH", "neg_idx", "-@category:{books}", "NOCONTENT")
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"item:1", "item:3"}

    def test_negation_with_numeric_and(self):
        """
        LEVEL 3: Test negation combined with positive numeric filter using AND.
        From COMMANDS.md: -@genre:{comedy} @year:[2015 2024]
        Returns books NOT comedy AND published 2015-2024.
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "book:1", "genre", "comedy", "year", "2020") == 2
        assert client.execute_command("HSET", "book:2", "genre", "horror", "year", "2018") == 2
        assert client.execute_command("HSET", "book:3", "genre", "drama", "year", "2020") == 2
        assert client.execute_command("HSET", "book:4", "genre", "comedy", "year", "2010") == 2
        assert client.execute_command("HSET", "book:5", "genre", "scifi", "year", "2024") == 2
        
        # Test -@genre:{comedy} @year:[2015 2024]
        result = client.execute_command("FT.SEARCH", "books_idx", "-@genre:{comedy} @year:[2015 2024]", "NOCONTENT")
        assert result[0] == 3
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"book:2", "book:3", "book:5"}

    def test_operator_precedence_and_before_or(self):
        """
        LEVEL 3: Test that AND (space) has higher precedence than OR (|).
        @genre:{comedy} @year:[2020 2024] | @rating:[4.5 +inf]
        Means: (comedy AND year 2020-2024) OR (rating >= 4.5)
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC", "rating", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "book:1", "genre", "comedy", "year", "2022", "rating", "4.0") == 3
        assert client.execute_command("HSET", "book:2", "genre", "horror", "year", "2020", "rating", "4.8") == 3
        assert client.execute_command("HSET", "book:3", "genre", "comedy", "year", "2015", "rating", "3.5") == 3
        assert client.execute_command("HSET", "book:4", "genre", "drama", "year", "2018", "rating", "4.7") == 3
        
        # Should match: book:1 (comedy+2022), book:2 (rating 4.8), book:4 (rating 4.7)
        result = client.execute_command(
            "FT.SEARCH", "books_idx",
            "@genre:{comedy} @year:[2020 2024] | @rating:[4.5 +inf]",
            "NOCONTENT"
        )
        assert result[0] == 3
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"book:1", "book:2", "book:4"}

    def test_parentheses_override_precedence(self):
        """
        LEVEL 3: Test that parentheses can override default precedence.
        (@genre:{comedy} | @genre:{horror}) @year:[2020 2024]
        Means: (comedy OR horror) AND (year 2020-2024)
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "book:1", "genre", "comedy", "year", "2022") == 2
        assert client.execute_command("HSET", "book:2", "genre", "horror", "year", "2020") == 2
        assert client.execute_command("HSET", "book:3", "genre", "comedy", "year", "2015") == 2
        assert client.execute_command("HSET", "book:4", "genre", "drama", "year", "2021") == 2
        
        # Should match book:1 and book:2 (comedy or horror, AND year 2020-2024)
        result = client.execute_command(
            "FT.SEARCH", "books_idx",
            "(@genre:{comedy} | @genre:{horror}) @year:[2020 2024]",
            "NOCONTENT"
        )
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"book:1", "book:2"}

    # =====================================================================
    # LEVEL 4 - Advanced Combinations (COMMANDS.md Examples)
    # =====================================================================

    # =====================================================================
    # NUMERIC RANGE OPERATORS - All 9 variants from COMMANDS.md
    # =====================================================================

    def test_numeric_range_inclusive_both(self):
        """
        Test numeric range with both bounds inclusive: @field:[min max]
        Example from docs: min <= field <= max  ->  @field:[min max]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        # Add test data with various prices
        assert client.execute_command("HSET", "item:1", "price", "50") == 1
        assert client.execute_command("HSET", "item:2", "price", "100") == 1
        assert client.execute_command("HSET", "item:3", "price", "150") == 1
        assert client.execute_command("HSET", "item:4", "price", "200") == 1
        assert client.execute_command("HSET", "item:5", "price", "250") == 1
        
        # Test [100 200] - should include 100, 150, 200
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[100 200]", "NOCONTENT")
        assert result[0] == 3
        # Result format: [count, key1, fields1, key2, fields2, ...]  
        # With NOCONTENT, fields are still present but empty/minimal
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"item:2", "item:3", "item:4"}

    def test_numeric_range_exclusive_min_inclusive_max(self):
        """
        Test numeric range with exclusive min, inclusive max: @field:[(min max]
        Example from docs: min < field <= max  ->  @field:[(min max]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "100") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "200") == 1
        
        # Test [(100 200] - should exclude 100, include 150 and 200
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[(100 200]", "NOCONTENT")
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"item:2", "item:3"}

    def test_numeric_range_inclusive_min_exclusive_max(self):
        """
        Test numeric range with inclusive min, exclusive max: @field:[min (max]
        Example from docs: min <= field < max  ->  @field:[min (max]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "100") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "200") == 1
        
        # Test [100 (200] - should include 100 and 150, exclude 200
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[100 (200]", "NOCONTENT")
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"item:1", "item:2"}

    def test_numeric_range_exclusive_both(self):
        """
        Test numeric range with both bounds exclusive: @field:[(min (max]
        Example from docs: min < field < max  ->  @field:[(min (max]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "100") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "200") == 1
        
        # Test [(100 (200] - should only include 150
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[(100 (200]", "NOCONTENT")
        assert result[0] == 1
        assert result[1] == b"item:2"

    def test_numeric_range_greater_or_equal(self):
        """
        Test numeric range for >= comparison: @field:[min +inf]
        Example from docs: field >= min  ->  @field:[min +inf]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "50") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "250") == 1
        
        # Test [150 +inf] - should include 150 and 250
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[150 +inf]", "NOCONTENT")
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"item:2", "item:3"}

    def test_numeric_range_greater_than(self):
        """
        Test numeric range for > comparison: @field:[(min +inf]
        Example from docs: field > min  ->  @field:[(min +inf]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "50") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "250") == 1
        
        # Test [(150 +inf] - should only include 250 (excludes 150)
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[(150 +inf]", "NOCONTENT")
        assert result[0] == 1
        assert result[1] == b"item:3"

    def test_numeric_range_less_or_equal(self):
        """
        Test numeric range for <= comparison: @field:[-inf max]
        Example from docs: field <= max  ->  @field:[-inf max]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "50") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "250") == 1
        
        # Test [-inf 150] - should include 50 and 150
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[-inf 150]", "NOCONTENT")
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"item:1", "item:2"}

    def test_numeric_range_less_than(self):
        """
        Test numeric range for < comparison: @field:[-inf (max]
        Example from docs: field < max  ->  @field:[-inf (max]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "50") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "250") == 1
        
        # Test [-inf (150] - should only include 50 (excludes 150)
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[-inf (150]", "NOCONTENT")
        assert result[0] == 1
        assert result[1] == b"item:1"

    def test_numeric_range_equality(self):
        """
        Test numeric range for equality: @field:[val val]
        Example from docs: field == val  ->  @field:[val val]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "range_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "item:1", "price", "100") == 1
        assert client.execute_command("HSET", "item:2", "price", "150") == 1
        assert client.execute_command("HSET", "item:3", "price", "200") == 1
        
        # Test [150 150] - should only include item with price == 150
        result = client.execute_command("FT.SEARCH", "range_idx", "@price:[150 150]", "NOCONTENT")
        assert result[0] == 1
        assert result[1] == b"item:2"

    # =====================================================================
    # LOGICAL NEGATION OPERATOR (-)
    # =====================================================================

    # =====================================================================
    # LEVEL 4 - Advanced Combinations (COMMANDS.md Examples)
    # =====================================================================

    def test_commands_md_tag_or_with_and(self):
        """
        LEVEL 4: COMMANDS.MD Example - Tag OR within braces combined with AND.
        Query: @genre:{comedy|horror} @year:[2015 2024]
        Returns: books with (comedy OR horror) AND (year 2015-2024)
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC"
        ) == b"OK"
        
        # Add test data
        assert client.execute_command("HSET", "book:1", "genre", "comedy", "year", "2020") == 2
        assert client.execute_command("HSET", "book:2", "genre", "horror", "year", "2018") == 2
        assert client.execute_command("HSET", "book:3", "genre", "drama", "year", "2020") == 2
        assert client.execute_command("HSET", "book:4", "genre", "comedy", "year", "2010") == 2
        assert client.execute_command("HSET", "book:5", "genre", "horror", "year", "2024") == 2
        assert client.execute_command("HSET", "book:6", "genre", "comedy", "year", "2025") == 2
        
        # Query: @genre:{comedy|horror} @year:[2015 2024]
        result = client.execute_command(
            "FT.SEARCH", "books_idx",
            "@genre:{comedy|horror} @year:[2015 2024]",
            "NOCONTENT"
        )
        assert result[0] == 3
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"book:1", "book:2", "book:5"}

    def test_commands_md_logical_or_comparison(self):
        """
        LEVEL 4: COMMANDS.MD Example - Demonstrates AND vs OR precedence.
        Shows difference between:
        - AND: @genre:{comedy|horror} @year:[2015 2024] 
        - OR:  @genre:{comedy|horror} | @year:[2015 2024]
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC"
        ) == b"OK"
        
        # Add test data with clear categorization
        assert client.execute_command("HSET", "book:1", "genre", "comedy", "year", "2020") == 2  # Matches both
        assert client.execute_command("HSET", "book:2", "genre", "horror", "year", "2018") == 2  # Matches both
        assert client.execute_command("HSET", "book:3", "genre", "comedy", "year", "2010") == 2  # Only genre match
        assert client.execute_command("HSET", "book:4", "genre", "drama", "year", "2020") == 2   # Only year match
        assert client.execute_command("HSET", "book:5", "genre", "drama", "year", "2010") == 2   # Matches neither
        
        # Test 1: AND - (genre comedy|horror) AND (year 2015-2024)
        result_and = client.execute_command(
            "FT.SEARCH", "books_idx",
            "@genre:{comedy|horror} @year:[2015 2024]",
            "NOCONTENT"
        )
        assert result_and[0] == 2  # book:1, book:2
        keys_and = set(result_and[i].decode('utf-8') for i in range(1, len(result_and)))
        assert keys_and == {"book:1", "book:2"}

        # Test 2: OR - (genre comedy|horror) OR (year 2015-2024)
        result_or = client.execute_command(
            "FT.SEARCH", "books_idx",
            "@genre:{comedy|horror} | @year:[2015 2024]",
            "NOCONTENT"
        )
        assert result_or[0] == 4  # book:1, book:2, book:3, book:4
        keys_or = set(result_or[i].decode('utf-8') for i in range(1, len(result_or)))
        assert keys_or == {"book:1", "book:2", "book:3", "book:4"}
        
        # Verify the difference
        assert keys_and.issubset(keys_or)  # AND results are a subset of OR results
        assert len(keys_or) > len(keys_and)  # OR returns more results

    def test_commands_md_complex_negation_and_or(self):
        """
        LEVEL 4: Complex precedence with negation and OR in parentheses.
        -@genre:{drama} (@year:[2020 2024] | @rating:[4.5 +inf])
        Means: NOT drama AND (year 2020-2024 OR rating >= 4.5)
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC", "rating", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "book:1", "genre", "comedy", "year", "2022", "rating", "4.0") == 3
        assert client.execute_command("HSET", "book:2", "genre", "horror", "year", "2015", "rating", "4.8") == 3
        assert client.execute_command("HSET", "book:3", "genre", "drama", "year", "2021", "rating", "4.9") == 3
        assert client.execute_command("HSET", "book:4", "genre", "comedy", "year", "2010", "rating", "3.5") == 3
        
        result = client.execute_command(
            "FT.SEARCH", "books_idx",
            "-@genre:{drama} (@year:[2020 2024] | @rating:[4.5 +inf])",
            "NOCONTENT"
        )
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"book:1", "book:2"}

    def test_commands_md_multiple_negations(self):
        """
        LEVEL 4: Multiple negations combined with AND.
        -@genre:{comedy} -@rating:[0 3.0] @year:[2015 2024]
        Returns: NOT comedy AND rating > 3.0 AND year 2015-2024
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC", "rating", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "book:1", "genre", "horror", "year", "2020", "rating", "4.5") == 3
        assert client.execute_command("HSET", "book:2", "genre", "comedy", "year", "2020", "rating", "4.5") == 3
        assert client.execute_command("HSET", "book:3", "genre", "horror", "year", "2020", "rating", "2.5") == 3
        assert client.execute_command("HSET", "book:4", "genre", "drama", "year", "2010", "rating", "4.5") == 3
        assert client.execute_command("HSET", "book:5", "genre", "scifi", "year", "2022", "rating", "4.0") == 3
        
        result = client.execute_command(
            "FT.SEARCH", "books_idx",
            "-@genre:{comedy} -@rating:[0 3.0] @year:[2015 2024]",
            "NOCONTENT"
        )
        assert result[0] == 2
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"book:1", "book:5"}

    def test_commands_md_hierarchical_parentheses(self):
        """
        LEVEL 4: Hierarchical expression with multiple levels of parentheses.
        (@genre:{comedy} | @genre:{horror}) (@year:[2015 2020] | @year:[2023 2024])
        Returns: (comedy OR horror) AND (year 2015-2020 OR year 2023-2024)
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "books_idx",
            "ON", "HASH",
            "PREFIX", "1", "book:",
            "SCHEMA", "genre", "TAG", "year", "NUMERIC"
        ) == b"OK"
        
        assert client.execute_command("HSET", "book:1", "genre", "comedy", "year", "2018") == 2  # Match
        assert client.execute_command("HSET", "book:2", "genre", "horror", "year", "2023") == 2  # Match
        assert client.execute_command("HSET", "book:3", "genre", "comedy", "year", "2021") == 2  # No match (year gap)
        assert client.execute_command("HSET", "book:4", "genre", "drama", "year", "2018") == 2   # No match (wrong genre)
        assert client.execute_command("HSET", "book:5", "genre", "horror", "year", "2015") == 2  # Match
        assert client.execute_command("HSET", "book:6", "genre", "scifi", "year", "2024") == 2   # No match (wrong genre)
        
        result = client.execute_command(
            "FT.SEARCH", "books_idx",
            "(@genre:{comedy} | @genre:{horror}) (@year:[2015 2020] | @year:[2023 2024])",
            "NOCONTENT"
        )
        assert result[0] == 3
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"book:1", "book:2", "book:5"}

    def test_deep_or_operator_nesting_depth_4(self):
        """
        LEVEL 4: Test deep nesting of OR operators (depth 4).
        Query structure: ((A | B) | (C | D)) | ((E | F) | (G | H))
        This tests the parser's ability to handle deeply nested logical OR expressions.
        """
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command(
            "FT.CREATE", "deep_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA", 
            "category", "TAG",
            "priority", "TAG",
            "status", "TAG",
            "region", "TAG"
        ) == b"OK"
        
        # Add test data covering different combinations
        assert client.execute_command("HSET", "item:1", "category", "A", "priority", "high", "status", "active", "region", "us") == 4
        assert client.execute_command("HSET", "item:2", "category", "B", "priority", "medium", "status", "active", "region", "eu") == 4
        assert client.execute_command("HSET", "item:3", "category", "C", "priority", "low", "status", "pending", "region", "asia") == 4
        assert client.execute_command("HSET", "item:4", "category", "D", "priority", "high", "status", "pending", "region", "us") == 4
        assert client.execute_command("HSET", "item:5", "category", "E", "priority", "medium", "status", "inactive", "region", "eu") == 4
        assert client.execute_command("HSET", "item:6", "category", "F", "priority", "low", "status", "inactive", "region", "asia") == 4
        assert client.execute_command("HSET", "item:7", "category", "G", "priority", "high", "status", "active", "region", "us") == 4
        assert client.execute_command("HSET", "item:8", "category", "H", "priority", "medium", "status", "pending", "region", "eu") == 4
        assert client.execute_command("HSET", "item:9", "category", "X", "priority", "low", "status", "active", "region", "asia") == 4  # Should not match
        
        # Deep OR nesting: ((A | B) | (C | D)) | ((E | F) | (G | H))
        # This creates a 4-level deep OR tree structure
        result = client.execute_command(
            "FT.SEARCH", "deep_idx",
            "((@category:{A} | @category:{B}) | (@category:{C} | @category:{D})) | ((@category:{E} | @category:{F}) | (@category:{G} | @category:{H}))",
            "NOCONTENT"
        )
        
        # Should match items 1-8 (all except item:9 with category X)
        assert result[0] == 8
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        expected = {f"item:{i}" for i in range(1, 9)}
        assert keys == expected
        
        # Test depth 4 with mixed operators: OR at multiple levels combined with AND
        # ((cat A|B) AND priority high) OR ((cat C|D) AND status pending) OR ((cat E|F) AND region eu) OR ((cat G|H) AND status active)
        result = client.execute_command(
            "FT.SEARCH", "deep_idx",
            "((@category:{A|B} @priority:{high}) | (@category:{C|D} @status:{pending})) | ((@category:{E|F} @region:{eu}) | (@category:{G|H} @status:{active}))",
            "NOCONTENT"
        )
        
        # Should match:
        # - item:1 (A, high) ✓
        # - item:3 (C, pending) ✓
        # - item:4 (D, pending) ✓
        # - item:5 (E, eu) ✓
        # - item:7 (G, active) ✓
        assert result[0] == 5
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert keys == {"item:1", "item:3", "item:4", "item:5", "item:7"}
        
        # Test depth 4 with negation at different levels
        # (NOT A AND NOT B) OR (NOT C AND NOT D) OR (NOT E AND NOT F) OR (NOT G AND NOT H)
        result = client.execute_command(
            "FT.SEARCH", "deep_idx",
            "((-@category:{A} -@category:{B}) | (-@category:{C} -@category:{D})) | ((-@category:{E} -@category:{F}) | (-@category:{G} -@category:{H}))",
            "NOCONTENT"
        )
        
        # This complex negation should match item:9 (category X doesn't match any A-H)
        assert result[0] >= 1
        keys = set(result[i].decode('utf-8') for i in range(1, len(result)))
        assert "item:9" in keys
