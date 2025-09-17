import time
import pytest
from valkey.client import Valkey
from valkey.exceptions import OutOfMemoryError
from valkey_search_test_case import ValkeySearchTestCaseBase
from indexes import *
from valkeytestframework.conftest import resource_port_tracker


class TestEviction(ValkeySearchTestCaseBase):
    HOUR_IN_MS = 60 * 60 * 1000

    # All Valkey eviction policies
    EVICTION_POLICIES = [
        "noeviction",
        "allkeys-lru",
        "allkeys-lfu",
        "allkeys-random",
        "volatile-lru",
        "volatile-lfu",
        "volatile-random",
        "volatile-ttl",
    ]
    @pytest.mark.parametrize("eviction_policy", EVICTION_POLICIES)
    def test_eviction_with_search_index(self, eviction_policy):
        """
        Test that evicted hashes are properly removed from search indexes
        and new data can still be added and searched after eviction.
        """
        index_name = "eviction_test_index"
        client: Valkey = self.server.get_new_client()

        # Create index with Vector, Tag, and Numeric fields
        index = Index(
            index_name,
            [Vector("embedding", 4, type="FLAT"), Tag("category"), Numeric("price")],
        )

        # Create the index
        index.create(client)

        # Load initial dataset (500 documents)
        initial_docs = 500
        if eviction_policy.startswith("volatile"):
            index.load_data_with_ttl(client, initial_docs, self.HOUR_IN_MS)
        else:
            index.load_data(client, initial_docs)

        # Verify initial index state
        info = index.info(client)
        assert info.num_docs == initial_docs

        # Verify search works on initial data
        self._verify_search_operations(client, index, expected_min_results=100)

        # Configure memory limit and eviction policy
        self._configure_eviction(client, eviction_policy)

        if eviction_policy == "noeviction":
            # Special case: noeviction should prevent new writes when memory is full
            self._test_noeviction_behavior(client, index, initial_docs)
        else:
            # Test eviction behavior
            self._test_eviction_behavior(client, index, eviction_policy, initial_docs)

    def _configure_eviction(self, client: Valkey, policy: str):
        """Configure memory limit and eviction policy"""
        # Set a relatively small memory limit to trigger eviction
        # This should be enough for initial data but will trigger eviction when we add more
        current_used_memory = client.info("memory")["used_memory"]
        

        client.config_set("maxmemory", current_used_memory)
        client.config_set("maxmemory-policy", policy)

    def _test_noeviction_behavior(
        self, client: Valkey, index: Index, initial_docs: int
    ):
        """Test noeviction policy - should fail when memory is full"""
        # Try to add more data - should fail with OOM
        total_docs = initial_docs + 1000
        with pytest.raises(OutOfMemoryError):
            index.load_data(client, total_docs)

        # Verify index still has original documents
        final_info = index.info(client)
        assert final_info.num_docs <= initial_docs

        # Verify search still works
        with pytest.raises(OutOfMemoryError):
            self._verify_search_operations(client, index, expected_min_results=50)

    def _test_eviction_behavior(
        self, client: Valkey, index: Index, policy: str, initial_docs: int
    ):
        """Test eviction policies that actually evict data"""
        # Add more data to trigger eviction - load total of 2000 docs (includes original 500)
        total_docs = initial_docs * 4
        
        # Load data gradually to allow eviction to keep up
        batch_size = 100
        current_docs = initial_docs
        max_oom_retries = 5
        
        while current_docs < total_docs:
            next_batch_end = min(current_docs + batch_size, total_docs)
            oom_retries = 0
            
            while oom_retries < max_oom_retries:
                try:
                    if policy.startswith("volatile"):
                        index.load_data_with_ttl(client, next_batch_end, self.HOUR_IN_MS, start_index=current_docs)
                    else:
                        index.load_data(client, next_batch_end, start_index=current_docs)
                    current_docs = next_batch_end
                    break
                    
                except OutOfMemoryError:
                    oom_retries += 1
                    if oom_retries >= max_oom_retries:
                        # If we've hit max retries, reduce batch size and continue
                        batch_size = max(10, batch_size // 2)
                        break
                    # Wait for eviction and retry
                    time.sleep(0.1)
            
            # Small delay between batches
            time.sleep(0.01)

        # Verify that some eviction occurred
        final_info = index.info(client)
        total_expected = total_docs

        # With eviction, we should have fewer docs than total added
        assert (
            final_info.num_docs < total_expected
        ), f"Expected eviction to reduce document count from {total_expected}, got {final_info.num_docs}"

        # But we should still have a reasonable number of documents
        min_expected_docs = total_expected // 10  # At least 10% should remain
        assert (
            final_info.num_docs >= min_expected_docs
        ), f"Too many documents evicted: {final_info.num_docs} < {min_expected_docs}"

        # Wait for eviction to stabilize before testing search
        time.sleep(0.2)
        
        # Retry search operations if they fail due to OOM
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._verify_search_operations(client, index, expected_min_results=10)
                break
            except OutOfMemoryError:
                if attempt == max_retries - 1:
                    raise
                time.sleep(0.1)

    def _verify_search_operations(
        self, client: Valkey, index: Index, expected_min_results: int
    ):
        """Verify that various search operations work correctly using random hash from DB"""

        # Get a random key from the database
        random_key = client.randomkey()
        if not random_key:
            raise Exception("No keys found in database")

        # Get the hash data from the random key
        hash_data = client.hgetall(random_key)
        if not hash_data:
            raise Exception(f"Key {random_key} is not a hash or has no data")

        # Test 1: Tag search using actual tag value from random hash
        actual_category = hash_data.get(b"category")
        if not actual_category:
            print(f"Random hash {random_key} {hash_data} missing 'category' field")
            raise Exception(
                f"Random hash {random_key} {hash_data} missing 'category' field"
            )

        tag_results = client.execute_command(
            "FT.SEARCH",
            index.name,
            f"@category:{{{actual_category.decode() if isinstance(actual_category, bytes) else actual_category}}}",
            "LIMIT",
            "0",
            "100",
        )
        assert tag_results[0] >= 1, "Should find results for tag search"

        # Test 2: Numeric range search using actual price value from random hash
        actual_price = hash_data.get(b"price")
        if not actual_price:
            raise Exception("Random hash missing 'price' field")

        price_value = int(
            actual_price.decode() if isinstance(actual_price, bytes) else actual_price
        )
        # Create a small range around the actual price value
        price_min = max(0, price_value - 10)
        price_max = price_value + 10

        numeric_results = client.execute_command(
            "FT.SEARCH",
            index.name,
            f"@price:[{price_min} {price_max}]",
            "LIMIT",
            "0",
            "100",
        )
        assert numeric_results[0] >= 1, "Should find results for numeric range search"

        # Test 3: Vector similarity search using actual embedding from random hash
        actual_embedding = hash_data.get(b"embedding")
        if not actual_embedding:
            raise Exception("Random hash missing 'embedding' field")

        # Use the actual embedding bytes for vector search
        vector_results = client.execute_command(
            "FT.SEARCH",
            index.name,
            "*=>[KNN 5 @embedding $query_vector]",
            "PARAMS",
            "2",
            "query_vector",
            actual_embedding,
            "LIMIT",
            "0",
            "5",
        )
        assert (
            vector_results[0] >= 1
        ), "Should find results for vector similarity search"
