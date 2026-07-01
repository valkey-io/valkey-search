from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper
from ft_info_parser import FTInfoParser
import time


class TestFTCreateFilter(ValkeySearchTestCaseBase):
    """Integration tests for the FT.CREATE FILTER option."""

    def test_filter_selective_indexing(self):
        """Test that FILTER selectively indexes only matching documents and that
        documents failing the filter don't disrupt other indexed keys."""
        client: Valkey = self.server.get_new_client()

        # Create an index with a FILTER that only includes active documents.
        assert client.execute_command(
            "FT.CREATE", "filtered_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "FILTER", "@status=='active'",
            "SCHEMA", "status", "TAG", "price", "NUMERIC"
        ) == b"OK"

        # Verify filter appears in FT.INFO
        info = FTInfoParser(client.execute_command("FT.INFO", "filtered_idx"))
        assert info.index_definition.get("filter") == "@status=='active'"

        # Insert documents: some pass the filter, some don't.
        client.execute_command("HSET", "item:1", "status", "active", "price", "100")
        client.execute_command("HSET", "item:2", "status", "inactive", "price", "200")
        client.execute_command("HSET", "item:3", "status", "active", "price", "300")
        client.execute_command("HSET", "item:4", "status", "pending", "price", "400")
        client.execute_command("HSET", "item:5", "status", "active", "price", "500")

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "filtered_idx")

        # Only active documents (item:1, item:3, item:5) should be in the index.
        result = client.execute_command("FT.SEARCH", "filtered_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 indexed docs, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"item:1", b"item:3", b"item:5"}

        # Verify inactive/pending docs are not found via tag search either.
        result = client.execute_command("FT.SEARCH", "filtered_idx", "@status:{inactive}")
        assert result[0] == 0

        # Insert another failing document after initial indexing — should not
        # disrupt the existing indexed keys.
        client.execute_command("HSET", "item:6", "status", "inactive", "price", "600")

        # The three active documents should still be searchable and unaffected.
        result = client.execute_command("FT.SEARCH", "filtered_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 indexed docs after adding filtered-out key, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"item:1", b"item:3", b"item:5"}

    def test_filter_mutation_removes_from_index(self):
        """Test that mutating an already-indexed document so it no longer
        satisfies the filter effectively removes it from the index."""
        client: Valkey = self.server.get_new_client()

        # Create an index that only includes documents with price > 50.
        assert client.execute_command(
            "FT.CREATE", "price_idx",
            "ON", "HASH",
            "PREFIX", "1", "prod:",
            "FILTER", "@price > 50",
            "SCHEMA", "price", "NUMERIC", "name", "TAG"
        ) == b"OK"

        # Insert documents that all pass the filter.
        client.execute_command("HSET", "prod:1", "price", "100", "name", "alpha")
        client.execute_command("HSET", "prod:2", "price", "200", "name", "beta")
        client.execute_command("HSET", "prod:3", "price", "300", "name", "gamma")

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "price_idx")

        # All three should be indexed.
        result = client.execute_command("FT.SEARCH", "price_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 docs initially, got {result[0]}"

        # Mutate prod:2 so its price drops below the filter threshold.
        client.execute_command("HSET", "prod:2", "price", "10", "name", "beta")
        time.sleep(0.5)

        # prod:2 should now be removed from the index.
        result = client.execute_command("FT.SEARCH", "price_idx", "@price:[0 +inf]")
        assert result[0] == 2, f"Expected 2 docs after mutation, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"prod:1", b"prod:3"}

        # Verify prod:2 is specifically not found.
        result = client.execute_command("FT.SEARCH", "price_idx", "@name:{beta}")
        assert result[0] == 0, "prod:2 should not be in the index after failing the filter"

        # Mutate prod:2 back above the threshold — it should re-enter the index.
        client.execute_command("HSET", "prod:2", "price", "250", "name", "beta")
        time.sleep(0.5)

        result = client.execute_command("FT.SEARCH", "price_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 docs after re-qualifying mutation, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"prod:1", b"prod:2", b"prod:3"}

    def test_filter_with_string_comparison(self):
        """Test FILTER with string-based expressions."""
        client: Valkey = self.server.get_new_client()

        # Only index documents where category is 'electronics'.
        assert client.execute_command(
            "FT.CREATE", "cat_idx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "FILTER", "@category=='electronics'",
            "SCHEMA", "category", "TAG", "rating", "NUMERIC"
        ) == b"OK"

        client.execute_command("HSET", "doc:1", "category", "electronics", "rating", "5")
        client.execute_command("HSET", "doc:2", "category", "books", "rating", "4")
        client.execute_command("HSET", "doc:3", "category", "electronics", "rating", "3")
        client.execute_command("HSET", "doc:4", "category", "clothing", "rating", "5")

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "cat_idx")

        result = client.execute_command("FT.SEARCH", "cat_idx", "@rating:[0 +inf]")
        assert result[0] == 2
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"doc:1", b"doc:3"}

        # Mutate doc:1 to a non-electronics category — should be removed.
        client.execute_command("HSET", "doc:1", "category", "books", "rating", "5")
        time.sleep(0.5)

        result = client.execute_command("FT.SEARCH", "cat_idx", "@rating:[0 +inf]")
        assert result[0] == 1
        assert result[1] == b"doc:3"

    def test_filter_preexisting_data(self):
        """Test that FILTER is applied during backfill of pre-existing data."""
        client: Valkey = self.server.get_new_client()

        # Insert data BEFORE creating the index.
        client.execute_command("HSET", "pre:1", "status", "active", "val", "10")
        client.execute_command("HSET", "pre:2", "status", "disabled", "val", "20")
        client.execute_command("HSET", "pre:3", "status", "active", "val", "30")

        # Now create an index with a filter on that data.
        assert client.execute_command(
            "FT.CREATE", "backfill_idx",
            "ON", "HASH",
            "PREFIX", "1", "pre:",
            "FILTER", "@status=='active'",
            "SCHEMA", "status", "TAG", "val", "NUMERIC"
        ) == b"OK"

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "backfill_idx")

        # Only the active docs should have been indexed during backfill.
        result = client.execute_command("FT.SEARCH", "backfill_idx", "@val:[0 +inf]")
        assert result[0] == 2
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"pre:1", b"pre:3"}
