"""Integration tests for FT.SEARCH INFIELDS in cluster mode.

Validates that INFIELDS survives coordinator fan-out (proto field 24 /
search_converter InfieldsFromGRPC) and produces the same field-scoping
semantics as standalone across multiple shards.
"""

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper


class TestFTSearchInfieldsCluster(ValkeySearchClusterTestCase):

    def _setup_two_field_index(self, client: ValkeyCluster, num_docs: int = 60):
        """HASH index with two TEXT fields, data spread across shards."""
        client.execute_command(
            "FT.CREATE", "idx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "body", "TEXT",
        )
        for i in range(num_docs):
            # Every doc has "apple" in title XOR body, split evenly.
            if i % 2 == 0:
                client.execute_command("HSET", f"doc:{i}", "title", "apple",
                                       "body", "unrelated")
            else:
                client.execute_command("HSET", f"doc:{i}", "title", "unrelated",
                                       "body", "apple")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

    def test_infields_scopes_across_shards(self):
        """INFIELDS restricting to `title` must exclude the body-only matches
        on every shard, not just the coordinator's local shard."""
        client: ValkeyCluster = self.new_cluster_client()
        num_docs = 60
        self._setup_two_field_index(client, num_docs=num_docs)

        # Unscoped: all docs contain "apple" somewhere.
        result = client.execute_command(
            "FT.SEARCH", "idx", "apple", "LIMIT", "0", str(num_docs),
            "DIALECT", "2")
        assert result[0] == num_docs

        # INFIELDS title: only the even-indexed docs (title="apple") match,
        # regardless of which shard they landed on.
        result = client.execute_command(
            "FT.SEARCH", "idx", "apple",
            "INFIELDS", "1", "title",
            "NOCONTENT", "LIMIT", "0", str(num_docs),
            "DIALECT", "2")
        assert result[0] == num_docs // 2
        returned = {k.decode() for k in result[1:]}
        assert returned == {f"doc:{i}" for i in range(0, num_docs, 2)}

    def test_infields_zero_count_is_noop_across_shards(self):
        """INFIELDS 0 must behave as a no-op fanned out to every shard."""
        client: ValkeyCluster = self.new_cluster_client()
        num_docs = 40
        self._setup_two_field_index(client, num_docs=num_docs)

        unscoped = client.execute_command(
            "FT.SEARCH", "idx", "apple", "LIMIT", "0", str(num_docs),
            "DIALECT", "2")
        zero_count = client.execute_command(
            "FT.SEARCH", "idx", "apple", "INFIELDS", "0",
            "LIMIT", "0", str(num_docs), "DIALECT", "2")
        assert zero_count[0] == unscoped[0] == num_docs

    def test_infields_nonexistent_field_errors_on_every_shard(self):
        """A validation error from INFIELDS must surface to the client even
        though the query fans out to multiple shards."""
        client: ValkeyCluster = self.new_cluster_client()
        self._setup_two_field_index(client, num_docs=20)

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "apple",
                "INFIELDS", "1", "nonexistent_field",
                "DIALECT", "2")

    def test_infields_with_sortby_across_shards(self):
        """INFIELDS + SORTBY in cluster mode: ordering and the narrowed match
        set are both correct once results are merged from all shards."""
        client: ValkeyCluster = self.new_cluster_client()
        client.execute_command(
            "FT.CREATE", "sidx",
            "ON", "HASH",
            "PREFIX", "1", "sdoc:",
            "SCHEMA",
            "title", "TEXT",
            "body", "TEXT",
            "rank", "NUMERIC", "SORTABLE",
        )
        num_docs = 30
        for i in range(num_docs):
            if i % 2 == 0:
                client.execute_command("HSET", f"sdoc:{i}", "title", "apple",
                                       "body", "unrelated", "rank", str(i))
            else:
                client.execute_command("HSET", f"sdoc:{i}", "title", "unrelated",
                                       "body", "apple", "rank", str(i))
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "sidx")

        result = client.execute_command(
            "FT.SEARCH", "sidx", "apple",
            "INFIELDS", "1", "title",
            "SORTBY", "rank", "ASC",
            "NOCONTENT", "LIMIT", "0", str(num_docs),
            "DIALECT", "2")

        expected_count = num_docs // 2
        assert result[0] == expected_count
        keys = [k.decode() for k in result[1:]]
        expected_order = [f"sdoc:{i}" for i in range(0, num_docs, 2)]
        assert keys == expected_order
