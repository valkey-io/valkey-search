"""
Cluster propagation integration tests for FT.ALIASADD, FT.ALIASDEL, and FT.ALIASUPDATE.
"""

import os
import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchClusterTestCase, ValkeySearchClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper

INDEX_NAME = "alias_cluster_idx"
INDEX_NAME_2 = "alias_cluster_idx2"
ALIAS_NAME = "cluster_alias"

CREATE_TAG_INDEX = [
    "FT.CREATE", INDEX_NAME,
    "ON", "HASH",
    "PREFIX", "1", "doc:",
    "SCHEMA", "category", "TAG",
]

CREATE_TAG_INDEX_2 = [
    "FT.CREATE", INDEX_NAME_2,
    "ON", "HASH",
    "PREFIX", "1", "doc2:",
    "SCHEMA", "category", "TAG",
]


def _wait_for_alias_on_all_nodes(nodes, alias_name, expect_present=True):
    """
    Poll until alias_name is (or is not) resolvable via FT.INFO on every node.
    """
    def check():
        for node in nodes:
            try:
                node.execute_command("FT.INFO", alias_name)
                if not expect_present:
                    return False
            except ResponseError:
                if expect_present:
                    return False
        return True

    waiters.wait_for_true(check)


def _wait_for_aliaslist_on_all_nodes(nodes, expected):
    """Poll until FT.ALIASLIST equals `expected` on every node."""
    def check():
        return all(node.execute_command("FT.ALIASLIST") == expected
                   for node in nodes)
    waiters.wait_for_true(check, timeout=30)


def _add_aliases_raising_version(node, index_name, count):
    """Add `count` padding aliases to index_name, raising its metadata version."""
    for i in range(count):
        assert node.execute_command(
            "FT.ALIASADD", f"pad{i}", index_name) == b"OK"


class TestFTAliasClusterPropagation(ValkeySearchClusterTestCase):
    """
    Verify that alias mutations on one primary propagate to all cluster nodes.
    """

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def _verify_alias_on_all_nodes(self, alias_name, expected_index):
        """Assert the alias resolves to expected_index on every primary."""
        for node in self._all_primaries():
            info = list(node.execute_command("FT.INFO", alias_name))
            idx = next(
                (i for i, v in enumerate(info) if v == b"index_name"),
                None,
            )
            assert idx is not None, "FT.INFO missing index_name field"
            assert info[idx + 1] == expected_index.encode(), (
                f"Expected alias to resolve to {expected_index}, "
                f"got {info[idx + 1]}"
            )

    def _verify_alias_absent_on_all_nodes(self, alias_name):
        """Assert the alias is not resolvable on any primary."""
        for node in self._all_primaries():
            with pytest.raises(ResponseError):
                node.execute_command("FT.INFO", alias_name)

    # ------------------------------------------------------------------
    # ALIASADD propagation
    # ------------------------------------------------------------------

    def test_aliasadd_propagates_to_all_nodes(self):
        """ALIASADD on node 0 must be visible via FT.INFO on every node."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        # Command waited for consistency — alias is visible immediately.
        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME)

    def test_aliasadd_on_non_zero_node_propagates(self):
        """ALIASADD issued on node 1 must propagate to all other nodes."""
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node1.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME)

    def test_aliasadd_duplicate_rejected_cluster_wide(self):
        """A duplicate ALIASADD is rejected even after the first has propagated."""
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        # Second add pointing to a different index must fail.
        with pytest.raises(ResponseError) as exc_info:
            node1.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME_2)
        assert "Alias already exists" in str(exc_info.value)

    # ------------------------------------------------------------------
    # ALIASDEL propagation
    # ------------------------------------------------------------------

    def test_aliasdel_propagates_to_all_nodes(self):
        """ALIASDEL on node 0 must remove the alias from every node."""
        node0 = self.new_client_for_primary(0)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        assert node0.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"

        # Command waited for consistency — alias is gone immediately.
        self._verify_alias_absent_on_all_nodes(ALIAS_NAME)

    def test_aliasdel_on_non_zero_node_propagates(self):
        """ALIASDEL issued on node 1 must remove the alias from all nodes."""
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        assert node1.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"

        self._verify_alias_absent_on_all_nodes(ALIAS_NAME)

    # ------------------------------------------------------------------
    # ALIASUPDATE propagation
    # ------------------------------------------------------------------

    def test_aliasupdate_create_propagates(self):
        """ALIASUPDATE (upsert) on node 0 propagates to all nodes."""
        node0 = self.new_client_for_primary(0)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME)

    def test_aliasupdate_to_lower_version_index_propagates(self):
        """FT.ALIASUPDATE to an index whose metadata version is lower than
        the current owner's moves the alias on every node."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        _add_aliases_raising_version(node0, INDEX_NAME, 2)
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert node0.execute_command(
            "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2) == b"OK"

        _wait_for_aliaslist_on_all_nodes(
            self._all_primaries(),
            [ALIAS_NAME.encode(), INDEX_NAME_2.encode(),
             b"pad0", INDEX_NAME.encode(),
             b"pad1", INDEX_NAME.encode()])
        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME_2)

    def test_aliasupdate_back_and_forth_follows_last_target(self):
        """Repeated FT.ALIASUPDATE leaves the alias on the last target on
        every node, regardless of which index has the higher version."""
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        _add_aliases_raising_version(node0, INDEX_NAME_2, 3)
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        for node, target in ((node1, INDEX_NAME_2), (node0, INDEX_NAME),
                             (node1, INDEX_NAME_2), (node0, INDEX_NAME)):
            assert node.execute_command(
                "FT.ALIASUPDATE", ALIAS_NAME, target) == b"OK"

        _wait_for_aliaslist_on_all_nodes(
            self._all_primaries(),
            [ALIAS_NAME.encode(), INDEX_NAME.encode(),
             b"pad0", INDEX_NAME_2.encode(),
             b"pad1", INDEX_NAME_2.encode(),
             b"pad2", INDEX_NAME_2.encode()])

    def test_aliasupdate_reassign_propagates(self):
        """ALIASUPDATE reassignment propagates the new target to all nodes."""
        node0 = self.new_client_for_primary(0)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2
        ) == b"OK"
        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME_2)

    # ------------------------------------------------------------------
    # DROPINDEX alias cleanup propagation
    # ------------------------------------------------------------------

    def test_dropindex_tombstones_alias_on_all_nodes(self):
        """DROPINDEX tombstones aliases so they disappear cluster-wide."""
        node0 = self.new_client_for_primary(0)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        assert node0.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        # DROPINDEX waits for index consistency, not alias cleanup — poll.
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=False
        )

    def test_dropindex_multiple_aliases_all_tombstoned(self):
        """All aliases pointing to a dropped index are tombstoned cluster-wide."""
        node0 = self.new_client_for_primary(0)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", "alias_a", INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASADD", "alias_b", INDEX_NAME
        ) == b"OK"

        assert node0.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "alias_a", expect_present=False
        )
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "alias_b", expect_present=False
        )

    # ------------------------------------------------------------------
    # Search via alias works cluster-wide
    # ------------------------------------------------------------------

    def test_search_via_alias_works_on_all_nodes(self):
        """FT.SEARCH using an alias returns results on every node."""
        node0 = self.new_client_for_primary(0)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        cluster_client = self.new_cluster_client()
        cluster_client.execute_command("HSET", "doc:1", "category", "books")
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        # Poll until alias search returns results on all primaries.
        def _alias_search_ready():
            for node in self._all_primaries():
                try:
                    result = node.execute_command(
                        "FT.SEARCH", ALIAS_NAME, "@category:{books}"
                    )
                except ResponseError:
                    # Alias may not yet be resolvable on this node; keep
                    # polling. Any other exception should surface.
                    return False
                if result[0] < 1:
                    return False
            return True

        from valkeytestframework.util import waiters as _waiters
        _waiters.wait_for_true(_alias_search_ready, timeout=15)

        for node in self._all_primaries():
            result = node.execute_command(
                "FT.SEARCH", ALIAS_NAME, "@category:{books}"
            )
            assert result[0] >= 1, (
                f"Expected at least 1 result via alias on node, got {result[0]}"
            )

    def test_aliasupdate_on_non_zero_node_propagates(self):
        """ALIASUPDATE issued on node 1 propagates the new target to all nodes."""
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME)

        # Issue ALIASUPDATE from node 1.
        assert node1.execute_command(
            "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2
        ) == b"OK"
        self._verify_alias_on_all_nodes(ALIAS_NAME, INDEX_NAME_2)


class TestFTAliasSearchCluster(ValkeySearchClusterTestCase):
    """Cluster tests for FT.SEARCH and FT.AGGREGATE via alias."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_aggregate_via_alias_cluster(self):
        """FT.AGGREGATE accepts an alias name and returns results on all nodes."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        # Use a cluster client for data writes so keys route to the correct slot owner.
        cluster_client = self.new_cluster_client()
        cluster_client.execute_command("HSET", "doc:1", "category", "books")
        cluster_client.execute_command("HSET", "doc:2", "category", "electronics")
        cluster_client.execute_command("HSET", "doc:3", "category", "books")

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        def _aggregate_ready():
            for node in self._all_primaries():
                try:
                    result = node.execute_command(
                        "FT.AGGREGATE", ALIAS_NAME, "@category:{books}",
                        "GROUPBY", "1", "@category",
                        "REDUCE", "COUNT", "0", "AS", "count",
                    )
                except ResponseError:
                    return False
                if result[0] < 1:
                    return False
            return True

        waiters.wait_for_true(_aggregate_ready, timeout=15)

        for node in self._all_primaries():
            result = node.execute_command(
                "FT.AGGREGATE", ALIAS_NAME, "@category:{books}",
                "GROUPBY", "1", "@category",
                "REDUCE", "COUNT", "0", "AS", "count",
            )
            assert result[0] >= 1


class TestFTAliasDropIndexCluster(ValkeySearchClusterTestCase):
    """Cluster tests for FT.DROPINDEX interactions with aliases."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_dropindex_via_alias_name_cluster(self):
        """FT.DROPINDEX called with an alias name drops the real index cluster-wide."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        # Drop using the alias name.
        assert node0.execute_command("FT.DROPINDEX", ALIAS_NAME) == b"OK"

        # Both the index and alias should be gone on all nodes.
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=False
        )
        for node in self._all_primaries():
            with pytest.raises(ResponseError):
                node.execute_command("FT.INFO", INDEX_NAME)

    def test_dropindex_leaves_other_index_aliases_intact_cluster(self):
        """Dropping one index only removes aliases pointing to it, not others."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", "alias_idx1", INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASADD", "alias_idx2", INDEX_NAME_2
        ) == b"OK"

        assert node0.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        # alias_idx1 should be gone.
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "alias_idx1", expect_present=False
        )
        # alias_idx2 must still resolve on all nodes.
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "alias_idx2", expect_present=True
        )

    def test_alias_readd_after_drop_and_recreate_cluster(self):
        """After drop+recreate, an alias can be re-added in cluster mode."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        assert node0.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=False
        )

        # Recreate and re-add alias.
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )


class TestFTAliasDelEdgeCasesCluster(ValkeySearchClusterTestCase):
    """Cluster edge-case tests for FT.ALIASDEL."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_aliasdel_on_index_name_returns_error_cluster(self):
        """FT.ALIASDEL called with a real index name returns an error in cluster."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        with pytest.raises(ResponseError) as exc_info:
            node0.execute_command("FT.ALIASDEL", INDEX_NAME)
        assert "Alias does not exist" in str(exc_info.value)

    def test_aliasdel_after_drop_index_returns_error_cluster(self):
        """FT.ALIASDEL on an alias whose index was dropped returns an error."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        assert node0.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=False
        )

        with pytest.raises(ResponseError) as exc_info:
            node0.execute_command("FT.ALIASDEL", ALIAS_NAME)
        assert "Alias does not exist" in str(exc_info.value)


class TestFTAliasNameCollisionCluster(ValkeySearchClusterTestCase):
    """Cluster tests for alias name matching an existing index name."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_aliasadd_name_same_as_existing_index_cluster(self):
        """FT.ALIASADD allows alias name matching a real index in cluster."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command("FT.ALIASADD", INDEX_NAME, INDEX_NAME_2) == b"OK"

    def test_aliasupdate_name_same_as_existing_index_allowed_cluster(self):
        """FT.ALIASUPDATE allows alias name matching a real index in cluster."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASUPDATE", INDEX_NAME, INDEX_NAME_2
        ) == b"OK"


class TestFTListDoesNotExposeAliasesCluster(ValkeySearchClusterTestCase):
    """FT._LIST must not include alias names in cluster mode."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_alias_not_in_ft_list_cluster(self):
        """FT._LIST does not include alias names on any node."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        for node in self._all_primaries():
            listed = node.execute_command("FT._LIST")
            assert INDEX_NAME.encode() in listed
            assert ALIAS_NAME.encode() not in listed

    def test_ft_list_unchanged_after_aliasdel_cluster(self):
        """FT._LIST is unchanged after alias deletion on all nodes."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        assert node0.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"

        for node in self._all_primaries():
            listed = node.execute_command("FT._LIST")
            assert INDEX_NAME.encode() in listed
            assert ALIAS_NAME.encode() not in listed


class TestFTAliasListMultiIndexCluster(ValkeySearchClusterTestCase):
    """Cluster tests for FT.ALIASLIST across multiple indexes."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_aliaslist_multiple_indexes_cluster(self):
        """FT.ALIASLIST shows aliases across multiple indexes, sorted by alias."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", "beta", INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASADD", "alpha", INDEX_NAME_2
        ) == b"OK"

        expected = [
            b"alpha", INDEX_NAME_2.encode(),
            b"beta", INDEX_NAME.encode(),
        ]
        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == expected

    def test_aliaslist_dropindex_only_removes_its_aliases_cluster(self):
        """Dropping one index only removes its aliases from ALIASLIST on all nodes."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", "alias_idx1", INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASADD", "alias_idx2", INDEX_NAME_2
        ) == b"OK"

        assert node0.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "alias_idx1", expect_present=False
        )

        expected = [b"alias_idx2", INDEX_NAME_2.encode()]
        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == expected


class TestFTAliasFlushAllCluster(ValkeySearchClusterTestCase):
    """Tests that FLUSHALL preserves aliases in coordinator mode.

    In cluster/coordinator mode, indexes are cluster-level constructs that
    survive FLUSHALL (they are recreated from coordinator metadata). Aliases
    follow the same semantics — they are reinstalled from MetadataManager
    after the local flush completes.
    """

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_flushall_preserves_aliases_cluster_wide(self):
        """FLUSHALL preserves indexes and aliases in coordinator mode."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        # Verify alias exists on all nodes before flush.
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

        # FLUSHALL from cluster client.
        cluster_client = self.new_cluster_client()
        cluster_client.execute_command("FLUSHALL")

        # In coordinator mode, indexes and aliases survive FLUSHALL.
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

    def test_flushall_preserves_multiple_aliases(self):
        """FLUSHALL preserves all aliases across multiple indexes."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", "alias_a", INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASADD", "alias_b", INDEX_NAME_2
        ) == b"OK"

        cluster_client = self.new_cluster_client()
        cluster_client.execute_command("FLUSHALL")

        # Aliases survive FLUSHALL in coordinator mode.
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "alias_a", expect_present=True
        )
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "alias_b", expect_present=True
        )

        expected = [
            b"alias_a", INDEX_NAME.encode(),
            b"alias_b", INDEX_NAME_2.encode(),
        ]
        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == expected

    def test_alias_operations_work_after_flushall(self):
        """After FLUSHALL, alias operations (add/del/update) still work."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        cluster_client = self.new_cluster_client()
        cluster_client.execute_command("FLUSHALL")

        # Index and alias survive.
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

        # Can delete the alias after FLUSHALL.
        assert node0.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=False
        )

        # Can re-add alias.
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )


class TestFTAliasRDBPersistenceCluster(ValkeySearchClusterTestCaseDebugMode):
    """Tests that aliases survive save/restart in cluster mode."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def _save_and_restart_all(self):
        """Save RDB on all nodes, then restart all of them."""
        for rg in self.replication_groups:
            rg.primary.client.execute_command("SAVE")

        os.environ["SKIPLOGCLEAN"] = "1"
        try:
            for rg in self.replication_groups:
                rg.primary.server.restart(remove_rdb=False)

            # Wait for all nodes to be reachable.
            for rg in self.replication_groups:
                waiters.wait_for_true(
                    lambda rg=rg: _node_responsive(rg.primary),
                    timeout=30,
                )
        finally:
            os.environ.pop("SKIPLOGCLEAN", None)

    def _save_and_restart_node(self, node_idx):
        """Save RDB and restart a single node."""
        rg = self.replication_groups[node_idx]
        rg.primary.client.execute_command("SAVE")

        os.environ["SKIPLOGCLEAN"] = "1"
        try:
            rg.primary.server.restart(remove_rdb=False)
            waiters.wait_for_true(
                lambda: _node_responsive(rg.primary),
                timeout=30,
            )
        finally:
            os.environ.pop("SKIPLOGCLEAN", None)

    def test_alias_survives_cluster_restart(self):
        """Aliases persist across a full cluster save/restart cycle."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

        self._save_and_restart_all()

        # After restart, wait for index to reload.
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        # Alias should still resolve on all nodes.
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

    def test_aliaslist_survives_cluster_restart(self):
        """FT.ALIASLIST returns correct results after cluster restart."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", "alpha", INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASADD", "beta", INDEX_NAME_2
        ) == b"OK"

        self._save_and_restart_all()
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        expected = [
            b"alpha", INDEX_NAME.encode(),
            b"beta", INDEX_NAME_2.encode(),
        ]
        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == expected

    def test_deleted_alias_not_restored_after_restart(self):
        """An alias deleted before SAVE does not reappear after cluster restart."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        assert node0.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=False
        )

        self._save_and_restart_all()
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=False
        )

    def test_alias_survives_single_node_restart(self):
        """A single node restart re-loads alias from RDB; alias still visible."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

        # Restart only node 1.
        self._save_and_restart_node(1)
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        # Alias should still resolve on the restarted node.
        restarted_client = self.new_client_for_primary(1)
        info = restarted_client.execute_command("FT.INFO", ALIAS_NAME)
        info_list = list(info)
        idx = next(
            (i for i, v in enumerate(info_list) if v == b"index_name"), None
        )
        assert idx is not None
        assert info_list[idx + 1] == INDEX_NAME.encode()

    def test_search_via_alias_after_cluster_restart(self):
        """FT.SEARCH via alias returns correct results after cluster restart."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        cluster_client = self.new_cluster_client()
        cluster_client.execute_command("HSET", "doc:1", "category", "books")
        cluster_client.execute_command("HSET", "doc:2", "category", "electronics")

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        self._save_and_restart_all()
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        # Search via alias should work on all nodes after restart.
        def _search_ready():
            for node in self._all_primaries():
                try:
                    result = node.execute_command(
                        "FT.SEARCH", ALIAS_NAME, "@category:{books}"
                    )
                    if result[0] < 1:
                        return False
                except ResponseError:
                    return False
            return True

        waiters.wait_for_true(_search_ready, timeout=15)

    def test_aliasupdate_to_lower_version_index_survives_restart(self):
        """An alias moved to an index with a lower metadata version stays put
        on every node, before and after a full cluster restart."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        _add_aliases_raising_version(node0, INDEX_NAME, 2)
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert node0.execute_command(
            "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2) == b"OK"

        expected = [ALIAS_NAME.encode(), INDEX_NAME_2.encode(),
                    b"pad0", INDEX_NAME.encode(),
                    b"pad1", INDEX_NAME.encode()]
        _wait_for_aliaslist_on_all_nodes(self._all_primaries(), expected)

        self._save_and_restart_all()
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)
        _wait_for_aliaslist_on_all_nodes(self._all_primaries(), expected)


def _node_responsive(node):
    """Return True if a node responds to PING."""
    try:
        node.client.ping()
        return True
    except Exception:
        return False


class TestFTAliasFingerprintStability(ValkeySearchClusterTestCase):
    """Alias operations must not trigger index rebuild or break search."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_search_stable_after_aliasadd(self):
        """Search by real name must return full results immediately after ALIASADD."""
        node0 = self.new_client_for_primary(0)
        cluster_client = self.new_cluster_client()

        assert node0.execute_command(
            "FT.CREATE", "fp_idx", "ON", "HASH", "PREFIX", "1", "fp:",
            "SCHEMA", "price", "NUMERIC", "category", "TAG",
        ) == b"OK"
        self._wait_for_index("fp_idx")

        for i in range(1, 6):
            cluster_client.execute_command(
                "HSET", f"fp:{i}", "price", str(i * 10), "category", f"c{i}"
            )
        waiters.wait_for_true(
            lambda: all(
                IndexingTestHelper.is_indexing_complete_on_node(n, "fp_idx")
                for n in self._all_primaries()
            ),
            timeout=15,
        )

        result_before = cluster_client.execute_command(
            "FT.SEARCH", "fp_idx", "@price:[0 +inf]"
        )
        assert result_before[0] == 5

        assert node0.execute_command("FT.ALIASADD", "fp_alias", "fp_idx") == b"OK"

        result_real = cluster_client.execute_command(
            "FT.SEARCH", "fp_idx", "@price:[0 +inf]"
        )
        assert result_real[0] == 5, (
            f"Search by real name broken after ALIASADD: got {result_real[0]}"
        )

        result_alias = cluster_client.execute_command(
            "FT.SEARCH", "fp_alias", "@price:[0 +inf]"
        )
        assert result_alias[0] == 5, (
            f"Search by alias broken after ALIASADD: got {result_alias[0]}"
        )


class TestFTAliasHashtagValidationCluster(ValkeySearchClusterTestCase):
    """A single-slot index only accepts aliases with its hashtag, so the
    alias routes to the same slot. The rule's cases are covered by
    HashtagAliasValidationTest; this checks it end to end in a cluster."""

    def test_alias_hashtag_must_match_single_slot_index(self):
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(
            "FT.CREATE", "idx{slot1}",
            "ON", "HASH",
            "PREFIX", "1", "doc:{slot1}",
            "SCHEMA", "category", "TAG",
        ) == b"OK"
        nodes = [self.new_client_for_primary(i)
                 for i in range(self.CLUSTER_SIZE)]
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, "idx{slot1}")

        for cmd in ("FT.ALIASADD", "FT.ALIASUPDATE"):
            for alias in ("alias{slot2}", "plain_alias"):
                with pytest.raises(ResponseError) as exc_info:
                    node0.execute_command(cmd, alias, "idx{slot1}")
                assert "hashtag does not match" in str(exc_info.value)

        assert node0.execute_command(
            "FT.ALIASADD", "alias{slot1}", "idx{slot1}") == b"OK"
        _wait_for_aliaslist_on_all_nodes(
            nodes, [b"alias{slot1}", b"idx{slot1}"])
