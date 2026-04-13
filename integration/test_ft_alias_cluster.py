"""
Cluster propagation integration tests for FT.ALIASADD, FT.ALIASDEL, and FT.ALIASUPDATE.
"""

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchClusterTestCase
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


def _wait_for_alias_on_all_nodes(nodes, alias_name, expect_present=True,
                                  timeout=10):
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

    waiters.wait_for_true(check, timeout=timeout)


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
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        # Second add on a different node must fail.
        with pytest.raises(ResponseError) as exc_info:
            node1.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME)
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

        node0.execute_command("HSET", "doc:1", "category", "books")
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

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

    def test_aliasdel_nonexistent_alias_cluster(self):
        """FT.ALIASDEL on a non-existent alias returns an error in cluster mode."""
        node0 = self.new_client_for_primary(0)
        with pytest.raises(ResponseError) as exc_info:
            node0.execute_command("FT.ALIASDEL", "no_such_alias")
        assert "Alias does not exist" in str(exc_info.value)
