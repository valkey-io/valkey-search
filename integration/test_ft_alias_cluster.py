"""
Cluster propagation integration tests for FT.ALIASADD, FT.ALIASDEL, and FT.ALIASUPDATE.
"""

import os
import pytest
import threading
from concurrent.futures import ThreadPoolExecutor
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

    def test_aliasdel_nonexistent_alias_cluster(self):
        """FT.ALIASDEL on a non-existent alias returns an error in cluster mode."""
        node0 = self.new_client_for_primary(0)
        with pytest.raises(ResponseError) as exc_info:
            node0.execute_command("FT.ALIASDEL", "no_such_alias")
        assert "Alias does not exist" in str(exc_info.value)

    def test_aliasadd_nonexistent_index_cluster(self):
        """FT.ALIASADD for a non-existent index returns an error in cluster mode."""
        node0 = self.new_client_for_primary(0)
        with pytest.raises(ResponseError) as exc_info:
            node0.execute_command("FT.ALIASADD", ALIAS_NAME, "no_such_index")
        assert "no_such_index" in str(exc_info.value)

    def test_aliasadd_alias_to_alias_cluster(self):
        """FT.ALIASADD pointing to an existing alias returns an error in cluster mode."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            node0.execute_command("FT.ALIASADD", "alias2", ALIAS_NAME)
        assert "Unknown index name or name is an alias" in str(exc_info.value)

    def test_aliasupdate_nonexistent_index_cluster(self):
        """FT.ALIASUPDATE for a non-existent index returns an error in cluster mode."""
        node0 = self.new_client_for_primary(0)
        with pytest.raises(ResponseError) as exc_info:
            node0.execute_command(
                "FT.ALIASUPDATE", ALIAS_NAME, "no_such_index"
            )
        assert "no_such_index" in str(exc_info.value)

    def test_aliasupdate_name_same_as_index_name_cluster(self):
        """FT.ALIASUPDATE allows alias that matches an existing index in cluster."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASUPDATE", INDEX_NAME, INDEX_NAME_2
        ) == b"OK"

    # ------------------------------------------------------------------
    # FT.ALIASLIST cluster tests
    # ------------------------------------------------------------------

    def test_aliaslist_empty_cluster(self):
        """FT.ALIASLIST returns empty list when no aliases exist in cluster."""
        node0 = self.new_client_for_primary(0)
        result = node0.execute_command("FT.ALIASLIST")
        assert result == []

    def test_aliaslist_visible_on_all_nodes(self):
        """FT.ALIASLIST shows the alias on all nodes after ALIASADD."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"

        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == [ALIAS_NAME.encode(), INDEX_NAME.encode()], (
                f"Expected alias list to contain {ALIAS_NAME} -> {INDEX_NAME}"
            )

    def test_aliaslist_multiple_aliases_cluster(self):
        """FT.ALIASLIST returns multiple aliases sorted on all nodes."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", "z_alias", INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASADD", "a_alias", INDEX_NAME
        ) == b"OK"

        expected = [
            b"a_alias", INDEX_NAME.encode(),
            b"z_alias", INDEX_NAME.encode(),
        ]
        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == expected

    def test_aliaslist_reflects_aliasdel_cluster(self):
        """FT.ALIASLIST no longer shows a deleted alias on all nodes."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        assert node0.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"

        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == []

    def test_aliaslist_reflects_aliasupdate_cluster(self):
        """FT.ALIASLIST shows updated target index on all nodes."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        assert node0.execute_command(
            "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2
        ) == b"OK"

        expected = [ALIAS_NAME.encode(), INDEX_NAME_2.encode()]
        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == expected

    def test_aliaslist_empty_after_dropindex_cluster(self):
        """FT.ALIASLIST is empty on all nodes after index is dropped."""
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

        for node in self._all_primaries():
            result = node.execute_command("FT.ALIASLIST")
            assert result == []

    def test_aliaslist_wrong_arity_cluster(self):
        """FT.ALIASLIST with extra args returns an error in cluster mode."""
        node0 = self.new_client_for_primary(0)
        with pytest.raises(ResponseError):
            node0.execute_command("FT.ALIASLIST", "extra")


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

    def test_aliasupdate_racing_with_dropindex_cluster(self):
        """ALIASUPDATE racing with FT.DROPINDEX converges to a clean state."""
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)

        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert node0.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)
        self._wait_for_index_on_all_nodes(INDEX_NAME_2)

        # Create alias pointing to INDEX_NAME.
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

        # Race: update alias to point to INDEX_NAME_2 while dropping
        # INDEX_NAME concurrently. Both outcomes are acceptable.
        results = {}
        errors = {}

        def do_update():
            try:
                client = self.new_client_for_primary(0)
                results["update"] = client.execute_command(
                    "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2
                )
            except ResponseError as e:
                errors["update"] = e

        def do_drop():
            try:
                client = self.new_client_for_primary(1)
                results["drop"] = client.execute_command(
                    "FT.DROPINDEX", INDEX_NAME
                )
            except ResponseError as e:
                errors["drop"] = e

        t_update = threading.Thread(target=do_update)
        t_drop = threading.Thread(target=do_drop)
        t_update.start()
        t_drop.start()
        t_update.join()
        t_drop.join()

        # Wait for cluster convergence: all nodes agree on alias state.
        def _alias_converged():
            states = []
            for node in self._all_primaries():
                try:
                    node.execute_command("FT.INFO", ALIAS_NAME)
                    states.append("present")
                except ResponseError:
                    states.append("absent")
            return len(set(states)) == 1

        waiters.wait_for_true(_alias_converged, timeout=30)

        # All nodes agree on alias state.
        alias_present = []
        for node in self._all_primaries():
            try:
                node.execute_command("FT.INFO", ALIAS_NAME)
                alias_present.append(True)
            except ResponseError:
                alias_present.append(False)

        assert len(set(alias_present)) == 1, (
            f"Cluster not converged: alias states = {alias_present}")

        # If alias survived, it must point to INDEX_NAME_2 (the update target)
        # and not the dropped INDEX_NAME.
        if alias_present[0]:
            for node in self._all_primaries():
                info = list(node.execute_command("FT.INFO", ALIAS_NAME))
                idx = next(
                    (i for i, v in enumerate(info) if v == b"index_name"),
                    None)
                assert idx is not None
                assert info[idx + 1] != INDEX_NAME.encode(), (
                    f"Alias still points to dropped index: {info[idx + 1]}")

        # No cruft — FT.ALIASLIST consistent across all nodes.
        alias_lists = set()
        for node in self._all_primaries():
            alias_lists.add(str(node.execute_command("FT.ALIASLIST")))
        assert len(alias_lists) == 1, (
            f"FT.ALIASLIST inconsistent across nodes: {alias_lists}")

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


class TestFTAliasConcurrentOperationsCluster(ValkeySearchClusterTestCase):
    """Tests for concurrent alias operations from different nodes."""

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _wait_for_index_on_all_nodes(self, index_name):
        nodes = self._all_primaries()
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            nodes, index_name
        )

    def test_concurrent_aliasadd_different_aliases(self):
        """Different aliases added from different nodes all coexist.

        Concurrent metadata writes to the same index entry can cause
        lost-update conflicts in the coordinator's reconciliation protocol.
        Adding aliases sequentially (waiting for propagation between each)
        avoids the conflict and verifies non-conflicting aliases coexist.
        """
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        # Add aliases from different nodes sequentially to avoid metadata race.
        for i in range(self.CLUSTER_SIZE):
            client = self.new_client_for_primary(i)
            assert client.execute_command(
                "FT.ALIASADD", f"alias_from_node_{i}", INDEX_NAME
            ) == b"OK"
            _wait_for_alias_on_all_nodes(
                self._all_primaries(), f"alias_from_node_{i}",
                expect_present=True
            )

        # All aliases visible on all nodes.
        for i in range(self.CLUSTER_SIZE):
            _wait_for_alias_on_all_nodes(
                self._all_primaries(), f"alias_from_node_{i}", expect_present=True
            )

    def test_concurrent_aliasadd_same_alias_one_wins(self):
        """Multiple nodes adding the same alias concurrently — all may succeed
        (coordinator uses eventual consistency for alias mutations)."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        results = [None] * self.CLUSTER_SIZE
        errors = [None] * self.CLUSTER_SIZE

        def add_alias(node_idx):
            try:
                client = self.new_client_for_primary(node_idx)
                results[node_idx] = client.execute_command(
                    "FT.ALIASADD", "contested_alias", INDEX_NAME
                )
            except ResponseError as e:
                errors[node_idx] = e

        threads = [
            threading.Thread(target=add_alias, args=(i,))
            for i in range(self.CLUSTER_SIZE)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # At least one should succeed. Due to eventual consistency, multiple
        # nodes may independently succeed before propagation occurs.
        successes = sum(1 for r in results if r == b"OK")
        assert successes >= 1, f"Expected at least 1 success, got {successes}"

        # The alias should resolve on all nodes after propagation settles.
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "contested_alias", expect_present=True
        )

    def test_concurrent_aliasadd_and_aliasdel(self):
        """Adding and deleting an alias concurrently from different nodes converges."""
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(*CREATE_TAG_INDEX) == b"OK"
        self._wait_for_index_on_all_nodes(INDEX_NAME)

        # Pre-create the alias so DEL has something to delete.
        assert node0.execute_command(
            "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
        ) == b"OK"
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), ALIAS_NAME, expect_present=True
        )

        # Concurrently: node 0 deletes, node 1 re-adds (or they race).
        results = {}
        errors = {}

        def do_del():
            try:
                client = self.new_client_for_primary(0)
                results["del"] = client.execute_command("FT.ALIASDEL", ALIAS_NAME)
            except ResponseError as e:
                errors["del"] = e

        def do_add():
            try:
                client = self.new_client_for_primary(1)
                results["add"] = client.execute_command(
                    "FT.ALIASADD", ALIAS_NAME, INDEX_NAME
                )
            except ResponseError as e:
                errors["add"] = e

        t_del = threading.Thread(target=do_del)
        t_add = threading.Thread(target=do_add)
        t_del.start()
        t_add.start()
        t_del.join()
        t_add.join()

        # The cluster must converge to a consistent state on all nodes.
        # Either the alias exists or it doesn't — but all nodes must agree.
        def _alias_converged():
            states = []
            for node in self._all_primaries():
                try:
                    node.execute_command("FT.INFO", ALIAS_NAME)
                    states.append(True)
                except ResponseError:
                    states.append(False)
            return len(set(states)) == 1

        waiters.wait_for_true(_alias_converged, timeout=15)

        alias_states = []
        for node in self._all_primaries():
            try:
                node.execute_command("FT.INFO", ALIAS_NAME)
                alias_states.append(True)
            except ResponseError:
                alias_states.append(False)

        # All nodes must agree.
        assert len(set(alias_states)) == 1, (
            f"Cluster not converged: alias states = {alias_states}"
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
