"""
Consistency and collision tests for FT.ALIASADD, FT.ALIASDEL and
FT.ALIASUPDATE, driven by pausepoints in the same way as
test_ft_create_consistency.py and test_ft_dropindex_consistency.py.
"""

from concurrent.futures import ThreadPoolExecutor

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from test_ft_alias_cluster import (
    CREATE_TAG_INDEX,
    CREATE_TAG_INDEX_2,
    INDEX_NAME,
    INDEX_NAME_2,
    _wait_for_alias_on_all_nodes,
    _wait_for_aliaslist_on_all_nodes,
)
from test_ft_dropindex_consistency import RETRY_MIN_THRESHOLD, run_pausepoint_reset
from utils import IndexingTestHelper

INDEX_NAME_3 = "alias_cluster_idx3"
CREATE_TAG_INDEX_3 = [INDEX_NAME_3 if c == INDEX_NAME_2 else c
                      for c in CREATE_TAG_INDEX_2]
# A caller whose consistency check never sees its alias on every node.
CONSISTENCY_TIMEOUT_ERR = "Unable to contact all cluster members"


def _execute(node, *args):
    """Run a command, returning its reply or the error it raised."""
    try:
        return node.execute_command(*args)
    except ResponseError as e:
        return e


def _resume_cluster_messages(node):
    """Unpause cluster messages on node and wait for it to reconcile them."""
    before = int(node.info("SEARCH")[
        "search_coordinator_metadata_reconciliation_completed_count"])
    node.execute_command(
        "FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage no")
    waiters.wait_for_true(
        lambda: int(node.info("SEARCH")[
            "search_coordinator_metadata_reconciliation_completed_count"])
        > before)


def _assert_timed_out(reply):
    assert isinstance(reply, ResponseError), reply
    assert CONSISTENCY_TIMEOUT_ERR in str(reply), reply


class AliasClusterTestCase(ValkeySearchClusterTestCaseDebugMode):

    def _all_primaries(self):
        return [self.new_client_for_primary(i)
                for i in range(self.CLUSTER_SIZE)]

    def _create_indexes(self, *create_commands):
        node0 = self.new_client_for_primary(0)
        for create in create_commands:
            assert node0.execute_command(*create) == b"OK"
            IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
                self._all_primaries(), create[1])


class TestFTAliasConsistency(AliasClusterTestCase):
    """
    Block the remote consistency check on node0 and metadata handling on
    node1, then release them in either order. Releasing metadata first
    (order 0) needs few retries; releasing the check first (order 1) needs
    many.
    """

    def _run_with_pausepoints(self, order, *command):
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        retry_before = int(
            node0.info("SEARCH")["search_info_fanout_retry_count"])
        assert node0.execute_command(
            "FT._DEBUG PAUSEPOINT SET fanout_remote_pausepoint") == b"OK"
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage yes"
        ) == b"OK"

        with ThreadPoolExecutor() as executor:
            future_cmd = executor.submit(_execute, node0, *command)
            future_reset = executor.submit(
                run_pausepoint_reset, order, node0, node1)
            waiters.wait_for_true(
                lambda: future_reset.done() and future_cmd.done())
            _, _, reset_exception = future_reset.result()

        assert reset_exception is None, reset_exception
        assert future_cmd.result() == b"OK", future_cmd.result()
        retries = int(
            node0.info("SEARCH")["search_info_fanout_retry_count"]
        ) - retry_before
        if order == 0:
            assert retries <= RETRY_MIN_THRESHOLD, retries
        else:
            assert retries > RETRY_MIN_THRESHOLD, retries

    @pytest.mark.parametrize("order", [0, 1])
    def test_aliasadd(self, order):
        self._create_indexes(CREATE_TAG_INDEX)
        self._run_with_pausepoints(
            order, "FT.ALIASADD", "consistency_alias", INDEX_NAME)

    @pytest.mark.parametrize("order", [0, 1])
    def test_aliasdel(self, order):
        self._create_indexes(CREATE_TAG_INDEX)
        node0 = self.new_client_for_primary(0)
        assert node0.execute_command(
            "FT.ALIASADD", "consistency_alias", INDEX_NAME) == b"OK"
        self._run_with_pausepoints(order, "FT.ALIASDEL", "consistency_alias")


class TestFTAliasCollision(AliasClusterTestCase):
    """
    node1 stops handling metadata while two primaries write the same alias, so
    neither sees the other's claim. Both claims carry the same epoch, so the
    greater index name wins on every node. A caller whose write does not win,
    or whose consistency check needs the paused node, gets an error rather
    than OK.
    """

    def _pause(self, node):
        assert node.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage yes"
        ) == b"OK"

    def _resolves_on_all_nodes(self, alias, index_name):
        _wait_for_aliaslist_on_all_nodes(
            self._all_primaries(), [alias.encode(), index_name.encode()])
        for node in self._all_primaries():
            info = list(node.execute_command("FT.INFO", alias))
            assert info[info.index(b"index_name") + 1] == index_name.encode()

    def test_aliasadd_collision(self):
        self._create_indexes(CREATE_TAG_INDEX, CREATE_TAG_INDEX_2)
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)

        self._pause(node1)
        reply0 = _execute(node0, "FT.ALIASADD", "p1", INDEX_NAME)
        reply1 = _execute(node1, "FT.ALIASADD", "p1", INDEX_NAME_2)
        _resume_cluster_messages(node1)

        # node1 cannot see node0's claim while paused, so node0's check
        # times out. node0 already holds both claims when node1 checks.
        _assert_timed_out(reply0)
        assert reply1 == b"OK"
        self._resolves_on_all_nodes("p1", INDEX_NAME_2)

    def test_aliasupdate_collision(self):
        self._create_indexes(CREATE_TAG_INDEX, CREATE_TAG_INDEX_2)
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        assert node0.execute_command("FT.ALIASADD", "p1", INDEX_NAME) == b"OK"

        self._pause(node1)
        reply0 = _execute(node0, "FT.ALIASUPDATE", "p1", INDEX_NAME_2)
        # node1 still maps p1 to INDEX_NAME, so this is a no-op locally, but
        # the other primaries already resolve p1 to INDEX_NAME_2.
        reply1 = _execute(node1, "FT.ALIASUPDATE", "p1", INDEX_NAME)
        _resume_cluster_messages(node1)

        _assert_timed_out(reply0)
        _assert_timed_out(reply1)
        self._resolves_on_all_nodes("p1", INDEX_NAME_2)

    def test_aliasdel_vs_aliasupdate_collision(self):
        self._create_indexes(CREATE_TAG_INDEX)
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        assert node0.execute_command("FT.ALIASADD", "p1", INDEX_NAME) == b"OK"

        self._pause(node1)
        reply0 = _execute(node0, "FT.ALIASDEL", "p1")
        # node1 still maps p1 to INDEX_NAME, so this writes nothing.
        reply1 = _execute(node1, "FT.ALIASUPDATE", "p1", INDEX_NAME)
        _resume_cluster_messages(node1)

        _assert_timed_out(reply0)
        _assert_timed_out(reply1)
        _wait_for_aliaslist_on_all_nodes(self._all_primaries(), [])

    def test_aliasupdate_vs_dropindex_collision(self):
        self._create_indexes(CREATE_TAG_INDEX, CREATE_TAG_INDEX_2)
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        assert node0.execute_command("FT.ALIASADD", "p1", INDEX_NAME) == b"OK"

        self._pause(node1)
        reply0 = _execute(node0, "FT.ALIASUPDATE", "p1", INDEX_NAME_2)
        reply1 = _execute(node1, "FT.DROPINDEX", INDEX_NAME)
        _resume_cluster_messages(node1)

        _assert_timed_out(reply0)
        _assert_timed_out(reply1)
        # The alias moved off INDEX_NAME before the drop, so it survives on
        # INDEX_NAME_2.
        self._resolves_on_all_nodes("p1", INDEX_NAME_2)

    def _concurrent_updates(self):
        """node0 moves p1 to INDEX_NAME_2 while node1 moves it to
        INDEX_NAME_3. Both claim epoch 2; INDEX_NAME_3 wins the tie."""
        self._create_indexes(
            CREATE_TAG_INDEX, CREATE_TAG_INDEX_2, CREATE_TAG_INDEX_3)
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        assert node0.execute_command("FT.ALIASADD", "p1", INDEX_NAME) == b"OK"

        self._pause(node1)
        reply0 = _execute(node0, "FT.ALIASUPDATE", "p1", INDEX_NAME_2)
        reply1 = _execute(node1, "FT.ALIASUPDATE", "p1", INDEX_NAME_3)
        _resume_cluster_messages(node1)

        _assert_timed_out(reply0)
        assert reply1 == b"OK"
        self._resolves_on_all_nodes("p1", INDEX_NAME_3)
        return node0, node1

    def test_concurrent_aliasupdate_then_delete_leaves_no_claim(self):
        """After FT.ALIASDEL, dropping the winner does not bring the alias
        back from the losing claim."""
        node0, node1 = self._concurrent_updates()

        assert node0.execute_command("FT.ALIASDEL", "p1") == b"OK"
        _wait_for_aliaslist_on_all_nodes(self._all_primaries(), [])
        assert node1.execute_command("FT.DROPINDEX", INDEX_NAME_3) == b"OK"
        _wait_for_aliaslist_on_all_nodes(self._all_primaries(), [])
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "p1", expect_present=False)

    def test_concurrent_aliasupdate_then_drop_winner_deletes_alias(self):
        """Dropping the winner deletes the alias instead of handing it to the
        losing claim."""
        _, node1 = self._concurrent_updates()

        assert node1.execute_command("FT.DROPINDEX", INDEX_NAME_3) == b"OK"
        _wait_for_aliaslist_on_all_nodes(self._all_primaries(), [])
        _wait_for_alias_on_all_nodes(
            self._all_primaries(), "p1", expect_present=False)


class TestFTAliasNameCollisionConsistency(AliasClusterTestCase):
    """
    An alias named like a real index must still wait for every node to
    learn the alias.
    """

    def test_aliasadd_name_collision_does_not_skip_consistency_check(self):
        """FT.ALIASADD <existing index name> <other index> must not return OK
        while a paused node still lacks the alias."""
        self._create_indexes(CREATE_TAG_INDEX, CREATE_TAG_INDEX_2)
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)

        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage yes"
        ) == b"OK"

        # node1 is paused, so a correct fanout cannot finish inside 2s
        # (deadline is ~5s); finishing early means node1 was never checked.
        quick_result = None
        with ThreadPoolExecutor() as executor:
            future_cmd = executor.submit(
                _execute, node0, "FT.ALIASADD", INDEX_NAME, INDEX_NAME_2)
            try:
                waiters.wait_for_true(future_cmd.done, timeout=2)
                quick_result = future_cmd.result()
            except waiters.WaitTimeout:
                pass
            finally:
                _resume_cluster_messages(node1)
            cmd_result = future_cmd.result()

        assert quick_result is None, (
            "FT.ALIASADD completed with a same-named real index while node1 "
            f"was still paused and unreachable: {quick_result!r}. The "
            "consistency check must not be satisfiable without contacting "
            "node1.")
        if isinstance(cmd_result, ResponseError):
            _assert_timed_out(cmd_result)
        else:
            assert cmd_result == b"OK", cmd_result
        _wait_for_aliaslist_on_all_nodes(
            self._all_primaries(),
            [INDEX_NAME.encode(), INDEX_NAME_2.encode()])
