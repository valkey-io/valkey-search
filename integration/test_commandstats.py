"""Command timing belongs to the client-facing node, including async work."""

import time
from contextlib import ExitStack

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters

from utils import IndexingTestHelper, find_local_key, run_in_thread
from valkey_search_test_case import (
    ValkeySearchClusterTestCaseDebugMode,
    ValkeySearchTestCaseDebugMode,
)


PAUSEPOINT = "background_search_completing"
HOLD_SECONDS = 0.2
COMMANDS = ("FT.SEARCH", "FT.AGGREGATE")


def commandstats(client, command):
    # Command name casing in INFO can differ between server versions.
    stats = {key.lower(): value for key, value in client.info("commandstats").items()}
    entry = stats.get(f"cmdstat_{command.lower()}", {})
    return entry.get("calls", 0), entry.get("usec", 0)


def run_paused_query(query_client, nodes, command, expected_count):
    """Prove every shard runs background work and hold it inside the timer."""
    before = [commandstats(node, command) for node in nodes]
    thread = None
    try:
        with ExitStack() as cleanup:
            for node in nodes:
                node.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", PAUSEPOINT)
                cleanup.callback(
                    node.execute_command, "FT._DEBUG", "PAUSEPOINT", "RESET", PAUSEPOINT
                )
            thread, result, error = run_in_thread(
                lambda: query_client.execute_command(
                    command, "idx", "@score:[0 +inf]", "TIMEOUT", "30000"
                )
            )

            def all_shards_paused():
                if error[0] is not None:
                    raise error[0]
                assert thread.is_alive(), "Query completed without reaching the pausepoint"
                return all(
                    node.execute_command("FT._DEBUG", "PAUSEPOINT", "TEST", PAUSEPOINT) > 0
                    for node in nodes
                )

            waiters.wait_for_true(all_shards_paused)
            # This deliberate hold makes a missing background timer fail even if
            # command parsing and reply generation contribute nonzero usec.
            time.sleep(HOLD_SECONDS)
    finally:
        if thread is not None:
            thread.join(timeout=35)

    assert not thread.is_alive(), "Query did not finish after releasing pausepoints"
    assert error[0] is None, error[0]
    assert result[0][0] == expected_count
    return before, [commandstats(node, command) for node in nodes]


def assert_client_charge(before, after):
    assert after[0] - before[0] == 1, (before, after)
    assert after[1] - before[1] >= HOLD_SECONDS * 1_000_000, (before, after)


class TestCommandstatsLocal(ValkeySearchTestCaseDebugMode):
    @pytest.mark.parametrize("command", COMMANDS)
    def test_commandstats_include_local_background_time(self, command):
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "score", "NUMERIC",
        )
        client.hset("doc:1", mapping={"score": 1})
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")

        with self.server.get_new_client() as query_client:
            before, after = run_paused_query(query_client, [client], command, 1)
        assert_client_charge(before[0], after[0])


class TestCommandstatsCluster(ValkeySearchClusterTestCaseDebugMode):
    @pytest.mark.parametrize("command", COMMANDS)
    def test_commandstats_exclude_remote_background_time(self, command):
        nodes = [self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)]
        nodes[0].execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "score", "NUMERIC",
        )
        for node in nodes:
            IndexingTestHelper.wait_for_backfill_complete_on_node(node, "idx")
            node.hset(find_local_key(node, prefix="doc:"), mapping={"score": 1})
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(nodes, "idx")

        # Rotate the receiving node so unchanged remote stats cannot hide a
        # node whose command accounting never works. Use direct connections
        # to keep the cluster client from choosing a different coordinator.
        for coordinator in range(self.CLUSTER_SIZE):
            with self.new_client_for_primary(coordinator) as query_client:
                before, after = run_paused_query(
                    query_client, nodes, command, self.CLUSTER_SIZE
                )
            for i in range(self.CLUSTER_SIZE):
                if i == coordinator:
                    assert_client_charge(before[i], after[i])
                else:
                    assert after[i] == before[i], (i, before[i], after[i])
