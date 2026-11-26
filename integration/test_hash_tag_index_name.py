"""Cluster tests for DB-0 index names that start with a numeric hash tag."""

import pytest
from valkey.cluster import ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters

from utils import IndexingTestHelper
from valkey_search_test_case import ValkeySearchClusterTestCase


NAMES = ["{1}idx", "{2024-x}idx", "{0}idx"]


def tag_of(name: str) -> str:
    return name[: name.index("}") + 1]


class TestNumericHashTagIndexName(ValkeySearchClusterTestCase):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    def all_clients(self):
        return [node.client for node in self.nodes]

    def create_index(self, name: str):
        return self.client_for_primary(0).execute_command(
            "FT.CREATE",
            name,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            f"{tag_of(name)}doc:",
            "SCHEMA",
            "t",
            "TAG",
        )

    def wait_for_indexes(self, names):
        waiters.wait_for_true(
            lambda: all(
                IndexingTestHelper.get_ft_list(client) == set(names)
                for client in self.all_clients()
            )
        )

    def assert_every_node_alive(self):
        for node in self.nodes:
            assert node.server.is_alive(), f"node on port {node.server.port} died"
        waiters.wait_for_true(
            lambda: all(
                client.cluster("info")["cluster_state"] == "ok"
                for client in self.all_clients()
            )
        )

    @pytest.mark.parametrize("name", NAMES)
    def test_create_search_and_drop(self, name):
        assert self.create_index(name) == b"OK"
        self.wait_for_indexes([name])

        cluster: ValkeyCluster = self.new_cluster_client()
        cluster.hset(f"{tag_of(name)}doc:1", mapping={"t": "x"})
        for index in range(self.CLUSTER_SIZE):
            client = self.client_for_primary(index)
            waiters.wait_for_true(
                lambda: client.execute_command("FT.SEARCH", name, "@t:{x}")[0]
                == 1
            )

        assert self.client_for_primary(1).execute_command(
            "FT.DROPINDEX", name
        ) == b"OK"
        self.wait_for_indexes([])
        self.assert_every_node_alive()

    def test_restarted_nodes_keep_the_index(self):
        name = NAMES[0]
        assert self.create_index(name) == b"OK"
        self.wait_for_indexes([name])

        # A peer primary restores metadata reconciliation. The creator's
        # replica restores the replicated metadata entry.
        for node in (
            self.replication_groups[1].primary,
            self.replication_groups[0].replicas[0],
        ):
            node.client.execute_command("SAVE")
            node.server.restart(remove_rdb=False, remove_nodes_conf=False)
            waiters.wait_for_true(node.server.is_alive)

        self.wait_for_indexes([name])
        self.assert_every_node_alive()
