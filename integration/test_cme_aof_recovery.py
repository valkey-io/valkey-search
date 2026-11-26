"""
Regression tests for #1727: AOF-only restart in coordinator (CME) mode must
recover index schemas and searchable documents.

In coordinator mode, FT.CREATE and FT.DROPINDEX are persisted to AOF via
FT.INTERNAL_UPDATE. Without calling TriggerCallbacks() during LOADING in
MetadataManager::CreateEntryOnReplica(), restarting from AOF without an RDB
snapshot loses all index schemas.
"""

from typing import List

from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters

from utils import IndexingTestHelper
from valkey_search_test_case import ValkeySearchClusterTestCase


class TestCmeAofRecovery(ValkeySearchClusterTestCase):
    """Verify CME index schema & document recovery across AOF-only restarts."""

    CLUSTER_SIZE = 1

    def get_config_file_lines(self, testdir, port) -> List[str]:
        return super().get_config_file_lines(testdir, port) + [
            "appendonly yes",
            "appendfsync always",
            'save ""',
        ]

    def _restart_primary_aof_only(self):
        """Restart the single primary keeping AOF files and removing any RDB."""
        primary = self.get_primary(0)
        primary.restart(remove_rdb=True)
        client = self.new_client_for_primary(0)
        waiters.wait_for_true(lambda: client.ping(), timeout=15)
        return client

    def test_cme_aof_only_restart_recovers_index_and_documents(self):
        client = self.new_client_for_primary(0)
        index_name = "cme_aof_idx"

        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "cat", "TAG",
        )
        client.hset("doc:1", mapping={"title": "alpha engine", "cat": "core"})
        client.hset("doc:2", mapping={"title": "beta engine", "cat": "infra"})
        client.hset("doc:3", mapping={"title": "gamma system", "cat": "core"})

        IndexingTestHelper.wait_for_indexing_complete_on_node(client, index_name)
        assert index_name in IndexingTestHelper.get_ft_list(client)
        pre_res = client.execute_command("FT.SEARCH", index_name, "alpha")
        assert pre_res[0] == 1

        # Restart with AOF only (no RDB).
        client = self._restart_primary_aof_only()

        assert index_name in IndexingTestHelper.get_ft_list(client), (
            f"Expected {index_name} to be recovered from AOF after restart"
        )
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, index_name)

        info = IndexingTestHelper.get_ft_info(client, index_name)
        assert info.num_docs == 3

        post_res = client.execute_command("FT.SEARCH", index_name, "alpha")
        assert post_res[0] == 1
        assert post_res[1] == b"doc:1"

    def test_cme_aof_only_restart_preserves_drop_and_recreate(self):
        client = self.new_client_for_primary(0)
        dropped_idx = "cme_aof_dropped"
        recreated_idx = "cme_aof_recreated"

        client.execute_command(
            "FT.CREATE", dropped_idx,
            "ON", "HASH",
            "PREFIX", "1", "d:",
            "SCHEMA", "title", "TEXT",
        )
        client.execute_command(
            "FT.CREATE", recreated_idx,
            "ON", "HASH",
            "PREFIX", "1", "r:",
            "SCHEMA", "title", "TEXT",
        )
        client.hset("r:1", mapping={"title": "recovered", "tag": "v2"})
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, recreated_idx)

        client.execute_command("FT.DROPINDEX", dropped_idx)
        client.execute_command("FT.DROPINDEX", recreated_idx)
        client.execute_command(
            "FT.CREATE", recreated_idx,
            "ON", "HASH",
            "PREFIX", "1", "r:",
            "SCHEMA", "title", "TEXT", "tag", "TAG",
        )
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, recreated_idx)

        # Restart with AOF only (no RDB).
        client = self._restart_primary_aof_only()

        indexes = IndexingTestHelper.get_ft_list(client)
        assert dropped_idx not in indexes
        assert recreated_idx in indexes
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, recreated_idx)

        res = client.execute_command("FT.SEARCH", recreated_idx, "@tag:{v2}")
        assert res[0] == 1
        assert res[1] == b"r:1"
