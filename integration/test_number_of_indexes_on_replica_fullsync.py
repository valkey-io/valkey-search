"""
Verify a replica reports the correct search_number_of_indexes after a full sync.
"""

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkey_search_test_case import ValkeySearchTestCaseBase
from indexes import Index, Text, Vector
from util import waiters

idx = Index("idx_1", [Vector("v", 3, type="HNSW", m=2, efc=1), Text("txt")])
NUM_DOCS = 10


def replica_num_indexes(replica_client):
    return replica_client.info("search").get("search_number_of_indexes")


class TestNumberOfIndexesOnReplicaFullSync(ValkeySearchTestCaseBase):

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_number_of_indexes_after_full_sync(self):
        primary_client = self.get_primary_connection()
        replica_client = self.get_replica_connection(0)

        idx.create(primary_client, True)
        idx.load_data(primary_client, NUM_DOCS)
        assert primary_client.info("search")["search_number_of_indexes"] == 1

        waiters.wait_for_equal(
            lambda: replica_num_indexes(replica_client), 1, timeout=30
        )

        # Detach then re-attach to force a fresh full sync.
        sync_full_before = primary_client.info("stats")["sync_full"]
        replica_client.execute_command("REPLICAOF", "NO", "ONE")
        self.rg.setup_replications_cmd()

        assert primary_client.info("stats")["sync_full"] == sync_full_before + 1

        # Wait for the RDB load to finish before reading the index count.
        waiters.wait_for_true(
            lambda: replica_client.info("replication")["master_link_status"]
            == "up",
            timeout=30,
        )
        assert replica_num_indexes(replica_client) == 1
