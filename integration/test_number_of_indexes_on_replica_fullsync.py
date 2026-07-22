"""
Verify a replica reports the correct search_number_of_indexes after a full sync.
"""

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkey_search_test_case import ValkeySearchTestCaseBase
from indexes import *
from util import waiters

idx = Index("idx_1", [Vector("v", 3, type="HNSW", m=2, efc=1), Text("txt")])
NUM_DOCS = 10


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

        replica_num_indexes = lambda: replica_client.info("search").get(
            "search_number_of_indexes"
        )
        waiters.wait_for_equal(replica_num_indexes, 1, timeout=30)

        # Detach then re-attach to force a fresh full sync.
        replica_client.execute_command("REPLICAOF", "NO", "ONE")
        self.rg.setup_replications_cmd()

        # After the full sync completes, the replica must report exactly
        # the same number of indexes as the primary.
        waiters.wait_for_equal(replica_num_indexes, 1, timeout=30)
