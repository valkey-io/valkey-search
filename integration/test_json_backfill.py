from valkey_search_test_case import *
import valkey, time
import pytest
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
from valkeytestframework.util import waiters
from valkey.cluster import ValkeyCluster, ClusterNode

def search_command(index: str) -> list[str]:
    return [
        "FT.SEARCH",
        index,
        "*=>[KNN 10 @v $BLOB]",
        "PARAMS",
        "2",
        "BLOB",
        float_to_bytes([10.0, 10.0, 10.0]),
    ]

def index_on_node(client, name:str) -> bool:
    indexes = client.execute_command("FT._LIST")
    return name.encode() in indexes

def sum_of_remote_searches(nodes: list[Node]) -> int:
    return sum([n.client.info("search")["search_coordinator_server_search_index_partition_success_count"] for n in nodes])

def do_json_backfill_test(test, client, primary, replica):
    print("P Role:", primary.info("replication")["role"], primary)
    print("R Role:", replica.info("replication")["role"], replica)
    index = Index("test", [Vector("v", 3, type="FLAT")], type="JSON")
    index.load_data(client, 100)
    replica.readonly()

    index.create(primary)
    waiters.wait_for_true(lambda: index_on_node(primary, index.name), timeout=3)
    waiters.wait_for_true(lambda: index_on_node(replica, index.name), timeout=6)
    waiters.wait_for_true(lambda: index.backfill_complete(primary), timeout=3)
    waiters.wait_for_true(lambda: index.backfill_complete(replica), timeout=3)
    p_result = primary.execute_command(*search_command(index.name))
    for n in test.nodes:
        n.client.execute_command("config set search.test-force-replicas-only yes")
    r_result = replica.execute_command(*search_command(index.name))
    print("After second Search")
    print("P Role:", primary.info("replication")["role"])
    print("R Role:", replica.info("replication")["role"])
    print("PResult:", p_result)
    print("RResult:", r_result)
    assert len(p_result) != 0
    assert len(p_result) == len(r_result)

class TestJsonBackfill(ValkeySearchClusterTestCase):
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    # Mark as xfail until JSON fixes are merged.
    @pytest.mark.xfail
    def test_json_backfill_CME(self):
        """
        Validate that JSON backfill works correctly on a replica
        """
        rg = self.get_replication_group(0)
        primary = rg.get_primary_connection()
        replica = rg.get_replica_connection(0)
        do_json_backfill_test(self, self.new_cluster_client(), primary, replica)

class TestSearchFTDropindexCMD(ValkeySearchTestCaseBase):
    """
    Test suite for FT.DROPINDEX search command. We expect that
    clients will not be able to drop index on the replica.
    """
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_json_backfill_CMD(self):
        do_json_backfill_test(self, self.client, self.get_primary_connection(), self.get_replica_connection(0))

