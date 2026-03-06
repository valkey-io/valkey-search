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
    assert(primary.info("replication")["role"] == "master")
    assert(replica.info("replication")["role"] == "slave")
    index = Index("test", [Vector("v", 3, type="FLAT")], type=KeyDataType.JSON)
    index.load_data(client, 100)
    replica.readonly()

    index.create(primary)
    waiters.wait_for_true(lambda: index_on_node(primary, index.name))
    waiters.wait_for_true(lambda: index_on_node(replica, index.name))
    waiters.wait_for_true(lambda: index.backfill_complete(primary))
    waiters.wait_for_true(lambda: index.backfill_complete(replica))
    p_result = primary.execute_command(*search_command(index.name))
    for n in test.nodes:
        n.client.execute_command("ft._debug CONTROLLED_VARIABLE set ForceReplicasOnly yes")
    r_result = replica.execute_command(*search_command(index.name))
    print("After second Search")
    print("PResult:", p_result)
    print("RResult:", r_result)
    assert len(p_result) == 21
    assert len(r_result) == 21

class TestJsonBackfill(ValkeySearchClusterTestCaseDebugMode):
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_json_backfill_CME(self):
        """
        Validate that JSON backfill works correctly on a replica
        """
        rg = self.get_replication_group(0)
        primary = rg.get_primary_connection()
        replica = rg.get_replica_connection(0)
        do_json_backfill_test(self, self.new_cluster_client(), primary, replica)

class TestSearchFTDropindexCMD(ValkeySearchTestCaseDebugMode):
    """
    Test suite for FT.DROPINDEX search command. We expect that
    clients will not be able to drop index on the replica.
    """
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_json_backfill_CMD(self):
        do_json_backfill_test(self, self.client, self.get_primary_connection(), self.get_replica_connection(0))

class TestCreateNonVectorIndexes(ValkeySearchClusterTestCase):
    """
    Test create and search for JSON non-vector indexes
    """
    def test_non_vector_indexes(self):
        numeric_indx_name = "numeric"
        tag_idx_name = "tag"
        client = self.new_cluster_client()
        index_numeric = Index(numeric_indx_name, [Numeric("n")], type=KeyDataType.JSON)
        index_tag = Index(tag_idx_name, [Tag("t")], type=KeyDataType.JSON)
        indexes = [index_numeric, index_tag]
        for index in indexes:
            index.load_data(client, 10)
            index.create(client)
            for primary_client in self.get_all_primary_clients():
                waiters.wait_for_true(lambda: index_on_node(primary_client, index.name))
                waiters.wait_for_true(lambda: index.backfill_complete(primary_client))
        
        # Run numeric query
        numeric_result = client.execute_command(
            "FT.SEARCH",
            numeric_indx_name,
            f"@n:[-inf +inf]"
        )
        assert numeric_result[0] == 10
        # Run tag query
        tag_result = client.execute_command(
            "FT.SEARCH",
            tag_idx_name,
            f"@t:{{Tag:*}}"
        )
        assert tag_result[0] == 10
