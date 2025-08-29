from valkey_search_test_case import *
import valkey, time
import pytest
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
from valkeytestframework.util import waiters
from valkey.cluster import ValkeyCluster, ClusterNode
from ft_info_parser import FTInfoParser

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

class TestJsonBackfill(ValkeySearchClusterTestCase):
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 2}], indirect=True
    )
    def test_fanout(self):
        """
        Validate that queries are distributed correctly across the cluster.
        """
        rg = self.get_replication_group(0)
        primary = rg.get_primary_connection()
        replica = rg.get_replica_connection(0)
        assert(primary.info("replication")["role"] == "master")
        assert(replica.info("replication")["role"] == "slave")
        index = Index("test", [Vector("v", 3, type="FLAT")], type="HASH")
        index.create(primary)
        for node in self.get_nodes():
            waiters.wait_for_true(lambda: index_on_node(node.client, index.name))
        index.load_data(self.new_cluster_client(), 100)
        replica.readonly()
        waiters.wait_for_true(lambda: self.replication_lag() == 0)

        initial_primary_count = sum_of_remote_searches(self.get_primaries())
        initial_replica_count = sum_of_remote_searches(self.get_replicas())
        #
        # First do a bunch of searches with a ReadWrite client.
        #
        for i in range(100):
            p_result = primary.execute_command(*search_command(index.name))
            assert(len(p_result) > 1)
        readwrite_primary_count = sum_of_remote_searches(self.get_primaries()) - initial_primary_count
        readwrite_replica_count = sum_of_remote_searches(self.get_replicas()) - initial_replica_count
        #
        # Now do a bunch a read only client.
        #
        for _ in range(100):
            r_result = replica.execute_command(*search_command(index.name))
            assert(len(r_result) > 1)
        readonly_primary_count = sum_of_remote_searches(self.get_primaries()) - readwrite_primary_count
        readonly_replica_count = sum_of_remote_searches(self.get_replicas()) - readwrite_replica_count

        version_string = primary.info("SERVER")['valkey_version']
        major_version = int(version_string.split('.')[0])
        if major_version >= 9:
            # Valkey 9+ honors the readonly flag in the client context
            assert(readwrite_primary_count != 0)
            assert(readwrite_replica_count == 0)
            assert(readonly_primary_count != 0)
            assert(readonly_replica_count != 0)
        else:
            # Valkey 8 distributes requests across all nodes regardless of connection read/readwrite
            assert(readwrite_primary_count != 0)
            assert(readwrite_replica_count != 0)
            assert(readonly_primary_count != 0)
            assert(readonly_replica_count != 0)
