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

class TestShardDown(ValkeySearchClusterTestCaseDebugMode):
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_shard_down(self):
        """
        Validate that query logic works when shard down
        """
        client = self.new_cluster_client()
        index = Index("test", [Vector("v", 3, type="FLAT")], type="HASH")
        index.create(client)
        index.load_data(self, 100)
        for n in self.nodes:
            n.client.execute_command("config set search.enable-partial-results no")

        #
        # Mark one shard down.
        #
        rg = self.get_replication_group(2)
        shard_nodes = [rg.primary] + rg.replicas
        for n in shard_nodes:
            n.client.execute_command("FT._DEBUG PAUSEPOINT SET Search.gRPC")
        #
        # Execute command
        #
        with pytest.raises(ResponseError):
            r_result = self.get_replication_group(0).get_primary_connection().execute_command(*search_command(index.name))
            print("Result: ", r_result)                

        sum = 0
        for n in shard_nodes:
            t = n.client.execute_command("FT._DEBUG PAUSEPOINT TEST Search.gRPC")
            n.client.execute_command("FT._DEBUG PAUSEPOINT RESET Search.gRPC")
            sum += t
        assert sum > 0

        #
        # Flip partial results
        #
        for n in self.nodes:
            n.client.execute_command("config set search.enable-partial-results yes")

        for n in shard_nodes:
            n.client.execute_command("FT._DEBUG PAUSEPOINT SET Search.gRPC")

        #
        # Execute command, no exception
        #
        r_result = self.get_replication_group(0).get_primary_connection().execute_command(*search_command(index.name))
        print("Result: ", r_result)                

        sum = 0
        for n in shard_nodes:
            t = n.client.execute_command("FT._DEBUG PAUSEPOINT TEST Search.gRPC")
            n.client.execute_command("FT._DEBUG PAUSEPOINT RESET Search.gRPC")
            sum += t
        assert sum > 0
