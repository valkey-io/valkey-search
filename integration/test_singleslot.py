import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging, os, time
from typing import Any, Union
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser

class TestSingleSlot(ValkeySearchClusterTestCaseDebugMode):
    """A dummy test to enable debug mode for the cluster."""

    def test_single_slot_index_creation(self):
        client0 = self.new_client_for_primary(0)
        with pytest.raises(ResponseError) as e:
             client0.execute_command("FT.CREATE", "idx", "PREFIX", "1", "agg:{shard0}", "SCHEMA", "value", "NUMERIC")
        assert "PREFIX argument(s) must not contain a hash tag" in str(e.value)  
        
        with pytest.raises(ResponseError) as e:
             client0.execute_command("FT.CREATE", "idx{shard}", "SCHEMA", "value", "NUMERIC")
        assert "PREFIX parameter is required for hash-tagged indexes" in str(e.value)  

        with pytest.raises(ResponseError) as e:
             client0.execute_command("FT.CREATE", "idx{shard}", "PREFIX", "1", "agg:{shard2}", "SCHEMA", "value", "NUMERIC")
        assert "All PREFIX arguments must contain the same hash tag as the index" in str(e.value)  
        cluster = self.new_cluster_client()
        clients = [self.new_client_for_primary(i) for i in range(3)]
        for database in range(3):
            client0.execute_command("SELECT", str(database))
            client0.execute_command("FT.CREATE", "idx{shard0}", "PREFIX", "1", "agg:{shard0}", "SCHEMA", "value", "NUMERIC")

            for i in range(database+100):
                cluster.hset("agg:{shard0}:"+str(i), "value", str(i*10))

            num_docs = 0
            for client in clients:
                info = FTInfoParser(client.execute_command("FT.INFO", "idx{shard0}"))
                assert info.index_name == "idx{shard0}"
                num_docs += info.num_docs
            assert num_docs == database+100

        for database in range(3):
            client0.execute_command("SELECT", str(database))
            client0.execute_command("FT.DROPINDEX", "idx{shard0}")

    def test_single_slot_search_query(self):
        """Test that queries on hash-tagged indexes only hit the owning node."""
        cluster: ValkeyCluster = self.new_cluster_client()
        nodes = self.get_all_primary_clients()
        for node in nodes:
            node.execute_command("CONFIG SET search.info-developer-visible yes")

        slot0 = nodes[0].execute_command("CLUSTER","KEYSLOT", "idx{shard0}")
        assert int(slot0) == 14398
        
        # Create hash-tagged index on node0
        index = Index(
            name='idx{shard0}',
            fields=[Numeric('price')],
            prefixes=['doc:{shard0}'],
            type=KeyDataType.HASH
        )
        index.create(nodes[0])
        
        # Add data
        for i in range(10):
            cluster.execute_command("HSET", f"doc:{{shard0}}:{i}", "price", str(i * 10))

        requests = lambda: [node.info("SEARCH")["search_coordinator_client_search_index_partition_success_count"] for node in nodes]            
        rpcs = lambda: [node.info("SEARCH")["search_search_index_rpc_requests"] for node in nodes]            
        
        assert(requests() == [0, 0, 0])
        assert(rpcs() == [0, 0, 0])
        
        for i, expected_requests, expected_rpcs in [
            (0, [1, 0, 0], [0, 0, 1]),  # request to node 0, rpc'ed to node 2
            (1, [1, 1, 0], [0, 0, 2]),  # request to node 1, rpc'ed to node 2
            (2, [1, 1, 0], [0, 0, 2])   # request to node 2, handled locally
        ]:
            result = nodes[i].execute_command("FT.SEARCH", "idx{shard0}", "@price:[0 100]")
            assert 1 == int(nodes[i].info("SEARCH")["search_single_slot_queries"])
            print("Doing case ", i)
            assert(requests() == expected_requests)
            assert(rpcs() == expected_rpcs)
            assert result[0] == 10  # number of results
