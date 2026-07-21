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

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_single_slot_force_replicas_only(self):
        """
        Regression test: single-slot index + ForceReplicasOnly must not crash.

        ForceReplicasOnly sets mode=kOneReplicaPerShard in ComputeSearchTargets.
        For single-slot indexes this calls GetTargetsForSlot(kOneReplicaPerShard)
        which previously had no case for that mode → CHECK(false) → SIGABRT.

        Fix: added kOneReplicaPerShard case to GetTargetsForSlot in
        vmsdk/src/cluster_map.cc.
        """
        cluster = self.new_cluster_client()
        nodes = self.get_all_primary_clients()
        primary = nodes[0]

        # Create single-slot index
        primary.execute_command(
            "FT.CREATE", "idx{shard0}",
            "PREFIX", "1", "doc:{shard0}",
            "SCHEMA", "price", "NUMERIC"
        )

        # Add data
        for i in range(10):
            cluster.execute_command(
                "HSET", f"doc:{{shard0}}:{i}", "price", str(i * 10)
            )

        # Wait for backfill
        waiters.wait_for_true(
            lambda: primary.execute_command(
                "FT.SEARCH", "idx{shard0}", "@price:[0 100]"
            )[0] == 10
        )

        # Verify query works before ForceReplicasOnly (uses kRandom → handled)
        result = primary.execute_command(
            "FT.SEARCH", "idx{shard0}", "@price:[0 100]"
        )
        assert result[0] == 10

        # Enable ForceReplicasOnly on ALL nodes → mode = kOneReplicaPerShard
        for n in self.nodes:
            n.client.execute_command(
                "ft._debug CONTROLLED_VARIABLE set ForceReplicasOnly yes"
            )

        # Query with ForceReplicasOnly: validates that single-slot indexes
        # correctly handle replica-targeted fanout modes in GetTargetsForSlot
        result = primary.execute_command(
            "FT.SEARCH", "idx{shard0}", "@price:[0 100]"
        )
        assert result[0] == 10
