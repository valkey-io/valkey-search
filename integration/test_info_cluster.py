from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser
from test_fanout_base import MAX_RETRIES
from typing import Any, Union
from valkey.exceptions import ResponseError, ConnectionError
import pytest

class TestFTInfoCluster(ValkeySearchClusterTestCaseDebugMode):

    def execute_primaries(self, command: Union[str, list[str]]) -> list[Any]:
        return [
            self.client_for_primary(i).execute_command(*command)
            for i in range(len(self.replication_groups))
        ]

    def is_backfill_complete(self, node, index_name):
        raw = node.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)
        if not info:
            return False
        backfill_in_progress = int(info["backfill_in_progress"])
        state = info["state"]
        return backfill_in_progress == 0 and state == "ready"

    def test_ft_info_cluster_success(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check cluster info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"
    
    def test_ft_info_cluster_retry(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))

        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]
        
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 1") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check retry count
        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count_after == retry_count_before + 1, f"Expected retry_count increment by 1, got {retry_count_after - retry_count_before}"

        # check cluster info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"

    # force one remote node to fail and test SOMESHARDS arg
    # expect partial results
    def test_ft_info_cluster_someshards_one_shard_fail(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        # force node1 to process backfill very slowly
        assert node1.execute_command(
            "CONFIG SET search.backfill-batch-size 1"
        ) == b"OK"

        N = 10000
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        # run ft.info cluster with all nodes healthy
        # node1 should not complete backfill
        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER", "SOMESHARDS")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)
        assert float(info["backfill_complete_percent_min"]) < 1.000000
        assert str(info["state"]) != "ready"

        # force node1 to fail continuously
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            MAX_RETRIES
        ) == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER", "SOMESHARDS")
        info = parser._parse_key_value_list(raw)

        # check partial cluster info results
        # node1 is down, expected to return backfill completed results from other nodes
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"

        # reset remote fail to 0
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            0
        ) == b"OK"

    # force all nodes to fail
    # expect empty results
    def test_ft_info_cluster_someshards_all_shards_fail(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        N = 60
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        # force all remote nodes to fail continuously
        results = self.execute_primaries(["FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount", MAX_RETRIES])
        assert all(result == b"OK" for result in results)

        # force local node to fail continuously
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceLocalFailCount ", 
            MAX_RETRIES
        ) == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER", "SOMESHARDS")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check partial results should be 0, state should be empty
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 0.0
        assert float(info["backfill_complete_percent_min"]) == 0.0
        assert str(info["state"]) == ""

        # reset all remote nodes
        results = self.execute_primaries(["FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount", 0])
        assert all(result == b"OK" for result in results)

         # reset local node
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceLocalFailCount ", 
            0
        ) == b"OK"

    # force inconsistent errors
    # expect fail in CONSISTENT but success in INCONSISTENT arg
    def test_ft_info_cluster_consistent_arg(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        N = 60
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))

        # force inconsistent error
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceInfoClusterInconsistentError yes"
        ) == b"OK"

        # expect error in consistent mode
        with pytest.raises(ResponseError) as excinfo:
            node0.execute_command("FT.INFO", index_name, "CLUSTER", "ALLSHARDS", "CONSISTENT")
        assert "Unable to contact all cluster members" in str(excinfo.value)

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER", "ALLSHARDS", "INCONSISTENT")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # expect results in inconsistent mode
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"

        # reset inconsistent error
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceInfoClusterInconsistentError no"
        ) == b"OK"