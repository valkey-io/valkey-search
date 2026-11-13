from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode, ValkeySearchClusterTestCase, Node
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from valkey.exceptions import ResponseError, ConnectionError
import pytest
from indexes import *
from ft_info_parser import FTInfoParser
from utils import run_in_thread

MAX_RETRIES = "4294967295"

class TestFanoutBase(ValkeySearchClusterTestCaseDebugMode):

    # force retry by manually creating remote failure once
    def test_fanout_retry(self):
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

        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]

        # force remote node to fail once and trigger retry
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 1") == b"OK"

        node0.execute_command("FT.INFO", index_name, "PRIMARY")

        # check retry count
        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count_after == retry_count_before + 1, f"Expected retry_count increment by 1, got {retry_count_after - retry_count_before}"

    def test_fanout_shutdown(self):
        cluster = self.new_cluster_client()
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        
        index_name = "index1"
        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        try:
            node1.execute_command("SHUTDOWN", "NOSAVE")
        except:
            pass
        
        def is_node_down(node):
            try:
                node.ping()
                return False
            except ConnectionError:
                return True
    
        waiters.wait_for_true(lambda: is_node_down(node1))
        
        with pytest.raises(ResponseError) as excinfo:
            node0.execute_command("FT.INFO", index_name, "CLUSTER", "ALLSHARDS")
        
        assert "Unable to contact all cluster members" in str(excinfo.value)
    
    def test_fanout_timeout(self):
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

        # force timeout by enabling continuous remote failure
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            MAX_RETRIES
        ) == b"OK"

        with pytest.raises(ResponseError) as ei:
            node0.execute_command("FT.INFO", index_name, "PRIMARY", "ALLSHARDS")
        assert "Unable to contact all cluster members" in str(ei.value)

        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 0", 
        ) == b"OK"
    
    def test_fingerprint_version_create(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command("CONFIG SET search.info-developer-visible yes") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)
        fingerprint = int(info["index_fingerprint"])
        assert fingerprint is not None
        version = int(info["index_version"])
        assert version == 0

        # drop and create the index again with different definition
        assert node0.execute_command("FT.DROPINDEX", index_name) == b"OK"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH", 
            "PREFIX", "1", "text:",
            "SCHEMA", "count", "NUMERIC"
        ) == b"OK"

        # fingerprint should mismatch and version should be higher
        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")
        info = parser._parse_key_value_list(raw)
        assert fingerprint != int(info["index_fingerprint"])
        assert version < int(info["index_version"])

    def test_background_reply_after_dropindex(self):
        """A shard's background reply for a dropped index still frees safely.

        The reply task frees the responder on a reader thread, and here it
        holds the last IndexSchema reference, so ~IndexSchema runs there.
        """
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        # A one-thread pool replies inline on main, skipping that path.
        assert int(node0.execute_command(
            "CONFIG", "GET", "search.reader-threads")[1]) > 1
        node0.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "n", "NUMERIC")
        for i in range(10):
            cluster.hset(f"doc:{i}", "n", i)
        for node in self.nodes:
            node.client.execute_command(
                "CONFIG", "SET", "search.background-shard-reply", "yes")
            node.client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")
        thread, _, err = run_in_thread(
            lambda: self.new_client_for_primary(0).execute_command(
                "FT.SEARCH", "idx", "@n:[0 +inf]"))
        for node in self.nodes:
            waiters.wait_for_true(lambda: node.client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST",
                "background_search_completing") > 0)
        node0.execute_command("FT.DROPINDEX", "idx")
        for node in self.nodes:
            waiters.wait_for_true(lambda: not index_on_node(node.client, "idx"))
        for node in self.nodes:
            node.client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        thread.join()
        # Partial results (default on) turn each shard's not-found into empty.
        assert err[0] is None
        for node in self.nodes:
            assert node.client.ping()

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

class TestFanout(ValkeySearchClusterTestCase):
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 2}], indirect=True
    )
    @pytest.mark.parametrize("threshold", [0, 100])
    def test_fanout_low_utilization_fanout(self, threshold):
        number_of_searches_to_run = 10
        rg = self.get_replication_group(0)
        primary = rg.get_primary_connection()
        assert(primary.info("replication")["role"] == "master")
        
        # Set the fanout low utilization threshold
        primary.execute_command("CONFIG", "SET", "search.local-fanout-queue-wait-threshold", threshold)
        
        index = Index("test", [Vector("v", 3, type="FLAT")], type=KeyDataType.HASH)
        index.create(primary)
        for node in self.get_nodes():
            waiters.wait_for_true(lambda: index_on_node(node.client, index.name))
        index.load_data(self.new_cluster_client(), 100)

        waiters.wait_for_true(lambda: self.replication_lag() == 0)

        # Assert replicas of the primary didn't run any search queries
        assert(sum_of_remote_searches(rg.replicas) == 0)

        # Verify the threshold was applied
        result = primary.execute_command("CONFIG", "GET", "search.local-fanout-queue-wait-threshold")
        assert result[1].decode() == str(threshold)

        # Execute searches
        for i in range(number_of_searches_to_run):
            result = primary.execute_command(*search_command(index.name))
            assert(len(result) > 1)
        
        if threshold:
            # threshold == 100 means we are always under utilize and prefer to do local search on the shard
            # Assert replicas of the primary didn't run any search queries
            assert(sum_of_remote_searches(rg.replicas) == 0)
        else:
            # threshold == 0 means we are always "too busy"
            # Assert replicas of the primary run some of the search queries 
            assert(sum_of_remote_searches(rg.replicas) > 0)

    def test_sample_queue_size_config(self):
        """Test thread-pool-wait-time-samples configuration parameter"""
        rg = self.get_replication_group(0)
        client = rg.get_primary_connection()
        
        # Test default value
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"100"
        
        # Test setting new value
        client.execute_command("CONFIG", "SET", "search.thread-pool-wait-time-samples", "200")
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"200"
        
        # Test boundary values
        client.execute_command("CONFIG", "SET", "search.thread-pool-wait-time-samples", "10")
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"10"
        
        client.execute_command("CONFIG", "SET", "search.thread-pool-wait-time-samples", "10000")
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"10000"

    def test_async_fanout_and_background_reply(self):
        """Results do not depend on which thread does the gRPC work.

        async-fanout-threshold moves the coordinator's per-shard sends to a
        reader thread once the fanout has that many targets (default 1).
        background-shard-reply (default yes) moves a
        shard's reply to a reader thread when its search completes on the main
        thread, i.e. after content resolution. FT.SEARCH with content and
        FT.HYBRID (whose remote arms always resolve content) take that path;
        FT.SEARCH NOCONTENT completes on a reader thread and is the control.
        """
        index = "idx"
        docs = 30
        query_vec = float_to_bytes([1.0, 2.0, 3.0, 4.0])
        cluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        client.execute_command(
            "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT", "n", "NUMERIC", "SORTABLE",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        for i in range(1, docs + 1):
            cluster.hset(f"doc:{i}", mapping={
                "title": f"hello world {i}",
                "n": i,
                "vec": float_to_bytes([float(i), float(i * 2), float(i * 3),
                                       float(i * 4)])})
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", index, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == docs,
            timeout=15)
        # The coordinator only sends asynchronously from a pool of 2+ threads.
        assert int(client.execute_command(
            "CONFIG", "GET", "search.reader-threads")[1]) > 1

        # Each query returns a value independent of the order shards reply in,
        # so runs can be compared for equality.
        def run_all():
            content = client.execute_command(
                "FT.SEARCH", index, "@title:hello", "SORTBY", "n", "ASC",
                "RETURN", "2", "title", "n", "LIMIT", "0", "100")
            nocontent = client.execute_command(
                "FT.SEARCH", index, "@title:hello", "NOCONTENT",
                "LIMIT", "0", "100")
            hybrid = client.execute_command(
                "FT.HYBRID", index,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "COMBINE", "RRF", "2", "WINDOW", "1000",
                "LIMIT", "0", "100",
                "PARAMS", "2", "q", query_vec)
            # Keys only: text-score ties across shards can rank in
            # reply-arrival order, which varies run to run, so fused scores
            # are not compared.
            hybrid_keys = [
                {bytes(row[i]): row[i + 1]
                 for i in range(0, len(row), 2)}[b"__key"]
                for row in hybrid[1:]]
            return {
                "search_content": content,
                "search_nocontent": (nocontent[0], sorted(nocontent[1:])),
                "hybrid": (hybrid[0], sorted(hybrid_keys)),
            }

        baseline = run_all()
        assert baseline["search_content"][0] == docs
        assert baseline["search_nocontent"][0] == docs
        assert baseline["hybrid"][0] > 0

        # Remote RPC arms per run_all: one per remote shard for each FT.SEARCH,
        # two (one per arm) for FT.HYBRID.
        arms_per_run = (self.CLUSTER_SIZE - 1) * (1 + 1 + 2)

        for threshold, background_reply in [
                ("30", "no"), ("1", "no"), ("30", "yes"), ("1", "yes")]:
            for node in self.nodes:
                node.client.execute_command(
                    "CONFIG", "SET", "search.async-fanout-threshold", threshold)
                node.client.execute_command(
                    "CONFIG", "SET", "search.background-shard-reply",
                    background_reply)
            for _ in range(5):
                before = sum_of_remote_searches(self.nodes)
                assert run_all() == baseline, (
                    f"threshold={threshold} background_reply={background_reply}")
                # A background reply records its metric after sending, so the
                # shard may count it just after the coordinator has replied.
                waiters.wait_for_equal(
                    lambda: sum_of_remote_searches(self.nodes),
                    before + arms_per_run, timeout=5)

def load_fingerprint_version_from_rdb(test):
    client = test.new_client_for_primary(0)
    index_name = "index1"

    assert client.execute_command("CONFIG SET search.info-developer-visible yes") == b"OK"

    assert client.execute_command(
        "FT.CREATE", index_name,
        "ON", "HASH",
        "PREFIX", "1", "doc:",
        "SCHEMA", "price", "NUMERIC"
    ) == b"OK"

    raw = client.execute_command("FT.INFO", index_name, "PRIMARY")
    parser = FTInfoParser([])
    info = parser._parse_key_value_list(raw)
    fingerprint = int(info["index_fingerprint"])
    assert fingerprint is not None
    version = int(info["index_version"])
    assert version == 0

    # Save RDB on all nodes
    for rg in test.replication_groups:
        rg.primary.client.execute_command("SAVE")
        
    # Restart all nodes in the cluster
    for rg in test.replication_groups:
        rg.primary.server.restart(remove_rdb=False)

    # validate all nodes
    for rg in test.replication_groups:
        # validate primary node
        pc = rg.primary.client
        pc.execute_command("CONFIG SET search.info-developer-visible yes")
        info = parser._parse_key_value_list(
            pc.execute_command("FT.INFO", index_name, "PRIMARY")
        )
        assert fingerprint == int(info["index_fingerprint"])
        assert version == int(info["index_version"])

        # validate all replica nodes
        for rep in rg.replicas:
            rc = rep.client
            rc.execute_command("CONFIG SET search.info-developer-visible yes")
            info = parser._parse_key_value_list(
                rc.execute_command("FT.INFO", index_name, "PRIMARY")
            )
            assert fingerprint == int(info["index_fingerprint"])
            assert version == int(info["index_version"])
        
class TestLoadFingerprintVersionFromRDB_v1_v1(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "no"
        return args
    def test_load_fingerprint_version_from_rdb_v1_v1(self):
        load_fingerprint_version_from_rdb(self)

class TestLoadFingerprintVersionFromRDB_v1_v2(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "yes"
        return args
    def test_load_fingerprint_version_from_rdb_v1_v2(self):
        load_fingerprint_version_from_rdb(self)

class TestLoadFingerprintVersionFromRDB_v2_v1(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "no"
        return args
    def test_load_fingerprint_version_from_rdb_v2_v1(self):
        load_fingerprint_version_from_rdb(self)

class TestLoadFingerprintVersionFromRDB_v2_v2(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args
    def test_load_fingerprint_version_from_rdb_v2_v2(self):
        load_fingerprint_version_from_rdb(self)
