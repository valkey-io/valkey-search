from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode, ValkeySearchClusterTestCase, Node
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from valkey.exceptions import ResponseError, ConnectionError
import json
import struct

import pytest
from indexes import *
from utils import IndexingTestHelper, run_in_thread
from ft_info_parser import FTInfoParser

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

    # --- Bounded content fan-out (fanout-bounded-content) -------------------
    #
    # With 3 shards and K = 10 each shard returns content for 6 of its 10
    # candidates; winners beyond that are fetched from their shard in a second
    # round (FetchContent). Every reply must match the single-round fan-out.

    def _bounded_set(self, value, clients=None):
        for client in clients or self.get_all_primary_clients():
            assert client.config_set("search.fanout-bounded-content", value)
            assert client.config_set("search.info-developer-visible", "yes")

    def _bounded_counter(self, name):
        return sum(c.info("SEARCH")[f"search_{name}"]
                   for c in self.get_all_primary_clients())

    def _bounded_load(self, name, prefix, on_json=False, count=90,
                      clients=None):
        cluster: ValkeyCluster = self.new_cluster_client()
        schema = BOUNDED_JSON_SCHEMA if on_json else BOUNDED_HASH_SCHEMA
        cluster.execute_command(
            "FT.CREATE", name, "ON", "JSON" if on_json else "HASH",
            "PREFIX", "1", prefix, *schema)
        keys = []
        for i in range(count):
            key = f"{prefix}{{hot}}{i}" if "skew" in name else f"{prefix}{i}"
            doc = bounded_doc(i)
            if on_json:
                cluster.execute_command("JSON.SET", key, "$", json.dumps(doc))
            else:
                cluster.hset(key, mapping={
                    "body": doc["body"], "n": doc["n"],
                    "vec": struct.pack("<2f", *doc["vec"])})
            keys.append(key)
        for client in clients or self.get_all_primary_clients():
            IndexingTestHelper.wait_for_backfill_complete_on_node(client, name)
        return keys

    def _bounded_owner(self, key):
        """Index of the primary holding `key`. Asked by slot: a primary that
        does not own the key would answer a key command with MOVED."""
        primaries = self.get_all_primary_clients()
        slot = primaries[0].execute_command("CLUSTER", "KEYSLOT", key)
        for i, client in enumerate(primaries):
            if client.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot) > 0:
                return i
        raise AssertionError(f"no primary holds {key}")

    def _bounded_replies(self, name, query, coordinators=None):
        return [bounded_rows(self.new_client_for_primary(i), name, query)
                for i in (coordinators or range(self.CLUSTER_SIZE))]

    def _assert_bounded_matches_single_round(self, names, queries,
                                             coordinators=None):
        self._bounded_set("no")
        expected = {(n, i): self._bounded_replies(n, q, coordinators)
                    for n in names for i, q in enumerate(queries)}
        self._bounded_set("yes")
        for n in names:
            for i, query in enumerate(queries):
                assert self._bounded_replies(n, query, coordinators) == (
                    expected[n, i]), (n, query)

    def test_bounded_content_matches_single_round(self):
        names = {"even_hash": False, "skew_hash": False,
                 "even_json": True, "skew_json": True}
        for name, on_json in names.items():
            self._bounded_load(name, f"{name}:", on_json)
        self._bounded_set("no")
        fetches_before = self._bounded_counter("bounded_content_fetch_requests")
        self._assert_bounded_matches_single_round(list(names), BOUNDED_QUERIES)
        # The skewed layouts must have exercised the FetchContent round.
        assert self._bounded_counter("bounded_content_fetch_requests") > fetches_before

    def test_bounded_content_uneven_slot_ownership(self):
        # Before loading: move 2000 empty slots from shard 2 to shard 0, so
        # the shards own 7461 / 5461 / 3462 slots and get different limits.
        primaries = self.get_all_primary_clients()
        dest_id = primaries[0].execute_command("CLUSTER MYID").decode()
        # The current owner first, so it stops claiming a slot before anyone
        # else moves it; otherwise its gossip (with a higher config epoch) can
        # hand the slot back on a node that has already moved it.
        source, others = primaries[2], [primaries[0], primaries[1]]
        for slot in range(10923, 12923):
            for client in [source] + others:
                client.execute_command("CLUSTER SETSLOT", slot, "NODE", dest_id)
        # Make the new owner's claim outrank any view that is still stale.
        primaries[0].execute_command("CLUSTER BUMPEPOCH")
        # Converged once every node agrees on which node owns each range.
        # ("CLUSTER", "SLOTS" as separate arguments returns the raw reply;
        # as one string the client parses it into another shape.)
        def ownership(client):
            return sorted((start, end, owner[2])
                          for start, end, owner, *_ in
                          client.execute_command("CLUSTER", "SLOTS"))
        def agreed():
            views = [ownership(c) for c in primaries]
            return all(view == views[0] for view in views)
        waiters.wait_for_true(agreed, on_failure=lambda: print(
            "slot ownership per node:", [ownership(c) for c in primaries]))
        self._bounded_load("even_hash", "even_hash:")
        self._assert_bounded_matches_single_round(["even_hash"],
                                                  BOUNDED_QUERIES)

    @pytest.mark.parametrize("setup_test", [{"replica_count": 1}],
                             indirect=True)
    def test_bounded_content_replica_targets(self):
        replicas = [rg.replicas[0].client for rg in self.replication_groups]
        self._bounded_load("skew_hash", "skew_hash:")
        for primary, replica in zip(self.get_all_primary_clients(), replicas):
            waiters.wait_for_true(
                lambda p=primary, r=replica: r.dbsize() == p.dbsize())
            waiters.wait_for_true(lambda r=replica: IndexingTestHelper
                                  .is_backfill_complete_on_node(r, "skew_hash"))
        for client in self.get_all_primary_clients():
            assert client.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
                "ForceReplicasOnly", "yes") == b"OK"
        self._assert_bounded_matches_single_round(["skew_hash"],
                                                  BOUNDED_QUERIES)

    def test_bounded_content_shard_without_content_limit(self):
        # A shard that predates content_limit returns content for every
        # candidate and never marks any as omitted, so a coordinator never
        # asks it for content again and the reply is unchanged. (Such a node
        # would not coordinate a bounded query itself, so only the others
        # coordinate here.)
        keys = self._bounded_load("skew_hash", "skew_hash:")
        owner_index = self._bounded_owner(keys[0])
        owner = self.get_all_primary_clients()[owner_index]
        coordinators = [i for i in range(self.CLUSTER_SIZE) if i != owner_index]
        assert owner.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceIgnoreContentLimit", "yes") == b"OK"
        self._bounded_set("no")
        fetches_before = self._bounded_counter("bounded_content_fetch_requests")
        self._assert_bounded_matches_single_round(["skew_hash"],
                                                  BOUNDED_QUERIES,
                                                  coordinators)
        assert self._bounded_counter("bounded_content_fetch_requests") == fetches_before

    def test_bounded_content_local_winners_fetched_in_process(self):
        # The coordinator owns all the data: its winners past its content
        # limit are fetched in its own process, as its search runs, never
        # through an RPC to itself.
        keys = self._bounded_load("skew_hash", "skew_hash:")
        owner_index = self._bounded_owner(keys[0])
        owner = self.get_all_primary_clients()[owner_index]
        coordinator = self.new_client_for_primary(owner_index)
        self._bounded_set("no")
        expected = bounded_rows(coordinator, "skew_hash", BOUNDED_QUERIES[0])
        self._bounded_set("yes")
        rpcs = lambda: owner.info("SEARCH")["search_fetch_content_rpc_requests"]
        rpcs_before = rpcs()
        fetches_before = self._bounded_counter("bounded_content_fetch_requests")
        assert bounded_rows(coordinator, "skew_hash",
                            BOUNDED_QUERIES[0]) == expected
        assert self._bounded_counter(
            "bounded_content_fetch_requests") > fetches_before
        assert rpcs() == rpcs_before

    def _bounded_skew_from_non_owner(self):
        """Skewed data, and a coordinator that must fetch from its owner."""
        keys = self._bounded_load("skew_hash", "skew_hash:")
        owner = self._bounded_owner(keys[0])
        coordinator = (owner + 1) % self.CLUSTER_SIZE
        self._bounded_set("no")
        winners = bounded_rows(self.new_client_for_primary(coordinator),
                               "skew_hash", BOUNDED_QUERIES[0])[1]
        self._bounded_set("yes")
        return (self.get_all_primary_clients()[owner],
                self.new_client_for_primary(coordinator), winners)

    def test_bounded_content_fetch_failure(self):
        owner, coordinator, winners = self._bounded_skew_from_non_owner()
        # Held set: gRPC retries UNAVAILABLE.
        assert owner.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceRemoteFailCount", MAX_RETRIES) == b"OK"
        try:
            query = BOUNDED_QUERIES[0]
            with pytest.raises(ResponseError):
                coordinator.execute_command(
                    "FT.SEARCH", "skew_hash", *query, "ALLSHARDS")
            # Partial results: the winners that came with content, in order.
            count, rows = bounded_rows(coordinator, "skew_hash",
                                       query + ["SOMESHARDS"])
            assert rows == winners[:len(rows)]
            assert 0 < len(rows) < len(winners)
            assert count == 90 - (len(winners) - len(rows))
        finally:
            owner.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
                "ForceRemoteFailCount", "0")

    def test_bounded_content_fetch_consistency_failure(self):
        owner, coordinator, winners = self._bounded_skew_from_non_owner()
        assert owner.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceFetchContentSlotMismatch", "yes") == b"OK"
        query = BOUNDED_QUERIES[0]
        with pytest.raises(ResponseError) as error:
            coordinator.execute_command(
                "FT.SEARCH", "skew_hash", *query, "SOMESHARDS", "CONSISTENT")
        assert "consistency check failed" in str(error.value)
        # Without consistency it is an ordinary shard failure.
        _, rows = bounded_rows(coordinator, "skew_hash",
                               query + ["SOMESHARDS", "INCONSISTENT"])
        assert rows == winners[:len(rows)]

    def test_bounded_content_mutation_between_rounds(self):
        owner, coordinator, winners = self._bounded_skew_from_non_owner()
        # The ranking a little past the window, before anything changes.
        self._bounded_set("no")
        ranking = bounded_rows(coordinator, "skew_hash",
                               ["@body:hello", "LIMIT", "0", "12"])[1]
        self._bounded_set("yes")
        # Winners 7..10 are past the owner's content limit of 6: fetched.
        deleted, changed = winners[6][0], winners[7][0]
        owner.execute_command("FT._DEBUG PAUSEPOINT SET fetch_content")
        thread, result, error = run_in_thread(
            lambda: bounded_rows(coordinator, "skew_hash", BOUNDED_QUERIES[0]))
        waiters.wait_for_true(lambda: owner.execute_command(
            "FT._DEBUG PAUSEPOINT TEST fetch_content") > 0)
        owner.execute_command("DEL", deleted)
        owner.hset(changed, "body", "no longer matches")
        IndexingTestHelper.wait_for_indexing_complete_on_node(owner, "skew_hash")
        owner.execute_command("FT._DEBUG PAUSEPOINT RESET fetch_content")
        thread.join()
        assert error[0] is None
        count, rows = result[0]
        keys = [key for key, _ in rows]
        assert deleted not in keys and changed not in keys
        # The next candidates take their places, as they do when a document
        # changes between a single-round search and its content fetch.
        assert rows == [row for row in ranking
                        if row[0] not in (deleted, changed)][:len(winners)]
        assert count == 90 - 2


BOUNDED_HASH_SCHEMA = [
    "SCHEMA", "body", "TEXT", "n", "NUMERIC",
    "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
    "DISTANCE_METRIC", "L2",
]
BOUNDED_JSON_SCHEMA = [
    "SCHEMA", "$.body", "AS", "body", "TEXT", "$.n", "AS", "n", "NUMERIC",
    "$.vec", "AS", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
    "DISTANCE_METRIC", "L2",
]
_BOUNDED_QUERY_VEC = struct.pack("<2f", 0.0, 0.0)
BOUNDED_QUERIES = [
    ["@body:hello", "LIMIT", "0", "10"],
    ["@body:hello", "LIMIT", "5", "10"],  # Offset window.
    ["@n:[0 1000]", "LIMIT", "0", "10", "RETURN", "1", "body"],  # Projection.
    ["*=>[KNN 10 @vec $q]", "PARAMS", "2", "q", _BOUNDED_QUERY_VEC,
     "DIALECT", "2"],
    ["@body:hello=>[KNN 10 @vec $q]", "PARAMS", "2", "q", _BOUNDED_QUERY_VEC,
     "DIALECT", "2"],  # Hybrid: KNN cut, then relevance order.
    ["@body:hello", "LIMIT", "0", "10", "WITHCURSOR", "COUNT", "3"],
]


def bounded_doc(i):
    # Distinct term counts and distances give a strict ranking.
    return {"body": " ".join(["hello"] * (1 + i % 7) + ["x"] * (i % 5)),
            "n": i, "vec": [float(i), float(i % 3)]}


def bounded_rows(client, name, query):
    """(total, [(key, {field: value})]) for an FT.SEARCH, paging through a
    cursor to its end. Field order within a document follows hash-map
    iteration and is not stable between identical queries, so it is compared
    as a dict."""
    as_row = lambda key, fields: (key, dict(zip(fields[0::2], fields[1::2])))
    reply = client.execute_command("FT.SEARCH", name, *query)
    if "WITHCURSOR" not in query:
        return reply[0], [as_row(reply[j], reply[j + 1])
                          for j in range(1, len(reply), 2)]
    total, rows, cursor = reply
    rows = [as_row(*row) for row in rows]
    while cursor != 0:
        batch, cursor = client.execute_command("FT.CURSOR", "READ", name, cursor)
        rows += [as_row(*row) for row in batch[1:]]
    return total, rows


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
