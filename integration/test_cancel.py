from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging
from typing import Any, Union
from valkeytestframework.util import waiters
import threading
import time
from utils import wait_for_pausepoint

def canceller(client, client_id):
    my_id = client.execute_command("client id")
    assert my_id != client_id
    client.execute_command("client kill id ", client_id)


def search_command(index: str, filter: Union[int, None], enable_partial_results: bool = True, enable_consistency: bool = False) -> list[str]:
    predicate = "*" if filter is None else f"(@n:[0 {filter}])"
    return [
        "FT.SEARCH",
        index,
        predicate + "=>[KNN 10 @v $BLOB]",
        "PARAMS",
        "2",
        "BLOB",
        float_to_bytes([10.0, 10.0, 10.0]),
        "TIMEOUT",
        "10",
        "SOMESHARDS" if enable_partial_results else "ALLSHARDS",
        "CONSISTENT" if enable_consistency else "INCONSISTENT"
    ]



def search(
    client: valkey.client,
    index: str,
    timeout: bool,
    filter: Union[int, None] = None,
    enable_partial_results: bool = True,
    expect_consistency_error = False,
    enable_consistency: bool = False
) -> list[tuple[str, float]]:
    print("Search command: ", search_command(index, filter, enable_partial_results, enable_consistency))
    if expect_consistency_error:
        try:
            x = client.execute_command(*search_command(index, filter, enable_partial_results, enable_consistency))
            assert False, "Expected error, but got result: " + str(x)
        except ResponseError as e:
            print(e)
            assert str(e) == "Index or slot consistency check failed"
        return []
    elif not timeout:
        return client.execute_command(*search_command(index, filter, enable_partial_results, enable_consistency))
    else:
        try:
            x = client.execute_command(*search_command(index, filter, enable_partial_results, enable_consistency))
            assert False, "Expected timeout, but got result: " + str(x)
        except ResponseError as e:
            assert str(e) == "Search operation cancelled due to timeout"
        return []

def aggregate_command(index: str, filter: Union[int, None], stages: list[str] = None) -> list[str]:
    """Build FT.AGGREGATE command with specified stages and timeout."""
    predicate = "*" if filter is None else f"(@n:[0 {filter}])"
    cmd = [
        "FT.AGGREGATE",
        index,
        predicate + "=>[KNN 10 @v $BLOB]",
        "PARAMS",
        "2",
        "BLOB",
        float_to_bytes([10.0, 10.0, 10.0]),
        "TIMEOUT",
        "10"
    ]
    
    if stages:
        cmd.extend(stages)
    
    return cmd

def aggregate(
    client: valkey.client,
    index: str,
    expect_timeout: bool,
    filter: Union[int, None] = None,
    stages: list[str] = None
) -> list:
    """Execute FT.AGGREGATE and handle timeout/error cases."""
    cmd = aggregate_command(index, filter, stages)
    
    try:
        result = client.execute_command(*cmd)
        if expect_timeout:
            assert False, f"Expected timeout but got result: {result}"
        return result
    except ResponseError as e:
        if not expect_timeout:
            raise
        error_msg = str(e)
        assert "Aggregate operation cancelled due to timeout" in error_msg, f"Expected timeout error, got: {error_msg}"
        return []


def run_pausepoint_timeout_test(self, pausepoint_name, setup_fn, search_cmd):
    """
    Shared helper for pausepoint-based timeout tests.

    Verifies:
    1. Pausepoint is hit (proves the code path is exercised)
    2. Server remains responsive (not spinning at 100% CPU)
    3. Timeout fires and command returns error

    Args:
        pausepoint_name: Name of the pausepoint to set
        setup_fn: Callable(client) that creates indexes and loads data
        search_cmd: List of command args to execute as the search.
    """
    client = self.server.get_new_client()

    setup_fn(client)

    assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", pausepoint_name) == b"OK"

    error = [None]
    def run_search():
        tc = self.server.get_new_client()
        try:
            tc.execute_command(*search_cmd)
        except ResponseError as e:
            error[0] = str(e)
        finally:
            tc.close()

    thread = threading.Thread(target=run_search)
    thread.start()

    assert wait_for_pausepoint(client, pausepoint_name, timeout=10), \
        f"Pausepoint {pausepoint_name} was not hit"
    assert client.ping() == True, "Server not responsive while pausepoint is held"

    thread.join()

    assert error[0] is not None, f"Expected timeout error for pausepoint {pausepoint_name}"
    assert "search operation cancelled due to timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
        f"Expected timeout error, got: {error[0]}"
    client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", pausepoint_name)
    time.sleep(1)

class TestCancelCMD(ValkeySearchTestCaseDebugMode):

    def test_timeoutCMD(self):
        """
        Test CMD timeout logic
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FLUSHALL SYNC")
        # po
        assert (
            client.execute_command(
                "CONFIG SET search.info-developer-visible yes"
            )
            == b"OK"
        )
        assert client.info("SEARCH")["search_cancel-timeouts"] == 0
        hnsw_index = Index(
            "hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")]
        )
        flat_index = Index("flat", [Vector("v", 3, type="FLAT"), Numeric("n")])

        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 1000)

        #
        # Nominal case
        #
        nominal_hnsw_result = search(client, "hnsw", False)
        nominal_flat_result = search(client, "flat", False)

        assert client.info("SEARCH")["search_cancel-timeouts"] == 0
        assert nominal_hnsw_result[0] == 10
        assert nominal_flat_result[0] == 10

        #
        # Now, force timeouts quickly
        #
        assert (
            client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceTimeout yes")
            == b"OK"
        )
        assert (
            client.execute_command("ft._debug CONTROLLED_VARIABLE SET timeoutpollfrequency 1")
            == b"OK"
        )

        #
        # Enable timeout path, no error but message result
        #
        hnsw_result = search(client, "hnsw", True, None, enable_partial_results=False)
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 1

        flat_result = search(client, "flat", True, None, enable_partial_results=False)
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 2

        #
        # Enable partial results
        #
        hnsw_result = search(client, "hnsw", False, None, enable_partial_results=True)
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 3
        assert hnsw_result != nominal_hnsw_result

        flat_result = search(client, "flat", False, None, enable_partial_results=True)
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 4
        assert flat_result != nominal_flat_result

        #
        # Now, test pre-filtering case.
        #
        assert (
            client.info("SEARCH")["search_prefiltering_requests_count"] == 0
        )
        hnsw_result = search(client, "hnsw", False, 1, enable_partial_results=True)
        assert hnsw_result[0] == 1
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 5
        assert (
            client.info("SEARCH")["search_prefiltering_requests_count"] == 1
        )

        #
        # Disable partial results, and force timeout with pre-filtering
        #
        assert (
            client.info("SEARCH")["search_prefiltering_requests_count"] == 1
        )
        hnsw_result = search(client, "hnsw", True, 1, enable_partial_results=False)
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 6
        assert (
            client.info("SEARCH")["search_prefiltering_requests_count"] == 2
        )
        assert hnsw_result != nominal_hnsw_result

        #
        # Now force the race the other way, i.e., force a timeout via Valkey
        #
        assert (
            client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceTimeout no")
            == b"OK"
        )
        assert (
            client.execute_command("FT._DEBUG PAUSEPOINT SET Cancel")
            == b"OK"
        )
        assert(client.execute_command("FT._DEBUG PAUSEPOINT LIST") == [b"Cancel", []])

        hnsw_result = search(client, "hnsw", True, 2, enable_partial_results=False)
        waiters.wait_for_true(lambda: client.execute_command("FT._DEBUG PAUSEPOINT TEST Cancel") > 0)
        w = client.execute_command("FT._DEBUG PAUSEPOINT LIST")
        assert(w[0] == b'Cancel')
        assert(len(w[1]) > 0)
        assert (
            client.execute_command("FT._DEBUG PAUSEPOINT RESET Cancel")
            == b"OK"
        )
        assert(client.execute_command("FT._DEBUG PAUSEPOINT LIST") == [])

    def test_pausepoint_entries_fetcher(self):
        """
        Test timeout in entries fetcher loop (Issue #686 path 1).
        Path: SearchNonVectorQuery → entries fetcher iterator loop
        """
        def setup(client):
            Index("idx", [Tag("tag"), Numeric("n")], ["doc"]).create(client)
            for i in range(1000):
                client.hset(f"doc:{i}", mapping={"tag": f"value{i % 10}", "n": i})

        run_pausepoint_timeout_test(
            self, "search_entries_fetcher", setup,
            ["FT.SEARCH", "idx", "@tag:{value1}", "TIMEOUT", "5000"]
        )

    def test_pausepoint_prefilter_eval(self):
        """
        Test timeout in prefilter evaluation loop
        """
        def setup(client):
            Index("idx", [Numeric("num"), Tag("tag")], ["doc"]).create(client)
            for i in range(1000):
                client.hset(f"doc:{i}", mapping={"num": i, "tag": f"val{i % 10}"})

        run_pausepoint_timeout_test(
            self, "search_prefilter_eval", setup,
            ["FT.SEARCH", "idx", "@num:[0 50] @tag:{val5}", "TIMEOUT", "5000"]
        )

    def test_pausepoint_inline_filter(self):
        """
        Test timeout in HNSW inline filter callback
        """
        def setup(client):
            Index("idx", [Vector("v", 3, type="HNSW", distance="L2"), Numeric("num")], ["doc"]).create(client)
            for i in range(10000):
                client.hset(f"doc:{i}", mapping={
                    "v": float_to_bytes([float(i), float(i), float(i)]),
                    "num": i
                })

        run_pausepoint_timeout_test(
            self, "search_inline_filter", setup,
            ["FT.SEARCH", "idx", "@num:[0 9000]=>[KNN 10 @v $BLOB]",
             "PARAMS", "2", "BLOB", float_to_bytes([10.0, 10.0, 10.0]),
             "TIMEOUT", "5000"]
        )

    def test_pausepoint_term_predicate(self):
        """
        Test timeout in term predicate stem variant evaluation
        """
        def setup(client):
            client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                                "SCHEMA", "text", "TEXT", "num", "NUMERIC")
            for i in range(2000):
                # No literal "run" or "ran" - only stem variants like "running", "runs"
                client.hset(f"doc:{i}", mapping={
                    "text": f"running runner runs word{i}",
                    "num": i
                })

        run_pausepoint_timeout_test(
            self, "search_term_predicate", setup,
            ["FT.SEARCH", "idx", "-@num:[-inf 0] @text:ran", "TIMEOUT", "5000"]
        )


    def test_pausepoint_prefix_predicate(self):
        """
        Test timeout in prefix predicate word expansion
        """
        def setup(client):
            client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                                  "SCHEMA", "text", "TEXT", "num", "NUMERIC")
            for i in range(1000):
                client.hset(f"doc:{i}", mapping={"text": f"prefix{i} word{i}", "num": i})

        run_pausepoint_timeout_test(
            self, "search_prefix_predicate", setup,
            ["FT.SEARCH", "idx", "-@text:notexist @text:prefix* @num:[0 500]", "TIMEOUT", "5000"]
        )

    def test_pausepoint_suffix_predicate(self):
        """
        Test timeout in suffix predicate word expansion
        """
        def setup(client):
            client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                                  "SCHEMA", "text", "TEXT", "WITHSUFFIXTRIE", "num", "NUMERIC")
            for i in range(1000):
                client.hset(f"doc:{i}", mapping={"text": f"word{i}suffix", "num": i})

        run_pausepoint_timeout_test(
            self, "search_suffix_expansion", setup,
            ["FT.SEARCH", "idx", "-@text:notexist @text:*suffix @num:[0 500]", "TIMEOUT", "5000"]
        )

    def test_pausepoint_fuzzy_predicate(self):
        """
        Test timeout in fuzzy predicate search loop
        """
        def setup(client):
            client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                                  "SCHEMA", "text", "TEXT", "num", "NUMERIC")
            for i in range(1000):
                client.hset(f"doc:{i}", mapping={"text": f"fuzzy{i} word{i}", "num": i})

        run_pausepoint_timeout_test(
            self, "search_fuzzy_search", setup,
            ["FT.SEARCH", "idx", "-@text:notexist @text:%fuzzy% @num:[0 500]", "TIMEOUT", "5000"]
        )

    def test_pausepoint_composed_predicate(self):
        """
        Test timeout in composed predicate children iteration
        """
        def setup(client):
            client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                                  "SCHEMA", "text", "TEXT", "tag", "TAG", "num", "NUMERIC")
            for i in range(1000):
                client.hset(f"doc:{i}", mapping={
                    "text": f"word{i}",
                    "tag": f"tag{i % 10}",
                    "num": i
                })

        run_pausepoint_timeout_test(
            self, "search_composed_predicate", setup,
            ["FT.SEARCH", "idx", "(@text:word* | @tag:{tag1}) @num:[0 500]", "TIMEOUT", "5000"]
        )

    def test_aggregate_timeout(self):
        """Test FT.AGGREGATE timeout handling across all aggregation stages."""
        client: Valkey = self.server.get_new_client()
        
        assert client.execute_command("CONFIG SET search.info-developer-visible yes") == b"OK"
        
        # Create indexes
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
        flat_index = Index("flat", [Vector("v", 3, type="FLAT"), Numeric("n")])
        
        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 1000)
        
        # Baseline - verify normal operation
        nominal_hnsw = aggregate(client, "hnsw", False, stages=["LOAD", "1", "@n"])
        nominal_flat = aggregate(client, "flat", False, stages=["LOAD", "1", "@n"])
        assert nominal_hnsw[0] == 10
        assert nominal_flat[0] == 10
        
        # Enable forced timeouts
        assert client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceTimeoutAggregate yes") == b"OK"
        
        # Test timeout with SORTBY stage
        aggregate(client, "hnsw", True, stages=["LOAD", "2", "@n", "@__key", "SORTBY", "2", "@n", "DESC"])
        assert client.info("SEARCH")["search_test-counter-ForceTimeoutAggregateCancels"] == 1
        
        # Test timeout with GROUPBY stage
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "GROUPBY", "1", "@n", "REDUCE", "COUNT", "0"])
        assert client.info("SEARCH")["search_test-counter-ForceTimeoutAggregateCancels"] == 2
        
        # Test timeout with APPLY stage
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "APPLY", "@n*2", "AS", "double_n"])
        assert client.info("SEARCH")["search_test-counter-ForceTimeoutAggregateCancels"] == 3
        
        # Test timeout with FILTER stage
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "FILTER", "@n > 5"])
        assert client.info("SEARCH")["search_test-counter-ForceTimeoutAggregateCancels"] == 4
        
        # Test multiple stages pipeline
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "FILTER", "@n > 0", "SORTBY", "2", "@n", "ASC", "LIMIT", "0", "5"])
        assert client.info("SEARCH")["search_test-counter-ForceTimeoutAggregateCancels"] == 5
        
        aggregate(client, "hnsw", True, 2, stages=["LOAD", "1", "@n", "SORTBY", "2", "@n", "DESC"])
        assert client.info("SEARCH")["search_test-counter-ForceTimeoutAggregateCancels"] == 6
        
        aggregate(client, "flat", True, stages=["LOAD", "1", "@n", "SORTBY", "2", "@n", "DESC"])
        assert client.info("SEARCH")["search_test-counter-ForceTimeoutAggregateCancels"] == 7
        
        # Cleanup
        assert client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceTimeoutAggregate no") == b"OK"

class TestCancelCME(ValkeySearchClusterTestCaseDebugMode):

    def execute_primaries(self, command: Union[str, list[str]]) -> list[Any]:
        return [
            self.client_for_primary(i).execute_command(*command)
            for i in range(len(self.replication_groups))
        ]

    def config_set(self, config: str, value: str):
        assert self.execute_primaries(["config set", config, value]) == [True] * len(
            self.replication_groups
        )

    def control_set(self, key:str, value:str):
        assert all(
            [node.client.execute_command(*["ft._debug", "CONTROLLED_VARIABLE", "set", key, value]) == b"OK" for node in self.nodes]
        )

    def check_info(self, name: str, value: Union[str, int]):
        results = self.execute_primaries(["INFO", "SEARCH"])
        failed = False
        for ix, r in enumerate(results):
            if r[name] != value:
                print(
                    name,
                    " Expected:",
                    value,
                    " Received:",
                    r[name],
                    " on server:",
                    ix,
                )
                failed = True
        assert not failed
    
    def _check_info_sum(self, name: str) -> int:
        """Sum the values of a given info field across all servers"""
        results = self.execute_primaries(["INFO", "SEARCH"])
        return sum([int(r[name]) for r in results if name in r])

    def check_info_sum(self, name: str, sum_value: int):
        """Sum the values of a given info field across all servers"""
        waiters.wait_for_equal(
          lambda: self._check_info_sum(name), 
          sum_value, 
          timeout=5
        )
    
    def sum_docs(self, index: Index) -> int:
        return sum([index.info(self.client_for_primary(i)).num_docs for i in range(len(self.replication_groups))])

    def test_timeoutCME(self):
        self.execute_primaries(["flushall sync"])

        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()
        self.check_info("search_cancel-timeouts", 0)

        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW"), Numeric("n")])
        flat_index = Index("flat", [Vector("v", 3, type="FLAT"), Numeric("n")])

        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 100)
        # Let the index properly processed
        waiters.wait_for_equal(lambda: self.sum_docs(hnsw_index), 100, timeout=10)

        #
        # Nominal case
        #
        cluster = self.get_primary(0).get_new_client()
        # hnsw_result = search(cluster, "hnsw", False)
        # flat_result = search(cluster, "flat", False)

        self.check_info_sum("search_test-counter-ForceCancels", 0)

        # assert hnsw_result[0] == 10
        # assert flat_result[0] == 10
        #
        # Now, force timeouts quickly
        #
        self.control_set("ForceTimeout", "yes")
        self.control_set("TimeoutPollFrequency", "1")

        #
        # Normal HNSW path
        #
        hnsw_result = search(client, "hnsw", True, None, enable_partial_results=False)
        self.check_info_sum("search_test-counter-ForceCancels", 3)

        #
        # Pre-filtering FLAT path (flat always uses pre-filtering)
        #
        flat_result = search(client, "flat", True, 10, enable_partial_results=False)
        self.check_info_sum("search_test-counter-ForceCancels", 6)
        self.check_info_sum("search_prefiltering_requests_count", 3)

        #
        # Pre-filtering HNSW path
        # Set a high pre-filtering threshold so all shards use pre-filtering
        #
        self.config_set("search.prefiltering-threshold-ratio", "0.5")
        hnsw_result = search(client, "hnsw", True, 10, enable_partial_results=False)
        self.check_info_sum("search_prefiltering_requests_count", 6)
        self.check_info_sum("search_test-counter-ForceCancels", 9)

    def test_aggregate_timeout_cluster(self):
        """Test FT.AGGREGATE timeout handling in cluster mode."""
        self.config_set("search.info-developer-visible", "yes")

        client: Valkey = self.new_cluster_client()

        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW"), Numeric("n")])
        flat_index = Index("flat", [Vector("v", 3, type="FLAT"), Numeric("n")])

        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 1000)

        waiters.wait_for_equal(lambda: self.sum_docs(hnsw_index), 1000, timeout=10)
        self.check_info_sum("search_test-counter-ForceTimeoutAggregateCancels", 0)

        self.control_set("ForceTimeoutAggregate", "yes")

        # Test HNSW with SORTBY - expect 4 cancels (3 shards + 1 coordinator)
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "SORTBY", "2", "@n", "DESC"])
        self.check_info_sum("search_test-counter-ForceTimeoutAggregateCancels", 1)

        # Test other aggregation stages
        aggregate(client, "hnsw", True, stages=["GROUPBY", "1", "@n", "REDUCE", "COUNT", "0"])
        self.check_info_sum("search_test-counter-ForceTimeoutAggregateCancels", 2)

        aggregate(client, "hnsw", True, stages=["APPLY", "1+1", "AS", "result"])
        self.check_info_sum("search_test-counter-ForceTimeoutAggregateCancels", 3)

        aggregate(client, "hnsw", True, stages=["FILTER", "@n > 0"])
        self.check_info_sum("search_test-counter-ForceTimeoutAggregateCancels", 4)
        self.control_set("ForceTimeoutAggregate", "no")
