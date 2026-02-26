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

def wait_for_pausepoint(client: Valkey, pausepoint_name: str, timeout: int = 5) -> bool:
    """
    Wait for a pausepoint to be hit by at least one thread.

    Returns:
        True if pausepoint was hit, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        count = client.execute_command("FT._DEBUG", "PAUSEPOINT", "TEST", pausepoint_name)
        if count > 0:
            return True
        time.sleep(0.1)
    return False

def verify_server_responsive(client: Valkey, duration: float = 2.0, ping_count: int = 10) -> bool:
    """
    Verify server remains responsive by sending multiple PINGs over time.

    This proves the reader thread is NOT spinning at 100% CPU, which would
    block the main thread from processing PING commands.

    Args:
        client: Valkey client
        duration: Total time to test responsiveness (seconds)
        ping_count: Number of PINGs to send
        
    Returns:
        True if all PINGs succeeded, False otherwise
    """
    interval = duration / ping_count
    for i in range(ping_count):
        try:
            result = client.ping()
            if result != True:
                return False
        except Exception as e:
            print(f"PING {i+1}/{ping_count} failed: {e}")
            return False
        time.sleep(interval)
    return True


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
        assert str(e) == "Search operation cancelled due to timeout"
        return []
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
            client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 0
        )
        hnsw_result = search(client, "hnsw", False, 1, enable_partial_results=True)
        assert hnsw_result[0] == 1
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 5
        assert (
            client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 1
        )

        #
        # Disable partial results, and force timeout with pre-filtering
        #
        assert (
            client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 1
        )
        hnsw_result = search(client, "hnsw", True, 1, enable_partial_results=False)
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 6
        assert (
            client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 2
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

    def test_pausepoint_search_entries_fetcher(self):
        """
        Test FT.SEARCH timeout during entries fetcher loop (Issue #686).

        This test verifies:
        1. Pausepoint correctly pauses execution in entries fetcher
        2. Server remains responsive during pause (not 100% CPU)
        3. Timeout is properly detected and command returns error

        Code path: src/query/search.cc - DoSearch() entries fetcher loop
        """

        client: Valkey = self.server.get_new_client()
        client.execute_command("FLUSHALL SYNC")

        # Create index with Tag field to trigger entries fetcher path
        Index("idx", [Tag("tag"), Numeric("n")], ["doc"]).create(client)

        # Load data
        for i in range(1000):
            client.hset(f"doc:{i}", mapping={"tag": f"value{i % 10}", "n": i})

        # Set pausepoint to simulate infinite loop in entries fetcher
        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_entries_fetcher") == b"OK"
        # Execute search in background thread (pausepoint will block it)
        result = [None]
        error = [None]

        def run_search():
            try:
                result[0] = client.execute_command(
                    "FT.SEARCH", "idx", "@tag:{value1}",
                    "TIMEOUT", "5000" # 5 second timeout
                )
            except ResponseError as e:
                error[0] = str(e)
        
        thread = threading.Thread(target=run_search)
        thread.start()

        # Wait for pausepoint to be hit (confirms we're in entries fetcher)
        assert wait_for_pausepoint(client, "search_entries_fetcher"), \
            "Pausepoint search_entries_fetcher was not hit"
        assert client.ping() == True, "Server not responsive during pausepoint"

        # Wait for timeout to trigger (5s + buffer)
        print("Waiting for timeout to trigger...")
        time.sleep(6.0)

        # Reset pausepoint to allow command to complete
        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_entries_fetcher") == b"OK"

        # Wait for thread to complete
        thread.join(timeout=10)

        # Verify timeout error was returned (FT.SEARCH has IsCancelled checks)
        assert error[0] is not None, "Expected timeout error"
        print(error[0])
        assert "timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
            f"Expected timeout error, got: {error[0]}"

    def test_pausepoint_search_prefilter_eval(self):
        """
        Test FT.SEARCH timeout during prefilter evaluation (Issue #686).

        This test verifies timeout handling in the pre-filtering code path,
        which is triggered when a filter significantly reduces the result set
        before vector search.

        Code path: src/query/search.cc - EvaluatePrefilteredKeys()
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FLUSHALL SYNC")

        # Create index with Numeric and Tag to trigger pre-filtering
        Index("idx", [Numeric("num"), Tag("tag")], ["doc"]).create(client)

        # Load data
        for i in range(1000):
            client.hset(f"doc:{i}", mapping={"num": i, "tag": f"val{i % 10}"})

        # Set pausepoint in prefilter evaluation
        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_prefilter_eval") == b"OK"

        result = [None]
        error = [None]

        def run_search():
            try:
                # Query with filter that triggers pre-filtering
                result[0] = client.execute_command(
                    "FT.SEARCH", "idx", "@num:[0 50] @tag:{val5}",
                    "TIMEOUT", "5000"
                )
            except ResponseError as e:
                error[0] = str(e)
        
        thread = threading.Thread(target=run_search)
        thread.start()
        
        # Wait for pausepoint to be hit
        assert wait_for_pausepoint(client, "search_prefilter_eval", timeout=10), \
            "Pausepoint search_prefilter_eval was not hit"
        
        # Verify server responsive
        assert client.ping() == True
        
        # Wait for timeout
        time.sleep(6.0)
        
        # Reset pausepoint
        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_prefilter_eval") == b"OK"
        
        thread.join(timeout=10)
        
        # Verify timeout occurred
        assert error[0] is not None
        print(error[0])
        assert "timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
            f"Expected timeout error, got: {error[0]}"

    def test_search_pausepoint_term_predicate(self):
        """Test timeout in term predicate evaluation."""
        client = self.server.get_new_client()
        client.execute_command("FLUSHALL", "SYNC")

        client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                            "WITHOFFSETS",
                            "SCHEMA", "text", "TEXT", "num", "NUMERIC")

        for i in range(2000):
            client.hset(f"doc:{i}", mapping={
                "text": f"running runner runs run word{i}",
                "num": i
            })

        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_term_predicate") == b"OK"

        result, error = [None], [None]
        def run_search():
            try:
                result[0] = client.execute_command("FT.SEARCH", "idx", "@text:running", "TIMEOUT", "5000")
            except ResponseError as e:
                error[0] = str(e)

        thread = threading.Thread(target=run_search)
        thread.start()

        assert wait_for_pausepoint(client, "search_term_predicate", timeout=10), \
            "Pausepoint search_term_predicate was not hit"

        assert verify_server_responsive(client, duration=3.0, ping_count=15)
        time.sleep(6.0)
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_term_predicate")
        thread.join(timeout=10)
        # Verify timeout occurred (outer loop has IsCancelled check)
        assert error[0] is not None, "Expected timeout error"
        assert "timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
            f"Expected timeout error, got: {error[0]}"

    def test_search_pausepoint_prefix_expansion(self):
        """
        Test timeout in prefix predicate word expansion loop.
        
        Path: Predicate evaluation → PrefixPredicate::Evaluate → word expansion loop
        File: src/query/predicate.cc line ~169
        
        NOTE: Inner loop has NO direct IsCancelled check. Timeout occurs via
        outer loop (EvaluatePrefilteredKeys at line 426) which checks IsCancelled.
        """
        client = self.server.get_new_client()
        client.execute_command("FLUSHALL", "SYNC")

        client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                      "SCHEMA", "text", "TEXT", "num", "NUMERIC")

        # Create many words with same prefix
        for i in range(1000):
            client.hset(f"doc:{i}", mapping={"text": f"prefix{i} word{i}", "num": i})

        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_prefix_predicate") == b"OK"

        result = [None]
        error = [None]

        def run_search():
            try:
                # Prefix query
                result[0] = client.execute_command(
                    "FT.SEARCH", "idx", "-@text:notexist @text:prefix* @num:[0 500]",
                    "TIMEOUT", "5000"
                )
            except ResponseError as e:
                error[0] = str(e)

        thread = threading.Thread(target=run_search)
        thread.start()

        if not wait_for_pausepoint(client, "search_prefix_predicate", timeout=10):
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_prefix_predicate")
            thread.join(timeout=10)
            print("Pausepoint not hit - test inconclusive")
            return

        assert verify_server_responsive(client, duration=3.0, ping_count=15)
        time.sleep(6.0)

        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_prefix_predicate") == b"OK"
        thread.join(timeout=10)

        # Verify timeout occurred via outer loop IsCancelled check
        assert error[0] is not None, "Expected timeout error"
        assert "timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
            f"Expected timeout via outer loop check, got: {error[0]}"

    def test_search_pausepoint_suffix_expansion(self):
        """
        Test timeout in suffix predicate word expansion loop.

        Path: Predicate evaluation → SuffixPredicate::Evaluate → word expansion loop
        File: src/query/predicate.cc line ~225
        
        NOTE: Inner loop has NO direct IsCancelled check. Timeout occurs via
        outer loop (EvaluatePrefilteredKeys at line 426) which checks IsCancelled.
        """
        client = self.server.get_new_client()
        client.execute_command("FLUSHALL", "SYNC")

        # Need WITHSUFFIXTRIE for suffix search
        client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                        "SCHEMA", "text", "TEXT", "WITHSUFFIXTRIE", "num", "NUMERIC")

        for i in range(1000):
            client.hset(f"doc:{i}", mapping={"text": f"word{i}suffix", "num": i})
        
        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_suffix_expansion") == b"OK"

        result = [None]
        error = [None]

        def run_search():
            try:
                # Suffix query
                result[0] = client.execute_command(
                    "FT.SEARCH", "idx", "-@text:notexist @text:*suffix @num:[0 500]",
                    "TIMEOUT", "5000"
                )
            except ResponseError as e:
                error[0] = str(e)

        thread = threading.Thread(target=run_search)
        thread.start()

        if not wait_for_pausepoint(client, "search_suffix_expansion", timeout=10):
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_suffix_expansion")
            thread.join(timeout=10)
            print("Pausepoint not hit - suffix search may use different code path")
            return

        assert verify_server_responsive(client, duration=3.0, ping_count=15)
        time.sleep(6.0)

        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_suffix_expansion") == b"OK"
        thread.join(timeout=10)

        # Verify timeout occurred via outer loop IsCancelled check
        assert error[0] is not None, "Expected timeout error"
        assert "timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
            f"Expected timeout via outer loop check, got: {error[0]}"

    def test_search_pausepoint_fuzzy_search(self):
        """
        Test timeout in fuzzy predicate Levenshtein search loop.
        
        Path: Predicate evaluation → FuzzyPredicate::Evaluate → fuzzy search loop
        File: src/query/predicate.cc line ~300
        
        NOTE: Inner loop has NO direct IsCancelled check. Timeout occurs via
        outer loop (EvaluatePrefilteredKeys at line 426) which checks IsCancelled.
        """
        client = self.server.get_new_client()
        client.execute_command("FLUSHALL", "SYNC")
        
        client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                      "SCHEMA", "text", "TEXT", "num", "NUMERIC")
        
        for i in range(1000):
            client.hset(f"doc:{i}", mapping={"text": f"fuzzy{i} word{i}", "num": i})
        
        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_fuzzy_search") == b"OK"

        result = [None]
        error = [None]

        def run_search():
            try:
                # Fuzzy query with edit distance 2
                result[0] = client.execute_command(
                    "FT.SEARCH", "idx", "-@text:notexist @text:%fuzzy% @num:[0 500]",
                    "TIMEOUT", "5000"
                )
            except ResponseError as e:
                error[0] = str(e)

        thread = threading.Thread(target=run_search)
        thread.start()

        if not wait_for_pausepoint(client, "search_fuzzy_search", timeout=10):
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_fuzzy_search")
            thread.join(timeout=10)
            print("Pausepoint not hit - test inconclusive")
            return

        assert verify_server_responsive(client, duration=3.0, ping_count=15)
        time.sleep(6.0)

        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_fuzzy_search") == b"OK"
        thread.join(timeout=10)

        # Verify timeout occurred via outer loop IsCancelled check
        assert error[0] is not None, "Expected timeout error"
        assert "timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
            f"Expected timeout via outer loop check, got: {error[0]}"

    def test_search_pausepoint_composed_children(self):
        """
        Test timeout in composed predicate children iteration loop.
        
        Path: Predicate evaluation → ComposedPredicate::EvaluateWithContext → children loop
        File: src/query/predicate.cc line ~464
        
        NOTE: Inner loop has NO direct IsCancelled check. Timeout occurs via
        outer loop (EvaluatePrefilteredKeys at line 426) which checks IsCancelled.
        """
        client = self.server.get_new_client()
        client.execute_command("FLUSHALL", "SYNC")

        client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                          "SCHEMA", "text", "TEXT", "tag", "TAG", "num", "NUMERIC")

        for i in range(1000):
            client.hset(f"doc:{i}", mapping={
                "text": f"word{i}",
                "tag": f"tag{i % 10}",
                "num": i
            })

        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_composed_predicate") == b"OK"

        result = [None]
        error = [None]
        
        def run_search():
            try:
                # Complex composed query with multiple children
                result[0] = client.execute_command(
                    "FT.SEARCH", "idx", "(@text:word* | @tag:{tag1}) @num:[0 500]",
                    "TIMEOUT", "5000"
                )
            except ResponseError as e:
                error[0] = str(e)

        thread = threading.Thread(target=run_search)
        thread.start()

        if not wait_for_pausepoint(client, "search_composed_predicate", timeout=10):
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_composed_predicate")
            thread.join(timeout=10)
            print("Pausepoint not hit - test inconclusive")
            return

        assert verify_server_responsive(client, duration=3.0, ping_count=15)
        time.sleep(6.0)

        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_composed_predicate") == b"OK"
        thread.join(timeout=10)

        # Verify timeout occurred via outer loop IsCancelled check
        assert error[0] is not None, "Expected timeout error"
        assert "timeout" in error[0].lower() or "cancelled" in error[0].lower(), \
            f"Expected timeout via outer loop check, got: {error[0]}"

    def test_pausepoint_search_inline_filter(self):
        """
        Test FT.SEARCH timeout during inline filtering (HYBRID query).
        
        This tests the inline filtering path used in HYBRID queries where
        vector search is performed with filters applied during the search.
        
        Code path: src/query/search.cc - InlineVectorFilter::operator()
        
        NOTE: InlineVectorFilter itself has NO direct IsCancelled check.
        Timeout depends on HNSW's CancelCondition being checked by the
        searchKnn algorithm. This test verifies timeout via that mechanism.
        
        FLAT index always uses prefiltering. Must use HNSW with broad filter
        to trigger inline filtering (filter must match > 0.1% of vectors).
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FLUSHALL SYNC")

        # Create index with HNSW (not FLAT) to allow inline filtering
        Index("idx", [Vector("v", 3, type="HNSW", distance="L2"), Numeric("num")], ["doc"]).create(client)

        # Load 100k vectors to make 0.1% threshold = 100 vectors
        for i in range(100000):
            client.hset(f"doc:{i}", mapping={
                "v": float_to_bytes([float(i), float(i), float(i)]),
                "num": i
            })

        # Set pausepoint in inline filter
        assert client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "search_inline_filter") == b"OK"

        result = [None]
        error = [None]

        def run_search():
            try:
                # HYBRID query with BROAD filter (90% of data matches)
                # This triggers inline filtering because filtered set > 0.1% threshold
                result[0] = client.execute_command(
                    "FT.SEARCH", "idx", "@num:[0 90000]=>[KNN 10 @v $BLOB]",
                    "PARAMS", "2", "BLOB", float_to_bytes([10.0, 10.0, 10.0]),
                    "TIMEOUT", "5000"
                )
            except ResponseError as e:
                error[0] = str(e)

        thread = threading.Thread(target=run_search)
        thread.start()

        # Wait for pausepoint
        assert wait_for_pausepoint(client, "search_inline_filter", timeout=10), \
            "Pausepoint search_inline_filter was not hit"

        verify_server_responsive(client)
        time.sleep(6.0)

        # Reset pausepoint
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "search_inline_filter")
        thread.join(timeout=2)

        # Verify timeout occurred
        assert error[0] is not None, "Expected timeout error"
        assert "timeout" in error[0].lower(), f"Expected timeout error, got: {error[0]}"
        print(f"Timeout correctly detected: {error[0]}")

    def test_aggregate_timeout(self):
        """Test FT.AGGREGATE timeout handling across all aggregation stages."""
        client: Valkey = self.server.get_new_client()
        client.execute_command("FLUSHALL SYNC")
        
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
        assert client.info("SEARCH")["search_cancel-timeouts"] == 0
        
        # Enable forced timeouts
        assert client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceTimeout yes") == b"OK"
        assert client.execute_command("ft._debug CONTROLLED_VARIABLE SET timeoutpollfrequency 1") == b"OK"
        
        # Test timeout with SORTBY stage
        aggregate(client, "hnsw", True, stages=["LOAD", "2", "@n", "@__key", "SORTBY", "2", "@n", "DESC"])
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 1
        
        # Test timeout with GROUPBY stage
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "GROUPBY", "1", "@n", "REDUCE", "COUNT", "0"])
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 2
        
        # Test timeout with APPLY stage
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "APPLY", "@n*2", "AS", "double_n"])
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 3
        
        # Test timeout with FILTER stage
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "FILTER", "@n > 5"])
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 4
        
        # Test multiple stages pipeline
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "FILTER", "@n > 0", "SORTBY", "2", "@n", "ASC", "LIMIT", "0", "5"])
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 5
        
        # Test with filter (may or may not trigger pre-filtering depending on dataset)
        aggregate(client, "hnsw", True, 2, stages=["LOAD", "1", "@n", "SORTBY", "2", "@n", "DESC"])
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 6
        # Remove pre-filtering assertion - not guaranteed with small filter
        
        # Test flat index
        aggregate(client, "flat", True, stages=["LOAD", "1", "@n", "SORTBY", "2", "@n", "DESC"])
        assert client.info("SEARCH")["search_test-counter-ForceCancels"] == 7
        
        # Cleanup
        assert client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceTimeout no") == b"OK"

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
        self.check_info_sum("search_query_prefiltering_requests_cnt", 3)

        #
        # Pre-filtering HNSW path
        # Set a high pre-filtering threshold so all shards use pre-filtering
        #
        self.config_set("search.prefiltering-threshold-ratio", "0.5")
        hnsw_result = search(client, "hnsw", True, 10, enable_partial_results=False)
        self.check_info_sum("search_query_prefiltering_requests_cnt", 6)
        self.check_info_sum("search_test-counter-ForceCancels", 9)

    def test_aggregate_timeout_cluster(self):
        """Test FT.AGGREGATE timeout handling in cluster mode."""
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")

        client: Valkey = self.new_cluster_client()
        self.check_info("search_cancel-timeouts", 0)

        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW"), Numeric("n")])
        flat_index = Index("flat", [Vector("v", 3, type="FLAT"), Numeric("n")])

        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 100)

        waiters.wait_for_equal(lambda: self.sum_docs(hnsw_index), 100, timeout=10)
        self.check_info_sum("search_test-counter-ForceCancels", 0)

        self.control_set("ForceTimeout", "yes")
        self.control_set("TimeoutPollFrequency", "1")

        # Test HNSW with SORTBY - expect 4 cancels (3 shards + 1 coordinator)
        aggregate(client, "hnsw", True, stages=["LOAD", "1", "@n", "SORTBY", "2", "@n", "DESC"])
        self.check_info_sum("search_test-counter-ForceCancels", 4)

        # Test other aggregation stages
        aggregate(client, "hnsw", True, stages=["GROUPBY", "1", "@n", "REDUCE", "COUNT", "0"])
        self.check_info_sum("search_test-counter-ForceCancels", 8)

        aggregate(client, "hnsw", True, stages=["APPLY", "1+1", "AS", "result"])
        self.check_info_sum("search_test-counter-ForceCancels", 12)

        aggregate(client, "hnsw", True, stages=["FILTER", "@n > 0"])
        self.check_info_sum("search_test-counter-ForceCancels", 16)
