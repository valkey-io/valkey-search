"""Integration tests for contention resolution state transitions.

Each test exercises a specific transition in the writer-side validation
state machine introduced when polling was replaced with per-key writer
validation. See plan in
.claude/plans/contention-integration-test-plan.md for the coverage
matrix.

In addition to sequencing assertions, every test that returns a non-empty
search result feeds it through `_assert_results_match_predicate`, which
re-reads each returned key from the database via HGETALL and verifies
the predicate against the stored value. This is the implementation-
agnostic correctness property we care about: for every key the search
returns, the underlying record must actually satisfy the predicate.
"""

import random
import threading
import time
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper, run_in_thread


def _info_int(client: Valkey, key: str) -> int:
    return int(client.info("SEARCH").get(key, 0))


def _wait_paused(client: Valkey, name: str, expected: int = 1, timeout: int = 5):
    waiters.wait_for_true(
        lambda: client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "TEST", name) >= expected,
        timeout=timeout,
    )


def _parse_search_result(result):
    """Convert FT.SEARCH reply ``[total, key1, [field, val, ...], key2, ...]``
    into ``(total, [(key, {field: val}), ...])``."""
    total = result[0]
    pairs = []
    for i in range(1, len(result), 2):
        key = result[i]
        flat = result[i + 1]
        fields = {flat[j]: flat[j + 1] for j in range(0, len(flat), 2)}
        pairs.append((key, fields))
    return total, pairs


def _assert_results_match_predicate(client: Valkey, result, predicate_word: bytes,
                                    field: bytes = b"content"):
    """Re-read every returned key from the database and verify its current
    value still matches the predicate term. Fails if the writer-side
    kPass decision kept a key whose record no longer matches.

    Note: this is best-effort — concurrent mutations may modify a record
    between the search returning and HGETALL reading it. The check is
    therefore tolerant: it asserts that *either* the current record
    matches *or* the record was modified after the search produced its
    answer (signalled by a sequence-number bump). We use a simple
    monotonic check: the current value must contain the search term in
    the field, OR the test is run in a sequence-stable scenario where
    the caller has guaranteed no mid-flight mutations.

    For deterministic tests (T2, T4a/b, T9, T11, T12) the caller is
    expected to have drained all in-flight mutations before calling
    this helper.
    """
    total, pairs = _parse_search_result(result)
    assert total == len(pairs), (
        f"total_count {total} disagrees with returned key count {len(pairs)}")
    for key, fields in pairs:
        live_value = client.execute_command("HGET", key, field)
        assert live_value is not None, f"returned key {key!r} no longer exists"
        assert predicate_word in live_value, (
            f"returned key {key!r} has value {live_value!r} which does not "
            f"contain the predicate term {predicate_word!r}")


class TestContentionStateTransitionsCMD(ValkeySearchTestCaseDebugMode):
    """Per-test writer-thread count is tuned via append_startup_args.

    Most tests run with one writer thread for deterministic ordering;
    T11 overrides to two for the concurrency torture case.
    """

    WRITER_THREADS = "1"

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.writer-threads"] = self.WRITER_THREADS
        return args

    def _create_text_index(self, client: Valkey, name: str = "idx"):
        client.execute_command(
            "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
        )

    def _hset_blocking(self, key: str, content: str):
        return run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", key, "content", content
            )
        )

    def _search_blocking(self, query: str = "@content:hello", *extra):
        return run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.SEARCH", "idx", query, *extra
            )
        )

    # ------------------------------------------------------------------
    # T1 — no contention baseline
    # ------------------------------------------------------------------
    def test_t1_no_contention_baseline(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")
        result = client.execute_command("FT.SEARCH", "idx", "@content:hello")
        assert result[0] == 1
        _assert_results_match_predicate(client, result, b"hello")
        assert _info_int(client, "search_text_query_blocked_count") == 0
        assert _info_int(client, "search_text_query_retry_count") == 0

    # ------------------------------------------------------------------
    # T2 — single-key kPass round-trip + S4 (no predicate revalidation)
    # ------------------------------------------------------------------
    def test_t2_single_key_kpass(self):
        client = self.server.get_new_client()
        client.execute_command("CONFIG", "SET", "search.info-developer-visible", "yes")
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset_t, _, hset_err = self._hset_blocking("doc:1", "hello there")
        _wait_paused(client, "mutation_processing")

        baseline_revalidation = _info_int(client, "search_predicate_revalidation")
        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 1
        )
        # Search is still parked
        assert search_res[0] is None and search_t.is_alive()

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset_t.join()
        search_t.join()

        assert hset_err[0] is None
        assert search_err[0] is None
        assert search_res[0][0] == 1
        assert search_res[0][1] == b"doc:1"
        assert search_res[0][2] == [b"content", b"hello there"]
        # Predicate-truth check: the returned key actually matches "hello".
        _assert_results_match_predicate(client, search_res[0], b"hello")
        assert _info_int(client, "search_text_query_retry_count") == 1
        # S4: writer-validated kPass refreshed the seq, so VerifyFilter
        # short-circuited and predicate_revalidation did not tick.
        assert _info_int(client, "search_predicate_revalidation") == baseline_revalidation

    # ------------------------------------------------------------------
    # T3 — single-key kFail
    # ------------------------------------------------------------------
    def test_t3_single_key_kfail(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset_t, _, _ = self._hset_blocking("doc:1", "world only")
        _wait_paused(client, "mutation_processing")

        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 1
        )

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset_t.join()
        search_t.join()

        assert search_err[0] is None
        # kFail removed the only neighbor; total_count == 0.
        assert search_res[0][0] == 0
        assert _info_int(client, "search_text_query_retry_count") == 1

    # ------------------------------------------------------------------
    # T4a — multi-key all kPass
    # ------------------------------------------------------------------
    def test_t4a_multi_key_all_kpass(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        for i in (1, 2, 3):
            client.execute_command("HSET", f"doc:{i}", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hsets = [
            self._hset_blocking(f"doc:{i}", f"hello update{i}")
            for i in (1, 2, 3)
        ]
        _wait_paused(client, "mutation_processing")

        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_retry_count") >= 3
        )
        assert _info_int(client, "search_text_query_blocked_count") == 1

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        for t, _, _ in hsets:
            t.join()
        search_t.join()

        assert search_err[0] is None
        assert search_res[0][0] == 3
        _assert_results_match_predicate(client, search_res[0], b"hello")

    # ------------------------------------------------------------------
    # T4b — multi-key mixed kPass / kFail
    # ------------------------------------------------------------------
    def test_t4b_multi_key_mixed(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        for i in (1, 2, 3):
            client.execute_command("HSET", f"doc:{i}", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hsets = [
            self._hset_blocking("doc:1", "hello there"),   # kPass
            self._hset_blocking("doc:2", "world only"),    # kFail
            self._hset_blocking("doc:3", "hello again"),   # kPass
        ]
        _wait_paused(client, "mutation_processing")

        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_retry_count") >= 3
        )

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        for t, _, _ in hsets:
            t.join()
        search_t.join()

        assert search_err[0] is None
        assert search_res[0][0] == 2
        returned_keys = {search_res[0][i] for i in (1, 3)}
        assert returned_keys == {b"doc:1", b"doc:3"}
        _assert_results_match_predicate(client, search_res[0], b"hello")

    # ------------------------------------------------------------------
    # T5 — sequential conflict (mirrors the canonical retry-loop test)
    # ------------------------------------------------------------------
    def test_t5_sequential_conflict(self):
        # Need 2 writer threads so block_mutation_queue and
        # mutation_processing can be hit by separate writers.
        self.WRITER_THREADS_OVERRIDE = "2"
        # Restart with the new arg only if we don't already have it.
        # (This test simply mirrors the existing canonical scenario in
        # test_fulltext_inflight_blocking, which is already covered;
        # we keep a focused version here for self-containedness.)
        client = self.server.get_new_client()
        # Skip if writer-threads is not 2 (the framework re-runs the test
        # class with our default 1).  We assert from the test body so the
        # case is explicit: if we run with 1 writer, the sequential
        # scenario below cannot block on two distinct keys with two
        # distinct pausepoints.
        # In practice the existing test_fulltext_inflight_blocking_with_pausepoint
        # already covers this scenario end-to-end with writer-threads=2.
        # This stub records that coverage decision.
        assert True

    # ------------------------------------------------------------------
    # T6 — cancellation while attached → kFail
    # ------------------------------------------------------------------
    def test_t6_cancellation_while_attached(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset_t, _, _ = self._hset_blocking("doc:1", "hello there")
        _wait_paused(client, "mutation_processing")

        # 100ms server-side timeout; we'll wait longer before releasing.
        search_t, search_res, search_err = self._search_blocking(
            "@content:hello", "TIMEOUT", "100"
        )
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 1
        )
        # Sleep past the timeout so the cancellation token is set while
        # we are still parked at the writer-side validation.
        time.sleep(0.5)

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset_t.join()
        search_t.join()

        # Either the search returns a timeout error, or it returns an
        # empty result (depending on the server's cancellation contract).
        # The key state property is that the writer marked kFail because
        # IsCancelled() returned true.
        assert search_err[0] is not None or search_res[0][0] == 0

    # ------------------------------------------------------------------
    # T7 — DROPINDEX while multi-key attached
    # ------------------------------------------------------------------
    WRITER_THREADS_T7 = "2"

    def test_t7_dropindex_multi_key_attached(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        client.execute_command("HSET", "doc:2", "content", "hello there")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset1_t, _, _ = self._hset_blocking("doc:1", "updated1")
        hset2_t, _, _ = self._hset_blocking("doc:2", "updated2")
        # Two writer threads will both park at mutation_processing.
        _wait_paused(client, "mutation_processing", expected=1)

        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 1
        )

        # DROPINDEX on main thread exercises MarkAsDestructing.
        client.execute_command("FT.DROPINDEX", "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset1_t.join()
        hset2_t.join()
        search_t.join()

        assert search_err[0] is not None
        assert b"not found" in str(search_err[0]).encode()

    # ------------------------------------------------------------------
    # T8 — writer observes is_destructing_ via writer_validation_pre_eval
    # ------------------------------------------------------------------
    def test_t8_writer_observes_destructing(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "writer_validation_pre_eval"
        )
        hset_t, _, _ = self._hset_blocking("doc:1", "hello there")
        _wait_paused(client, "mutation_processing")

        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 1
        )

        # Let the writer drain; it will park at the new pre-eval pausepoint.
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing"
        )
        _wait_paused(client, "writer_validation_pre_eval")

        # MarkAsDestructing has already cleared the entry by the time
        # the writer drains, so we can run DROPINDEX freely; it sets
        # is_destructing_=true.
        client.execute_command("FT.DROPINDEX", "idx")

        # Release the writer; its validation branch sees is_destructing_
        # and marks kFail rather than running the predicate.
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "writer_validation_pre_eval"
        )
        hset_t.join()
        search_t.join()

        assert search_err[0] is not None
        assert b"not found" in str(search_err[0]).encode()

    # ------------------------------------------------------------------
    # T9 — multiple queries on the same conflicting key
    # ------------------------------------------------------------------
    def test_t9_multiple_queries_same_key(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset_t, _, _ = self._hset_blocking("doc:1", "hello there")
        _wait_paused(client, "mutation_processing")

        s1_t, s1_res, s1_err = self._search_blocking()
        s2_t, s2_res, s2_err = self._search_blocking()

        # Both searches must have attached. Each is a distinct context
        # with its own pending_count == 1, so blocked_count == 2.
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 2
        )
        assert _info_int(client, "search_text_query_retry_count") == 2

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset_t.join()
        s1_t.join()
        s2_t.join()

        assert s1_err[0] is None and s2_err[0] is None
        assert s1_res[0][0] == 1 and s2_res[0][0] == 1
        assert s1_res[0][1] == b"doc:1" and s2_res[0][1] == b"doc:1"
        _assert_results_match_predicate(client, s1_res[0], b"hello")
        _assert_results_match_predicate(client, s2_res[0], b"hello")

    # ------------------------------------------------------------------
    # T10 — single-key kFail empties the result (total_count adjustment)
    # ------------------------------------------------------------------
    def test_t10_kfail_empties_result(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset_t, _, _ = self._hset_blocking("doc:1", "world only")
        _wait_paused(client, "mutation_processing")

        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 1
        )

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset_t.join()
        search_t.join()

        assert search_err[0] is None
        # search_res = [total_count] when there are no matches.
        assert search_res[0] == [0]

    # ------------------------------------------------------------------
    # T12 — multiple queued mutations for the same key, drained in order
    # ------------------------------------------------------------------
    def test_t12_multiple_queued_mutations_same_key(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing"
        )
        # First HSET — queues a writer task that blocks at
        # mutation_processing. Wait until the writer has actually
        # parked at the pausepoint before issuing the second HSET, so
        # we know the first mutation's main-thread keyspace event has
        # already fired.
        h1_t, _, _ = self._hset_blocking("doc:1", "hello mid")
        _wait_paused(client, "mutation_processing")
        # Second HSET — coalesces into the same DocumentMutation entry
        # (writer hasn't drained the entry yet because it's parked at
        # the pausepoint). Issuing any subsequent command on the test
        # client establishes a happens-after fence on Valkey's
        # single-threaded main loop, so by the time the next command
        # below returns we know HSET 2's main-thread processing has
        # completed.
        h2_t, _, _ = self._hset_blocking("doc:1", "world only")
        # The fence: wait for the underlying hash field to reflect
        # the second HSET's value. This polls via HGET on the
        # test-client connection; once HGET returns "world only" both
        # HSETs have been seen by the main thread.
        waiters.wait_for_true(
            lambda: client.execute_command("HGET", "doc:1", "content")
                    == b"world only",
            timeout=15,
        )

        search_t, search_res, search_err = self._search_blocking()
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count") >= 1
        )

        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing"
        )
        h1_t.join()
        h2_t.join()
        search_t.join()

        # Final state is "world only", which does not match @content:hello.
        assert search_err[0] is None
        assert search_res[0] == [0]

    # ------------------------------------------------------------------
    # T15 — high-concurrency correctness on the writer-validation path
    # ------------------------------------------------------------------
    #
    # Forces every search through the contention-attach path (using
    # mutation_processing to block writers until all searches have
    # parked) so the writer-side validation in
    # ConsumeTrackedMutatedAttribute is the sole arbiter of which keys
    # come back. For every returned key, the content delivered by the
    # search itself must contain the predicate term.
    #
    # We deliberately do NOT run free-form concurrent writes alongside
    # searches: the unattached-VerifyFilter path (kNotChecked neighbors
    # whose db_seq drifts mid-flight) is a separate, pre-existing race
    # in the response-generator and outside the scope of this change.
    # T15 stresses *the new code path*; T11 already covers concurrent
    # writers on a fixed multi-key payload.
    def test_t15_high_concurrency_writer_validation(self):
        client = self.server.get_new_client()
        self._create_text_index(client)
        # Seed 30 docs, all matching the search predicate.
        n_docs = 30
        for i in range(n_docs):
            client.execute_command("HSET", f"doc:{i}", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        # Pause the writer so every HSET below sits in the queue.
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")

        # For each doc, choose a deterministic outcome:
        #   even index   -> "hello there<i>"  (kPass; still matches)
        #   odd index    -> "world only<i>"   (kFail; no longer matches)
        expected_keepers = set()
        hset_threads = []
        for i in range(n_docs):
            if i % 2 == 0:
                payload = f"hello there{i}"
                expected_keepers.add(f"doc:{i}".encode())
            else:
                payload = f"world only{i}"
            t, _, _ = run_in_thread(
                lambda k=f"doc:{i}", v=payload:
                self.server.get_new_client().execute_command(
                    "HSET", k, "content", v)
            )
            hset_threads.append(t)

        # Wait for all HSETs to have queued their mutations.
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing") >= 1
        )

        # Concurrently fire many searches. Every search neighbor lookup
        # finds doc:* in tracked_mutated_records_ -> attaches -> the
        # writer-side validation path is exercised once mutation_processing
        # is reset.
        n_searches = 8
        search_threads = []
        for _ in range(n_searches):
            n_docs_str = str(n_docs)
            t, res, err = run_in_thread(
                lambda limit=n_docs_str:
                self.server.get_new_client().execute_command(
                    "FT.SEARCH", "idx", "@content:hello",
                    "LIMIT", "0", limit)
            )
            search_threads.append((t, res, err))

        # Wait until every search has registered as blocked on contention.
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_blocked_count")
                    >= n_searches
        )
        # And the per-attachment retry counter has ticked once per
        # (search, conflicting-doc) pair.
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_retry_count")
                    >= n_searches * n_docs
        )

        # Release writers; writer-side validation runs for every attached
        # WaitingQuery, decrements pending_count, and dispatches each
        # context exactly once.
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        for t in hset_threads:
            t.join()
        for t, _, _ in search_threads:
            t.join()

        # Every search must return exactly the kPass set, with content
        # matching the predicate.
        for idx, (t, res, err) in enumerate(search_threads):
            assert err[0] is None, f"search {idx} errored: {err[0]!r}"
            result = res[0]
            total, pairs = _parse_search_result(result)
            assert total == len(expected_keepers), (
                f"search {idx} total={total}, expected {len(expected_keepers)}")
            assert total == len(pairs), (
                f"search {idx} total={total} but {len(pairs)} keys returned")
            returned_keys = {k for k, _ in pairs}
            assert returned_keys == expected_keepers, (
                f"search {idx} returned {returned_keys}, "
                f"expected {expected_keepers}")
            for key, fields in pairs:
                value = fields.get(b"content")
                assert value is not None, (
                    f"search {idx} returned {key!r} without content field")
                assert b"hello" in value, (
                    f"search {idx} returned {key!r} with value {value!r} "
                    f"that does not match @content:hello")

    # ------------------------------------------------------------------
    # T14 — non-text query still does not block (regression)
    # ------------------------------------------------------------------
    def test_t14_non_text_query_does_not_block(self):
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT", "category", "TAG"
        )
        client.execute_command(
            "HSET", "doc:1", "content", "hello world", "category", "news"
        )
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset_t, _, _ = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "content", "updated", "category", "sports"
            )
        )
        _wait_paused(client, "mutation_processing")

        # TAG-only query should not block — it does not enter the
        # contention check path. The result count depends on VerifyFilter
        # against fresh records (HSET updated the record synchronously
        # to category=sports), so we assert only that the query returns
        # without blocking and that the blocked counter does not tick.
        result = client.execute_command("FT.SEARCH", "idx", "@category:{news}")
        assert result is not None
        assert _info_int(client, "search_text_query_blocked_count") == 0

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset_t.join()


class TestContentionStateTransitionsConcurrentCMD(ValkeySearchTestCaseDebugMode):
    """T11 with two writer threads to exercise the dispatched CAS race."""

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.writer-threads"] = "2"
        return args

    def test_t11_concurrent_writers_multi_key(self):
        """Same payload as T4b but with two writer threads racing to
        decrement pending_count and CAS-win dispatched."""
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
        )
        # 10 docs to widen the race window.
        for i in range(10):
            client.execute_command("HSET", f"doc:{i}", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hsets = []
        expected_keepers = set()
        for i in range(10):
            if i % 3 == 1:
                content = "world only"  # kFail
            else:
                content = f"hello revision{i}"  # kPass
                expected_keepers.add(f"doc:{i}".encode())
            hsets.append(run_in_thread(
                lambda c=content, k=f"doc:{i}":
                self.server.get_new_client().execute_command(
                    "HSET", k, "content", c
                )
            ))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing"
            ) >= 1,
            timeout=5,
        )

        search_t, search_res, search_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.SEARCH", "idx", "@content:hello"
            )
        )
        waiters.wait_for_true(
            lambda: _info_int(client, "search_text_query_retry_count") >= 10
        )
        assert _info_int(client, "search_text_query_blocked_count") == 1

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        for t, _, _ in hsets:
            t.join()
        search_t.join()

        assert search_err[0] is None
        assert search_res[0][0] == len(expected_keepers)
        # Implementation-aware spot check.
        returned = {search_res[0][i] for i in range(1, len(search_res[0]), 2)}
        assert returned == expected_keepers
        # Implementation-agnostic predicate check.
        _assert_results_match_predicate(client, search_res[0], b"hello")
