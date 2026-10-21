"""Save/restore tests for ALGORITHM SVS_VAMANA.

Covers `VectorSVS<T>::SaveIndexImpl` / `LoadFromRDB` (docs/svs-rdb/OVERVIEW.md).
`DEBUG RELOAD` is the fast inner loop for the wire format (it does not fork,
so it is blind to anything fork-related); the `BGSAVE`-plus-restart case is
the actual gate per docs/svs-rdb/design.md "Verification order".

Four cases carried forward from the round-2 throwaway (docs/svs-rdb/OVERVIEW.md
"AR6 has a proven starting point"), each written twice: once against
`DEBUG RELOAD` and once against a real `BGSAVE`-plus-restart, so fork
coverage comes from every case's promoted variant rather than a separate
test. A fifth class, `TestSvsSaveRestoreScale`, repeats the same two
variants at 1000 vectors / dim 128 so the payload spans multiple RDB
chunks, with recall checked against a brute-force ground truth.
"""

import struct

import numpy as np
import pytest
from valkey.client import Valkey

from ft_info_parser import FTInfoParser
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from util import waiters


DIM = 4


def _fp32(values):
    return struct.pack(f"<{len(values)}f", *values)


def _v_index(client: Valkey, index: str) -> dict:
    """Return the parsed `attributes[0]["index"]` dict for the "v" attribute."""
    return FTInfoParser(
        client.execute_command("FT.INFO", index)
    ).attributes[0]["index"]


def _score_map(reply):
    """FT.SEARCH ... DIALECT 2 returns [count, key, [payload], key, [payload], ...].
    Each payload is a flat list containing at least b'__v_score' and the score.
    Returns {key: float(score)}."""
    out = {}
    for i in range(1, len(reply), 2):
        key = reply[i]
        payload = dict(zip(reply[i + 1][::2], reply[i + 1][1::2]))
        out[key] = float(payload[b"__v_score"])
    return out


def _create_svs_index(
    client: Valkey,
    index: str,
    compression: str = "NONE",
    dim: int = DIM,
    search_window_size=None,
    training_threshold=None,
):
    """FT.CREATE ALGORITHM SVS_VAMANA. Raw command rather than the Index/Vector
    helper: that helper has no SVS_VAMANA-specific COMPRESSION argument, same
    as test_svs_smoke.py."""
    params = [
        "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2",
    ]
    if compression != "NONE":
        params += ["COMPRESSION", compression]
    if search_window_size is not None:
        params += ["SEARCH_WINDOW_SIZE", str(search_window_size)]
    if training_threshold is not None:
        params += ["TRAINING_THRESHOLD", str(training_threshold)]
    # The attribute's parameter count must equal len(params); deriving it keeps
    # the two from drifting when an optional parameter is added below.
    client.execute_command(
        "FT.CREATE", index,
        "SCHEMA", "v", "VECTOR", "SVS_VAMANA", str(len(params)),
        *params,
    )


def _populate(
    client: Valkey,
    n: int,
    start: int = 1,
    dim: int = DIM,
    vectors=None,
):
    """HSET doc:start..doc:start+n-1. Defaults to (float(i), 0, ..., 0);
    pass `vectors` (length n, each of length dim) to HSET explicit payloads
    instead, e.g. for a brute-force recall baseline."""
    for offset in range(n):
        i = start + offset
        if vectors is None:
            values = [float(i)] + [0.0] * (dim - 1)
        else:
            values = vectors[offset]
        client.hset(f"doc:{i}", mapping={"v": _fp32(values)})


def _random_vectors(n: int, dim: int, seed: int = 42) -> np.ndarray:
    """n x dim float32 vectors, deterministic given seed."""
    rng = np.random.default_rng(seed)
    return rng.random((n, dim), dtype=np.float32)


def _compute_l2_distances(query: np.ndarray, vectors: np.ndarray) -> np.ndarray:
    diff = vectors - query
    return np.sum(diff * diff, axis=1)


def _brute_force_knn(query: np.ndarray, vectors: np.ndarray, k: int) -> set:
    """Ground-truth k nearest row indices into `vectors`, exact L2."""
    distances = _compute_l2_distances(query, vectors)
    return set(np.argsort(distances)[:k].tolist())


def _knn(client: Valkey, index: str, query, k: int):
    reply = client.execute_command(
        "FT.SEARCH", index, f"*=>[KNN {k} @v $q]",
        "PARAMS", "2", "q", _fp32(query),
        "DIALECT", "2",
    )
    return reply


def _collect_knn(client: Valkey, index: str, num_vectors: int, anchor=None, dim: int = DIM):
    """Ordered (key, score) list for a handful of K probes against `anchor`
    (default: [num_vectors // 2, 0, ..., 0], matching the integer-encoded
    vectors `_populate` writes by default), so a reload that reorders or
    perturbs scores is caught."""
    if anchor is None:
        anchor = [num_vectors // 2] + [0.0] * (dim - 1)
    results = []
    for k in (1, 5, 10):
        if k > num_vectors:
            continue
        reply = _knn(client, index, anchor, k)
        scores = _score_map(reply)
        results.append((k, [(key, scores[key]) for key in scores]))
    return results


def _bgsave_and_restart(test, backfill_timeout: int = 30):
    """BGSAVE, wait for it to finish, then hard-restart the server against
    the resulting RDB. Unlike DEBUG RELOAD this forks, which is the only way
    to exercise the AtForkPrepare-suspended save path the design doc calls
    out as the actual gate."""
    # valkey-py's default BGSAVE callback maps every reply to True.
    test.client.set_response_callback("BGSAVE", lambda r, **kw: r)
    reply = test.client.execute_command("BGSAVE")
    if isinstance(reply, bytes):
        reply = reply.decode()
    # A "Background saving scheduled" reply leaves rdb_bgsave_in_progress at 0
    # too, so without this assert the restart would read the stale RDB.
    assert reply == "Background saving started", reply
    test.server.wait_for_save_done(test.client)
    test.server.restart(remove_rdb=False)
    assert test.client.ping()
    waiters.wait_for_true(
        lambda: FTInfoParser(
            test.client.execute_command("FT.INFO", test.index_name)
        ).backfill_in_progress == 0,
        timeout=backfill_timeout,
    )


class TestSvsSaveRestorePopulated(ValkeySearchTestCaseDebugMode):
    """Case 1: populated round-trip. 40 vectors, reload, identical KNN key
    order and scores, FT.INFO size preserved."""

    NUM_VECTORS = 40

    def _setup_index(self, index_name: str):
        self.index_name = index_name
        client: Valkey = self.client
        _create_svs_index(client, index_name)
        _populate(client, self.NUM_VECTORS)
        waiters.wait_for_equal(
            lambda: _v_index(client, index_name)["size"], self.NUM_VECTORS, timeout=30
        )
        return _collect_knn(client, index_name, self.NUM_VECTORS)

    def test_debug_reload_preserves_knn_and_size(self):
        client: Valkey = self.client
        pre = self._setup_index("svs_pop_reload")

        client.execute_command("DEBUG", "RELOAD")

        assert _v_index(client, self.index_name)["size"] == self.NUM_VECTORS
        post = _collect_knn(client, self.index_name, self.NUM_VECTORS)
        assert len(pre) == len(post)
        for (k1, r1), (k2, r2) in zip(pre, post):
            assert k1 == k2
            assert [key for key, _ in r1] == [key for key, _ in r2], (
                f"KNN key order changed across DEBUG RELOAD for k={k1}"
            )
            for (key1, score1), (key2, score2) in zip(r1, r2):
                assert key1 == key2
                assert score1 == pytest.approx(score2, abs=1e-4)

    def test_bgsave_restart_preserves_knn_and_size(self):
        client: Valkey = self.client
        pre = self._setup_index("svs_pop_bgsave")

        _bgsave_and_restart(self)

        assert _v_index(client, self.index_name)["size"] == self.NUM_VECTORS
        post = _collect_knn(client, self.index_name, self.NUM_VECTORS)
        assert len(pre) == len(post)
        for (k1, r1), (k2, r2) in zip(pre, post):
            assert k1 == k2
            assert [key for key, _ in r1] == [key for key, _ in r2], (
                f"KNN key order changed across BGSAVE+restart for k={k1}"
            )
            for (key1, score1), (key2, score2) in zip(r1, r2):
                assert key1 == key2
                assert score1 == pytest.approx(score2, abs=1e-4)


class TestSvsSaveRestoreEmpty(ValkeySearchTestCaseDebugMode):
    """Case 2: empty index. Reload at size 0 -- the handle-less path,
    docs/svs-rdb/design.md "Empty index", the one path that never calls
    into SVS -- then HSET and confirm it still bootstraps and answers KNN."""

    def test_debug_reload_empty_then_bootstraps(self):
        client: Valkey = self.client
        index_name = "svs_empty_reload"
        _create_svs_index(client, index_name)
        assert _v_index(client, index_name)["size"] == 0

        client.execute_command("DEBUG", "RELOAD")
        assert _v_index(client, index_name)["size"] == 0

        _populate(client, 5)
        waiters.wait_for_equal(
            lambda: _v_index(client, index_name)["size"], 5, timeout=30
        )
        reply = _knn(client, index_name, [3.0, 0.0, 0.0, 0.0], 1)
        assert reply[0] == 1
        assert reply[1] == b"doc:3"

    def test_bgsave_restart_empty_then_bootstraps(self):
        client: Valkey = self.client
        self.index_name = "svs_empty_bgsave"
        _create_svs_index(client, self.index_name)
        assert _v_index(client, self.index_name)["size"] == 0

        _bgsave_and_restart(self)
        assert _v_index(client, self.index_name)["size"] == 0

        _populate(client, 5)
        waiters.wait_for_equal(
            lambda: _v_index(client, self.index_name)["size"], 5, timeout=30
        )
        reply = _knn(client, self.index_name, [3.0, 0.0, 0.0, 0.0], 1)
        assert reply[0] == 1
        assert reply[1] == b"doc:3"


class TestSvsSaveRestoreWritesBetweenReloads(ValkeySearchTestCaseDebugMode):
    """Case 3: writes between two reloads. Populate, reload, HSET ten *new*
    keys, reload again. The real check that `inc_id_ = GetMaxLoadedLabel() + 1`
    did not reset and hand SVS a label it already holds
    (docs/svs-rdb/design.md "Labels") -- a plain round-trip test would not
    catch a regression here, since a reset label collides silently with an
    already-loaded one rather than raising."""

    FIRST_BATCH = 20
    SECOND_BATCH = 10

    def test_debug_reload_twice_with_writes_between(self):
        client: Valkey = self.client
        index_name = "svs_writes_between_reload"
        _create_svs_index(client, index_name)
        _populate(client, self.FIRST_BATCH, start=1)
        waiters.wait_for_equal(
            lambda: _v_index(client, index_name)["size"], self.FIRST_BATCH, timeout=30
        )

        client.execute_command("DEBUG", "RELOAD")
        assert _v_index(client, index_name)["size"] == self.FIRST_BATCH

        _populate(client, self.SECOND_BATCH, start=self.FIRST_BATCH + 1)
        waiters.wait_for_equal(
            lambda: _v_index(client, index_name)["size"],
            self.FIRST_BATCH + self.SECOND_BATCH,
            timeout=30,
        )

        client.execute_command("DEBUG", "RELOAD")
        assert (
            _v_index(client, index_name)["size"]
            == self.FIRST_BATCH + self.SECOND_BATCH
        )

        # Every doc, old and new, must still be retrievable by exact KNN --
        # if a reused label silently overwrote an old vector's slot, some
        # k=1 query below would return the wrong key or a stale score.
        for i in range(1, self.FIRST_BATCH + self.SECOND_BATCH + 1):
            reply = _knn(client, index_name, [float(i), 0.0, 0.0, 0.0], 1)
            assert reply[0] == 1
            assert reply[1] == f"doc:{i}".encode()

    def test_bgsave_restart_twice_with_writes_between(self):
        client: Valkey = self.client
        self.index_name = "svs_writes_between_bgsave"
        _create_svs_index(client, self.index_name)
        _populate(client, self.FIRST_BATCH, start=1)
        waiters.wait_for_equal(
            lambda: _v_index(client, self.index_name)["size"],
            self.FIRST_BATCH,
            timeout=30,
        )

        _bgsave_and_restart(self)
        assert _v_index(client, self.index_name)["size"] == self.FIRST_BATCH

        _populate(client, self.SECOND_BATCH, start=self.FIRST_BATCH + 1)
        waiters.wait_for_equal(
            lambda: _v_index(client, self.index_name)["size"],
            self.FIRST_BATCH + self.SECOND_BATCH,
            timeout=30,
        )

        _bgsave_and_restart(self)
        assert (
            _v_index(client, self.index_name)["size"]
            == self.FIRST_BATCH + self.SECOND_BATCH
        )

        for i in range(1, self.FIRST_BATCH + self.SECOND_BATCH + 1):
            reply = _knn(client, self.index_name, [float(i), 0.0, 0.0, 0.0], 1)
            assert reply[0] == 1
            assert reply[1] == f"doc:{i}".encode()


class TestSvsSaveRestoreCompression(ValkeySearchTestCaseDebugMode):
    """Case 4: FP16 compression round-trip. `compression` is the one
    build-config field in the header (docs/svs-rdb/design.md "Header") that
    genuinely has to match the payload, since the storage element type
    derives from it alone."""

    NUM_VECTORS = 30

    def _setup_index(self, index_name: str):
        self.index_name = index_name
        client: Valkey = self.client
        _create_svs_index(client, index_name, compression="FP16")
        assert _v_index(client, index_name)["algorithm"]["compression"] == \
            "SVS_COMPRESSION_FP16"
        _populate(client, self.NUM_VECTORS)
        waiters.wait_for_equal(
            lambda: _v_index(client, index_name)["size"], self.NUM_VECTORS, timeout=30
        )
        return _collect_knn(client, index_name, self.NUM_VECTORS)

    def test_debug_reload_preserves_compression_and_knn(self):
        client: Valkey = self.client
        pre = self._setup_index("svs_fp16_reload")

        client.execute_command("DEBUG", "RELOAD")

        idx_info = _v_index(client, self.index_name)
        assert idx_info["algorithm"]["compression"] == "SVS_COMPRESSION_FP16"
        assert idx_info["size"] == self.NUM_VECTORS
        post = _collect_knn(client, self.index_name, self.NUM_VECTORS)
        for (k1, r1), (k2, r2) in zip(pre, post):
            assert k1 == k2
            assert [key for key, _ in r1] == [key for key, _ in r2]
            for (key1, score1), (key2, score2) in zip(r1, r2):
                assert key1 == key2
                assert score1 == pytest.approx(score2, abs=1e-4)

    def test_bgsave_restart_preserves_compression_and_knn(self):
        client: Valkey = self.client
        pre = self._setup_index("svs_fp16_bgsave")

        _bgsave_and_restart(self)

        idx_info = _v_index(client, self.index_name)
        assert idx_info["algorithm"]["compression"] == "SVS_COMPRESSION_FP16"
        assert idx_info["size"] == self.NUM_VECTORS
        # Quantization happens once, at ingest; save/load persists and
        # reloads the same quantized codes rather than re-quantizing, so
        # the pre-save baseline is still the right comparison.
        post = _collect_knn(client, self.index_name, self.NUM_VECTORS)
        for (k1, r1), (k2, r2) in zip(pre, post):
            assert k1 == k2
            assert [key for key, _ in r1] == [key for key, _ in r2]
            for (key1, score1), (key2, score2) in zip(r1, r2):
                assert key1 == key2
                assert score1 == pytest.approx(score2, abs=1e-4)


class TestSvsSaveRestoreSq8(ValkeySearchTestCaseDebugMode):
    """SQ8 saved before its conversion carries an uncompressed payload; it
    must load as one, convert when it reaches TRAINING_THRESHOLD, and then
    round-trip compressed."""

    THRESHOLD = 64  # the minimum TRAINING_THRESHOLD

    def _reload(self, how: str):
        if how == "debug_reload":
            self.client.execute_command("DEBUG", "RELOAD")
        else:
            _bgsave_and_restart(self)

    def _assert_knn_preserved(self, pre, post):
        for (k1, r1), (k2, r2) in zip(pre, post):
            assert k1 == k2
            assert [key for key, _ in r1] == [key for key, _ in r2]
            for (_, score1), (_, score2) in zip(r1, r2):
                assert score1 == pytest.approx(score2, abs=1e-4)

    @pytest.mark.parametrize("how", ["debug_reload", "bgsave_restart"])
    def test_reload_before_and_after_conversion(self, how):
        client: Valkey = self.client
        index = self.index_name = f"svs_sq8_{how}"
        _create_svs_index(client, index, compression="SQ8",
                          training_threshold=self.THRESHOLD)
        half = self.THRESHOLD // 2

        def wait_size(n):
            waiters.wait_for_equal(
                lambda: _v_index(client, index)["size"], n, timeout=30)

        _populate(client, half)
        wait_size(half)
        pre = _collect_knn(client, index, half)
        self._reload(how)
        wait_size(half)
        assert _v_index(client, index)["algorithm"]["compression_pending"] == 1
        self._assert_knn_preserved(pre, _collect_knn(client, index, half))

        # Reaching the threshold converts the index; it keeps answering.
        _populate(client, self.THRESHOLD - half, start=half + 1)
        wait_size(self.THRESHOLD)
        pre = _collect_knn(client, index, self.THRESHOLD)
        for k, result in pre:
            assert len(result) == k
        self._reload(how)
        wait_size(self.THRESHOLD)
        info = _v_index(client, index)
        assert info["algorithm"]["compression"] == "SVS_COMPRESSION_SQ8"
        assert info["algorithm"]["training_threshold"] == self.THRESHOLD
        assert info["algorithm"]["compression_pending"] == 0
        self._assert_knn_preserved(pre,
                                   _collect_knn(client, index, self.THRESHOLD))


class TestSvsSaveRestoreScale(ValkeySearchTestCaseDebugMode):
    """Case 5: realistic-scale round-trip. 1000 vectors at dim 128, big
    enough that the SVS payload spans multiple RDB chunks (unlike the
    40-vector populated case, which fits in one), so this is the only class
    that exercises the multi-chunk read path against a real index. Checks
    the same exact KNN preservation as the populated case, plus recall
    against a brute-force ground truth on the restored index."""

    DIM = 128
    NUM_VECTORS = 1000
    K = 10
    RECALL_THRESHOLD = 0.9
    # The default search window size equals K, which would make the recall
    # assertion a measure of the beam width rather than of the restored graph.
    SEARCH_WINDOW_SIZE = 64
    # 1000 vectors take materially longer to ingest and backfill than 40.
    WAIT_TIMEOUT = 120

    def _setup_index(self, index_name: str):
        self.index_name = index_name
        client: Valkey = self.client
        self.vectors = _random_vectors(self.NUM_VECTORS, self.DIM)
        self.anchor = self.vectors[0].tolist()
        _create_svs_index(
            client,
            index_name,
            dim=self.DIM,
            search_window_size=self.SEARCH_WINDOW_SIZE,
        )
        _populate(client, self.NUM_VECTORS, dim=self.DIM, vectors=self.vectors)
        waiters.wait_for_equal(
            lambda: _v_index(client, index_name)["size"],
            self.NUM_VECTORS,
            timeout=self.WAIT_TIMEOUT,
        )
        return _collect_knn(
            client, index_name, self.NUM_VECTORS, anchor=self.anchor, dim=self.DIM
        )

    def _assert_recall(self, post):
        # post entries are (k, [(key, score), ...]); doc:i maps to
        # self.vectors[i - 1].
        checked = False
        for k, results in post:
            if k != self.K:
                continue
            got = {int(key.split(b":")[1]) - 1 for key, _ in results}
            expected = _brute_force_knn(self.anchor, self.vectors, self.K)
            recall = len(got & expected) / self.K
            checked = True
            assert recall >= self.RECALL_THRESHOLD, (
                f"recall too low after restore: {recall}"
            )
        # Without this the whole check silently becomes a no-op if the probe
        # list in _collect_knn ever stops including K.
        assert checked, f"no k={self.K} probe to check recall against"

    def test_debug_reload_preserves_knn_and_recall(self):
        client: Valkey = self.client
        pre = self._setup_index("svs_scale_reload")

        client.execute_command("DEBUG", "RELOAD")

        assert _v_index(client, self.index_name)["size"] == self.NUM_VECTORS
        post = _collect_knn(
            client, self.index_name, self.NUM_VECTORS, anchor=self.anchor, dim=self.DIM
        )
        assert len(pre) == len(post)
        for (k1, r1), (k2, r2) in zip(pre, post):
            assert k1 == k2
            assert [key for key, _ in r1] == [key for key, _ in r2], (
                f"KNN key order changed across DEBUG RELOAD for k={k1}"
            )
            for (key1, score1), (key2, score2) in zip(r1, r2):
                assert key1 == key2
                assert score1 == pytest.approx(score2, abs=1e-4)
        self._assert_recall(post)

    def test_bgsave_restart_preserves_knn_and_recall(self):
        client: Valkey = self.client
        pre = self._setup_index("svs_scale_bgsave")

        _bgsave_and_restart(self, backfill_timeout=self.WAIT_TIMEOUT)

        assert _v_index(client, self.index_name)["size"] == self.NUM_VECTORS
        post = _collect_knn(
            client, self.index_name, self.NUM_VECTORS, anchor=self.anchor, dim=self.DIM
        )
        assert len(pre) == len(post)
        for (k1, r1), (k2, r2) in zip(pre, post):
            assert k1 == k2
            assert [key for key, _ in r1] == [key for key, _ in r2], (
                f"KNN key order changed across BGSAVE+restart for k={k1}"
            )
            for (key1, score1), (key2, score2) in zip(r1, r2):
                assert key1 == key2
                assert score1 == pytest.approx(score2, abs=1e-4)
        self._assert_recall(post)
