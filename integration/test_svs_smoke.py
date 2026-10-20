"""End-to-end smoke test for ALGORITHM SVS_VAMANA.

Exercises the minimum surface a benchmark or ad-hoc verification needs:
FT.CREATE, HSET (ingest and first-add bootstrap), FT.SEARCH KNN,
FT.INFO shape, delete, modify, and a filtered KNN. Not a recall test
and not a stress test -- those live elsewhere (e.g. saverestore tests
for round-trip recall). This file is the canonical answer to "did I
break SVS?" during iteration on the branch, and is what to run before
opening a review.
"""

import random
import struct

import pytest
from valkey.client import Valkey
from valkeytestframework.util import waiters

from ft_info_parser import FTInfoParser
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker


DIM = 4

# Five documents per metric whose scores against METRIC_QUERY are
# METRIC_EXPECTED (doc number -> 1 - similarity). IP uses unit vectors.
# COSINE scales them, so a missing normalization shows up as a wrong score,
# and makes doc 5 a zero vector, which must score 1.0 rather than NaN.
METRIC_DOCS = {
    "IP": [[0, 0, 1, 0], [0.8, 0.6, 0, 0], [1, 0, 0, 0], [0.6, 0.8, 0, 0],
           [0, 0, 0, 1]],
    "COSINE": [[0, 0, 5, 0], [4, 3, 0, 0], [2, 0, 0, 0], [0.3, 0.4, 0, 0],
               [0, 0, 0, 0]],
}
METRIC_QUERY = {"IP": [1.0, 0.0, 0.0, 0.0], "COSINE": [3.0, 0.0, 0.0, 0.0]}
METRIC_EXPECTED = {1: 1.0, 2: 0.2, 3: 0.0, 4: 0.4, 5: 1.0}


def _fp32(values):
    return struct.pack(f"<{len(values)}f", *values)


def _fp16(values):
    return struct.pack(f"<{len(values)}e", *values)


def _v_index(client: Valkey, index: str) -> dict:
    """Return the parsed `attributes[0]["index"]` dict for the "v" attribute.
    This is where dimensions/distance_metric/size/data_type/algorithm all
    live in FT.INFO's response."""
    return FTInfoParser(
        client.execute_command("FT.INFO", index)
    ).attributes[0]["index"]


def _score_map(reply):
    """FT.SEARCH ... DIALECT 2 returns [count, key, [payload], key, [payload], ...].
    Each payload is a flat list containing at least b'__v_score' and the score.
    Returns {key: float(score)} preserving iteration order."""
    out = {}
    for i in range(1, len(reply), 2):
        key = reply[i]
        payload = dict(zip(reply[i + 1][::2], reply[i + 1][1::2]))
        out[key] = float(payload[b"__v_score"])
    return out


def _module_external_memory(client: Valkey) -> int:
    """Read `used_memory_module_external` from `INFO debug`. This is the
    counter Valkey PR #4128 exposes; every byte our custom SVS allocator
    passes through ValkeyModule_IncrExternalMemory lands here."""
    info = client.info("debug")
    if "used_memory_module_external" not in info:
        # Before Valkey 9.2 the module allocates SVS memory through its
        # malloc wrappers instead: still accounted, but no huge pages and no
        # external counter to check.
        pytest.skip(
            "used_memory_module_external not present in INFO debug; "
            "this Valkey build predates valkey-io/valkey#4128"
        )
    return int(info["used_memory_module_external"])


def _create_metric_index(client: Valkey, index: str,
                         algorithm: str, metric: str):
    client.execute_command(
        "FT.CREATE", index, "PREFIX", "1", f"{index}:",
        "SCHEMA",
        "v", "VECTOR", algorithm,
        "6", "TYPE", "FLOAT32", "DIM", str(DIM),
        "DISTANCE_METRIC", metric,
        "color", "TAG",
    )
    for i, values in enumerate(METRIC_DOCS[metric], start=1):
        client.hset(f"{index}:{i}",
                    mapping={"v": _fp32(values), "color": "red"})

    def indexed():
        info = FTInfoParser(client.execute_command("FT.INFO", index))
        return info.num_docs == len(METRIC_DOCS[metric]) and \
            info.mutation_queue_size == 0

    waiters.wait_for_equal(indexed, True, timeout=5)


def _knn_scores(client: Valkey, index: str, query, filter="*"):
    reply = client.execute_command(
        "FT.SEARCH", index, f"{filter}=>[KNN 5 @v $q]",
        "PARAMS", "2", "q", _fp32(query), "DIALECT", "2",
    )
    assert reply[0] == 5, reply
    prefix = f"{index}:".encode()
    return {int(k[len(prefix):]): s for k, s in _score_map(reply).items()}


def _create_svs_and_flat_on_same_keys(client: Valkey):
    """Indexes svs_h (SVS_VAMANA) and flat_h (FLAT) over h:1..h:5, each with
    a TEXT title and an L2 vector (i, 0, 0, 0), as the reference answer for
    SVS queries."""
    for index, algorithm in (("svs_h", "SVS_VAMANA"), ("flat_h", "FLAT")):
        client.execute_command(
            "FT.CREATE", index, "PREFIX", "1", "h:",
            "SCHEMA",
            "v", "VECTOR", algorithm,
            "6", "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
            "title", "TEXT",
        )
    for i in range(1, 6):
        client.hset(f"h:{i}", mapping={
            "v": _fp32([float(i), 0.0, 0.0, 0.0]), "title": "hello world"})

    def indexed():
        return all(
            FTInfoParser(client.execute_command("FT.INFO", index)).num_docs
            == 5 for index in ("svs_h", "flat_h"))

    waiters.wait_for_equal(indexed, True, timeout=5)


class TestSVSVamanaSmoke(ValkeySearchTestCaseBase):

    def _create_default_index(self, client: Valkey, index: str = "svs_idx"):
        """FT.CREATE with default SVS_VAMANA params (FP32, L2, no filter)."""
        client.execute_command(
            "FT.CREATE", index,
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "6", "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )

    def _populate(self, client: Valkey, n: int, encode=_fp32):
        """HSET n docs along the x-axis: doc:i -> (float(i), 0, 0, 0)."""
        for i in range(1, n + 1):
            values = [float(i)] + [0.0] * (DIM - 1)
            client.hset(f"doc:{i}", mapping={"v": encode(values)})

    def test_ft_create_reports_svs_vamana(self):
        """FT.INFO exposes the algorithm name and every build-config field."""
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client)
        idx_info = _v_index(client, "svs_idx")
        assert idx_info["dimensions"] == DIM
        assert idx_info["distance_metric"] == "L2"
        assert idx_info["size"] == 0
        assert idx_info["data_type"] == "FLOAT32"
        algo = idx_info["algorithm"]
        assert algo["name"] == "SVS_VAMANA"
        assert algo["graph_max_degree"] == 64
        assert algo["construction_window_size"] == 128
        assert algo["search_window_size"] == 10
        assert algo["compression"] == "SVS_COMPRESSION_NONE"

    @pytest.mark.parametrize("metric,alpha_args,expected", [
        ("L2", [], 1.2),
        ("IP", [], 0.95),
        ("COSINE", [], 0.95),
        ("L2", ["ALPHA", "1.5"], 1.5),
        ("COSINE", ["ALPHA", "0.9"], 0.9),
    ])
    def test_ft_info_reports_effective_alpha(self, metric, alpha_args,
                                             expected):
        """FT.INFO shows the alpha SVS builds with: ALPHA when given,
        otherwise SVS's default for the metric."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "alpha_idx",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            str(6 + len(alpha_args)), "TYPE", "FLOAT32", "DIM", str(DIM),
            "DISTANCE_METRIC", metric, *alpha_args,
        )
        alpha = _v_index(client, "alpha_idx")["algorithm"]["alpha"]
        assert float(alpha) == pytest.approx(expected, abs=1e-6)

    def test_ingest_bootstrap_and_add_points(self):
        """First HSET bootstraps svs_index_; subsequent HSETs go through
        svs_index_dynamic_add_points. FT.INFO size tracks label_to_record_."""
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client)
        self._populate(client, 5)
        assert _v_index(client, "svs_idx")["size"] == 5

    def test_delete_decrements_size(self):
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client)
        self._populate(client, 5)
        client.delete("doc:2", "doc:5")
        assert _v_index(client, "svs_idx")["size"] == 3

    def test_modify_preserves_size(self):
        """HSET on an existing key replaces the vector under one lock
        (delete + add); the label count stays constant."""
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client)
        self._populate(client, 5)
        client.hset("doc:3", mapping={"v": _fp32([99.0, 99.0, 99.0, 99.0])})
        assert _v_index(client, "svs_idx")["size"] == 5

    def test_knn_returns_correct_neighbors_and_distances(self):
        """L2 distances are exact for a small deterministic axis-aligned set."""
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client)
        self._populate(client, 5)

        # Query at (3.7, 0, 0, 0). Analytic L2^2:
        #   doc:4 (4.0) -> 0.09
        #   doc:3 (3.0) -> 0.49
        #   doc:5 (5.0) -> 1.69
        #   doc:2 (2.0) -> 2.89
        #   doc:1 (1.0) -> 7.29
        reply = client.execute_command(
            "FT.SEARCH", "svs_idx", "*=>[KNN 5 @v $q]",
            "PARAMS", "2", "q", _fp32([3.7, 0.0, 0.0, 0.0]),
            "DIALECT", "2",
        )
        assert reply[0] == 5

        scores = _score_map(reply)
        assert list(scores.keys()) == [b"doc:4", b"doc:3", b"doc:5",
                                        b"doc:2", b"doc:1"]
        assert scores[b"doc:4"] == pytest.approx(0.09, abs=1e-3)
        assert scores[b"doc:3"] == pytest.approx(0.49, abs=1e-3)
        assert scores[b"doc:5"] == pytest.approx(1.69, abs=1e-3)
        assert scores[b"doc:2"] == pytest.approx(2.89, abs=1e-3)
        assert scores[b"doc:1"] == pytest.approx(7.29, abs=1e-2)

    def test_knn_caps_k_at_index_size(self):
        """KNN with count > size returns exactly size results, sorted."""
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client)
        self._populate(client, 3)
        reply = client.execute_command(
            "FT.SEARCH", "svs_idx", "*=>[KNN 10 @v $q]",
            "PARAMS", "2", "q", _fp32([2.0, 0.0, 0.0, 0.0]),
            "DIALECT", "2",
        )
        assert reply[0] == 3

    def test_knn_on_empty_index_returns_no_results(self):
        """FT.SEARCH before any HSET short-circuits to an empty result set
        (svs_index_ is null until the first-add bootstrap)."""
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client)
        reply = client.execute_command(
            "FT.SEARCH", "svs_idx", "*=>[KNN 5 @v $q]",
            "PARAMS", "2", "q", _fp32([1.0, 0.0, 0.0, 0.0]),
            "DIALECT", "2",
        )
        assert reply[0] == 0

    def test_type_bfloat16_rejected_at_parse(self):
        """TYPE BFLOAT16 is not supported for SVS_VAMANA (parse-time reject)."""
        client: Valkey = self.server.get_new_client()
        with pytest.raises(Exception) as exc_info:
            client.execute_command(
                "FT.CREATE", "bf16_idx",
                "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
                "6", "TYPE", "BFLOAT16", "DIM", str(DIM),
                "DISTANCE_METRIC", "L2",
            )
        assert "BFLOAT16" in str(exc_info.value)

    def test_compression_sq8_rejected_at_parse(self):
        """COMPRESSION SQ8 is fenced in v1: the first-add bootstrap
        calibrates the int8 range on one vector and later components
        outside that range saturate. Re-enabled when calibration lands."""
        client: Valkey = self.server.get_new_client()
        with pytest.raises(Exception) as exc_info:
            client.execute_command(
                "FT.CREATE", "sq8_idx",
                "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
                "8", "TYPE", "FLOAT32", "DIM", str(DIM),
                "DISTANCE_METRIC", "L2", "COMPRESSION", "SQ8",
            )
        assert "SQ8" in str(exc_info.value)

    @pytest.mark.parametrize("metric", ["IP", "COSINE"])
    def test_alpha_above_one_rejected_for_ip_cosine(self, metric):
        """IP and COSINE take 0 < ALPHA <= 1.0; checked at FT.CREATE."""
        client: Valkey = self.server.get_new_client()
        with pytest.raises(Exception) as exc_info:
            client.execute_command(
                "FT.CREATE", f"{metric.lower()}_alpha_idx",
                "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
                "8", "TYPE", "FLOAT32", "DIM", str(DIM),
                "DISTANCE_METRIC", metric, "ALPHA", "1.5",
            )
        assert "ALPHA" in str(exc_info.value)

    @pytest.mark.parametrize("metric", ["IP", "COSINE"])
    def test_knn_scores_ip_cosine(self, metric):
        """__v_score is the distance 1 - similarity: the exact match comes
        first with 0, and every score matches FLAT on the same data. Under
        COSINE the zero vector (doc 5) scores 1.0."""
        client: Valkey = self.server.get_new_client()
        _create_metric_index(client, "svs_m", "SVS_VAMANA", metric)
        _create_metric_index(client, "flat_m", "FLAT", metric)

        scores = _knn_scores(client, "svs_m", METRIC_QUERY[metric])
        flat_scores = _knn_scores(client, "flat_m", METRIC_QUERY[metric])
        assert list(scores)[0] == 3, scores
        assert list(scores.values()) == sorted(scores.values()), scores
        for doc, expected in METRIC_EXPECTED.items():
            assert scores[doc] == pytest.approx(expected, abs=1e-5), scores
            assert scores[doc] == pytest.approx(flat_scores[doc], abs=1e-5)

    @pytest.mark.parametrize("metric", ["IP", "COSINE"])
    def test_vector_range_on_svs_field(self, metric):
        """VECTOR_RANGE on an SVS_VAMANA field returns docs 2 (0.2) and
        3 (0.0) for radius 0.3."""
        client: Valkey = self.server.get_new_client()
        _create_metric_index(client, "svs_m", "SVS_VAMANA", metric)
        _create_metric_index(client, "flat_m", "FLAT", metric)

        def range_docs(index):
            reply = client.execute_command(
                "FT.SEARCH", index, "@v:[VECTOR_RANGE $r $q]", "NOCONTENT",
                "PARAMS", "4", "r", "0.3", "q", _fp32(METRIC_QUERY[metric]),
                "DIALECT", "2",
            )
            prefix = f"{index}:".encode()
            return {int(k[len(prefix):]) for k in reply[1:]}

        assert range_docs("svs_m") == range_docs("flat_m") == {2, 3}

    def test_vector_range_accepts_epsilon(self):
        """$EPSILON is accepted on a VECTOR_RANGE over an SVS_VAMANA field,
        as for HNSW, and doesn't change the result."""
        client: Valkey = self.server.get_new_client()
        _create_metric_index(client, "svs_m", "SVS_VAMANA", "COSINE")

        def range_docs(attrs):
            reply = client.execute_command(
                "FT.SEARCH", "svs_m", f"@v:[VECTOR_RANGE $r $q]{attrs}",
                "NOCONTENT", "PARAMS", "4", "r", "0.3",
                "q", _fp32(METRIC_QUERY["COSINE"]), "DIALECT", "2",
            )
            return {int(k[len(b"svs_m:"):]) for k in reply[1:]}

        assert range_docs("=>{$epsilon: 0.1}") == range_docs("") == {2, 3}

    def test_ft_hybrid_vsim_on_svs_field(self):
        """FT.HYBRID accepts an SVS_VAMANA field in its VSIM arm."""
        client: Valkey = self.server.get_new_client()
        _create_svs_and_flat_on_same_keys(client)

        def hybrid(index):
            return client.execute_command(
                "FT.HYBRID", index,
                "SEARCH", "@title:hello",
                "VSIM", "@v", "$q", "KNN", "2", "K", "3",
                "COMBINE", "RRF", "2", "WINDOW", "10",
                "PARAMS", "2", "q", _fp32([0.0, 0.0, 0.0, 0.0]),
            )

        svs_reply = hybrid("svs_h")
        assert svs_reply[0] > 0, svs_reply
        assert svs_reply == hybrid("flat_h")

    def test_ft_aggregate_references_svs_field(self):
        """FT.AGGREGATE can reference an SVS_VAMANA field by name."""
        client: Valkey = self.server.get_new_client()
        _create_svs_and_flat_on_same_keys(client)

        def aggregate(index):
            return client.execute_command(
                "FT.AGGREGATE", index, "*",
                "GROUPBY", "1", "@v", "REDUCE", "COUNT", "0", "AS", "n",
                "DIALECT", "2",
            )

        svs_reply = aggregate("svs_h")
        assert svs_reply[0] > 0, svs_reply
        assert sorted(map(repr, svs_reply[1:])) == \
            sorted(map(repr, aggregate("flat_h")[1:]))

    @pytest.mark.parametrize("metric", ["IP", "COSINE"])
    def test_updates_keep_scores_matching_flat(self, metric):
        """Updates normalize (COSINE) and re-score like adds: overwrite a doc
        with a scaled, rotated vector, then churn past the consolidation
        trigger, and SVS scores keep matching FLAT."""
        client: Valkey = self.server.get_new_client()
        _create_metric_index(client, "svs_m", "SVS_VAMANA", metric)
        _create_metric_index(client, "flat_m", "FLAT", metric)
        query = METRIC_QUERY[metric]

        def update(doc, vector):
            for index in ("svs_m", "flat_m"):
                client.hset(f"{index}:{doc}", mapping={"v": _fp32(vector)})

        def assert_matches_flat():
            def settled():
                return all(
                    FTInfoParser(client.execute_command("FT.INFO", i))
                    .mutation_queue_size == 0 for i in ("svs_m", "flat_m"))

            waiters.wait_for_equal(settled, True, timeout=30)
            svs = _knn_scores(client, "svs_m", query)
            flat = _knn_scores(client, "flat_m", query)
            assert list(svs)[0] == list(flat)[0], (svs, flat)
            for doc, score in flat.items():
                assert svs[doc] == pytest.approx(score, abs=1e-5), (svs, flat)

        # Doc 1 was orthogonal to the query; make it the nearest, scaled and
        # rotated so a missing normalization would show in its score.
        update(1, [2.9, 0.4, 0.0, 0.0] if metric == "COSINE"
               else [0.99, 0.14, 0.0, 0.0])
        assert_matches_flat()

        # 1,100 updates: past the 1,024 deleted-slot consolidation trigger.
        rng = random.Random(5)
        for _ in range(1100):
            doc = rng.randint(1, 5)
            v = [rng.uniform(-1, 1) for _ in range(DIM)]
            if metric == "COSINE":
                v = [x * rng.uniform(0.5, 4.0) for x in v]
            update(doc, v)
        assert_matches_flat()

    @pytest.mark.parametrize("metric", ["IP", "COSINE"])
    def test_zero_query_scores_one(self, metric):
        """A zero query has no similarity to any document: every score is
        1.0, never NaN."""
        client: Valkey = self.server.get_new_client()
        _create_metric_index(client, "svs_m", "SVS_VAMANA", metric)
        scores = _knn_scores(client, "svs_m", [0.0] * DIM)
        for doc, score in scores.items():
            assert score == pytest.approx(1.0, abs=1e-5), (doc, scores)

    def test_alpha_below_one_rejected_at_parse(self):
        """ALPHA < 1.0 under L2 fails inside SVS's build_dynamic and
        would leave the index permanently un-ingestable with no visible
        error. Checked at FT.CREATE instead."""
        client: Valkey = self.server.get_new_client()
        with pytest.raises(Exception) as exc_info:
            client.execute_command(
                "FT.CREATE", "alpha_idx",
                "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
                "8", "TYPE", "FLOAT32", "DIM", str(DIM),
                "DISTANCE_METRIC", "L2", "ALPHA", "0.5",
            )
        assert "ALPHA" in str(exc_info.value)

    def test_nan_vector_rejected_at_hset(self):
        """A vector with a non-finite component used to pin a reader
        thread for the life of the process on first search, and later
        writes to the same index would drain the writer pool. The
        writer path now rejects it: the NaN vector never joins the
        index, a follow-up good vector does, and the server stays
        responsive throughout."""
        client: Valkey = self.server.get_new_client()
        self._create_default_index(client, "nan_idx")
        # Three good vectors to make sure the first-add bootstrap already
        # ran, then one HSET whose first fp32 component is NaN.
        self._populate(client, 3)
        waiters.wait_for_equal(
            lambda: _v_index(client, "nan_idx")["size"], 3, timeout=5,
        )

        nan_bytes = struct.pack("<I", 0x7FC00000) + _fp32([0.0, 0.0, 0.0])
        client.hset("doc:nan", mapping={"v": nan_bytes})

        # The reader-thread lock-up bug surfaced as hangs on the next
        # FT.SEARCH / HSET; prove the server still responds quickly.
        assert client.ping() is True
        reply = client.execute_command(
            "FT.SEARCH", "nan_idx", "*=>[KNN 3 @v $q]",
            "PARAMS", "2", "q", _fp32([1.0, 0.0, 0.0, 0.0]),
            "DIALECT", "2",
        )
        assert reply[0] == 3

        # Follow-up good HSET; the writer processes in-order so waiting
        # for size == 4 implicitly waits for the NaN HSET to have been
        # dispositioned. The NaN vector was rejected, so size settles
        # at 4 (3 original + 1 good), not 5.
        client.hset("doc:4", mapping={"v": _fp32([4.0, 0.0, 0.0, 0.0])})
        waiters.wait_for_equal(
            lambda: _v_index(client, "nan_idx")["size"], 4, timeout=5,
        )
        assert _v_index(client, "nan_idx")["size"] == 4

    def test_fp16_wire_format_ingest_and_search(self):
        """TYPE FLOAT16 goes through the FP16->FP32 conversion in Add and
        Search. Vectors and query are 8-byte payloads instead of 16."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "fp16_idx",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "6", "TYPE", "FLOAT16", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        self._populate(client, 5, encode=_fp16)
        assert _v_index(client, "fp16_idx")["size"] == 5

        reply = client.execute_command(
            "FT.SEARCH", "fp16_idx", "*=>[KNN 3 @v $q]",
            "PARAMS", "2", "q", _fp16([2.0, 0.0, 0.0, 0.0]),
            "DIALECT", "2",
        )
        assert reply[0] == 3
        scores = _score_map(reply)
        # FP16 quantization introduces small error; check ordering and rough
        # distance rather than exact values.
        assert list(scores.keys())[0] == b"doc:2"
        assert scores[b"doc:2"] == pytest.approx(0.0, abs=1e-2)

    def test_external_memory_tracks_svs_allocations(self):
        """The custom SVS allocator routes graph-adjacency mmaps through
        ValkeyModule_IncrExternalMemory (Valkey PR #4128). Verify the
        counter moves with the lifetime of an SVS index: zero → positive
        after ingest → back near zero after drop. Also confirms the
        allocator vtable is actually installed."""
        client: Valkey = self.server.get_new_client()
        baseline = _module_external_memory(client)

        self._create_default_index(client, "mem_idx")
        # 200 vectors is enough to force the SVS graph builder to
        # allocate through our allocator; using DIM=4 keeps the raw data
        # small so any external growth is dominated by the graph.
        self._populate(client, 200)
        after_ingest = _module_external_memory(client)
        assert after_ingest > baseline, (
            f"external memory did not grow (baseline={baseline}, "
            f"after_ingest={after_ingest}); custom allocator vtable is "
            f"not being reached, or VM_IncrExternalMemory is unavailable"
        )

        client.execute_command("FT.DROPINDEX", "mem_idx")
        client.execute_command("FLUSHALL")
        after_drop = _module_external_memory(client)
        # Allow some residual — index teardown may not free every
        # allocation synchronously — but require it to shrink well
        # below the ingested-state figure.
        assert after_drop < baseline + (after_ingest - baseline) // 10, (
            f"external memory not reclaimed after DROPINDEX (baseline="
            f"{baseline}, after_ingest={after_ingest}, after_drop="
            f"{after_drop})"
        )

    def test_float16_input_stored_as_fp16(self):
        """TYPE FLOAT16 with COMPRESSION NONE stores FP16 inside SVS: its
        first reservation is half that of the same FLOAT32 index. FT.INFO
        reports INITIAL_CAP as the capacity."""
        client: Valkey = self.server.get_new_client()

        def reserved_bytes(index, vtype, encode):
            before = _module_external_memory(client)
            client.execute_command(
                "FT.CREATE", index, "PREFIX", "1", f"{index}:",
                "SCHEMA", "v", "VECTOR", "SVS_VAMANA", "8",
                "TYPE", vtype, "DIM", "64", "DISTANCE_METRIC", "L2",
                "INITIAL_CAP", "4096",
            )
            client.hset(f"{index}:1", mapping={"v": encode([1.0] * 64)})
            waiters.wait_for_equal(
                lambda: _v_index(client, index)["size"], 1, timeout=5)
            assert _v_index(client, index)["capacity"] == 4096
            return _module_external_memory(client) - before

        fp32 = reserved_bytes("cap32", "FLOAT32", _fp32)
        fp16 = reserved_bytes("cap16", "FLOAT16", _fp16)
        assert fp16 * 2 == pytest.approx(fp32, rel=0.25), (fp16, fp32)

    def test_knn_with_tag_filter_returns_only_matching(self):
        """Filtered KNN returns docs matching the tag predicate. Regression
        guard for the filter_rate hint: SVS's adaptive batch iterator takes
        an empty-return early-exit when the observed hit rate is less than
        the hinted rate, so a mis-hint of 1.0 would silently return zero
        results whenever the filter rejects any candidate."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "tag_idx",
            "SCHEMA",
            "color", "TAG",
            "v", "VECTOR", "SVS_VAMANA",
            "6", "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        # Ingest 10 docs. Even ids -> red; odd ids -> blue.
        for i in range(1, 11):
            values = [float(i)] + [0.0] * (DIM - 1)
            client.hset(f"doc:{i}", mapping={
                "v": _fp32(values),
                "color": "red" if i % 2 == 0 else "blue",
            })
        # Filter: only red. Query at (3.7,0,0,0). Nearest red is doc:4.
        reply = client.execute_command(
            "FT.SEARCH", "tag_idx", "@color:{red}=>[KNN 3 @v $q]",
            "PARAMS", "2", "q", _fp32([3.7, 0.0, 0.0, 0.0]),
            "DIALECT", "2",
        )
        assert reply[0] == 3, (
            f"filtered KNN returned {reply[0]} results (expected 3). "
            f"If zero, SvsFilterRate is likely hinting above the actual "
            f"hit rate."
        )
        # Every returned key must be an even doc (i.e. red).
        for i in range(1, len(reply), 2):
            key = reply[i]
            assert int(key.split(b":")[1]) % 2 == 0, \
                f"filter leaked non-red doc: {key!r}"

    @pytest.mark.parametrize("compression", ["NONE", "FP16"])
    def test_ft_create_supports_each_open_compression(self, compression):
        """NONE / FP16 are the v1 open compression kinds. SQ8 is fenced
        (see test_compression_sq8_rejected_at_parse); LVQ / LEANVEC are
        proprietary and rejected at parse."""
        client: Valkey = self.server.get_new_client()
        idx = f"comp_{compression.lower()}_idx"
        client.execute_command(
            "FT.CREATE", idx,
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "8", "TYPE", "FLOAT32", "DIM", str(DIM),
            "DISTANCE_METRIC", "L2", "COMPRESSION", compression,
        )
        self._populate(client, 3)
        idx_info = _v_index(client, idx)
        assert idx_info["size"] == 3
        assert idx_info["algorithm"]["compression"] == \
            f"SVS_COMPRESSION_{compression}"


class TestSVSVamanaDebugMode(ValkeySearchTestCaseDebugMode):
    """Tests that need debug-mode: changing the planner threshold ratio and
    reading developer INFO fields."""

    def test_search_timeout_without_partial_results(self):
        """A timed-out SVS search reports the timeout when partial results
        are disabled, and returns a (possibly truncated) reply when they are
        enabled, as HNSW and FLAT do. SVS polls the token only a few times
        per query, so this runs at the default poll frequency."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "to_idx", "PREFIX", "1", "t:",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "6", "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        for i in range(20):
            client.hset(f"t:{i}", mapping={"v": _fp32([float(i), 0, 0, 0])})
        waiters.wait_for_equal(
            lambda: _v_index(client, "to_idx")["size"], 20, timeout=5,
        )

        def knn(partial):
            return client.execute_command(
                "FT.SEARCH", "to_idx", "*=>[KNN 5 @v $q]",
                "PARAMS", "2", "q", _fp32([0.0, 0.0, 0.0, 0.0]),
                "TIMEOUT", "10", "SOMESHARDS" if partial else "ALLSHARDS",
                "DIALECT", "2",
            )

        assert knn(partial=False)[0] == 5
        client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "ForceTimeout", "yes")
        try:
            with pytest.raises(Exception) as exc_info:
                knn(partial=False)
            assert "cancelled due to timeout" in str(exc_info.value)
            assert isinstance(knn(partial=True), list)
        finally:
            client.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "ForceTimeout", "no")

    def test_nan_vector_counts_as_skipped(self):
        """A vector with a non-finite component is skipped as invalid data,
        like a wrong-size vector, rather than counted as an indexing
        failure."""
        client: Valkey = self.server.get_new_client()

        def failures():
            return int(client.info("SEARCH").get(
                "search_ingest_total_failures", 0))

        client.execute_command(
            "FT.CREATE", "nan_idx", "PREFIX", "1", "n:",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "6", "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        client.hset("n:1", mapping={"v": _fp32([1.0, 0.0, 0.0, 0.0])})
        before = failures()
        nan_bytes = struct.pack("<I", 0x7FC00000) + _fp32([0.0, 0.0, 0.0])
        client.hset("n:nan", mapping={"v": nan_bytes})
        client.hset("n:2", mapping={"v": _fp32([2.0, 0.0, 0.0, 0.0])})
        waiters.wait_for_equal(
            lambda: _v_index(client, "nan_idx")["size"], 2, timeout=5,
        )
        assert failures() == before

    def test_info_counts_svs_vector_field(self):
        """An SVS_VAMANA field is counted in number_of_vector_attributes and
        its ingests in ingest_field_vector."""
        client: Valkey = self.server.get_new_client()

        def info(field):
            return int(client.info("SEARCH").get(f"search_{field}", 0))

        attributes_before = info("number_of_vector_attributes")
        ingests_before = info("ingest_field_vector")
        index = "svs_info"
        client.execute_command(
            "FT.CREATE", index, "PREFIX", "1", "i:",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "6", "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        client.hset("i:1", mapping={"v": _fp32([1.0, 0.0, 0.0, 0.0])})
        waiters.wait_for_equal(
            lambda: FTInfoParser(
                client.execute_command("FT.INFO", index)).num_docs, 1,
            timeout=5,
        )

        assert info("number_of_vector_attributes") == attributes_before + 1
        assert info("ingest_field_vector") == ingests_before + 1

    @pytest.mark.parametrize("metric", ["IP", "COSINE"])
    def test_prefilter_and_index_paths_agree(self, metric):
        """The same filtered query gives the same scores whether the planner
        scores the filtered keys itself (pre-filter) or passes the filter to
        SVS's search. The threshold ratio forces each path in turn."""
        client: Valkey = self.server.get_new_client()
        _create_metric_index(client, "svs_m", "SVS_VAMANA", metric)

        def prefilter_count():
            return int(client.info("SEARCH").get(
                "search_prefiltering_requests_count", 0))

        ratio = "search.prefiltering-threshold-ratio"
        client.execute_command("CONFIG", "SET", ratio, "1.0")
        before = prefilter_count()
        prefiltered = _knn_scores(client, "svs_m", METRIC_QUERY[metric],
                                       "@color:{red}")
        assert prefilter_count() == before + 1

        client.execute_command("CONFIG", "SET", ratio, "0.0")
        before = prefilter_count()
        in_index = _knn_scores(client, "svs_m", METRIC_QUERY[metric],
                                    "@color:{red}")
        assert prefilter_count() == before
        client.execute_command("CONFIG", "SET", ratio, "0.001")

        assert list(prefiltered)[0] == list(in_index)[0] == 3
        for doc, expected in METRIC_EXPECTED.items():
            assert prefiltered[doc] == pytest.approx(expected, abs=1e-5)
            assert in_index[doc] == pytest.approx(expected, abs=1e-5)
