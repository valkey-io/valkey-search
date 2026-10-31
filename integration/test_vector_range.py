"""
Integration tests for Vector Range Search feature.

Tests the @field:[VECTOR_RANGE radius $blob] syntax in FT.SEARCH,
covering standalone queries, filter composition, rejection of a VECTOR_RANGE
predicate in a KNN filter,
query attributes, FT.SEARCH options, error handling, and dialect compatibility.
"""

import math
import struct
import time
import numpy as np
import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper, find_local_key


def float_to_bytes(floats):
    """Pack a list of floats into a little-endian binary blob."""
    return struct.pack(f"<{len(floats)}f", *floats)


def l2_distance(a, b):
    """Compute L2 (squared Euclidean) distance between two float lists."""
    return sum((x - y) ** 2 for x, y in zip(a, b, strict=True))


def parse_result_keys(result):
    """Extract key names from a NOCONTENT FT.SEARCH result."""
    return set(result[i].decode("utf-8") for i in range(1, len(result)))


def parse_result_with_fields(result):
    """Parse FT.SEARCH result into {key: {field: value}} dict."""
    parsed = {}
    for i in range(1, len(result), 2):
        key = result[i].decode("utf-8")
        fields = result[i + 1]
        field_dict = {
            fields[j].decode("utf-8"): fields[j + 1]
            for j in range(0, len(fields), 2)
        }
        parsed[key] = field_dict
    return parsed


# ---------------------------------------------------------------------------
# HNSW range search against FLAT at scale
# ---------------------------------------------------------------------------
# HNSW range search is approximate, FLAT is exact. On seeded clustered data,
# every HNSW result must be a FLAT result at the same distance, and HNSW must
# find most of the FLAT results. Radii lie midway between the FLAT distances
# of the n-th and (n+1)-th nearest keys, so each holds exactly n keys.
SCALE_KEYS = 10000
SCALE_DIM = 16
SCALE_QUERIES = 20
RANGE_SIZES = (1, 10, 100)
# None leaves $epsilon out of the query (default 0.01). $epsilon must be
# positive, so 1e-6 stands for a traversal shell at the radius itself.
EPSILONS = (None, 1e-6, 0.1)
# Mean recall over every query, radius and epsilon, and the floor for the mean
# of any one (range size, epsilon) cell.
MIN_RECALL = 0.95
MIN_CELL_RECALL = 0.9


def clustered_vectors(rng, n, dim=SCALE_DIM, clusters=20):
    """n float32 vectors around `clusters` random centers."""
    centers = rng.normal(0.0, 4.0, (clusters, dim))
    labels = rng.integers(0, clusters, n)
    return (centers[labels] + rng.normal(0.0, 1.0, (n, dim))).astype(
        np.float32)


def unit_rows(vectors):
    """Each row scaled to unit length, as float32 (a zero row stays zero)."""
    norms = np.linalg.norm(vectors.astype(np.float64), axis=1, keepdims=True)
    return (vectors / np.where(norms == 0.0, 1.0, norms)).astype(np.float32)


def range_field(metric):
    """Field holding the vectors for `metric`: IP uses the unit vectors."""
    return "u" if metric == "IP" else "v"


def create_range_indexes(client, metric, prefix="sc:", dim=SCALE_DIM,
                         ef_runtime=None):
    """Create FLAT and HNSW indexes over `prefix` for `metric`. Returns their
    names (flat, hnsw)."""
    names = []
    for algo in ("FLAT", "HNSW"):
        name = f"{algo.lower()}_{metric.lower()}"
        extra = []
        if algo == "HNSW" and ef_runtime is not None:
            extra = ["EF_RUNTIME", str(ef_runtime)]
        assert client.execute_command(
            "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", prefix,
            "SCHEMA", range_field(metric), "VECTOR", algo, str(6 + len(extra)),
            "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", metric,
            *extra,
        ) == b"OK"
        names.append(name)
    return tuple(names)


def write_vectors(client, ids, vectors, prefix="sc:"):
    """HSET key prefix+id with v = the vector and u = its unit vector."""
    pipe = client.pipeline(transaction=False)
    for i, vec, unit in zip(ids, vectors, unit_rows(vectors), strict=True):
        pipe.hset(f"{prefix}{i}", mapping={
            "v": vec.astype("<f4").tobytes(),
            "u": unit.astype("<f4").tobytes(),
        })
    pipe.execute()


def wait_indexed(clients, indexes, timeout=120):
    """Wait until every index is fully indexed on every client's node."""
    deadline = time.monotonic() + timeout
    for client in clients:
        for index in indexes:
            while True:
                try:
                    if IndexingTestHelper.is_indexing_complete_on_node(
                            client, index):
                        break
                except ResponseError:
                    pass  # Not yet created on this node.
                assert time.monotonic() < deadline, f"{index} not indexed"
                time.sleep(0.1)


def scale_queries(rng, vectors, metric, count=SCALE_QUERIES):
    """Query vectors near `count` random stored vectors."""
    picked = vectors[rng.choice(len(vectors), count, replace=False)]
    queries = (picked + rng.normal(0.0, 0.3, picked.shape)).astype(np.float32)
    return unit_rows(queries) if metric == "IP" else queries


def range_distances(client, index, field, query, radius, epsilon=None,
                    ef_runtime=None):
    """VECTOR_RANGE query yielding the distance. Returns {key: distance}."""
    attrs = "$yield_distance_as: d"
    if epsilon is not None:
        attrs = f"$epsilon: {epsilon}; " + attrs
    ef = f" EF_RUNTIME {ef_runtime}" if ef_runtime is not None else ""
    result = client.execute_command(
        "FT.SEARCH", index,
        f"@{field}:[VECTOR_RANGE $r $BLOB{ef}]=>{{{attrs}}}",
        "PARAMS", "4", "r", f"{radius:.9g}",
        "BLOB", query.astype("<f4").tobytes(),
        "RETURN", "1", "d", "LIMIT", "0", str(2 * SCALE_KEYS),
        "DIALECT", "2",
    )
    parsed = {key: float(fields["d"])
              for key, fields in parse_result_with_fields(result).items()}
    assert result[0] == len(parsed)
    return parsed


def knn_radii(client, flat_index, field, query, sizes=RANGE_SIZES):
    """[(n, radius holding the n nearest keys)] from a FLAT KNN query."""
    k = max(sizes) + 1
    result = client.execute_command(
        "FT.SEARCH", flat_index, f"*=>[KNN {k} @{field} $BLOB AS d]",
        "PARAMS", "2", "BLOB", query.astype("<f4").tobytes(),
        "RETURN", "1", "d", "LIMIT", "0", str(k), "DIALECT", "2",
    )
    dists = sorted(float(fields["d"])
                   for fields in parse_result_with_fields(result).values())
    assert len(dists) == k
    return [(n, (dists[n - 1] + dists[n]) / 2) for n in sizes]


def check_hnsw_range_matches_flat(client, metric, queries, flat_index,
                                  hnsw_index):
    """Every HNSW range result is a FLAT result at the same distance, and
    HNSW finds at least MIN_RECALL of the FLAT results on average. Returns
    the mean recall per (range size, epsilon)."""
    field = range_field(metric)
    recalls = {(n, eps): [] for n in RANGE_SIZES for eps in EPSILONS}
    for query in queries:
        for n, radius in knn_radii(client, flat_index, field, query):
            flat = range_distances(client, flat_index, field, query, radius)
            assert len(flat) == n, (metric, n, radius)
            for eps in EPSILONS:
                hnsw = range_distances(
                    client, hnsw_index, field, query, radius, eps)
                extra = hnsw.keys() - flat.keys()
                assert not extra, (metric, n, eps, extra)
                for key, dist in hnsw.items():
                    assert math.isclose(dist, flat[key], rel_tol=1e-5,
                                        abs_tol=1e-5), (metric, key)
                recalls[(n, eps)].append(len(hnsw) / n)
    means = {cell: sum(r) / len(r) for cell, r in recalls.items()}
    overall = sum(means.values()) / len(means)
    assert overall >= MIN_RECALL, (metric, overall, means)
    assert min(means.values()) >= MIN_CELL_RECALL, (metric, means)
    return means


def delete_and_reinsert(client, rng, vectors):
    """Delete 30% of the SCALE_KEYS keys, add 2000 new keys and give 1000
    surviving keys new vectors. Returns the vectors now stored."""
    deleted = rng.choice(SCALE_KEYS, 3 * SCALE_KEYS // 10, replace=False)
    pipe = client.pipeline(transaction=False)
    for i in deleted:
        pipe.delete(f"sc:{i}")
    pipe.execute()
    survivors = np.setdiff1d(np.arange(SCALE_KEYS), deleted)
    updated = rng.choice(survivors, 1000, replace=False)
    fresh = clustered_vectors(rng, 3000)
    added = np.arange(SCALE_KEYS, SCALE_KEYS + 2000)
    write_vectors(client, added, fresh[:2000])
    write_vectors(client, updated, fresh[2000:])
    stored = dict(zip(survivors, vectors[survivors]))
    stored.update(zip(added, fresh[:2000]))
    stored.update(zip(updated, fresh[2000:]))
    return np.array(list(stored.values()), dtype=np.float32)


def check_saturated_epsilon_matches_flat(client, flat_index, hnsw_index):
    """$epsilon values beyond float range (accepted as in Redis) widen the
    HNSW walk to the whole graph, so the HNSW range result is the FLAT one.
    Expects the 20 x 20 grid of integer points written by the caller."""
    query = np.array([3.3, 4.7], dtype=np.float32)
    radius = 20.5
    flat = range_distances(client, flat_index, "v", query, radius)
    assert 0 < len(flat) < 400
    for epsilon in ("inf", "nan", "1e40"):
        hnsw = range_distances(client, hnsw_index, "v", query, radius, epsilon)
        assert hnsw == pytest.approx(flat), epsilon


# ---------------------------------------------------------------------------
# Vectors used across tests (3-dimensional, L2 distance metric)
# ---------------------------------------------------------------------------
# Placed at known positions so distances are easy to reason about.
#   v0 = (0, 0, 0)  -- origin
#   v1 = (1, 0, 0)
#   v2 = (2, 0, 0)
#   v3 = (3, 0, 0)
#   v4 = (10, 0, 0) -- far away
VECTORS = {
    "doc:0": [0.0, 0.0, 0.0],
    "doc:1": [1.0, 0.0, 0.0],
    "doc:2": [2.0, 0.0, 0.0],
    "doc:3": [3.0, 0.0, 0.0],
    "doc:4": [10.0, 0.0, 0.0],
}

QUERY_VEC = [0.0, 0.0, 0.0]  # query from origin


class TestVectorRange(ValkeySearchTestCaseBase):
    """Integration tests for Vector Range queries."""

    # =================================================================
    # Helper methods
    # =================================================================

    def _create_hnsw_index(self, client, index_name="idx", prefix="doc:",
                           extra_fields=None, dim=3, distance="L2"):
        """Create an HNSW vector index with optional extra fields."""
        cmd = [
            "FT.CREATE", index_name, "ON", "HASH",
            "PREFIX", "1", prefix,
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(dim),
            "DISTANCE_METRIC", distance,
        ]
        if extra_fields:
            cmd.extend(extra_fields)
        assert client.execute_command(*cmd) == b"OK"

    def _create_flat_index(self, client, index_name="idx", prefix="doc:",
                           extra_fields=None, dim=3, distance="L2"):
        """Create a FLAT vector index with optional extra fields."""
        cmd = [
            "FT.CREATE", index_name, "ON", "HASH",
            "PREFIX", "1", prefix,
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", str(dim),
            "DISTANCE_METRIC", distance,
        ]
        if extra_fields:
            cmd.extend(extra_fields)
        assert client.execute_command(*cmd) == b"OK"

    def _load_vector_data(self, client, vectors=None, extra_data=None):
        """Load vector data into hash keys. extra_data maps key -> {field: val}."""
        if vectors is None:
            vectors = VECTORS
        for key, vec in vectors.items():
            mapping = {"vec": float_to_bytes(vec)}
            if extra_data and key in extra_data:
                mapping.update(extra_data[key])
            client.hset(key, mapping=mapping)

    def _search(self, client, index, query, *args):
        """Execute FT.SEARCH and return raw result."""
        cmd = ["FT.SEARCH", index, query] + list(args)
        return client.execute_command(*cmd)

    def _cosine_indexes(self, client, dim):
        """Create a COSINE FLAT and a COSINE HNSW index with a tag field.
        Returns the (index name, key prefix) of each."""
        indexes = []
        for algo, create in (("FLAT", self._create_flat_index),
                             ("HNSW", self._create_hnsw_index)):
            name = f"{algo}{dim}"
            create(client, index_name=name, prefix=f"{name}:", dim=dim,
                   distance="COSINE", extra_fields=["tag", "TAG"])
            indexes.append((name, f"{name}:"))
        return indexes

    def _yielded_distances(self, client, index, query, blob, radius, *args):
        """Run a VECTOR_RANGE query yielding `dist`, sorted by it. `query` has
        a {} placeholder for the range clause. Returns [(key, dist)] in reply
        order."""
        clause = "@vec:[VECTOR_RANGE $r $blob]=>{$yield_distance_as: dist}"
        result = self._search(
            client, index, query.format(clause),
            "PARAMS", "4", "blob", blob, "r", str(radius),
            "SORTBY", "dist", "RETURN", "1", "dist", *args,
        )
        return [(key, float(fields["dist"])) for key, fields
                in parse_result_with_fields(result).items()]


    # =================================================================
    # 1. Standalone Vector Range — HNSW index
    # =================================================================

    def test_standalone_vector_range_hnsw(self):
        """
        Standalone Vector Range query on HNSW index returns exactly the
        vectors within the specified radius.
        Req: 1.1, 2.1
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        # L2 distances from origin: doc:0=0, doc:1=1, doc:2=4, doc:3=9, doc:4=100
        # radius=5 should include doc:0 (0), doc:1 (1), doc:2 (4)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 2. Standalone Vector Range — FLAT index
    # =================================================================

    def test_standalone_vector_range_flat(self):
        """
        Standalone Vector Range query on FLAT index returns exactly the
        vectors within the specified radius.
        Req: 1.1, 2.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=1 should include doc:0 (0) and doc:1 (1)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 1 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1"}

    # =================================================================
    # 3. Radius = 0 returns only exact matches
    # =================================================================

    def test_vector_range_radius_zero(self):
        """
        Radius 0 returns only vectors at distance exactly 0.
        Req: 2.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 0 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 1
        keys = parse_result_keys(result)
        assert keys == {"doc:0"}

    # =================================================================
    # 4. Large radius returns all vectors
    # =================================================================

    def test_vector_range_large_radius(self):
        """
        A sufficiently large radius returns all indexed vectors.
        Req: 2.1
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 1000 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 5

    # =================================================================
    # HNSW candidate cap and the fetch-limited counter
    # =================================================================

    def test_flat_range_hard_cap_and_counter(self):
        """FLAT distinguishes exactly N matches from an N+1 witness."""
        client = self.server.get_new_client()
        client.config_set("search.info-developer-visible", "yes")
        client.config_set("search.max-nonvector-search-results-fetched", "3")
        self._create_flat_index(client, dim=2)
        vectors = {
            "doc:0": [0.0, 0.0],
            "doc:1": [1.0, 0.0],
            "doc:2": [2.0, 0.0],
            "doc:3": [3.0, 0.0],
        }
        self._load_vector_data(
            client, vectors={key: vectors[key] for key in list(vectors)[:3]})
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")

        counter = "search_nonvector_results_fetched_limited_count"
        query = "@vec:[VECTOR_RANGE 100 $blob]=>{$yield_distance_as: dist}"
        args = (
            "PARAMS", "2", "blob", float_to_bytes([0.0, 0.0]),
            "RETURN", "1", "dist", "LIMIT", "0", "10",
        )
        before = client.info("search").get(counter, 0)
        exact = self._search(client, "idx", query, *args)
        assert exact[0] == 3
        assert len(parse_result_with_fields(exact)) == 3
        assert client.info("search").get(counter, 0) == before

        self._load_vector_data(client, vectors={"doc:3": vectors["doc:3"]})
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        truncated = self._search(client, "idx", query, *args)
        retained = parse_result_with_fields(truncated)
        assert truncated[0] == len(retained) == 3
        assert retained.keys() <= vectors.keys()
        for key, fields in retained.items():
            assert float(fields["dist"]) == pytest.approx(
                l2_distance([0.0, 0.0], vectors[key]))
        after_truncated = client.info("search").get(counter, 0)
        assert after_truncated == before + 1

        client.config_set("search.max-nonvector-search-results-fetched", "0")
        assert self._search(client, "idx", query, *args) == [0]
        assert client.info("search").get(counter, 0) == after_truncated + 1

    def test_hnsw_range_hard_cap_walk_and_exact_fallback(self):
        """The HNSW walk and its COSINE radius-2 exact fallback cap at N."""
        client = self.server.get_new_client()
        client.config_set("search.info-developer-visible", "yes")
        client.config_set("search.max-nonvector-search-results-fetched", "5")
        self._create_hnsw_index(
            client, index_name="walk", prefix="walk:", dim=2)
        self._create_hnsw_index(
            client, index_name="exact", prefix="exact:", dim=2,
            distance="COSINE")
        for i in range(6):
            client.hset(
                f"walk:{i}", mapping={"vec": float_to_bytes([0.0, 0.0])})
            client.hset(
                f"exact:{i}", mapping={"vec": float_to_bytes([1.0, 0.0])})
        wait_indexed([client], ("walk", "exact"))

        counter = "search_nonvector_results_fetched_limited_count"
        before = client.info("search").get(counter, 0)
        for index, radius, query in (
                ("walk", 0, [0.0, 0.0]),
                ("exact", 2, [1.0, 0.0])):
            result = self._search(
                client, index, f"@vec:[VECTOR_RANGE {radius} $blob]",
                "PARAMS", "2", "blob", float_to_bytes(query),
                "NOCONTENT", "LIMIT", "0", "10",
            )
            assert result[0] == 5
            assert len(result) == 6
            after = client.info("search").get(counter, 0)
            assert after == before + 1
            before = after

    def test_hnsw_pagination_matches_flat_at_cap(self):
        """When the true count is at most N, HNSW pages match exact FLAT."""
        client = self.server.get_new_client()
        client.config_set("search.info-developer-visible", "yes")
        client.config_set("search.max-nonvector-search-results-fetched", "4")
        flat_index, hnsw_index = create_range_indexes(
            client, "L2", prefix="page:", dim=2)
        vectors = np.array([
            [0.0, 0.0],
            [1.0, 0.0],
            [2.0, 0.0],
            [3.0, 0.0],
            [10.0, 0.0],
            [11.0, 0.0],
        ], dtype=np.float32)
        write_vectors(client, range(len(vectors)), vectors, prefix="page:")
        wait_indexed([client], (flat_index, hnsw_index))

        counter = "search_nonvector_results_fetched_limited_count"
        before = client.info("search").get(counter, 0)
        query = "@v:[VECTOR_RANGE 10 $blob]=>{$yield_distance_as: dist}"

        def page(index, offset, count):
            return self._search(
                client, index, query,
                "PARAMS", "2", "blob", float_to_bytes([0.0, 0.0]),
                "SORTBY", "dist", "ASC", "NOCONTENT",
                "LIMIT", str(offset), str(count),
            )

        for offset, count, expected_keys in (
                (0, 2, [b"page:0", b"page:1"]),
                (2, 2, [b"page:2", b"page:3"])):
            flat = page(flat_index, offset, count)
            hnsw = page(hnsw_index, offset, count)
            assert flat == hnsw == [4, *expected_keys]
        assert client.info("search").get(counter, 0) == before

    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_hnsw_range_matches_flat_at_scale(self, metric):
        """
        On 10k clustered 16-d vectors, HNSW range results are FLAT results at
        the same distance, and HNSW recall against FLAT is at least
        MIN_RECALL, for radii holding ~1, ~10 and ~100 keys and $epsilon
        omitted, 1e-6 and 0.1.
        """
        client = self.server.get_new_client()
        rng = np.random.default_rng(41)
        flat_index, hnsw_index = create_range_indexes(client, metric)
        vectors = clustered_vectors(rng, SCALE_KEYS)
        write_vectors(client, range(SCALE_KEYS), vectors)
        wait_indexed([client], (flat_index, hnsw_index))
        check_hnsw_range_matches_flat(
            client, metric, scale_queries(rng, vectors, metric),
            flat_index, hnsw_index)

    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_hnsw_range_matches_flat_after_deletes(self, metric):
        """
        HNSW range results still match FLAT after 30% of the keys are
        deleted, 2000 keys are added and 1000 keys get new vectors: no
        deleted key and no stale distance is returned, and recall holds.
        """
        client = self.server.get_new_client()
        rng = np.random.default_rng(42)
        flat_index, hnsw_index = create_range_indexes(client, metric)
        vectors = clustered_vectors(rng, SCALE_KEYS)
        write_vectors(client, range(SCALE_KEYS), vectors)
        wait_indexed([client], (flat_index, hnsw_index))
        stored = delete_and_reinsert(client, rng, vectors)
        wait_indexed([client], (flat_index, hnsw_index))
        assert client.dbsize() == len(stored) == 9000
        check_hnsw_range_matches_flat(
            client, metric, scale_queries(rng, stored, metric),
            flat_index, hnsw_index)

    def test_hnsw_range_small_ball_far_from_entry_point(self):
        """
        A small ball around a tight cluster far from the rest of the data,
        inserted after it so the graph entry point lies elsewhere, returns
        the keys FLAT returns: all 20 keys of the cluster, or its 5 nearest.
        The walk locates the ball with an EF_RUNTIME-wide beam, as KNN does;
        with the default EF_RUNTIME of 10, KNN and range search both miss the
        third cluster here, so the index uses 100.
        """
        client = self.server.get_new_client()
        rng = np.random.default_rng(43)
        flat_index, hnsw_index = create_range_indexes(
            client, "L2", ef_runtime=100)
        cloud = rng.normal(0.0, 1.0, (5000, SCALE_DIM)).astype(np.float32)
        write_vectors(client, range(5000), cloud)
        centers = [np.full(SCALE_DIM, 30.0 * sign, dtype=np.float32)
                   for sign in (1, -1)]
        centers.append(np.where(np.arange(SCALE_DIM) % 2, 30.0, -30.0)
                       .astype(np.float32))
        balls = []
        for j, center in enumerate(centers):
            ids = range(5000 + 20 * j, 5020 + 20 * j)
            ball = (center + rng.normal(0.0, 0.05, (20, SCALE_DIM))).astype(
                np.float32)
            write_vectors(client, ids, ball)
            balls.append(({f"sc:{i}" for i in ids}, center, ball))
        wait_indexed([client], (flat_index, hnsw_index))

        for keys, center, ball in balls:
            # Squared distances from the center: the ball's keys lie within
            # ~0.1, the nearest other key ~3000 away.
            dists = np.sort(((ball.astype(np.float64) - center) ** 2).sum(1))
            for radius, expected in ((1.0, 20), ((dists[4] + dists[5]) / 2, 5)):
                flat = range_distances(client, flat_index, "v", center, radius)
                assert len(flat) == expected and flat.keys() <= keys
                for eps in EPSILONS:
                    hnsw = range_distances(
                        client, hnsw_index, "v", center, radius, eps)
                    assert hnsw.keys() == flat.keys(), (radius, eps)

    # =================================================================
    # 5. Vector Range AND tag filter
    # =================================================================

    def test_vector_range_and_tag(self):
        """
        Vector Range combined with tag filter via AND returns intersection.
        Req: 2.3
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client, extra_fields=["category", "TAG"])
        extra = {
            "doc:0": {"category": "A"},
            "doc:1": {"category": "B"},
            "doc:2": {"category": "A"},
            "doc:3": {"category": "B"},
            "doc:4": {"category": "A"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=5 matches doc:0,1,2; category A matches doc:0,2,4
        # intersection = doc:0, doc:2
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] @category:{A}",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:2"}

    def test_vector_range_and_prefix(self):
        """
        Vector Range AND a prefix. One evaluator handles every candidate and
        fetches each key's own text tree lazily; a prefix is answered from
        that tree, where a term is answered from the shared postings. Each
        candidate matches through a different word, so a tree left over from
        the previous key gets the later ones wrong. The negated form is the
        one that reaches the evaluator; without the negation the AND trusts
        the prefix fetcher.
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client, extra_fields=["body", "TEXT"])
        extra = {
            "doc:0": {"body": "alpha beta"},
            "doc:1": {"body": "alpha gamma"},
            "doc:2": {"body": "beta gamut"},
            "doc:3": {"body": "alpha"},
            "doc:4": {"body": "gamble"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=5 matches doc:0,1,2; gam* matches doc:1,2,4.
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] @body:gam*",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2, result
        assert parse_result_keys(result) == {"doc:1", "doc:2"}

        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] -@body:gam*",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 1, result
        assert parse_result_keys(result) == {"doc:0"}

    # =================================================================
    # 6. Vector Range AND numeric filter
    # =================================================================

    def test_vector_range_and_numeric(self):
        """
        Vector Range combined with numeric filter via AND.
        Req: 2.3
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client, extra_fields=["score", "NUMERIC"])
        extra = {
            "doc:0": {"score": "10"},
            "doc:1": {"score": "20"},
            "doc:2": {"score": "30"},
            "doc:3": {"score": "40"},
            "doc:4": {"score": "50"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=5 matches doc:0,1,2; score>=25 matches doc:2,3,4
        # intersection = doc:2
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] @score:[25 +inf]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 1
        keys = parse_result_keys(result)
        assert keys == {"doc:2"}

    # =================================================================
    # 7. Vector Range OR tag filter
    # =================================================================

    def test_vector_range_or_tag(self):
        """
        Vector Range combined with tag filter via OR returns union.
        Req: 2.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["category", "TAG"])
        extra = {
            "doc:0": {"category": "A"},
            "doc:1": {"category": "B"},
            "doc:2": {"category": "A"},
            "doc:3": {"category": "B"},
            "doc:4": {"category": "A"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=0 matches doc:0 only; category B matches doc:1, doc:3
        # union = doc:0, doc:1, doc:3
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 0 $blob] | @category:{B}",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:3"}

    def test_vector_range_or_hard_cap_has_unspecified_survivors(self):
        """OR retains N unspecified survivors and records one truncation."""
        client = self.server.get_new_client()
        client.config_set("search.info-developer-visible", "yes")
        client.config_set("search.max-nonvector-search-results-fetched", "3")
        self._create_flat_index(client, extra_fields=["category", "TAG"])
        vectors = {f"doc:v{i}": [i * 0.1, 0.0, 0.0] for i in range(6)}
        extra = {key: {"category": "ordinary"} for key in vectors}
        self._load_vector_data(client, vectors=vectors, extra_data=extra)
        client.hset("doc:special", mapping={"category": "special"})
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")

        expected_union = {*vectors, "doc:special"}
        counter = "search_nonvector_results_fetched_limited_count"
        params = ("PARAMS", "2", "blob", float_to_bytes(QUERY_VEC))
        for query in (
                "@vec:[VECTOR_RANGE 1 $blob] | @category:{special}",
                "@category:{special} | @vec:[VECTOR_RANGE 1 $blob]"):
            before = client.info("search").get(counter, 0)
            result = self._search(
                client, "idx", query, *params,
                "NOCONTENT", "LIMIT", "0", "100",
            )
            survivors = parse_result_keys(result)
            assert result[0] == len(survivors) == 3
            assert survivors <= expected_union
            # The VECTOR_RANGE child and the final OR union both have more
            # matches than the cap, but one query increments the counter once.
            assert client.info("search").get(counter, 0) == before + 1


    # =================================================================
    # 8. Negated Vector Range
    # =================================================================

    def test_vector_range_negate(self):
        """
        Negated Vector Range returns keys whose distance exceeds the radius
        plus keys lacking the vector field.
        Req: 2.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["category", "TAG"])
        extra = {
            "doc:0": {"category": "A"},
            "doc:1": {"category": "A"},
            "doc:2": {"category": "A"},
            "doc:3": {"category": "A"},
            "doc:4": {"category": "A"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=1 matches doc:0 (0), doc:1 (1)
        # negation should return doc:2, doc:3, doc:4
        result = self._search(
            client, "idx",
            "-@vec:[VECTOR_RANGE 1 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:2", "doc:3", "doc:4"}

    # =================================================================
    # 9. Vector Range pre-filter + KNN (rejected)
    # =================================================================

    def test_vector_range_prefilter_knn_rejected(self):
        """
        A VECTOR_RANGE predicate in a KNN pre-filter is rejected in the
        single-VR model (RC1). Req: 3.1, 3.2 (rejection).
        """
        client = self.server.get_new_client()
        # Need two vector fields: one for range filter, one for KNN
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
            "vec2", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
        ]
        assert client.execute_command(*cmd) == b"OK"

        # Load data: vec for range filter, vec2 for KNN
        for key, v in VECTORS.items():
            client.hset(key, mapping={
                "vec": float_to_bytes(v),
                "vec2": float_to_bytes(v),
            })

        query_blob = float_to_bytes(QUERY_VEC)
        knn_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError, match="KNN"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $range_blob]=>[KNN 2 @vec2 $knn_blob]",
                "PARAMS", "4", "range_blob", query_blob, "knn_blob", knn_blob,
                "NOCONTENT",
            )

    # =================================================================
    # 10. Vector Range pre-filter + KNN with fewer candidates than K (rejected)
    # =================================================================

    def test_vector_range_prefilter_knn_fewer_candidates_rejected(self):
        """
        A VECTOR_RANGE predicate in a KNN pre-filter is rejected in the
        single-VR model, regardless of candidate count. Req: 3.2 (rejection).
        """
        client = self.server.get_new_client()
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
            "vec2", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
        ]
        assert client.execute_command(*cmd) == b"OK"

        for key, v in VECTORS.items():
            client.hset(key, mapping={
                "vec": float_to_bytes(v),
                "vec2": float_to_bytes(v),
            })

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError, match="KNN"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 1 $range_blob]=>[KNN 10 @vec2 $knn_blob]",
                "PARAMS", "4", "range_blob", query_blob, "knn_blob", query_blob,
                "NOCONTENT",
            )

    # =================================================================
    # 11. Query attributes: $yield_distance_as
    # =================================================================

    def test_query_attr_yield_distance_as(self):
        """
        Custom distance field name via =>{$yield_distance_as: name} suffix.
        Req: 4.1, 4.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: my_dist}",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] == 3
        # Check that the custom distance field name is used
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "my_dist" in fields, f"Expected 'my_dist' field in {key}"

    # =================================================================
    # 12. Query attributes: $epsilon
    # =================================================================

    def test_query_attr_epsilon(self):
        """
        $epsilon query attribute is accepted and does not cause errors.
        Req: 4.2
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$epsilon: 0.5}",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] >= 1

    def test_query_attr_epsilon_saturated(self):
        """
        $epsilon: inf, nan and 1e40 on an HNSW field return the FLAT result.
        """
        client = self.server.get_new_client()
        flat_index, hnsw_index = create_range_indexes(client, "L2", dim=2)
        grid = np.array([[i % 20, i // 20] for i in range(400)],
                        dtype=np.float32)
        write_vectors(client, range(400), grid)
        wait_indexed([client], (flat_index, hnsw_index))
        check_saturated_epsilon_matches_flat(client, flat_index, hnsw_index)

    # =================================================================
    # 13. Query attributes: both $yield_distance_as and $epsilon
    # =================================================================

    def test_query_attr_both(self):
        """
        Both $yield_distance_as and $epsilon together.
        Req: 4.1, 4.2
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: dist; $epsilon: 0.1}",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] >= 1
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "dist" in fields

    # =================================================================
    # 14. Distance field requires an explicit name (no __<field>_score)
    # =================================================================

    def test_distance_field_requires_explicit_alias(self):
        """
        The VR distance is surfaced only under an explicit $yield_distance_as
        alias (Redisearch parity): with the alias the field is present, and
        without it no default __<field>_score field is emitted.
        Req: 4.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)

        # With an explicit alias the distance field is emitted under that name.
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: dist}",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] >= 1
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "dist" in fields, f"Expected 'dist' in {key}, got {list(fields.keys())}"

        # Without an alias no default distance field is emitted.
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
        )
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "__vec_score" not in fields, (
                f"Unexpected default '__vec_score' in {key}: {list(fields.keys())}"
            )


    # =================================================================
    # 15. Default order (no SORTBY) is key order, not distance order
    # =================================================================

    def test_results_default_key_order(self):
        """
        Without SORTBY, Vector Range results use the default non-vector order
        (key order, as Redis returns doc-id order), not ascending distance.
        Req: 5.1, 5.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        # Distances from the origin are deliberately not in key order.
        self._load_vector_data(client, vectors={
            "doc:0": [3.0, 0.0, 0.0],
            "doc:1": [0.0, 0.0, 0.0],
            "doc:2": [2.0, 0.0, 0.0],
            "doc:3": [1.0, 0.0, 0.0],
            "doc:4": [10.0, 0.0, 0.0],
        })

        query_blob = float_to_bytes(QUERY_VEC)
        # Default order (no SORTBY): document/key order, not distance order.
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]=>{$yield_distance_as: dist}",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] == 5
        keys = [result[i].decode("utf-8") for i in range(1, len(result), 2)]
        assert keys == ["doc:0", "doc:1", "doc:2", "doc:3", "doc:4"]

    def test_limit_page_and_withscores_match_redis(self):
        """
        Without SORTBY, LIMIT pages and WITHSCORES follow the non-vector
        defaults, as in Redis 8.10.2 (expected replies observed there): a
        standalone Vector Range scores 0 and returns key order; with a tag
        predicate the tag relevance orders the results and the VR predicate
        adds 0. The distance stays in the yielded field, and SORTBY on it
        still orders by distance.
        """
        client = self.server.get_new_client()
        self._create_flat_index(
            client, extra_fields=["category", "TAG", "body", "TEXT"])
        # L2 distances from the origin: doc:0=9, doc:1=0, doc:2=4, doc:3=1,
        # doc:4=100 (outside radius 10). Body lengths differ so the tag
        # relevance score differs per document.
        vectors = {
            "doc:0": [3.0, 0.0, 0.0],
            "doc:1": [0.0, 0.0, 0.0],
            "doc:2": [2.0, 0.0, 0.0],
            "doc:3": [1.0, 0.0, 0.0],
            "doc:4": [10.0, 0.0, 0.0],
        }
        extra = {
            "doc:0": {"category": "A", "body": "alpha beta"},
            "doc:1": {"category": "A", "body": "alpha beta gamma delta"},
            "doc:2": {"category": "B", "body": "alpha beta gamma"},
            "doc:3": {"category": "A", "body": "alpha"},
            "doc:4": {"category": "B",
                      "body": "alpha beta gamma delta epsilon"},
        }
        self._load_vector_data(client, vectors=vectors, extra_data=extra)
        params = ["PARAMS", "2", "blob", float_to_bytes(QUERY_VEC)]
        vr = "@vec:[VECTOR_RANGE 10 $blob]=>{$yield_distance_as: dist}"

        def keys_and_scores(result):
            keys = [result[i].decode("utf-8") for i in range(1, len(result), 2)]
            return keys, [float(result[i]) for i in range(2, len(result), 2)]

        # Standalone VR: key order and a score of 0 for every document.
        result = self._search(client, "idx", vr, *params,
                              "WITHSCORES", "NOCONTENT")
        assert keys_and_scores(result) == (
            ["doc:0", "doc:1", "doc:2", "doc:3"], [0.0] * 4)
        result = self._search(client, "idx", vr, *params,
                              "NOCONTENT", "LIMIT", "0", "2")
        assert result == [4, b"doc:0", b"doc:1"]

        # VR AND tag: ordered and scored like the tag-only query, not by
        # distance (which would be doc:1, doc:3, doc:0).
        vr_and_tag = f"{vr} @category:{{A}}"
        result = self._search(client, "idx", vr_and_tag, *params,
                              "WITHSCORES", "NOCONTENT")
        keys, scores = keys_and_scores(result)
        assert keys == ["doc:3", "doc:0", "doc:1"]
        assert scores == pytest.approx([0.741120, 0.624101, 0.474317],
                                       abs=1e-5)
        tag_only = self._search(client, "idx", "@category:{A}",
                                "WITHSCORES", "NOCONTENT")
        assert keys_and_scores(tag_only) == (keys, scores)
        result = self._search(client, "idx", vr_and_tag, *params,
                              "NOCONTENT", "LIMIT", "0", "2")
        assert result == [3, b"doc:3", b"doc:0"]
        result = self._search(client, "idx", vr_and_tag, *params,
                              "WITHSCORES", "RETURN", "1", "dist",
                              "LIMIT", "1", "2")
        assert result[0] == 3
        assert (result[1], result[3]) == (b"doc:0", [b"dist", b"9"])
        assert (result[4], result[6]) == (b"doc:1", [b"dist", b"0"])

        # SORTBY on the yielded distance still orders by distance.
        for order, expected in (
                ("ASC", [b"doc:1", b"doc:3", b"doc:2", b"doc:0"]),
                ("DESC", [b"doc:0", b"doc:2", b"doc:3", b"doc:1"])):
            result = self._search(client, "idx", vr, *params,
                                  "SORTBY", "dist", order, "NOCONTENT")
            assert result == [4] + expected, order

    # =================================================================
    # 16. Distance scores are correct
    # =================================================================

    def test_distance_scores_correct(self):
        """
        Returned distance scores match expected L2 distances.
        Req: 5.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]=>{$yield_distance_as: dist}",
            "PARAMS", "2", "blob", query_blob,
        )
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            actual_dist = float(fields["dist"])
            expected_dist = l2_distance(QUERY_VEC, VECTORS[key])
            assert abs(actual_dist - expected_dist) < 0.01, (
                f"{key}: expected dist {expected_dist}, got {actual_dist}"
            )

    # =================================================================
    # 17. SORTBY overrides the default ordering
    # =================================================================

    def test_sortby_overrides_distance_order(self):
        """
        SORTBY on a non-distance field overrides the default order.
        Req: 5.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["score", "NUMERIC"])
        extra = {
            "doc:0": {"score": "50"},
            "doc:1": {"score": "10"},
            "doc:2": {"score": "30"},
            "doc:3": {"score": "20"},
            "doc:4": {"score": "40"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "SORTBY", "score", "ASC",
            "RETURN", "1", "score",
        )
        assert result[0] == 5
        # Extract scores in result order
        scores = []
        for i in range(1, len(result), 2):
            fields = result[i + 1]
            field_dict = {
                fields[j].decode("utf-8"): fields[j + 1]
                for j in range(0, len(fields), 2)
            }
            scores.append(float(field_dict["score"]))
        assert scores == sorted(scores), f"Scores not in ASC order: {scores}"

    # =================================================================
    # 18. LIMIT applied after sorting
    # =================================================================

    def test_limit_after_sort(self):
        """
        LIMIT offset/count slicing works correctly with Vector Range results.
        Req: 5.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        # Get all 5 results first
        full_result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert full_result[0] == 5

        # LIMIT 1 2: skip first, take next 2
        limited_result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "LIMIT", "1", "2",
            "NOCONTENT",
        )
        # Total count should still be 5
        assert limited_result[0] == 5
        # But only 2 keys returned
        returned_keys = [limited_result[i] for i in range(1, len(limited_result))]
        assert len(returned_keys) == 2

    # =================================================================
    # 19. LIMIT 0 0 returns only count
    # =================================================================

    def test_limit_zero(self):
        """
        LIMIT 0 0 returns only the total count, no keys.
        Req: 5.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "LIMIT", "0", "0",
        )
        assert result[0] == 5
        assert len(result) == 1  # only count

    # =================================================================
    # 20. NOCONTENT suppresses fields and distance scores
    # =================================================================

    def test_nocontent(self):
        """
        NOCONTENT returns only key names, no fields or distance scores.
        Req: 6.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        count = result[0]
        assert count == 3
        # With NOCONTENT, result is [count, key1, key2, ...]
        assert len(result) == count + 1
        for i in range(1, len(result)):
            assert isinstance(result[i], bytes)  # just key names

    # =================================================================
    # 21. RETURN includes distance score plus specified fields
    # =================================================================

    def test_return_with_fields(self):
        """
        RETURN includes specified fields plus the distance score field.
        Req: 6.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["category", "TAG"])
        extra = {k: {"category": "test"} for k in VECTORS}
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: dist}",
            "PARAMS", "2", "blob", query_blob,
            "RETURN", "2", "category", "dist",
        )
        assert result[0] == 3
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "dist" in fields, f"Missing distance score in {key}"
            assert "category" in fields, f"Missing category in {key}"


    # =================================================================
    # 22. Error: missing PARAMS for vector blob
    # =================================================================

    def test_error_missing_blob_param(self):
        """
        Missing PARAMS entry for the vector blob parameter returns error.
        Req: 1.6, 7.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $nonexistent]",
                "PARAMS", "0",
            )

    # =================================================================
    # 23. Error: dimension mismatch
    # =================================================================

    def test_error_dimension_mismatch(self):
        """
        Vector blob with wrong dimensions returns error.
        Req: 1.7
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, dim=3)
        self._load_vector_data(client)

        # Provide a 2D vector for a 3D index
        wrong_blob = float_to_bytes([1.0, 2.0])
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob]",
                "PARAMS", "2", "blob", wrong_blob,
            )

    # =================================================================
    # 24. Error: missing closing bracket
    # =================================================================

    def test_error_missing_closing_bracket(self):
        """
        Missing closing ] in Vector Range clause returns parse error.
        Req: 7.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 25. Error: missing radius
    # =================================================================

    def test_error_missing_radius(self):
        """
        VECTOR_RANGE without radius returns error.
        Req: 7.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError, match="radius"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 26. Error: missing vector blob parameter
    # =================================================================

    def test_error_missing_blob_in_syntax(self):
        """
        VECTOR_RANGE with radius but no blob param returns error.
        Req: 7.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        with pytest.raises(ResponseError, match="blob"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5]",
                "PARAMS", "0",
            )

    # =================================================================
    # 27. Error: negative radius
    # =================================================================

    def test_error_negative_radius(self):
        """
        Negative radius returns error.
        Req: 1.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError, match="non-negative"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE -1 $blob]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 28. Error: unknown optional parameter
    # =================================================================

    def test_error_unknown_optional_param(self):
        """
        Unknown keyword in optional params section returns error.
        Req: 7.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob UNKNOWN_PARAM 42]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 29. Error: non-vector field
    # =================================================================

    def test_error_non_vector_field(self):
        """
        Using VECTOR_RANGE on a non-vector field returns error.
        Req: 1.4
        """
        client = self.server.get_new_client()
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "price", "NUMERIC",
        ]
        assert client.execute_command(*cmd) == b"OK"
        client.hset("doc:0", mapping={"price": "10"})

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@price:[VECTOR_RANGE 5 $blob]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 30. Error: empty $yield_distance_as
    # =================================================================

    def test_error_empty_yield_distance_as(self):
        """
        Empty $yield_distance_as value returns error.
        Req: 4.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: }",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 31. Error: invalid $epsilon
    # =================================================================

    def test_error_invalid_epsilon(self):
        """
        Invalid $epsilon value returns error.
        Req: 4.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob]=>{$epsilon: notanumber}",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 32. Error: negative $epsilon
    # =================================================================

    def test_error_negative_epsilon(self):
        """
        Negative $epsilon value returns error.
        Req: 4.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob]=>{$epsilon: -0.5}",
                "PARAMS", "2", "blob", query_blob,
            )


    # =================================================================
    # 33. DIALECT 2 compatibility
    # =================================================================

    def test_dialect_2(self):
        """
        Vector Range query works with DIALECT 2.
        Req: 6.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "DIALECT", "2",
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 34. DIALECT 3 compatibility
    # =================================================================

    def test_dialect_3(self):
        """
        Vector Range query works with DIALECT 3.
        Req: 6.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "DIALECT", "3",
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 35. DIALECT 4 compatibility
    # =================================================================

    def test_dialect_4(self):
        """
        Vector Range query works with DIALECT 4.
        Req: 6.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "DIALECT", "4",
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 36. Parameterized radius via $param
    # =================================================================

    def test_parameterized_radius(self):
        """
        Radius specified as $param is resolved from PARAMS.
        Req: 1.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE $rad $blob]",
            "PARAMS", "4", "rad", "5", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 37. Optional params: EF_RUNTIME
    # =================================================================

    def test_optional_param_ef_runtime_flat_ignored(self):
        """
        FLAT accepts and ignores the VECTOR_RANGE EF_RUNTIME override.
        Req: 1.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        results = []
        for suffix in ("", " EF_RUNTIME 100"):
            result = self._search(
                client, "idx",
                f"@vec:[VECTOR_RANGE 5 $blob{suffix}]",
                "PARAMS", "2", "blob", query_blob,
                "NOCONTENT",
            )
            results.append(parse_result_keys(result))
        assert results == [
            {"doc:0", "doc:1", "doc:2"},
            {"doc:0", "doc:1", "doc:2"},
        ]

    # =================================================================
    # 38. Optional params: AS (alias for distance field)
    # =================================================================

    def test_optional_param_as(self):
        """
        AS optional parameter sets the distance field name.
        Req: 1.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob AS my_score]",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] >= 1
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "my_score" in fields, f"Expected 'my_score' in {key}"

    # =================================================================
    # 39. Multiple Vector Range predicates in one query
    # =================================================================

    def test_multiple_vector_range_predicates_rejected(self):
        """
        Multiple VECTOR_RANGE clauses are rejected in the single-VR model
        (RC1): only one VR predicate is supported per query. Req: 2.2
        (rejection).
        """
        client = self.server.get_new_client()
        # Two vector fields
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vec1", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
            "vec2", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
        ]
        assert client.execute_command(*cmd) == b"OK"

        # doc:0 close on both, doc:1 close on vec1 only, doc:2 close on vec2 only
        client.hset("doc:0", mapping={
            "vec1": float_to_bytes([0.0, 0.0, 0.0]),
            "vec2": float_to_bytes([0.0, 0.0, 0.0]),
        })
        client.hset("doc:1", mapping={
            "vec1": float_to_bytes([1.0, 0.0, 0.0]),
            "vec2": float_to_bytes([10.0, 0.0, 0.0]),
        })
        client.hset("doc:2", mapping={
            "vec1": float_to_bytes([10.0, 0.0, 0.0]),
            "vec2": float_to_bytes([1.0, 0.0, 0.0]),
        })

        blob1 = float_to_bytes([0.0, 0.0, 0.0])
        blob2 = float_to_bytes([0.0, 0.0, 0.0])
        with pytest.raises(ResponseError, match="single VECTOR_RANGE"):
            self._search(
                client, "idx",
                "@vec1:[VECTOR_RANGE 2 $b1] @vec2:[VECTOR_RANGE 2 $b2]",
                "PARAMS", "4", "b1", blob1, "b2", blob2,
                "NOCONTENT",
            )

    # =================================================================
    # 40. COSINE distance metric
    # =================================================================

    def test_cosine_distance_metric(self):
        """
        Vector Range works with COSINE distance metric.
        Req: 2.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, distance="COSINE")

        # Cosine distance: 1 - cos(angle)
        # Same direction = 0, orthogonal = 1, opposite = 2
        client.hset("doc:0", mapping={"vec": float_to_bytes([1.0, 0.0, 0.0])})
        client.hset("doc:1", mapping={"vec": float_to_bytes([0.9, 0.1, 0.0])})
        client.hset("doc:2", mapping={"vec": float_to_bytes([0.0, 1.0, 0.0])})
        client.hset("doc:3", mapping={"vec": float_to_bytes([-1.0, 0.0, 0.0])})

        query_blob = float_to_bytes([1.0, 0.0, 0.0])
        # Small radius should match only very similar vectors
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 0.1 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        # doc:0 (distance=0) and doc:1 (very small distance) should match
        assert result[0] >= 1
        keys = parse_result_keys(result)
        assert "doc:0" in keys

    def test_cosine_hnsw_vector_range_after_delete(self):
        """
        COSINE HNSW VECTOR_RANGE must survive traversal past a soft-deleted
        node. Regression: SearchRange built its QueryVector with normalize=false
        on a normalized index, so the normalized buffer was empty and hnswlib
        dereferenced null when scoring a deleted node -> SIGSEGV. Req: 2.1
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client, dim=4, distance="COSINE")

        num_docs = 40
        for i in range(num_docs):
            angle = i / num_docs
            client.hset(f"doc:{i}", mapping={
                "vec": float_to_bytes([1.0 + angle, 0.5 - angle, 0.25 * i, 1.0])
            })
        for i in range(0, num_docs, 2):
            client.delete(f"doc:{i}")

        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 0.5 $blob]",
            "PARAMS", "2", "blob", float_to_bytes([1.0, 0.5, 0.0, 1.0]),
            "NOCONTENT",
        )
        assert result[0] >= 0  # completed without crashing
        assert client.ping()

    def test_cosine_near_duplicates_report_true_distance(self):
        """
        COSINE VECTOR_RANGE yields the distance as computed. At 1536 dims two
        near-duplicates of the query, at true distances 2e-5 and 1.4e-4, rank
        nearest first on the yielded distance and radius 5e-5 keeps only the
        nearer one, in plain and compound (VR AND tag) queries. A window of
        dims * FLT_EPSILON around 0 (1.8e-4 here) reported both as 0, so the
        farther one, with the smaller key, ranked first. Req: 2.1
        """
        client = self.server.get_new_client()
        dim = 1536
        # A unit query q, a unit w orthogonal to it, and unit vectors at
        # cosine distance d from q: (1 - d) q + sqrt(1 - (1 - d)^2) w.
        q = [math.sin(0.37 * i + 1.0) for i in range(dim)]
        q_norm = math.sqrt(sum(x * x for x in q))
        q = [x / q_norm for x in q]
        w = [math.cos(1.13 * i + 0.5) for i in range(dim)]
        w_on_q = sum(a * b for a, b in zip(w, q))
        w = [a - w_on_q * b for a, b in zip(w, q)]
        w_norm = math.sqrt(sum(x * x for x in w))
        w = [x / w_norm for x in w]

        def at_distance(d):
            c = 1.0 - d
            s = math.sqrt(1.0 - c * c)
            return struct.unpack(f"<{dim}f", float_to_bytes(
                [c * a + s * b for a, b in zip(q, w)]))

        def true_distance(v):
            query = struct.unpack(f"<{dim}f", float_to_bytes(q))
            dot = sum(a * b for a, b in zip(v, query))
            return 1.0 - dot / math.sqrt(
                sum(a * a for a in v) * sum(b * b for b in query))

        # The farther one gets the key that sorts first.
        docs = {"a": at_distance(1.4e-4), "b": at_distance(2e-5)}
        blob = float_to_bytes(q)
        for index, prefix in self._cosine_indexes(client, dim):
            for key, vec in docs.items():
                client.hset(prefix + key, mapping={
                    "vec": float_to_bytes(vec), "tag": "x"})
            for query in ("{}", "{} @tag:{{x}}"):
                got = self._yielded_distances(client, index, query, blob,
                                              0.01)
                assert [key for key, _ in got] == [prefix + "b", prefix + "a"]
                for key, dist in got:
                    expected = true_distance(docs[key[len(prefix):]])
                    assert abs(dist - expected) < 1e-5, (index, query, key)
                got = self._yielded_distances(client, index, query, blob,
                                              0.01, "LIMIT", "0", "1")
                assert [key for key, _ in got] == [prefix + "b"]
                got = self._yielded_distances(client, index, query, blob,
                                              5e-5)
                assert [key for key, _ in got] == [prefix + "b"]

    def test_cosine_radius_two_keeps_exact_antipodes(self):
        """
        COSINE distance is bounded to [0, 2], so radius 2 keeps an exact
        antipode (-s * v) however rounding computes its distance, in plain
        and compound (VR AND tag) queries. Pushing a computed distance near 2
        just above 2 excluded nearly all of them. Req: 2.1
        """
        client = self.server.get_new_client()
        for dim in (2, 384):
            vectors = [[math.sin(0.7 * j + 1.3 * i + 0.1) for j in range(dim)]
                       for i in range(8)]
            for index, prefix in self._cosine_indexes(client, dim):
                for i, vec in enumerate(vectors):
                    client.hset(f"{prefix}{i}", mapping={
                        "vec": float_to_bytes(vec), "tag": "x"})
                for i, vec in enumerate(vectors):
                    scale = 0.25 + 0.5 * i
                    blob = float_to_bytes([-scale * x for x in vec])
                    for query in ("{}", "{} @tag:{{x}}"):
                        got = dict(self._yielded_distances(
                            client, index, query, blob, 2))
                        assert len(got) == len(vectors), (index, i, query)
                        dist = got[f"{prefix}{i}"]
                        assert 2 - 1e-6 < dist <= 2, (index, i, query, dist)

    # =================================================================
    # 41. SORTBY DESC with Vector Range
    # =================================================================

    def test_sortby_desc(self):
        """
        SORTBY field DESC overrides the default order.
        Req: 5.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["priority", "NUMERIC"])
        extra = {
            "doc:0": {"priority": "1"},
            "doc:1": {"priority": "3"},
            "doc:2": {"priority": "2"},
        }
        vectors = {
            "doc:0": [0.0, 0.0, 0.0],
            "doc:1": [1.0, 0.0, 0.0],
            "doc:2": [2.0, 0.0, 0.0],
        }
        self._load_vector_data(client, vectors=vectors, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "SORTBY", "priority", "DESC",
            "RETURN", "1", "priority",
        )
        assert result[0] == 3
        # Extract priorities in result order
        priorities = []
        for i in range(1, len(result), 2):
            fields = result[i + 1]
            field_dict = {
                fields[j].decode("utf-8"): fields[j + 1]
                for j in range(0, len(fields), 2)
            }
            priorities.append(int(field_dict["priority"]))
        assert priorities == sorted(priorities, reverse=True), (
            f"Priorities not in DESC order: {priorities}"
        )

    # =================================================================
    # 42. Vector Range with tag OR filter in parentheses
    # =================================================================

    def test_vector_range_with_tag_or_in_parens(self):
        """
        Vector Range AND (tag1 OR tag2) using parentheses.
        Req: 2.3, 2.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["color", "TAG"])
        extra = {
            "doc:0": {"color": "red"},
            "doc:1": {"color": "blue"},
            "doc:2": {"color": "green"},
            "doc:3": {"color": "red"},
            "doc:4": {"color": "blue"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=5 matches doc:0,1,2; color red|blue matches doc:0,1,3,4
        # AND = doc:0, doc:1
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] @color:{red|blue}",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1"}

    # =================================================================
    # FT.AGGREGATE tests — distance scores in the aggregation pipeline
    # =================================================================

    def _aggregate(self, client, index, query, *args):
        """Execute FT.AGGREGATE and return raw result."""
        cmd = ["FT.AGGREGATE", index, query] + list(args)
        return client.execute_command(*cmd)

    def test_aggregate_load_distance_field(self):
        """
        FT.AGGREGATE with LOAD picks up the VR distance field (emitted under an
        explicit $yield_distance_as alias) and exposes it as a numeric
        attribute in the aggregation pipeline.
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        # Collect results via AGGREGATE with LOAD of the distance field.
        result = self._aggregate(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: dist}",
            "PARAMS", "2", "blob", query_blob,
            "LOAD", "1", "dist",
        )
        # result[0] is total count; rest are per-row field arrays
        assert result[0] >= 3
        # Verify each row has dist as a numeric string
        for row in result[1:]:
            fields = {row[i].decode("utf-8"): row[i + 1]
                      for i in range(0, len(row), 2)}
            assert "dist" in fields, (
                f"dist missing from row: {list(fields.keys())}"
            )
            dist = float(fields["dist"])
            assert dist >= 0.0, f"Distance should be non-negative, got {dist}"

    def test_aggregate_apply_on_distance_field(self):
        """
        FT.AGGREGATE APPLY expression can reference the VR distance field
        (emitted under an explicit $yield_distance_as alias) to compute derived
        values.
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._aggregate(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: dist}",
            "PARAMS", "2", "blob", query_blob,
            "LOAD", "1", "dist",
            "APPLY", "(@dist * 2)", "AS", "double_dist",
        )
        assert result[0] >= 3
        for row in result[1:]:
            fields = {row[i].decode("utf-8"): row[i + 1]
                      for i in range(0, len(row), 2)}
            assert "double_dist" in fields, (
                f"double_dist missing from row: {list(fields.keys())}"
            )
            score = float(fields["dist"])
            double_dist = float(fields["double_dist"])
            assert abs(double_dist - score * 2) < 0.01, (
                f"double_dist ({double_dist}) != 2 * score ({score})"
            )

    def test_aggregate_custom_yield_distance_as(self):
        """
        Custom distance field name set via =>{$yield_distance_as: name} is
        usable in an FT.AGGREGATE APPLY expression.
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._aggregate(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]=>{$yield_distance_as: my_dist}",
            "PARAMS", "2", "blob", query_blob,
            "LOAD", "1", "my_dist",
            "APPLY", "(@my_dist + 0)", "AS", "dist_copy",
        )
        assert result[0] >= 3
        for row in result[1:]:
            fields = {row[i].decode("utf-8"): row[i + 1]
                      for i in range(0, len(row), 2)}
            assert "my_dist" in fields, (
                f"my_dist missing from row: {list(fields.keys())}"
            )
            assert "dist_copy" in fields, (
                f"dist_copy missing from row: {list(fields.keys())}"
            )
            my_dist = float(fields["my_dist"])
            dist_copy = float(fields["dist_copy"])
            assert abs(my_dist - dist_copy) < 0.01

    def test_aggregate_sortby_distance_field(self):
        """
        FT.AGGREGATE SORTBY on the VR distance alias orders results by
        ascending distance from the query vector.
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._aggregate(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]=>{$yield_distance_as: dist}",
            "PARAMS", "2", "blob", query_blob,
            "LOAD", "1", "dist",
            "SORTBY", "2", "@dist", "ASC",
        )
        assert result[0] == 5
        distances = []
        for row in result[1:]:
            fields = {row[i].decode("utf-8"): row[i + 1]
                      for i in range(0, len(row), 2)}
            distances.append(float(fields["dist"]))
        assert distances == sorted(distances), (
            f"Distances not in ASC order after SORTBY: {distances}"
        )

    def test_aggregate_hard_cap_reducer_and_cursor(self):
        """Reducers and cursors consume only the N retained range matches."""
        client = self.server.get_new_client()
        client.config_set("search.info-developer-visible", "yes")
        client.config_set("search.max-nonvector-search-results-fetched", "3")
        self._create_flat_index(client)
        vectors = {f"doc:{i}": [float(i), 0.0, 0.0] for i in range(5)}
        self._load_vector_data(client, vectors=vectors)
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")

        counter = "search_nonvector_results_fetched_limited_count"
        query = "@vec:[VECTOR_RANGE 100 $blob]"
        params = ("PARAMS", "2", "blob", float_to_bytes(QUERY_VEC))
        before = client.info("search").get(counter, 0)
        reduced = self._aggregate(
            client, "idx", query, *params,
            "GROUPBY", "0", "REDUCE", "COUNT", "0", "AS", "count",
        )
        assert reduced == [1, [b"count", b"3"]]
        after_reducer = client.info("search").get(counter, 0)
        assert after_reducer == before + 1

        batch, cursor = self._aggregate(
            client, "idx", query, *params,
            "LOAD", "1", "@__key", "SORTBY", "2", "@__key", "ASC",
            "WITHCURSOR", "COUNT", "2",
        )
        assert batch[0] == 2
        assert len(batch[1:]) == 2
        assert cursor != 0
        rows = list(batch[1:])
        batch, cursor = client.execute_command(
            "FT.CURSOR", "READ", "idx", cursor, "COUNT", "2")
        assert batch[0] == 1
        assert cursor == 0
        rows.extend(batch[1:])
        assert len(rows) == 3
        assert {row[1].decode() for row in rows} <= vectors.keys()
        assert client.info("search").get(counter, 0) == after_reducer + 1

    # =================================================================
    # KNN pre-filter containing a VECTOR_RANGE predicate (rejected)
    # =================================================================
    # A VECTOR_RANGE predicate inside a KNN pre-filter is unsupported in the
    # single-VR model (FEEDBACK.md scenario 2) and rejected at parse time.

    def test_knn_with_vr_filter_rejected_aggregate(self):
        """
        FT.AGGREGATE: a KNN query with a VECTOR_RANGE predicate in its filter
        is rejected at parse time.
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)

        with pytest.raises(ResponseError, match="KNN"):
            self._aggregate(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $vrblob]=>{$yield_distance_as: vr_dist}"
                "=>[KNN 5 @vec $kblob AS knn_dist]",
                "PARAMS", "4", "vrblob", query_blob, "kblob", query_blob,
                "LOAD", "2", "knn_dist", "vr_dist",
            )

    def test_knn_with_vr_filter_rejected_ft_search(self):
        """
        FT.SEARCH: a KNN query with a VECTOR_RANGE predicate in its filter is
        rejected at parse time.
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)

        with pytest.raises(ResponseError, match="KNN"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $vrblob]=>{$yield_distance_as: vr_dist}"
                "=>[KNN 5 @vec $kblob AS knn_dist]",
                "PARAMS", "4", "vrblob", query_blob, "kblob", query_blob,
            )


class TestVectorRangeReplaceDeleted(ValkeySearchTestCaseDebugMode):
    """HNSW range search with search.hnsw-allow-replace-deleted, a developer
    config, so new keys reuse deleted graph slots under fresh labels."""

    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_hnsw_range_matches_flat_after_replaced_deletes(self, metric):
        """
        After deletes and re-inserts that reuse the deleted slots, HNSW range
        results are FLAT results at the same distance and recall holds: a
        result reported by slot id instead of label names the wrong key.
        """
        client = self.server.get_new_client()
        client.config_set("search.hnsw-allow-replace-deleted", "yes")
        rng = np.random.default_rng(44)
        flat_index, hnsw_index = create_range_indexes(client, metric)
        vectors = clustered_vectors(rng, SCALE_KEYS)
        write_vectors(client, range(SCALE_KEYS), vectors)
        wait_indexed([client], (flat_index, hnsw_index))
        stored = delete_and_reinsert(client, rng, vectors)
        wait_indexed([client], (flat_index, hnsw_index))
        assert client.dbsize() == len(stored)
        check_hnsw_range_matches_flat(
            client, metric, scale_queries(rng, stored, metric),
            flat_index, hnsw_index)


class TestVectorRangeCluster(ValkeySearchClusterTestCase):
    """VECTOR_RANGE through the coordinator on 3 primaries."""

    def _primaries(self):
        return [self.client_for_primary(i)
                for i in range(len(self.replication_groups))]

    def test_hnsw_range_matches_flat_cluster(self):
        """
        On 10k L2 vectors spread over 3 shards, HNSW range results through
        the coordinator are FLAT results at the same distance, with recall of
        at least MIN_RECALL.
        """
        cluster_client = self.new_cluster_client()
        rng = np.random.default_rng(45)
        flat_index, hnsw_index = create_range_indexes(cluster_client, "L2")
        vectors = clustered_vectors(rng, SCALE_KEYS)
        write_vectors(cluster_client, range(SCALE_KEYS), vectors)
        primaries = self._primaries()
        wait_indexed(primaries, (flat_index, hnsw_index))
        assert sum(p.dbsize() for p in primaries) == SCALE_KEYS
        check_hnsw_range_matches_flat(
            primaries[0], "L2", scale_queries(rng, vectors, "L2"),
            flat_index, hnsw_index)

    def test_flat_range_hard_cap_is_per_shard(self):
        """The cap applies independently on shards, not to the global total."""
        cluster_client = self.new_cluster_client()
        flat_index, _ = create_range_indexes(cluster_client, "L2", dim=2)
        primaries = self._primaries()
        cap = 3
        tags = [find_local_key(primary, f"vr-cap-owner-{i}:")
                for i, primary in enumerate(primaries)]

        def add(shard, indexes):
            keys = set()
            pipe = primaries[shard].pipeline(transaction=False)
            for i in indexes:
                key = f"sc:{{{tags[shard]}}}:{i}"
                keys.add(key)
                pipe.hset(key, mapping={"v": float_to_bytes([0.0, 0.0])})
            pipe.execute()
            return keys

        shard_keys = [add(shard, range(2)) for shard in range(len(primaries))]
        wait_indexed(primaries, (flat_index,))
        counter = "search_nonvector_results_fetched_limited_count"
        for primary in primaries:
            primary.config_set("search.info-developer-visible", "yes")
            primary.config_set(
                "search.max-nonvector-search-results-fetched", str(cap))

        def counts():
            return [primary.info("search").get(counter, 0)
                    for primary in primaries]

        before = counts()
        query = np.zeros(2, dtype=np.float32)
        below_cap = range_distances(
            primaries[0], flat_index, "v", query, radius=0)
        assert below_cap.keys() == set().union(*shard_keys)
        assert len(below_cap) == 2 * len(primaries) > cap
        assert counts() == before

        overloaded = 1
        shard_keys[overloaded] |= add(overloaded, range(2, 5))
        wait_indexed(primaries, (flat_index,))
        before = counts()
        capped = range_distances(
            primaries[0], flat_index, "v", query, radius=0)
        assert capped.keys() <= set().union(*shard_keys)
        for shard, keys in enumerate(shard_keys):
            retained = capped.keys() & keys
            if shard == overloaded:
                assert len(retained) == cap
            else:
                assert retained == keys
        assert len(capped) == cap + 2 * (len(primaries) - 1)
        deltas = [after - old
                  for after, old in zip(counts(), before, strict=True)]
        assert deltas == [1 if i == overloaded else 0
                          for i in range(len(primaries))]

    def test_query_ef_runtime_reaches_non_owner_shards(self):
        """A per-query EF_RUNTIME reaches remote shards and raises recall.

        The coordinator owns no documents. Each other primary owns 10k vectors,
        and the two clouds are far apart so each query's 10-key ball is served
        by one remote shard. If the optional predicate field is lost in fanout,
        EF_RUNTIME 50 behaves like the field default of 1 and this test fails.
        """
        cluster_client = self.new_cluster_client()
        keys_per_shard = 10000
        dim = 32
        field_ef = 1
        flat_index, hnsw_index = create_range_indexes(
            cluster_client, "L2", prefix="ef:", dim=dim,
            ef_runtime=field_ef)
        primaries = self._primaries()

        # Reusing each returned key as a hash tag keeps all derived document
        # keys on that primary. Primary 0 remains a non-owner coordinator.
        tags = [find_local_key(primaries[i], f"ef-owner-{i}:")
                for i in (1, 2)]
        rng = np.random.default_rng(54)
        first = clustered_vectors(rng, keys_per_shard, dim=dim)
        second = clustered_vectors(rng, keys_per_shard, dim=dim) + 100.0
        write_vectors(primaries[1], range(keys_per_shard), first,
                      prefix=f"ef:{{{tags[0]}}}:")
        write_vectors(primaries[2], range(keys_per_shard), second,
                      prefix=f"ef:{{{tags[1]}}}:")
        wait_indexed(primaries, (flat_index, hnsw_index), timeout=600)
        assert [p.dbsize() for p in primaries] == [0, keys_per_shard,
                                                   keys_per_shard]

        default_recalls = []
        override_recalls = []
        queries = scale_queries(rng, first, "L2", count=20)
        for query in queries:
            _, radius = knn_radii(
                primaries[0], flat_index, "v", query, sizes=(10,))[0]
            flat = range_distances(
                primaries[0], flat_index, "v", query, radius)
            default = range_distances(
                primaries[0], hnsw_index, "v", query, radius)
            override = range_distances(
                primaries[0], hnsw_index, "v", query, radius,
                ef_runtime=50)
            assert len(flat) == 10
            for ef, result in ((field_ef, default), (50, override)):
                extra = result.keys() - flat.keys()
                assert not extra, (ef, extra)
                for key, distance in result.items():
                    assert math.isclose(
                        distance, flat[key], rel_tol=1e-5, abs_tol=1e-5), (
                            ef, key, distance, flat[key])
            default_recalls.append(len(default) / len(flat))
            override_recalls.append(len(override) / len(flat))

        default_mean = sum(default_recalls) / len(default_recalls)
        override_mean = sum(override_recalls) / len(override_recalls)
        print("\nRemote-shard VECTOR_RANGE recall: "
              f"field EF_RUNTIME {field_ef}={default_mean:.3f}, "
              f"query EF_RUNTIME 50={override_mean:.3f}")
        assert override_mean > default_mean, (default_mean, override_mean)
        assert override_mean >= 0.95, override_mean

    def test_query_attr_epsilon_saturated_cluster(self):
        """
        $epsilon: inf, nan and 1e40 reach every shard through the coordinator
        and return the FLAT result.
        """
        cluster_client = self.new_cluster_client()
        flat_index, hnsw_index = create_range_indexes(
            cluster_client, "L2", dim=2)
        grid = np.array([[i % 20, i // 20] for i in range(400)],
                        dtype=np.float32)
        write_vectors(cluster_client, range(400), grid)
        primaries = self._primaries()
        wait_indexed(primaries, (flat_index, hnsw_index))
        check_saturated_epsilon_matches_flat(
            primaries[0], flat_index, hnsw_index)


class TestVectorRangeBenchmark(ValkeySearchTestCaseBase):
    """Small-radius VECTOR_RANGE on HNSW against FLAT: the walk must match
    FLAT's recall while the test records comparative latency."""

    def test_small_radius_hnsw_recall_with_latency_report(self):
        """
        Over 10k 16-d vectors, small-radius (~1, ~10 keys) HNSW range queries
        match FLAT key-for-key at MIN_RECALL. The HNSW and FLAT median latency
        is measured and reported without making shared CI timing a gate.
        """
        client = self.server.get_new_client()
        rng = np.random.default_rng(47)
        flat_index, hnsw_index = create_range_indexes(client, "L2")
        vectors = clustered_vectors(rng, SCALE_KEYS)
        write_vectors(client, range(SCALE_KEYS), vectors)
        wait_indexed([client], (flat_index, hnsw_index))
        queries = scale_queries(rng, vectors, "L2", count=SCALE_QUERIES)

        def timed(index, query, radius):
            """(latency in seconds, result key set) for one range query."""
            start = time.perf_counter()
            got = range_distances(client, index, "v", query, radius)
            return time.perf_counter() - start, got.keys()

        flat_latencies, hnsw_latencies, recalls = [], [], []
        for query in queries:
            for _, radius in knn_radii(client, flat_index, "v", query,
                                       sizes=(1, 10)):
                flat_dt, flat_keys = timed(flat_index, query, radius)
                hnsw_dt, hnsw_keys = timed(hnsw_index, query, radius)
                assert hnsw_keys <= flat_keys, (radius, hnsw_keys - flat_keys)
                flat_latencies.append(flat_dt)
                hnsw_latencies.append(hnsw_dt)
                recalls.append(len(hnsw_keys) / max(len(flat_keys), 1))

        flat_median = sorted(flat_latencies)[len(flat_latencies) // 2]
        hnsw_median = sorted(hnsw_latencies)[len(hnsw_latencies) // 2]
        recall = sum(recalls) / len(recalls)
        print(f"\nVECTOR_RANGE small-radius latency over {len(recalls)} "
              f"queries: FLAT median {flat_median * 1e3:.3f} ms, "
              f"HNSW median {hnsw_median * 1e3:.3f} ms, HNSW recall {recall:.3f}")
        assert recall >= MIN_RECALL, recall


class TestVectorRangeEfRuntimeSweep(ValkeySearchTestCaseBase):
    """Per-query EF_RUNTIME sets the HNSW range walk's initial beam. When a
    ball holds about EF_RUNTIME keys, the walk can stop as a KNN@ef search
    would, so a wider per-query beam must recover the recall."""

    KEYS = 50000
    DIM = 32
    QUERIES = 30
    FIELD_EF = 10
    QUERY_EFS = (5, 10, 50)
    # ef/2, ef, 2ef and 10ef for each of QUERY_EFS.
    BALL_SIZES = (2, 5, 10, 20, 25, 50, 100, 500)
    # Range recall may trail KNN recall at the same ef by at most this much.
    KNN_SLACK = 0.05
    # Floor for every cell at the widest per-query EF_RUNTIME (measured
    # minimum 0.967).
    WIDE_EF_MIN_RECALL = 0.93

    def knn_keys(self, client, index, field, query, n, ef):
        """Keys of an HNSW KNN query for n neighbors at EF_RUNTIME ef."""
        result = client.execute_command(
            "FT.SEARCH", index,
            f"*=>[KNN {n} @{field} $BLOB EF_RUNTIME {ef} AS d]",
            "PARAMS", "2", "BLOB", query.astype("<f4").tobytes(),
            "NOCONTENT", "LIMIT", "0", str(n), "DIALECT", "2",
        )
        return parse_result_keys(result)

    @pytest.mark.parametrize("metric", ["L2", "COSINE"])
    def test_hnsw_range_recall_follows_query_ef_runtime(self, metric):
        """
        On 50k clustered 32-d vectors with field EF_RUNTIME 10, sweep the
        per-query EF_RUNTIME over 5, 10 and 50 and the ball over 2..500 keys.
        Every HNSW key is a FLAT key at the same distance; each cell's range
        recall is within KNN_SLACK of HNSW KNN recall at the same ef; and
        raising the per-query EF_RUNTIME from 5 to 50 raises mean recall.
        """
        client = self.server.get_new_client()
        rng = np.random.default_rng(53)
        field = range_field(metric)
        flat_index, hnsw_index = create_range_indexes(
            client, metric, dim=self.DIM, ef_runtime=self.FIELD_EF)
        vectors = clustered_vectors(rng, self.KEYS, dim=self.DIM)
        write_vectors(client, range(self.KEYS), vectors)
        wait_indexed([client], (flat_index, hnsw_index), timeout=600)
        queries = scale_queries(rng, vectors, metric, count=self.QUERIES)

        cells = [(n, ef) for n in self.BALL_SIZES for ef in self.QUERY_EFS]
        range_recall = {cell: [] for cell in cells}
        knn_recall = {cell: [] for cell in cells}
        for query in queries:
            for n, radius in knn_radii(client, flat_index, field, query,
                                       sizes=self.BALL_SIZES):
                flat = range_distances(client, flat_index, field, query,
                                       radius)
                assert len(flat) == n, (metric, n, radius)
                for ef in self.QUERY_EFS:
                    hnsw = range_distances(client, hnsw_index, field, query,
                                           radius, ef_runtime=ef)
                    extra = hnsw.keys() - flat.keys()
                    assert not extra, (metric, n, ef, extra)
                    for key, dist in hnsw.items():
                        assert math.isclose(dist, flat[key], rel_tol=1e-5,
                                            abs_tol=1e-5), (metric, key)
                    range_recall[(n, ef)].append(len(hnsw) / n)
                    knn = self.knn_keys(client, hnsw_index, field, query, n,
                                        ef)
                    knn_recall[(n, ef)].append(len(knn & flat.keys()) / n)

        mean = lambda xs: sum(xs) / len(xs)
        range_means = {cell: mean(r) for cell, r in range_recall.items()}
        knn_means = {cell: mean(r) for cell, r in knn_recall.items()}
        print(f"\n{metric} recall, range (KNN) by ball size x query "
              f"EF_RUNTIME; field EF_RUNTIME {self.FIELD_EF}")
        print("ball " + "".join(f"{f'ef={ef}':>16}" for ef in self.QUERY_EFS))
        for n in self.BALL_SIZES:
            print(f"{n:>4} " + "".join(
                f"{range_means[(n, ef)]:>8.3f} ({knn_means[(n, ef)]:.3f})"
                for ef in self.QUERY_EFS))

        for cell in cells:
            assert range_means[cell] >= knn_means[cell] - self.KNN_SLACK, (
                metric, cell, range_means[cell], knn_means[cell])
        by_ef = {ef: mean([range_means[(n, ef)] for n in self.BALL_SIZES])
                 for ef in self.QUERY_EFS}
        assert by_ef[50] > by_ef[5], (metric, by_ef)
        for n in self.BALL_SIZES:
            assert range_means[(n, 50)] >= self.WIDE_EF_MIN_RECALL, (
                metric, n, range_means[(n, 50)])
