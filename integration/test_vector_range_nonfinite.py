"""VECTOR_RANGE with non-finite distances and radii.

A NaN or infinite component in a stored or a query vector makes its distance
non-finite. A NaN or +inf distance is within no radius, not even an infinite
one, and neither is any non-finite COSINE distance, so plain, compound and
FT.AGGREGATE queries exclude the document and a negated range includes it. An
IP distance of -inf is within every radius and reported as -inf, as on Redis,
also through the cluster coordinator. An infinite radius matches every
document whose distance is finite.
"""

import struct

from utils import IndexingTestHelper
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker

NAN = float("nan")
INF = float("inf")


def _vec(*xs):
    return struct.pack(f"<{len(xs)}f", *xs)


# a and b are at distance 0 and 1 (2 for L2) from Q. Every distance of the
# other documents to Q is non-finite: n holds a NaN, p and m an infinity (for
# IP, p is at -inf and m at +inf).
DOCS = {
    "a": _vec(1.0, 0.0, 0.0, 0.0),
    "b": _vec(0.0, 1.0, 0.0, 0.0),
    "n": _vec(NAN, 0.0, 0.0, 0.0),
    "p": _vec(INF, 0.0, 0.0, 0.0),
    "m": _vec(-INF, 0.0, 0.0, 0.0),
}
Q = _vec(1.0, 0.0, 0.0, 0.0)
# Every distance to these queries is non-finite. For IP, a and p are at -inf
# from INF_Q.
NAN_Q = _vec(1.0, NAN, 0.0, 0.0)
INF_Q = _vec(INF, 0.0, 0.0, 0.0)
# For IP, the documents at -inf from each query.
IP_NEG_INF = {Q: {"p"}, NAN_Q: set(), INF_Q: {"a", "p"}}
YIELD = "=>{$yield_distance_as: d}"


def _create_index(client, index, algo, metric):
    client.execute_command(
        "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", index + ":",
        "SCHEMA", "tag", "TAG",
        "vec", "VECTOR", algo, "6", "TYPE", "FLOAT32", "DIM", "4",
        "DISTANCE_METRIC", metric,
    )


def _distance(fields):
    """The yielded distance d in a reply's field list, or None if absent."""
    fields = dict(zip(fields[::2], fields[1::2]))
    return float(fields[b"d"]) if b"d" in fields else None


def _keys(client, index, query, *args):
    result = client.execute_command(
        "FT.SEARCH", index, query, *args, "NOCONTENT", "LIMIT", "0", "100"
    )
    return {key.decode()[len(index) + 1:] for key in result[1:]}


def _distances(client, index, query, *args):
    result = client.execute_command(
        "FT.SEARCH", index, query, *args, "RETURN", "1", "d",
        "LIMIT", "0", "100",
    )
    return {
        key.decode()[len(index) + 1:]: _distance(fields)
        for key, fields in zip(result[1::2], result[2::2])
    }


def _aggregate_keys(client, index, query, *args):
    result = client.execute_command(
        "FT.AGGREGATE", index, query, *args, "LOAD", "1", "@__key"
    )
    return {row[1].decode()[len(index) + 1:] for row in result[1:]}


def _aggregate_distances(client, index, query, *args):
    result = client.execute_command(
        "FT.AGGREGATE", index, query, *args, "LOAD", "2", "@__key", "@d"
    )
    distances = {}
    for row in result[1:]:
        key = dict(zip(row[::2], row[1::2]))[b"__key"].decode()
        distances[key[len(index) + 1:]] = _distance(row)
    return distances


class TestVectorRangeNonFinite(ValkeySearchTestCaseBase):

    def test_non_finite_distance(self):
        client = self.server.get_new_client()
        # (query vector, radius) -> documents within the radius at a finite
        # distance. The radius is passed as $r.
        cases = {
            (Q, "0.5"): {"a"},
            (Q, "inf"): {"a", "b"},
            (NAN_Q, "inf"): set(),
            (INF_Q, "inf"): set(),
        }
        for algo in ("FLAT", "HNSW"):
            for metric in ("L2", "IP", "COSINE"):
                index = f"{algo}_{metric}"
                _create_index(client, index, algo, metric)
                for key, vec in DOCS.items():
                    client.hset(f"{index}:{key}",
                                mapping={"vec": vec, "tag": "x"})
                for (blob, radius), finite in cases.items():
                    neg_inf = IP_NEG_INF[blob] if metric == "IP" else set()
                    within = finite | neg_inf
                    vr = "@vec:[VECTOR_RANGE $r $blob]"
                    params = ["PARAMS", "4", "r", radius, "blob", blob]
                    got = _distances(client, index, vr + YIELD, *params)
                    assert set(got) == within, (index, radius, vr, got)
                    for key in neg_inf:
                        assert got[key] == -INF, (index, radius, key, got)
                    for query, expected in (
                        (vr + " @tag:{x}", within),
                        ("-" + vr, set(DOCS) - within),
                    ):
                        got = _keys(client, index, query, *params)
                        assert got == expected, (index, radius, query, got)
                    got = _aggregate_keys(client, index, vr, *params)
                    assert got == within, (index, radius, "FT.AGGREGATE", got)


class TestVectorRangeNonFiniteCluster(ValkeySearchClusterTestCase):

    def test_ip_neg_inf_distance_through_coordinator(self):
        """Every coordinator reports an IP -inf distance as -inf, as a
        standalone server does, whichever shard holds the document. A document
        that only the tag branch of an OR returns has no VR distance and still
        yields none."""
        cluster = self.new_cluster_client()
        coordinators = [
            self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)
        ]
        vr = "@vec:[VECTOR_RANGE 0.5 $blob]" + YIELD
        params = ["PARAMS", "2", "blob", Q]
        for algo in ("FLAT", "HNSW"):
            index = f"{algo}_IP"
            _create_index(cluster, index, algo, "IP")
            # One document at -inf on each shard, so that every coordinator
            # merges a local one and remote ones. b and m are outside the
            # radius (at 1 and +inf) and match only @tag:{y}.
            docs = {"a": ("x", DOCS["a"]), "b": ("y", DOCS["b"]),
                    "m": ("y", DOCS["m"])}
            shards = set()
            i = 0
            while len(shards) < self.CLUSTER_SIZE:
                node = cluster.get_node_from_key(f"{index}:p{i}")
                if node.name not in shards:
                    shards.add(node.name)
                    docs[f"p{i}"] = ("x", DOCS["p"])
                i += 1
            for key, (tag, vec) in docs.items():
                cluster.hset(f"{index}:{key}", mapping={"vec": vec, "tag": tag})
            IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
                coordinators, index)
            within = {key: -INF for key in docs if key.startswith("p")}
            within["a"] = 0.0
            with_tag = {**within, "b": None, "m": None}
            for coordinator in coordinators:
                for query, expected in (
                    (vr, within),
                    (vr + " @tag:{x}", within),
                    ("@tag:{y} | " + vr, with_tag),
                ):
                    got = _distances(coordinator, index, query, *params)
                    assert got == expected, (index, query, got)
                    got = _aggregate_distances(
                        coordinator, index, query, *params)
                    assert got == expected, (index, "FT.AGGREGATE", query, got)
                # The -inf documents sort before a, not after it as documents
                # without a VR distance would.
                result = coordinator.execute_command(
                    "FT.SEARCH", index, vr, *params, "RETURN", "1", "d",
                    "SORTBY", "d",
                )
                order = [key.decode()[len(index) + 1:] for key in result[1::2]]
                assert sorted(order) == sorted(within), (index, order)
                assert order[-1] == "a", (index, "SORTBY d", order)
