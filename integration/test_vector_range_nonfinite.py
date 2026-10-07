"""VECTOR_RANGE with non-finite distances and radii.

A stored vector with a NaN or infinite component, or a magnitude too large
for float, is rejected at ingest and left out of the vector index. A COSINE
query vector
like that is rejected with an error. For L2 and IP a non-finite query vector
is accepted and makes its distances non-finite. A NaN or +inf distance is
within no radius, not even an infinite one, so plain, compound and
FT.AGGREGATE queries exclude the document and a negated range includes it. An
IP distance of -inf is within every radius and reported as -inf, as on Redis,
also through the cluster coordinator. An infinite radius matches every
document whose distance is finite.
"""

import struct

import pytest
from valkey.exceptions import ResponseError

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


# a and b are at distance 0 and 1 (2 for L2) from Q.
DOCS = {
    "a": _vec(1.0, 0.0, 0.0, 0.0),
    "b": _vec(0.0, 1.0, 0.0, 0.0),
}
# Rejected at ingest: n holds a NaN, p and m an infinity.
REJECTED = {
    "n": _vec(NAN, 0.0, 0.0, 0.0),
    "p": _vec(INF, 0.0, 0.0, 0.0),
    "m": _vec(-INF, 0.0, 0.0, 0.0),
}
Q = _vec(1.0, 0.0, 0.0, 0.0)
# Every distance to these queries is non-finite. For IP, a is at -inf from
# INF_Q.
NAN_Q = _vec(1.0, NAN, 0.0, 0.0)
INF_Q = _vec(INF, 0.0, 0.0, 0.0)
NON_FINITE_QUERIES = (NAN_Q, INF_Q)
# For IP, the documents at -inf from each query.
IP_NEG_INF = {Q: set(), NAN_Q: set(), INF_Q: {"a"}}
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
                for key, vec in {**DOCS, **REJECTED}.items():
                    client.hset(f"{index}:{key}",
                                mapping={"vec": vec, "tag": "x"})
                IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
                    [client], index)
                for (blob, radius), finite in cases.items():
                    vr = "@vec:[VECTOR_RANGE $r $blob]"
                    params = ["PARAMS", "4", "r", radius, "blob", blob]
                    if metric == "COSINE" and blob in NON_FINITE_QUERIES:
                        with pytest.raises(ResponseError,
                                           match="NaN or infinite"):
                            _keys(client, index, vr, *params)
                        continue
                    neg_inf = IP_NEG_INF[blob] if metric == "IP" else set()
                    within = finite | neg_inf
                    got = _distances(client, index, vr + YIELD, *params)
                    assert set(got) == within, (index, radius, vr, got)
                    for key in neg_inf:
                        assert got[key] == -INF, (index, radius, key, got)
                    # A rejected vector counts as a missing field, so its key
                    # stays in the tag index and a negated range returns it.
                    for query, expected in (
                        (vr + " @tag:{x}", within),
                        ("-" + vr, (set(DOCS) | set(REJECTED)) - within),
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
        yields none.

        Stored vectors must be finite, so the -inf comes from a finite query
        whose dot product with p overflows float: 2^120 * 2^10 = 2^130. a is at
        exactly 0 (2^120 * 2^-120 = 1), b at 1 and m at +inf."""
        cluster = self.new_cluster_client()
        coordinators = [
            self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)
        ]
        query = _vec(2.0**120, 0.0, 0.0, 0.0)
        vr = "@vec:[VECTOR_RANGE 0.5 $blob]" + YIELD
        params = ["PARAMS", "2", "blob", query]
        for algo in ("FLAT", "HNSW"):
            index = f"{algo}_IP"
            _create_index(cluster, index, algo, "IP")
            # One document at -inf on each shard, so that every coordinator
            # merges a local one and remote ones. b and m are outside the
            # radius and match only @tag:{y}.
            docs = {"a": ("x", _vec(2.0**-120, 0.0, 0.0, 0.0)),
                    "b": ("y", DOCS["b"]),
                    "m": ("y", _vec(-(2.0**10), 0.0, 0.0, 0.0))}
            shards = set()
            i = 0
            while len(shards) < self.CLUSTER_SIZE:
                node = cluster.get_node_from_key(f"{index}:p{i}")
                if node.name not in shards:
                    shards.add(node.name)
                    docs[f"p{i}"] = ("x", _vec(2.0**10, 0.0, 0.0, 0.0))
                i += 1
            for key, (tag, vec) in docs.items():
                cluster.hset(f"{index}:{key}", mapping={"vec": vec, "tag": tag})
            IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
                coordinators, index)
            within = {key: -INF for key in docs if key.startswith("p")}
            within["a"] = 0.0
            with_tag = {**within, "b": None, "m": None}
            for coordinator in coordinators:
                for q, expected in (
                    (vr, within),
                    (vr + " @tag:{x}", within),
                    ("@tag:{y} | " + vr, with_tag),
                ):
                    got = _distances(coordinator, index, q, *params)
                    assert got == expected, (index, q, got)
                    got = _aggregate_distances(coordinator, index, q, *params)
                    assert got == expected, (index, "FT.AGGREGATE", q, got)
                # The -inf documents sort before a, not after it as documents
                # without a VR distance would.
                result = coordinator.execute_command(
                    "FT.SEARCH", index, vr, *params, "RETURN", "1", "d",
                    "SORTBY", "d",
                )
                order = [key.decode()[len(index) + 1:] for key in result[1::2]]
                assert sorted(order) == sorted(within), (index, order)
                assert order[-1] == "a", (index, "SORTBY d", order)
