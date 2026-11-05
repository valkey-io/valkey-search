"""JPARAMS: parameters given as JSON values.

A JPARAMS vector is a JSON array, converted to the index's binary format, and
a JPARAMS number works as the same PARAMS text. Every query below is run once
with PARAMS (binary blob) and once with JPARAMS (JSON) and must give the same
reply, standalone and through each cluster coordinator.
"""

import struct

import pytest
from valkey.exceptions import ResponseError
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from utils import IndexingTestHelper

INDEX = "idx"
# L2 distances from QUERY: doc:0=0.25, doc:1=0.25, doc:2=2.25, doc:3=6.25,
# doc:4=90.25.
VECTORS = {
    "doc:0": [0.5, 0.0, 0.0],
    "doc:1": [1.5, 0.0, 0.0],
    "doc:2": [2.5, 0.0, 0.0],
    "doc:3": [3.5, 0.0, 0.0],
    "doc:4": [10.5, 0.0, 0.0],
}
QUERY = [1.0, 0.0, 0.0]
QUERY_JSON = "[1.0, 0.0, 0.0]"


def _blob(floats):
    return struct.pack(f"<{len(floats)}f", *floats)


def _create_index(client):
    assert client.execute_command(
        "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "doc:",
        "SCHEMA", "title", "TEXT", "n", "NUMERIC",
        "vec", "VECTOR", "HNSW", "6",
        "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
    ) == b"OK"


def _load(client):
    for i, (key, vec) in enumerate(VECTORS.items()):
        client.hset(key, mapping={"title": "hello", "n": i,
                                  "vec": _blob(vec)})


def _rows(reply):
    """Rows of an FT.AGGREGATE or FT.HYBRID reply, order-insensitive."""
    return reply[0], sorted(map(tuple, reply[1:]))


def _keys(reply):
    """Count and keys of an FT.SEARCH NOCONTENT reply."""
    return reply[0], sorted(reply[1:])


# (name, command builder, normalizer). Each builder takes the parameter clause
# so the same query runs with PARAMS and with JPARAMS.
QUERIES = [
    ("search_knn",
     lambda p: ["FT.SEARCH", INDEX, "*=>[KNN $k @vec $q]", *p, "NOCONTENT"],
     _keys),
    ("search_range",
     lambda p: ["FT.SEARCH", INDEX, "@vec:[VECTOR_RANGE $r $q]", *p,
                "NOCONTENT"],
     _keys),
]

PARAMS = ["PARAMS", "6", "q", _blob(QUERY), "k", "2", "r", "3"]
JPARAMS = ["JPARAMS", "6", "q", QUERY_JSON, "k", "2", "r", "3"]
# The vector as JSON, the numbers as plain PARAMS.
MIXED = ["PARAMS", "4", "k", "2", "r", "3", "JPARAMS", "2", "q", QUERY_JSON]


def _check_same_replies(client):
    for name, build, normalize in QUERIES:
        expected = normalize(client.execute_command(*build(PARAMS)))
        assert expected[0] > 0, name
        for clause in (JPARAMS, MIXED):
            got = normalize(client.execute_command(*build(clause)))
            assert got == expected, (name, clause[0])


class TestJParams(ValkeySearchTestCaseBase):

    def _setup(self):
        client = self.server.get_new_client()
        _create_index(client)
        _load(client)
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, INDEX)
        return client

    def test_jparams_match_params(self):
        _check_same_replies(self._setup())

    def test_invalid_vector_rejected(self):
        client = self._setup()
        bad = ["JPARAMS", "6", "q", "[1.0, nope, 0.0]", "k", "2", "r", "3"]
        for _, build, _ in QUERIES:
            with pytest.raises(ResponseError,
                               match="JPARAMS `q` is not a valid vector"):
                client.execute_command(*build(bad))

    def test_defined_by_params_and_jparams_rejected(self):
        client = self._setup()
        clause = ["PARAMS", "6", "q", _blob(QUERY), "k", "2", "r", "3",
                  "JPARAMS", "2", "k", "2"]
        for _, build, _ in QUERIES:
            with pytest.raises(ResponseError,
                               match="Parameter k is already defined"):
                client.execute_command(*build(clause))


class TestJParamsCluster(ValkeySearchClusterTestCase):

    def test_jparams_match_params_through_coordinator(self):
        cluster = self.new_cluster_client()
        coordinators = [
            self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)
        ]
        _create_index(coordinators[0])
        _load(cluster)
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            coordinators, INDEX)
        for coordinator in coordinators:
            _check_same_replies(coordinator)
