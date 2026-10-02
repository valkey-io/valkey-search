import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from util import waiters

# JPARAMS supplies a parameter as JSON. A query vector given as a JSON array must
# produce the same results as the equivalent binary blob given through PARAMS.

VECTORS = [[float(i), float(i) / 2, 1.0] for i in range(10)]
# Exactly representable in FLOAT16, so both encodings agree bit for bit.
QUERY = [3.25, 1.5, 1.0]
QUERY_JSON = "[3.25, 1.5, 1.0]"
KNN_QUERY = "*=>[KNN 3 @v $q]"


def pack(vector, fmt):
    return struct.pack(f"<{len(vector)}{fmt}", *vector)


def load(client, prefix, fmt):
    for i, vector in enumerate(VECTORS):
        client.execute_command("HSET", f"{prefix}{i}", "v", pack(vector, fmt))


def search(client, index, clause, value):
    return client.execute_command(
        "FT.SEARCH", index, KNN_QUERY, clause, 2, "q", value, "DIALECT", 2)


def wait_for_docs(client, index):
    waiters.wait_for_equal(
        lambda: client.execute_command(
            "FT.SEARCH", index, "*=>[KNN 20 @v $q]", "JPARAMS", 2, "q",
            QUERY_JSON, "DIALECT", 2)[0], len(VECTORS))


class TestJParams(ValkeySearchTestCaseBase):

    @pytest.mark.parametrize("data_type,fmt", [("FLOAT32", "f"), ("FLOAT16", "e")])
    def test_jparams_matches_params(self, data_type, fmt):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "doc:", "SCHEMA",
            "v", "VECTOR", "FLAT", 6, "TYPE", data_type, "DIM", 3,
            "DISTANCE_METRIC", "L2") == b"OK"
        load(client, "doc:", fmt)
        wait_for_docs(client, "idx")

        expected = search(client, "idx", "PARAMS", pack(QUERY, fmt))
        assert expected[0] == 3
        assert search(client, "idx", "JPARAMS", QUERY_JSON) == expected

        # K supplied through JPARAMS as a JSON number.
        assert client.execute_command(
            "FT.SEARCH", "idx", "*=>[KNN $k @v $q]",
            "JPARAMS", 4, "k", "3", "q", QUERY_JSON, "DIALECT", 2) == expected

        aggregate = ["FT.AGGREGATE", "idx", "*=>[KNN 3 @v $q AS dist]",
                     "LOAD", 2, "@__key", "@dist", "SORTBY", 2, "@dist", "ASC",
                     "DIALECT", 2]
        assert client.execute_command(
            *aggregate, "JPARAMS", 2, "q", QUERY_JSON) == client.execute_command(
            *aggregate, "PARAMS", 2, "q", pack(QUERY, fmt))

    def test_jparams_errors(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "doc:", "SCHEMA",
            "v", "VECTOR", "FLAT", 6, "TYPE", "FLOAT32", "DIM", 3,
            "DISTANCE_METRIC", "L2") == b"OK"

        with pytest.raises(ResponseError, match="Parameter q is already defined"):
            client.execute_command(
                "FT.SEARCH", "idx", KNN_QUERY, "PARAMS", 2, "q",
                pack(QUERY, "f"), "JPARAMS", 2, "q", QUERY_JSON)
        with pytest.raises(ResponseError, match="Parameter q is already defined"):
            client.execute_command(
                "FT.AGGREGATE", "idx", KNN_QUERY, "JPARAMS", 2, "q", QUERY_JSON,
                "PARAMS", 2, "q", pack(QUERY, "f"))
        for bad in ["3.25, 1.5, 1.0", "[3.25, \"a\", 1.0]", "{\"v\": [1, 2, 3]}"]:
            with pytest.raises(ResponseError,
                               match="JPARAMS query vector must be a JSON array of numbers"):
                search(client, "idx", "JPARAMS", bad)
        with pytest.raises(ResponseError, match="query vector blob size"):
            search(client, "idx", "JPARAMS", "[1.0, 2.0]")


class TestJParamsCluster(ValkeySearchClusterTestCase):

    def test_jparams_matches_params_cluster(self):
        # The coordinator converts the JSON before fanning the query out, so
        # every shard receives the binary vector.
        client = self.new_cluster_client()
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "doc:", "SCHEMA",
            "v", "VECTOR", "FLAT", 6, "TYPE", "FLOAT32", "DIM", 3,
            "DISTANCE_METRIC", "L2") == b"OK"
        load(client, "doc:", "f")
        node = self.client_for_primary(0)
        wait_for_docs(node, "idx")

        expected = search(node, "idx", "PARAMS", pack(QUERY, "f"))
        assert expected[0] == 3
        assert search(node, "idx", "JPARAMS", QUERY_JSON) == expected
