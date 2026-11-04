"""FLOAT64 precision across the cluster fanout.

A shard returns each neighbor's distance to the coordinator over gRPC. These
tests pick distances that float cannot represent, so a value narrowed to float
anywhere between the index and the reply shows up as a different answer.
"""

import struct

import pytest
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker


def _fp64(values):
    return struct.pack(f"<{len(values)}d", *values)


def _fp32(value):
    return struct.unpack("<f", struct.pack("<f", value))[0]


class TestVectorFloat64Cluster(ValkeySearchClusterTestCase):

    def _create(self, client: ValkeyCluster, algo: str):
        params = ["TYPE", "FLOAT64", "DIM", "2", "DISTANCE_METRIC", "L2"]
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "v", "VECTOR", algo, str(len(params)), *params,
        )

    @pytest.mark.parametrize("algo", ["FLAT", "HNSW"])
    def test_knn_distance_keeps_double_precision(self, algo):
        client: ValkeyCluster = self.new_cluster_client()
        self._create(client, algo)
        # Enough keys that several shards, not just the one answering the
        # client, return neighbors.
        components = [1.1111111111 + i * 0.0123456789 for i in range(12)]
        for i, x in enumerate(components):
            client.execute_command("HSET", f"doc:{i}", "v", _fp64([x, 0.0]))

        result = client.execute_command(
            "FT.SEARCH", "idx", f"*=>[KNN {len(components)} @v $q AS score]",
            "PARAMS", "2", "q", _fp64([0.0, 0.0]),
            "RETURN", "1", "score", "LIMIT", "0", str(len(components)),
            "DIALECT", "2",
        )
        assert result[0] == len(components)
        scores = {
            result[i].decode(): result[i + 1][1].decode()
            for i in range(1, len(result), 2)
        }
        for i, x in enumerate(components):
            expected = "%.12g" % (x * x)
            narrowed = "%.12g" % (_fp32(_fp32(x) * _fp32(x)))
            assert expected != narrowed
            assert scores[f"doc:{i}"] == expected, f"doc:{i}"

    @pytest.mark.parametrize("algo", ["FLAT", "HNSW"])
    def test_range_resolves_beyond_float_precision(self, algo):
        client: ValkeyCluster = self.new_cluster_client()
        self._create(client, algo)
        # L2 distances from the origin are 1 + 1e-12 and 1 + 2.89e-12, both 1
        # in float; the radius lies between them.
        for i in range(6):
            client.execute_command("HSET", f"doc:in{i}", "v", _fp64([1.0, 1e-6]))
            client.execute_command(
                "HSET", f"doc:out{i}", "v", _fp64([1.0, 1.7e-6]))

        result = client.execute_command(
            "FT.SEARCH", "idx", "@v:[VECTOR_RANGE $r $q]",
            "PARAMS", "4", "r", "1.000000000002", "q", _fp64([0.0, 0.0]),
            "NOCONTENT", "DIALECT", "2",
        )
        assert result[0] == 6
        assert sorted(k.decode() for k in result[1:]) == sorted(
            f"doc:in{i}" for i in range(6))
