"""SVS_VAMANA across deployment paths: replica full sync, AOF rewrite and
reload, and cluster fan-out. Each compares KNN answers with exact ground
truth computed here, on a small dataset where SVS search is exact."""

import random
import struct

import pytest
from valkey import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters

from ft_info_parser import FTInfoParser
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)

DIM = 8
NUM_DOCS = 200
K = 5


def _fp32(values):
    return struct.pack(f"<{len(values)}f", *values)


def _vectors(seed=11):
    rng = random.Random(seed)
    return [[rng.uniform(-1, 1) for _ in range(DIM)] for _ in range(NUM_DOCS)]


def _exact_top_k(vectors, query):
    """Doc numbers of the K nearest vectors by squared L2."""
    dist = [(sum((a - b) ** 2 for a, b in zip(v, query)), i)
            for i, v in enumerate(vectors)]
    return [i for _, i in sorted(dist)[:K]]


def _create_and_load(client, vectors):
    client.execute_command(
        "FT.CREATE", "svs_dep", "PREFIX", "1", "d:",
        "SCHEMA", "v", "VECTOR", "SVS_VAMANA", "6",
        "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
    )
    for i, v in enumerate(vectors):
        client.hset(f"d:{i}", mapping={"v": _fp32(v)})


def _wait_indexed(client, n=NUM_DOCS):
    def indexed():
        info = FTInfoParser(client.execute_command("FT.INFO", "svs_dep"))
        return info.num_docs >= n and info.mutation_queue_size == 0

    waiters.wait_for_equal(indexed, True, timeout=30)


def _knn(client, query):
    reply = client.execute_command(
        "FT.SEARCH", "svs_dep", f"*=>[KNN {K} @v $q]", "NOCONTENT",
        "PARAMS", "2", "q", _fp32(query), "DIALECT", "2",
    )
    return [int(k[2:]) for k in reply[1:]]


def _assert_exact(client, vectors, queries):
    for q in queries:
        assert _knn(client, q) == _exact_top_k(vectors, q)


class TestSVSReplicaFullSync(ValkeySearchTestCaseBase):

    @pytest.mark.parametrize("setup_test", [{"replica_count": 1}],
                             indirect=True)
    def test_replica_serves_knn_after_full_sync(self):
        """A replica gets the SVS index through a full sync (the SVS payload
        in the RDB) and answers KNN like the primary."""
        primary = self.get_primary_connection()
        replica = self.get_replica_connection(0)
        vectors = _vectors()
        _create_and_load(primary, vectors)
        _wait_indexed(primary)

        sync_full_before = primary.info("stats")["sync_full"]
        replica.execute_command("REPLICAOF", "NO", "ONE")
        self.rg.setup_replications_cmd()
        assert primary.info("stats")["sync_full"] == sync_full_before + 1
        waiters.wait_for_true(
            lambda: replica.info("replication")["master_link_status"] == "up",
            timeout=30,
        )
        _wait_indexed(replica)

        queries = vectors[:5] + [[0.0] * DIM]
        _assert_exact(primary, vectors, queries)
        _assert_exact(replica, vectors, queries)


class TestSVSAofRewrite(ValkeySearchTestCaseBase):

    def test_knn_survives_aof_rewrite_and_reload(self):
        """Turning AOF on rewrites the file (with the RDB preamble carrying
        the SVS index); reloading it gives the same answers."""
        client: Valkey = self.server.get_new_client()
        vectors = _vectors(seed=12)
        _create_and_load(client, vectors)
        _wait_indexed(client)
        queries = vectors[:5]
        _assert_exact(client, vectors, queries)

        client.config_set("appendonly", "yes")
        waiters.wait_for_true(
            lambda: client.info("persistence")["aof_rewrite_in_progress"] == 0
            and client.info("persistence")["aof_enabled"] == 1,
            timeout=30,
        )
        client.execute_command("DEBUG", "LOADAOF")
        _wait_indexed(client)
        _assert_exact(client, vectors, queries)


class TestSVSClusterFanout(ValkeySearchClusterTestCase):

    def test_knn_fans_out_across_shards(self):
        """With keys spread over the shards, a KNN query merges the
        per-shard SVS answers into the exact global top-k."""
        cluster = self.new_cluster_client()
        vectors = _vectors(seed=13)
        _create_and_load(cluster, vectors)
        for i in range(self.CLUSTER_SIZE):
            _wait_indexed(self.client_for_primary(i), n=0)
        waiters.wait_for_equal(
            lambda: sum(
                FTInfoParser(self.client_for_primary(i).execute_command(
                    "FT.INFO", "svs_dep")).num_docs
                for i in range(self.CLUSTER_SIZE)),
            NUM_DOCS, timeout=30,
        )

        node = self.client_for_primary(0)
        _assert_exact(node, vectors, vectors[:5] + [[0.0] * DIM])
