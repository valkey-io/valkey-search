"""FT.HYBRID under injected cluster failures.

FT.HYBRID fans out both arms to every shard and fuses what comes back, so the
ways a fanout can fail are the ways it can silently answer from less than the
whole cluster. These drive each failure the module can inject and assert which
of them is an error and which is a short answer, because getting that backwards
is how a wrong result reads as a correct one.

Modelled on test_ft_search_partition_consistency_controls.py, which does the
same for FT.SEARCH.
"""

import struct

from valkey import ResponseError
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from indexes import ClusterTestUtils
import pytest


def _vec(*xs: float) -> bytes:
    return struct.pack(f"<{len(xs)}f", *xs)


class TestFtHybridClusterControls(ClusterTestUtils,
                                  ValkeySearchClusterTestCaseDebugMode):
    INDEX = "hyb"
    Q = _vec(0.0, 0.0, 0.0, 0.0)
    DOCS = 60

    def _setup(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        for i in range(1, self.DOCS + 1):
            client.hset(f"d:{i:03d}", mapping={
                "title": "hello world",
                "vec": _vec(float(i), 0.0, 0.0, 0.0)})
        return client

    def _hybrid(self, client: Valkey, *extra):
        return client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "40",
            "COMBINE", "RRF", "4", "WINDOW", "100", "YIELD_SCORE_AS", "h",
            "LIMIT", "0", "100",
            *extra,
            "PARAMS", "2", "q", self.Q,
            target_nodes=self.new_cluster_client().get_default_node())

    def _count(self, client: Valkey, *extra) -> int:
        reply = self._hybrid(client, *extra)
        return reply[0] if isinstance(reply, list) else 0

    def test_slot_fingerprint_mismatch(self):
        """A cluster-map change under the query. With the consistency check
        refused the reply stands; with it demanded the command has to fail
        rather than answer from a map it no longer trusts."""
        client = self._setup()
        nominal = self._count(client)
        assert nominal > 0

        self.control_set("ForceInvalidSlotFingerprint", "yes")
        try:
            self.config_set("search.enable-consistent-results", "no")
            assert self._count(client) == nominal

            self.config_set("search.enable-consistent-results", "yes")
            # A consistency failure fails the command as a consistency
            # failure whether or not partial results are allowed:
            # partial-results tolerance covers a shard that dropped out, not a
            # cluster map the reply can no longer be trusted to have been
            # assembled from. The message is asserted because with partial
            # results allowed the query used to fail only indirectly, through
            # the cancellation the failing shard fires, and so reported itself
            # as a timeout.
            for partial in ("no", "yes"):
                self.config_set("search.enable-partial-results", partial)
                with pytest.raises(ResponseError,
                                   match="consistency check failed"):
                    self._hybrid(client)
        finally:
            self.control_set("ForceInvalidSlotFingerprint", "no")
            self.config_set("search.enable-consistent-results", "no")
            self.config_set("search.enable-partial-results", "yes")

    def test_fanout_timeout_is_an_error_either_way(self):
        """A forced timeout must surface as an error, never as an empty-but-fine
        reply -- a timeout that reads as "no matches" is the worst shape this
        failure can take.

        It errors whether or not partial results are allowed, and that is the
        right answer rather than a gap in the setting: the injection cancels
        every token including the coordinator's own, so there is no surviving
        shard for a partial reply to be assembled from. Per-shard tolerance is
        covered by test_index_fingerprint_mismatch_on_one_shard, where the
        other shards stay healthy."""
        client = self._setup()
        assert self._count(client) > 0

        self.control_set("ForceTimeout", "yes")
        self.control_set("TimeoutPollFrequency", "0")
        try:
            for partial in ("no", "yes"):
                self.config_set("search.enable-partial-results", partial)
                with pytest.raises(ResponseError, match=r"(?i)timeout|cancel"):
                    self._hybrid(client)
        finally:
            self.control_set("ForceTimeout", "no")
            self.control_set("TimeoutPollFrequency", "50")
            self.config_set("search.enable-partial-results", "yes")

    def test_shard_side_rejection(self):
        """A shard refusing the work for queue depth is a failure of that
        shard's arm, so it follows the partial-results setting like any
        other."""
        client = self._setup()
        nominal = self._count(client)
        assert nominal > 0

        self.control_set("ForceServerQueueDepthExceeded", "yes")
        try:
            self.config_set("search.enable-partial-results", "no")
            with pytest.raises(ResponseError):
                self._hybrid(client)
        finally:
            self.control_set("ForceServerQueueDepthExceeded", "no")
            self.config_set("search.enable-partial-results", "yes")

    def test_index_fingerprint_mismatch_on_one_shard(self):
        """The index differing on a single shard, which is what a remote node
        carrying a different or unknown definition looks like to the
        coordinator. The other shards are healthy, so with partial results
        allowed the reply is short rather than absent."""
        client = self._setup()
        nominal = self._count(client)
        assert nominal > 0

        one = self.new_client_for_primary(0)
        one.execute_command("ft._debug", "CONTROLLED_VARIABLE", "set",
                            "ForceInvalidIndexFingerprint", "yes")
        try:
            self.config_set("search.enable-partial-results", "no")
            with pytest.raises(ResponseError):
                self._hybrid(client)

            self.config_set("search.enable-partial-results", "yes")
            tolerated = self._hybrid(client)
            assert isinstance(tolerated, list)
            assert 0 < tolerated[0] < nominal, (
                "one shard failed, so the reply should be short but not empty")
        finally:
            one.execute_command("ft._debug", "CONTROLLED_VARIABLE", "set",
                                "ForceInvalidIndexFingerprint", "no")
            self.config_set("search.enable-partial-results", "yes")
