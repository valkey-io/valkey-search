"""
Cluster-mode FT.SEARCH / FT.AGGREGATE while a slot is being migrated.

A legacy migration (SETSLOT IMPORTING/MIGRATING + MIGRATE ... COPY) is held
open so that both the source and the destination index the same keys while
only the source owns their slot. While ownership is stable, every reply mode
must return each key at most once and count it exactly once -- including the
modes that never fetch content (NOCONTENT, LIMIT 0 0, RETURN of an indexed
field, FT.AGGREGATE without LOAD). During the hand-over itself the nodes'
cluster-map snapshots refresh independently, so only the weaker invariants in
_assert_handover_invariants are checked until every node has caught up.
"""

import time

from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser
from indexes import float_to_bytes

INDEX = "idx"
TAG = "mig"
NUM_DOCS = 20
NUM_BUCKETS = 4
KEYS = [f"vec:{{{TAG}}}:{i}" for i in range(NUM_DOCS)]
BUCKET0 = [k for i, k in enumerate(KEYS) if i % NUM_BUCKETS == 0]
KNN = ["*=>[KNN 10 @vec $q]", "PARAMS", "2", "q",
       float_to_bytes([0.0, 0.0, 0.0, 0.0]), "DIALECT", "2"]


def _keys(reply):
    """Keys of a NOCONTENT reply: [total, key, key, ...]."""
    return [k.decode() for k in reply[1:]]


def _keys_with_content(reply):
    """Keys of a reply carrying fields: [total, key, [f, v, ...], key, ...]."""
    return [k.decode() for k in reply[1::2]]


def _num_docs(client):
    return FTInfoParser(client.execute_command("FT.INFO", INDEX)).num_docs


def _dropped_unowned(client):
    return client.info("search").get("search_unowned_slot_results_dropped_count", 0)


class TestSlotMigrationSearchCME(ValkeySearchClusterTestCaseDebugMode):

    def _owner_index(self, slot):
        for idx, (start, end) in enumerate(
                self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)):
            if start <= slot < end:
                return idx
        raise AssertionError(f"slot {slot} outside the cluster's slot ranges")

    def _assert_knn_distinct(self, client, where):
        reply = client.execute_command(
            "FT.SEARCH", INDEX, *KNN, "NOCONTENT", "LIMIT", "0", "10")
        keys = _keys(reply)
        assert len(set(keys)) == len(keys), f"{where}: KNN NOCONTENT {keys}"
        return keys

    def _assert_handover_invariants(self, client, where):
        """What still holds while the two nodes' cluster maps disagree during
        the hand-over. Each node filters against its own snapshot, refreshed
        independently (up to cluster-map-expiration-ms plus a cron tick), so a
        key can be counted by both nodes or, briefly, by neither. Only the
        fan-out merge's guarantees are unconditional: no reply repeats a key,
        no reply invents one, and no total exceeds both nodes' counts."""
        self._assert_knn_distinct(client, where)

        reply = client.execute_command(
            "FT.SEARCH", INDEX, "@n:[0 0]", "NOCONTENT", "LIMIT", "0", "1000",
            "DIALECT", "2")
        keys = _keys(reply)
        assert len(set(keys)) == len(keys), f"{where}: page rows {keys}"
        assert set(keys) <= set(BUCKET0), f"{where}: page key set {keys}"

        total = client.execute_command(
            "FT.SEARCH", INDEX, "@n:[0 0]", "LIMIT", "0", "0", "DIALECT", "2")[0]
        assert total <= 2 * len(BUCKET0), f"{where}: LIMIT 0 0 total {total}"

    def _wait_for_reply_consistent(self, nodes, where, timeout=10):
        """Poll until every node gives exact replies, i.e. until every node's
        cluster-map snapshot has caught up with the new owner. Fails with the
        last mismatch if that does not happen within `timeout` seconds."""
        deadline = time.time() + timeout
        while True:
            try:
                for name, client in nodes:
                    self._assert_reply_consistent(client, f"{where} via {name}")
                return
            except AssertionError:
                if time.time() >= deadline:
                    raise
                time.sleep(0.1)

    def _assert_reply_consistent(self, client, where):
        """Every reply mode: distinct keys, exact counts."""
        keys = self._assert_knn_distinct(client, where)
        assert len(keys) == 10, f"{where}: KNN NOCONTENT {keys}"

        reply = client.execute_command(
            "FT.SEARCH", INDEX, *KNN, "RETURN", "1", "n", "LIMIT", "0", "10")
        keys = _keys_with_content(reply)
        assert len(keys) == 10 and len(set(keys)) == 10, f"{where}: KNN RETURN n {keys}"

        reply = client.execute_command(
            "FT.SEARCH", INDEX, "@n:[0 0]", "NOCONTENT", "LIMIT", "0", "1000", "DIALECT", "2")
        assert reply[0] == len(BUCKET0), f"{where}: page total {reply[0]}"
        assert sorted(_keys(reply)) == sorted(BUCKET0), f"{where}: page rows {_keys(reply)}"

        reply = client.execute_command(
            "FT.SEARCH", INDEX, "@n:[0 0]", "LIMIT", "0", "0", "DIALECT", "2")
        assert reply == [len(BUCKET0)], f"{where}: LIMIT 0 0 {reply}"

        reply = client.execute_command(
            "FT.AGGREGATE", INDEX, "@n:[0 0]",
            "GROUPBY", "1", "@n", "REDUCE", "COUNT", "0", "AS", "cnt", "DIALECT", "2")
        assert reply[0] == 1, f"{where}: aggregate groups {reply}"
        row = dict(zip(reply[1][::2], reply[1][1::2]))
        assert row[b"cnt"] == str(len(BUCKET0)).encode(), f"{where}: aggregate {row}"

    def test_slot_migration_no_duplicate_results_CME(self):
        probe = self.new_client_for_primary(0)
        slot = probe.execute_command("CLUSTER KEYSLOT", f"{{{TAG}}}")
        src_idx = self._owner_index(slot)
        dst_idx = (src_idx + 1) % self.CLUSTER_SIZE
        other_idx = (src_idx + 2) % self.CLUSTER_SIZE
        source = self.new_client_for_primary(src_idx)
        dest = self.new_client_for_primary(dst_idx)
        other = self.new_client_for_primary(other_idx)
        nodes = (("source", source), ("dest", dest), ("other", other))
        source_id = source.execute_command("CLUSTER MYID").decode()
        dest_id = dest.execute_command("CLUSTER MYID").decode()

        source.execute_command(
            "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "vec:", "SCHEMA",
            "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4",
            "DISTANCE_METRIC", "L2", "n", "NUMERIC")
        for _, client in nodes:
            waiters.wait_for_true(
                lambda c=client: INDEX.encode() in c.execute_command("FT._LIST"))
        # A node builds its cluster-map snapshot when it coordinates a query
        # or on its next cron tick; make sure every node has one before the
        # keys are copied, so the reader-thread ownership filter is active.
        for _, client in nodes:
            client.execute_command("FT.SEARCH", INDEX, "*", "LIMIT", "0", "0")
        for i, key in enumerate(KEYS):
            source.hset(key, mapping={
                "vec": float_to_bytes([float(i + 1), 0.0, 0.0, 0.0]),
                "n": str(i % NUM_BUCKETS),
            })
        waiters.wait_for_equal(lambda: _num_docs(source), NUM_DOCS)
        dest.execute_command("CONFIG", "SET", "search.info-developer-visible", "yes")
        dropped_before = _dropped_unowned(dest)

        # Hold the slot open on both sides and copy the keys over, so both
        # nodes index them while only the source owns the slot.
        dest.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", source_id)
        source.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", dest_id)
        dest_server = self.get_primary(dst_idx)
        source.execute_command(
            "MIGRATE", dest_server.bind_ip, dest_server.port, "", 0, 5000,
            "COPY", "KEYS", *KEYS)
        waiters.wait_for_equal(lambda: _num_docs(dest), NUM_DOCS)

        for name, client in nodes:
            self._assert_reply_consistent(client, f"during migration via {name}")
        # The destination's hits were filtered on the reader thread.
        assert _dropped_unowned(dest) > dropped_before

        # Hand the slot over. The source still holds the keys so it refuses
        # SETSLOT NODE; it learns of the new owner through gossip and then
        # drops its copies. Until every node's snapshot has caught up, the
        # nodes' views disagree; see _assert_handover_invariants.
        dest.execute_command("CLUSTER SETSLOT", slot, "NODE", dest_id)
        other.execute_command("CLUSTER SETSLOT", slot, "NODE", dest_id)
        deadline = time.time() + 30
        while _num_docs(source) != 0:
            assert time.time() < deadline, "source never dropped the migrated keys"
            for name, client in nodes:
                self._assert_handover_invariants(client, f"handover via {name}")
            time.sleep(0.05)

        # Once the source's copies are gone and the snapshots have refreshed,
        # every reply mode must be exact again.
        self._wait_for_reply_consistent(nodes, "after migration")
