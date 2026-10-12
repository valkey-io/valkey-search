"""
Cluster-mode FT.SEARCH / FT.AGGREGATE while a slot is being migrated.

A legacy migration (SETSLOT IMPORTING/MIGRATING + MIGRATE) is held open and
every reply mode is checked from every node, including the modes that never
fetch content (NOCONTENT, LIMIT 0 0, RETURN of an indexed field, FT.AGGREGATE
without LOAD):

1. Keys moved without COPY, as `valkey-cli --cluster reshard` does: each key
   exists on exactly one node, and the moved ones only on the importing node,
   which doesn't own the slot yet. Every reply must be exact.
2. The remaining keys copied with COPY, so they exist on both nodes. No reply
   may repeat a key; counts the fan-out merge can't see (LIMIT 0 0) are only
   bounded.
3. The hand-over: the nodes' cluster-map snapshots refresh independently, so
   only the weaker invariants of _assert_handover_invariants are checked until
   every node has caught up, then every reply must be exact again.
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
# Key i has vector [i+1, 0, 0, 0], so the exact KNN-10 of the origin is keys
# 0..9, which are also the keys moved without COPY.
TOP10 = KEYS[:10]
MOVED = KEYS[:10]
COPIED = KEYS[10:]
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


def _aggregate_count(client, *groupby):
    """COUNT of the bucket-0 documents, grouped by `groupby` (one group)."""
    reply = client.execute_command(
        "FT.AGGREGATE", INDEX, "@n:[0 0]", "GROUPBY", str(len(groupby)),
        *groupby, "REDUCE", "COUNT", "0", "AS", "cnt", "DIALECT", "2")
    assert reply[0] == 1, f"aggregate groups {reply}"
    row = dict(zip(reply[1][::2], reply[1][1::2]))
    return int(row[b"cnt"])


class TestSlotMigrationSearchCME(ValkeySearchClusterTestCaseDebugMode):

    def _owner_index(self, slot):
        for idx, (start, end) in enumerate(
                self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)):
            if start <= slot < end:
                return idx
        raise AssertionError(f"slot {slot} outside the cluster's slot ranges")

    def _assert_rows_distinct(self, client, where):
        """No reply repeats a key, and the row-level results are exact."""
        reply = client.execute_command(
            "FT.SEARCH", INDEX, *KNN, "NOCONTENT", "LIMIT", "0", "10")
        keys = _keys(reply)
        assert sorted(keys) == sorted(TOP10), f"{where}: KNN NOCONTENT {keys}"

        reply = client.execute_command(
            "FT.SEARCH", INDEX, *KNN, "RETURN", "1", "n", "LIMIT", "0", "10")
        keys = _keys_with_content(reply)
        assert sorted(keys) == sorted(TOP10), f"{where}: KNN RETURN n {keys}"

        reply = client.execute_command(
            "FT.SEARCH", INDEX, "@n:[0 0]", "NOCONTENT", "LIMIT", "0", "1000",
            "DIALECT", "2")
        keys = _keys(reply)
        assert sorted(keys) == sorted(BUCKET0), f"{where}: page rows {keys}"

        # Content-free aggregate: rows are merged across shards, so the merge
        # dedup makes the count exact.
        cnt = _aggregate_count(client)
        assert cnt == len(BUCKET0), f"{where}: GROUPBY 0 COUNT {cnt}"

    def _assert_reply_consistent(self, client, where):
        """Every reply mode: exact keys and exact counts."""
        self._assert_rows_distinct(client, where)

        reply = client.execute_command(
            "FT.SEARCH", INDEX, "@n:[0 0]", "NOCONTENT", "LIMIT", "0", "1000",
            "DIALECT", "2")
        assert reply[0] == len(BUCKET0), f"{where}: page total {reply[0]}"

        reply = client.execute_command(
            "FT.SEARCH", INDEX, "@n:[0 0]", "LIMIT", "0", "0", "DIALECT", "2")
        assert reply == [len(BUCKET0)], f"{where}: LIMIT 0 0 {reply}"

        # Loads n, so it takes the content path.
        cnt = _aggregate_count(client, "@n")
        assert cnt == len(BUCKET0), f"{where}: GROUPBY @n COUNT {cnt}"

    def _assert_handover_invariants(self, client, where):
        """What still holds while the nodes' cluster maps disagree during the
        hand-over. Each node filters against its own snapshot, refreshed
        independently (up to cluster-map-expiration-ms plus a cron tick), so a
        key can be counted by both nodes or, briefly, by neither. Only the
        fan-out merge's guarantees are unconditional: no reply repeats a key,
        no reply invents one, and no total exceeds both nodes' counts."""
        reply = client.execute_command(
            "FT.SEARCH", INDEX, *KNN, "NOCONTENT", "LIMIT", "0", "10")
        keys = _keys(reply)
        assert len(set(keys)) == len(keys), f"{where}: KNN NOCONTENT {keys}"
        assert set(keys) <= set(KEYS), f"{where}: KNN key set {keys}"

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
        cluster-map snapshot has caught up. Fails with the last mismatch if
        that does not happen within `timeout` seconds."""
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
        dest_server = self.get_primary(dst_idx)

        source.execute_command(
            "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "vec:", "SCHEMA",
            "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4",
            "DISTANCE_METRIC", "L2", "n", "NUMERIC")
        for _, client in nodes:
            waiters.wait_for_true(
                lambda c=client: INDEX.encode() in c.execute_command("FT._LIST"))
        # A node builds its cluster-map snapshot when it coordinates a query
        # or on its next cron tick; make sure every node has one before keys
        # move, so the reader-thread ownership filter is active.
        for _, client in nodes:
            client.execute_command("FT.SEARCH", INDEX, "*", "LIMIT", "0", "0")
        for i, key in enumerate(KEYS):
            source.hset(key, mapping={
                "vec": float_to_bytes([float(i + 1), 0.0, 0.0, 0.0]),
                "n": str(i % NUM_BUCKETS),
            })
        waiters.wait_for_equal(lambda: _num_docs(source), NUM_DOCS)

        dest.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", source_id)
        source.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", dest_id)

        # Phase 1: a reshard moves keys without COPY. The moved keys exist only
        # on the importing node, which doesn't own the slot yet; it must still
        # answer for them, or no node returns them.
        source.execute_command(
            "MIGRATE", dest_server.bind_ip, dest_server.port, "", 0, 5000,
            "KEYS", *MOVED)
        waiters.wait_for_equal(lambda: _num_docs(dest), len(MOVED))
        waiters.wait_for_equal(lambda: _num_docs(source), NUM_DOCS - len(MOVED))
        # Let every node's cluster map (250 ms expiry) see the importing slot.
        self._wait_for_reply_consistent(nodes, "moved without COPY")

        # Phase 2: the rest are copied with COPY, so both nodes hold them. No
        # reply may repeat a key; LIMIT 0 0 sums per-shard counts the merge
        # can't see, so it is only bounded.
        source.execute_command(
            "MIGRATE", dest_server.bind_ip, dest_server.port, "", 0, 5000,
            "COPY", "KEYS", *COPIED)
        waiters.wait_for_equal(lambda: _num_docs(dest), NUM_DOCS)
        for name, client in nodes:
            where = f"keys on both nodes via {name}"
            self._assert_rows_distinct(client, where)
            total = client.execute_command(
                "FT.SEARCH", INDEX, "@n:[0 0]", "LIMIT", "0", "0",
                "DIALECT", "2")[0]
            assert len(BUCKET0) <= total <= 2 * len(BUCKET0), \
                f"{where}: LIMIT 0 0 total {total}"

        # Phase 3: hand the slot over. The source still holds its copies so it
        # refuses SETSLOT NODE; it learns of the new owner through gossip and
        # then drops them. Until every node's snapshot has caught up, the
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
