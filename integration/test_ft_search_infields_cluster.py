"""Integration tests for FT.SEARCH INFIELDS in cluster mode.

The coordinator resolves INFIELDS into the text predicates' field masks before
fan-out, so every shard must return only matches from the INFIELDS set. Each
test checks that from every primary acting as coordinator, against data that
is verified to sit on every shard.
"""

import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper

NUM_DOCS = 60
TITLE_DOCS = {f"doc:{i}" for i in range(0, NUM_DOCS, 2)}
BODY_DOCS = {f"doc:{i}" for i in range(1, NUM_DOCS, 2)}
ALL_DOCS = TITLE_DOCS | BODY_DOCS


def _vec(*xs: float) -> bytes:
    return struct.pack(f"<{len(xs)}f", *xs)


def _index_on_node(client: Valkey, name: str) -> bool:
    return name.encode() in client.execute_command("FT._LIST")


def _remote_partition_searches(clients) -> int:
    return sum(
        c.info("search")[
            "search_coordinator_server_search_index_partition_success_count"]
        for c in clients)


def _doc_fields(i: int) -> dict:
    """Even docs hold "apple pie" in title only; odd docs in body only."""
    match, other = "apple pie", "unrelated words"
    title, body = (match, other) if i % 2 == 0 else (other, match)
    return {
        "title": title,
        "body": body,
        "rank": i,
        "color": "red" if i % 4 < 2 else "blue",
        "vec": _vec(float(i), 0.0),
    }


class TestFTSearchInfieldsCluster(ValkeySearchClusterTestCase):

    def _create_and_wait(self, *create_cmd):
        name = create_cmd[1]
        self.new_client_for_primary(0).execute_command(*create_cmd)
        for node in self.nodes:
            waiters.wait_for_true(lambda: _index_on_node(node.client, name))

    def _wait_indexed(self, name: str):
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            self.get_all_primary_clients(), name)

    def _assert_every_shard_holds_both_kinds(self):
        for client in self.get_all_primary_clients():
            local = {k.decode() for k in client.scan_iter(match="doc:*")}
            assert local & TITLE_DOCS and local & BODY_DOCS, (
                f"shard {client} does not hold both title and body matches")

    def _setup_hash(self):
        # title supports suffix search, body does not.
        self._create_and_wait(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT", "WITHSUFFIXTRIE",
            "body", "TEXT",
            "rank", "NUMERIC", "SORTABLE",
            "color", "TAG",
            "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2")
        cluster: ValkeyCluster = self.new_cluster_client()
        for i in range(NUM_DOCS):
            cluster.hset(f"doc:{i}", mapping=_doc_fields(i))
        self._wait_indexed("idx")
        self._assert_every_shard_holds_both_kinds()

    def _keys_from_every_coordinator(self, index: str, query: str, *args):
        """Runs the NOCONTENT query through each primary and asserts they
        agree; returns (total, keys) from the first."""
        replies = []
        for i in range(self.CLUSTER_SIZE):
            res = self.new_client_for_primary(i).execute_command(
                "FT.SEARCH", index, query, *args,
                "NOCONTENT", "LIMIT", "0", str(NUM_DOCS), "DIALECT", "2")
            replies.append((res[0], {k.decode() for k in res[1:]}))
        assert all(r == replies[0] for r in replies), replies
        total, keys = replies[0]
        assert total == len(keys)
        return keys

    def test_infields_scopes_every_shard(self):
        self._setup_hash()
        cases = [
            ("apple", (), ALL_DOCS),
            ("apple", ("INFIELDS", "0"), ALL_DOCS),
            ("apple", ("INFIELDS", "1", "title"), TITLE_DOCS),
            ("apple", ("INFIELDS", "1", "body"), BODY_DOCS),
            ("apple", ("INFIELDS", "2", "title", "body"), ALL_DOCS),
            ("apple", ("INFIELDS", "3", "title", "title", "body"), ALL_DOCS),
            ("app*", ("INFIELDS", "1", "body"), BODY_DOCS),
            ("%aple%", ("INFIELDS", "1", "title"), TITLE_DOCS),
            ('"apple pie"', ("INFIELDS", "1", "body"), BODY_DOCS),
            ("(apple|pie)", ("INFIELDS", "1", "title"), TITLE_DOCS),
            ("apple @color:{red}", ("INFIELDS", "1", "body"),
             {d for d in BODY_DOCS if int(d[4:]) % 4 < 2}),
            ("apple @rank:[0 9]", ("INFIELDS", "1", "title"),
             {d for d in TITLE_DOCS if int(d[4:]) <= 9}),
            # Non-text predicates are unaffected by INFIELDS.
            ("@rank:[0 9]", ("INFIELDS", "1", "title"),
             {f"doc:{i}" for i in range(10)}),
        ]
        for query, args, expected in cases:
            keys = self._keys_from_every_coordinator("idx", query, *args)
            assert keys == expected, (query, args, keys ^ expected)

    def test_infields_suffix_with_mixed_suffix_support(self):
        """Only title has a suffix trie: a suffix term under INFIELDS matches
        the suffix-capable subset, and errors when that subset is empty."""
        self._setup_hash()
        assert self._keys_from_every_coordinator(
            "idx", "*ple", "INFIELDS", "2", "title", "body") == TITLE_DOCS
        for i in range(self.CLUSTER_SIZE):
            with pytest.raises(ResponseError,
                               match="No INFIELDS field supports suffix search"):
                self.new_client_for_primary(i).execute_command(
                    "FT.SEARCH", "idx", "*ple", "INFIELDS", "1", "body",
                    "DIALECT", "2")

    def test_infields_hybrid_knn_prefilter(self):
        self._setup_hash()
        keys = self._keys_from_every_coordinator(
            "idx", f"(apple)=>[KNN {NUM_DOCS} @vec $BLOB]",
            "INFIELDS", "1", "title",
            "PARAMS", "2", "BLOB", _vec(0.0, 0.0))
        assert keys == TITLE_DOCS

    def test_infields_sortby_paging_merges_shards(self):
        self._setup_hash()
        ordered = sorted(TITLE_DOCS, key=lambda d: int(d[4:]))
        for i in range(self.CLUSTER_SIZE):
            client = self.new_client_for_primary(i)
            for offset in (0, 5, 25):
                res = client.execute_command(
                    "FT.SEARCH", "idx", "apple", "INFIELDS", "1", "title",
                    "SORTBY", "rank", "ASC", "NOCONTENT",
                    "LIMIT", str(offset), "5", "DIALECT", "2")
                assert res[0] == len(TITLE_DOCS)
                assert [k.decode() for k in res[1:]] == \
                    ordered[offset:offset + 5]

    def test_infields_reaches_remote_shards(self):
        """The narrowed result must come from a real fan-out, not the
        coordinator's local shard alone."""
        self._setup_hash()
        coordinator = self.new_client_for_primary(0)
        remotes = [self.client_for_primary(i)
                   for i in range(1, self.CLUSTER_SIZE)]
        before = _remote_partition_searches(remotes)
        res = coordinator.execute_command(
            "FT.SEARCH", "idx", "apple", "INFIELDS", "1", "title",
            "NOCONTENT", "LIMIT", "0", str(NUM_DOCS), "DIALECT", "2")
        assert {k.decode() for k in res[1:]} == TITLE_DOCS
        assert _remote_partition_searches(remotes) - before == \
            self.CLUSTER_SIZE - 1

    @pytest.mark.parametrize("args, message", [
        (("INFIELDS", "1", "nonexistent"), "does not exist"),
        (("INFIELDS", "1", "rank"), "is not a TEXT field"),
        (("INFIELDS", "1", "color"), "is not a TEXT field"),
        (("INFIELDS", "65", *[f"f{i}" for i in range(65)]),
         "exceeds maximum"),
        (("INFIELDS", "2", "title"), None),
    ])
    def test_infields_validation_errors_from_every_coordinator(
            self, args, message):
        """Rejected on the coordinator before any shard is contacted."""
        self._setup_hash()
        remotes = self.get_all_primary_clients()
        before = _remote_partition_searches(remotes)
        for i in range(self.CLUSTER_SIZE):
            with pytest.raises(ResponseError, match=message):
                self.new_client_for_primary(i).execute_command(
                    "FT.SEARCH", "idx", "apple", *args, "DIALECT", "2")
        assert _remote_partition_searches(remotes) == before

    def test_infields_explicit_field_outside_set_errors(self):
        self._setup_hash()
        for i in range(self.CLUSTER_SIZE):
            with pytest.raises(ResponseError, match="is not in INFIELDS list"):
                self.new_client_for_primary(i).execute_command(
                    "FT.SEARCH", "idx", "@title:apple",
                    "INFIELDS", "1", "body", "DIALECT", "2")

    def test_infields_json(self):
        self._create_and_wait(
            "FT.CREATE", "jidx", "ON", "JSON", "PREFIX", "1", "doc:",
            "SCHEMA",
            "$.title", "AS", "title", "TEXT",
            "$.body", "AS", "body", "TEXT")
        cluster: ValkeyCluster = self.new_cluster_client()
        for i in range(NUM_DOCS):
            f = _doc_fields(i)
            cluster.execute_command(
                "JSON.SET", f"doc:{i}", "$",
                f'{{"title": "{f["title"]}", "body": "{f["body"]}"}}')
        self._wait_indexed("jidx")
        self._assert_every_shard_holds_both_kinds()
        assert self._keys_from_every_coordinator(
            "jidx", "apple", "INFIELDS", "1", "title") == TITLE_DOCS
        assert self._keys_from_every_coordinator(
            "jidx", "apple", "INFIELDS", "1", "body") == BODY_DOCS

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True)
    def test_infields_when_replicas_serve_fanout(self, setup_test):
        """With the local-fanout threshold at 0 the coordinator sends work to
        replicas; they must apply the INFIELDS scope too."""
        self._setup_hash()
        waiters.wait_for_true(lambda: self.replication_lag() == 0)
        for client in self.get_all_primary_clients():
            client.execute_command(
                "CONFIG", "SET", "search.local-fanout-queue-wait-threshold",
                "0")
        replicas = [r.client for rg in self.replication_groups
                    for r in rg.replicas]
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(
            replicas, "idx")
        before = _remote_partition_searches(replicas)
        for _ in range(5):
            assert self._keys_from_every_coordinator(
                "idx", "apple", "INFIELDS", "1", "title") == TITLE_DOCS
        assert _remote_partition_searches(replicas) > before
