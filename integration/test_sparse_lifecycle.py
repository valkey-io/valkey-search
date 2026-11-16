"""Sparse HASH lifecycle, durable rebuild, and replication regressions."""

import random

import pytest
from test_managed_sparse import search, sparse
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


def create_index(client):
    """Index sparse values and a filter field, including pre-existing HASHes."""
    client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1,
                           "doc:", "SCHEMA", "s", "SPARSE", "region", "TAG")


def expect_hits(client, query, expected, predicate="*"):
    """Wait for asynchronous ingestion, then compare keys, scores and order."""
    waiters.wait_for_equal(
        lambda: search(client, sparse(query), 100, predicate), expected,
        timeout=15,
    )


class TestSparseLifecycle(ValkeySearchTestCaseBase):
    """Exercise online mutations and replication through public commands."""

    def test_set_replacement_removes_postings(self):
        """SET replacing a HASH invalidates both sparse and TAG postings."""
        c = self.server.get_new_client()
        create_index(c)
        c.hset("doc:a", mapping={"s": sparse([(1, 2)]), "region": "1"})
        expect_hits(c, [(1, 1)], [(b"doc:a", -2)])
        c.set("doc:a", "replacement")
        expect_hits(c, [(1, 1)], [])
        assert c.execute_command("FT.SEARCH", "idx", "@region:{1}", "NOCONTENT") == [0]
        c.delete("doc:a")
        c.hset("doc:a", "s", sparse([(1, 3)]))
        expect_hits(c, [(1, 1)], [(b"doc:a", -3)])

    def test_crud_churn_backfill_and_recreate(self):
        """Mixed mutations leave exactly the current valid overlapping documents."""
        c = self.server.get_new_client()
        rng = random.Random(16621)
        state = {}
        queries = [[(1, 2), (2, -1), (3, 1)], [(1, -1), (19, 1)]]
        for i in range(32):
            key = f"doc:{i:03d}".encode()
            entries = [(1, i % 5 - 2), (2, 1)]
            c.hset(key, mapping={"s": sparse(entries), "region": str(i % 2)})
            state[key] = (dict(entries), str(i % 2))
        create_index(c)

        def check():
            """Compare both signs and a selective filter with exhaustive scoring."""
            for query in queries:
                for region in (None, "1"):
                    expected = []
                    for key, (entries, value) in state.items():
                        if region is not None and value != region:
                            continue
                        if any(entries.get(d, 0) for d, _ in query):
                            expected.append((key, -sum(entries.get(d, 0) * w
                                                        for d, w in query)))
                    expected.sort(key=lambda hit: (hit[1], hit[0]))
                    expect_hits(c, query, expected,
                                "*" if region is None else "@region:{1}")

        check()
        for step in range(300):
            key = f"doc:{rng.randrange(32):03d}".encode()
            operation = step % 10
            if operation in (0, 1, 7, 8):
                if c.type(key) == b"string":
                    c.delete(key)
                entries = [(d, rng.randrange(-4, 5)) for d in (1, 2, 3, 19)]
                region = str(rng.randrange(2))
                value = b"invalid" if operation == 7 else sparse(
                    [(1, 0)] if operation == 8 else entries)
                c.hset(key, mapping={"s": value, "region": region})
                if operation in (7, 8):
                    state.pop(key, None)
                else:
                    state[key] = (dict(entries), region)
            elif operation == 2:
                if c.type(key) != b"string":
                    c.hdel(key, "s")
                state.pop(key, None)
            elif operation in (3, 4, 9):
                if operation == 3:
                    c.delete(key)
                elif operation == 4:
                    c.unlink(key)
                else:
                    c.pexpireat(key, 1)
                state.pop(key, None)
                assert key not in dict(search(c, sparse(queries[0]), 100))
            elif operation == 5:
                c.set(key, "not a HASH")
                state.pop(key, None)
            elif c.exists(key):
                target = f"doc:{rng.randrange(32):03d}".encode()
                if target != key:
                    c.rename(key, target)
                    state.pop(target, None)
                    if key in state:
                        state[target] = state.pop(key)
            if step % 10 == 9:
                check()
        c.execute_command("FT.DROPINDEX", "idx")
        create_index(c)
        check()
        c.execute_command("FLUSHDB")
        # FLUSHDB drops the schema as well as its documents.
        create_index(c)
        c.hset("doc:new", "s", sparse([(1, 3)]))
        expect_hits(c, [(1, 1)], [(b"doc:new", -3)])

    @pytest.mark.parametrize("setup_test", [{"replica_count": 1}], indirect=True)
    def test_full_sync_mutations_and_promotion(self):
        """A replica rebuilds postings and remains writable/searchable after promotion."""
        primary = self.get_primary_connection()
        replica = self.get_replica_connection(0)
        create_index(primary)
        for i in range(64):
            primary.hset(f"doc:{i:03d}", "s", sparse([(1, i + 1)]))
        expected = [(f"doc:{i:03d}".encode(), -(i + 1)) for i in reversed(range(64))]
        expect_hits(primary, [(1, 1)], expected)
        assert primary.execute_command("WAIT", 1, 10000) == 1
        expect_hits(replica, [(1, 1)], expected)
        # Restart without persisted state, so PSYNC cannot reuse the old offset.
        full_syncs = primary.info("stats")["sync_full"]
        node = self.rg.replicas[0]
        node.server.restart(remove_rdb=True)
        replica = node.client = node.server.get_new_client()
        self.rg.setup_replications_cmd()
        assert primary.info("stats")["sync_full"] > full_syncs
        # Primary-side online state precedes the replica finishing its RDB load.
        waiters.wait_for_true(
            lambda: replica.info("replication")["master_link_status"] == "up")
        expect_hits(replica, [(1, 1)], expected)
        primary.hset("doc:000", "s", sparse([(1, 100)]))
        primary.delete("doc:063")
        primary.hdel("doc:062", "s")
        expected = [(b"doc:000", -100)] + expected[2:-1]
        assert primary.execute_command("WAIT", 1, 10000) == 1
        expect_hits(replica, [(1, 1)], expected)
        replica.execute_command("REPLICAOF", "NO", "ONE")
        assert replica.info("replication")["role"] == "master"
        replica.hset("doc:000", "s", sparse([(1, -1)]))
        expect_hits(replica, [(1, 1)], expected[1:] + [(b"doc:000", 1)])
        primary.hset("doc:only-primary", "s", sparse([(1, 500)]))
        assert b"doc:only-primary" not in dict(search(replica, sparse([(1, 1)]), 100))


class TestSparseAof(ValkeySearchTestCaseBase):
    """Restore schemas and current HASH values from rewritten AOF files."""

    def get_config_file_lines(self, testdir, port):
        """Retain schema aux metadata through the AOF's RDB preamble."""
        return super().get_config_file_lines(testdir, port) + [
            'save ""', "appendonly yes", "appendfsync always",
            "aof-use-rdb-preamble yes",
        ]

    def test_rewrite_and_replay(self):
        """AOF rewrite restores schema and replays subsequent updates and deletes."""
        c = self.server.get_new_client()
        create_index(c)
        c.hset("doc:a", "s", sparse([(1, 2)]))
        c.hset("doc:b", "s", sparse([(1, 3)]))
        expect_hits(c, [(1, 1)], [(b"doc:b", -3), (b"doc:a", -2)])
        c.execute_command("BGREWRITEAOF")
        waiters.wait_for_true(lambda: not c.info("persistence")["aof_rewrite_in_progress"])
        assert c.info("persistence")["aof_last_bgrewrite_status"] == "ok"
        c.hset("doc:a", "s", sparse([(1, 5)]))
        c.delete("doc:b")
        c.hset("doc:c", "s", sparse([(1, -2)]))
        expect_hits(c, [(1, 1)], [(b"doc:a", -5), (b"doc:c", 2)])
        # The framework's remove_rdb flag also deletes the AOF directory.
        self.server.restart(remove_rdb=False)
        c = self.server.get_new_client()
        expect_hits(c, [(1, 1)], [(b"doc:a", -5), (b"doc:c", 2)])
        c.hset("doc:a", "s", sparse([(9, 1)]))
        expect_hits(c, [(1, 1)], [(b"doc:c", 2)])
