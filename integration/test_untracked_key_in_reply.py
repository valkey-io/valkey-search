"""A key the index has stopped tracking must leave the reply, not fault it.

The database write and the index update do not happen together. A mutation
updates IndexSchema's key bookkeeping synchronously, on the main thread, and
only then queues the index update for a worker. So there is a window in which
a search still finds a document through the index while the schema has already
forgotten it -- and the reply path, which runs later on the main thread, asks
the schema about exactly that key.

Deleting the key is the benign shape: the key is gone from the database too,
so the content fetch fails first and the neighbor is dropped before anything
asks about its sequence number. The shape that reaches the question is an
index-level FILTER rejection: the key is still there and still readable, but
the schema erased its entry because the document no longer belongs in the
index.
"""

import struct

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper, run_in_thread


def _vec(*xs: float) -> bytes:
    return struct.pack(f"<{len(xs)}f", *xs)


class TestUntrackedKeyInReply(ValkeySearchTestCaseDebugMode):
    INDEX = "flt"
    Q = _vec(0.0, 0.0, 0.0, 0.0)

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        # A second writer thread so the parked mutation does not starve the
        # rest of the pool.
        args["search.writer-threads"] = "2"
        return args

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "d:",
            "FILTER", "@status=='active'",
            "SCHEMA",
            "status", "TAG",
            "price", "NUMERIC",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        for i in range(1, 5):
            client.hset(f"d:{i}", mapping={
                "status": "active", "price": i,
                "vec": _vec(float(i), 0.0, 0.0, 0.0)})
        IndexingTestHelper.wait_for_indexing_complete_on_node(
            client, self.INDEX)

    def keys(self, client: Valkey, *args):
        """The keys of an FT.SEARCH reply, in reply order."""
        result = client.execute_command(*args)
        return [result[i] for i in range(1, len(result), 2)]

    def filtered(self, client: Valkey):
        """A non-vector query carrying a filter, so the reply path
        revalidates the filter for every document it fetches."""
        return self.keys(client, "FT.SEARCH", self.INDEX, "@price:[0 100]",
                         "RETURN", "1", "price")

    def knn(self, client: Valkey):
        """A KNN query with no pre-filter, so the reply path skips filter
        revalidation and goes straight to the distance refresh."""
        return self.keys(client, "FT.SEARCH", self.INDEX,
                         "*=>[KNN 4 @vec $q AS dist]", "RETURN", "1", "dist",
                         "DIALECT", "2", "PARAMS", "2", "q", self.Q)

    def _park_mutation(self, client: Valkey, *command):
        """Run a write and hold its index update at the pausepoint.

        The database write lands immediately and the schema's key bookkeeping
        is updated with it; only the index update waits. While this is parked
        the index still offers the old document and the schema has already
        moved on, which is the window under test.
        """
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        thread, _, err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(*command))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing") >= 1,
            timeout=5)
        return thread, err

    def _release(self, client: Valkey, thread, err):
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        thread.join()
        assert err[0] is None
        IndexingTestHelper.wait_for_indexing_complete_on_node(
            client, self.INDEX)

    def test_a_deleted_key_leaves_the_reply(self):
        """The benign shape. The key is gone from the database as well, so
        the content fetch fails and the neighbor is dropped before its
        sequence number is ever looked up. Recorded here because it is the
        obvious guess at reaching the lookup, and it does not."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)
        assert sorted(self.filtered(client)) == [b"d:1", b"d:2", b"d:3",
                                                 b"d:4"]

        thread, err = self._park_mutation(client, "DEL", "d:1")
        assert sorted(self.filtered(client)) == [b"d:2", b"d:3", b"d:4"]
        assert sorted(self.knn(client)) == [b"d:2", b"d:3", b"d:4"]
        self._release(client, thread, err)

    def test_a_filter_rejected_key_leaves_a_filtered_reply(self):
        """The key is still readable, so the content fetch succeeds and the
        filter revalidation asks the schema for a key it no longer tracks."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)

        thread, err = self._park_mutation(
            client, "HSET", "d:1", "status", "inactive")
        during = self.filtered(client)
        self._release(client, thread, err)

        assert sorted(during) == [b"d:2", b"d:3", b"d:4"], (
            f"a document the index stopped tracking is still in the reply: "
            f"{during}")
        assert sorted(self.filtered(client)) == [b"d:2", b"d:3", b"d:4"]

    def test_a_filter_rejected_key_leaves_a_knn_reply(self):
        """The same key, reached through the vector path instead: no
        pre-filter, so the distance refresh is what asks the schema."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)

        thread, err = self._park_mutation(
            client, "HSET", "d:1", "status", "inactive")
        during = self.knn(client)
        self._release(client, thread, err)

        assert sorted(during) == [b"d:2", b"d:3", b"d:4"], (
            f"a document the index stopped tracking is still in the reply: "
            f"{during}")
        assert sorted(self.knn(client)) == [b"d:2", b"d:3", b"d:4"]
