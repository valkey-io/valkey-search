"""Regression test for the fork-window deadlock reported in issue #1260.

A write to an indexed key inside MULTI/EXEC is deferred to the multi/exec queue
and only drained on the next FT.SEARCH. If a fork is in flight at that point the
mutations thread pool is suspended, and draining it from the main thread never
completes: the only paths that resume the pool run on that same thread.
"""

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import run_in_thread
from indexes import *

# A single pending mutation must stay below the auto-drain threshold, which is
# search.writer-threads.
WRITER_THREADS = 2
FILLER_KEYS = 1000
SEARCH_TIMEOUT = 10

index = Index("idx", [Tag("t")], prefixes=["p:"])


class TestMultiQueueForkDeadlock(ValkeySearchTestCaseBase):
    def test_search_during_fork_with_pending_multi_mutation(self):
        """FT.SEARCH must not hang while a fork is in flight."""
        client: Valkey = self.server.get_new_client()
        client.execute_command("CONFIG SET", "search.writer-threads", WRITER_THREADS)
        index.create(client)

        # Give the RDB child enough work to outlive the pipeline below
        for i in range(FILLER_KEYS):
            client.hset(f"filler:{i}", mapping={"t": "x"})

        # One pipeline, so the server handles all of it within a single event
        # loop iteration and the fork child cannot be reaped in between
        def bgsave_then_search():
            pipe = self.server.get_new_client().pipeline(transaction=False)
            pipe.execute_command("BGSAVE")
            pipe.execute_command("MULTI")
            pipe.execute_command("HSET", "p:1", "t", "run")
            pipe.execute_command("EXEC")
            pipe.execute_command("FT.SEARCH", index.name, "@t:{run}", "LIMIT", "0", "1")
            return pipe.execute(raise_on_error=False)

        thread, result, error = run_in_thread(bgsave_then_search)
        thread.join(SEARCH_TIMEOUT)
        assert not thread.is_alive(), "FT.SEARCH deadlocked during the fork window"
        assert error[0] is None, error[0]

        search_reply = result[0][-1]
        assert not isinstance(search_reply, Exception), search_reply
        assert search_reply[0] == 1
        assert client.ping()
