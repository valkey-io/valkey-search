"""
Verify search_number_of_indexes is updated during RDB restore if search indexes exist instead of after restore
"""

import time
import threading
from valkeytestframework.conftest import resource_port_tracker
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from indexes import *
from util import waiters

index_1 = Index("idx_1", [Vector("v", 3, type="HNSW", m=2, efc=1), Text("txt")])
index_2 = Index("idx_2", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n"), Tag("t"), Text("description")])
NUM_DOCS = 10000


class TestNumberOfIndexesOnRestore(ValkeySearchTestCaseDebugMode):

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        # Small queue creates backpressure during V2 key loading, slowing
        # LoadIndexExtension enough to observe rdb_restore_in_progress=1.
        args["search.max-mutation-queue-size-on-restore"] = "1"
        return args

    def test_number_of_indexes_after_restore(self):
        index_1.create(self.client, True)
        index_2.create(self.client, True)
        index_1.load_data(self.client, NUM_DOCS)
        index_2.load_data(self.client, NUM_DOCS)
        assert self.client.info("search")["search_number_of_indexes"] == 2

        self.client.execute_command("SAVE")

        # Monitor INFO from a separate connection during DEBUG RELOAD
        monitor_client = self.server.get_new_client()
        done = threading.Event()
        violation = [None]

        def monitor():
            while not done.is_set():
                try:
                    info = monitor_client.info("search")
                    if info.get("search_rdb_restore_in_progress", 0) and \
                       info["search_number_of_indexes"] < 2:
                        violation[0] = info["search_number_of_indexes"]
                        return
                except Exception:
                    pass
                time.sleep(0.001)

        threading.Thread(target=monitor, daemon=True).start()
        self.client.execute_command("DEBUG", "RELOAD")
        done.set()

        assert violation[0] is None, \
            f"number_of_indexes was {violation[0]} during rdb_restore_in_progress=1"

        waiters.wait_for_true(lambda: index_1.backfill_complete(self.client))
        waiters.wait_for_true(lambda: index_2.backfill_complete(self.client))
        assert self.client.info("search")["search_number_of_indexes"] == 2
