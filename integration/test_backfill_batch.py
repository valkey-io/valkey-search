import time
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters

NUM_KEYS = 20000
INDEX_NAMES = ("idx1", "idx2", "idx3")


def backfill_percent(client, name):
    info = client.execute_command("FT.INFO", name)
    fields = dict(zip(info[::2], info[1::2]))
    return float(fields[b"backfill_complete_percent"])


class TestBackfillBatch(ValkeySearchTestCaseDebugMode):

    def test_backfill_batch_size_shared_across_indexes(self):
        """
        backfill-batch-size caps the keys scanned per cron tick across all
        indexes. A Scan call returns a whole bucket, so an index can overshoot
        the remaining budget; that must not hand the next indexes an
        unbounded budget and let them scan the whole keyspace in one tick.
        """
        client = self.client
        client.execute_command("DEBUG POPULATE", NUM_KEYS, "key")
        client.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET StopBackfill yes")
        client.execute_command("CONFIG SET search.backfill-batch-size 1")
        for name in INDEX_NAMES:
            client.execute_command("FT.CREATE", name, "ON", "HASH", "PREFIX",
                                   "1", "doc:", "SCHEMA", "n", "NUMERIC")
        client.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET StopBackfill no")

        def percents():
            return {name: backfill_percent(client, name)
                    for name in INDEX_NAMES}

        # Backfill must actually run, or the upper bound below is vacuous.
        waiters.wait_for_true(lambda: max(percents().values()) > 0,
                              timeout=10)

        # With a budget of 1 key per tick, a few seconds of cron ticks cover
        # only a small fraction of the keyspace.
        time.sleep(2)
        result = percents()
        assert max(result.values()) < 0.5, result
