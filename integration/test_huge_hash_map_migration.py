"""
Advisory HugeFlatHashMap migration (vmsdk/src/huge/flat_hash_map.h).

While a table grows it keeps its old table (the background) and moves entries
out of it a few at a time. Besides each mutation's own moves, "advisory"
callers -- ingest, queries and server cron -- move more, so backgrounds finish
even when traffic stops. The dev config huge-hash-map-advisory-migration turns
all of those off, which lets a test leave backgrounds in place and then turn
one source back on at a time. The dev INFO field huge_hash_maps_migrating
counts the tables that still have a background.
"""

import time

from valkey import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector, KeyDataType, float_to_bytes
from util import waiters

DIM = 4
NUM_KEYS = 100000
PIPELINE = 1000


def _maps_migrating(client: Valkey) -> int:
    for entry in client.execute_command("FT._DEBUG", "SHOW_INFO"):
        name = entry[3].decode() if isinstance(entry[3], bytes) else entry[3]
        if name.split(".")[-1] == "huge_hash_maps_migrating":
            return int(entry[5])
    raise AssertionError("huge_hash_maps_migrating not in FT._DEBUG SHOW_INFO")


def _config_set(client: Valkey, name: str, value: str) -> None:
    client.execute_command("CONFIG", "SET", f"search.{name}", value)


class TestHugeHashMapMigration(ValkeySearchTestCaseDebugMode):

    def test_server_cron_drains_main_thread_maps(self):
        """
        With every advisory caller off, ingest leaves backgrounds behind and
        they stay. Turned back on with no traffic at all, only server cron
        runs, and it drains the main-thread maps it owns (the string intern
        store, and the vector registry's tables).
        """
        client: Valkey = self.server.get_new_client()
        _config_set(client, "huge-hash-map-advisory-migration", "no")
        # A mutation moves the fewest entries it can, so a table that grows
        # keeps its background for nearly the whole of its next cycle.
        _config_set(client, "huge-hash-map-migrate-batch", "1")
        try:
            index = Index(
                "idx",
                [Vector("vec", DIM, type="FLAT", distance="L2")],
                prefixes=["doc:"],
                type=KeyDataType.HASH,
            )
            index.create(client)
            pipe = client.pipeline(transaction=False)
            for i in range(NUM_KEYS):
                pipe.hset(f"doc:{i}",
                          mapping={"vec": float_to_bytes([float(i)] * DIM)})
                if (i + 1) % PIPELINE == 0:
                    pipe.execute()
            pipe.execute()
            waiters.wait_for_equal(lambda: index.info(client).num_docs,
                                   NUM_KEYS)

            frozen = _maps_migrating(client)
            assert frozen > 0, "ingest left no background behind"
            # Ten or more cron ticks at the default hz, with nothing draining.
            time.sleep(1.5)
            assert _maps_migrating(client) == frozen

            # From here on, no command touches search: whatever drains now is
            # server cron.
            _config_set(client, "huge-hash-map-advisory-migration", "yes")
            waiters.wait_for_true(lambda: _maps_migrating(client) < frozen,
                                  timeout=30)
        finally:
            _config_set(client, "huge-hash-map-advisory-migration", "yes")
            _config_set(client, "huge-hash-map-migrate-batch", "8")
