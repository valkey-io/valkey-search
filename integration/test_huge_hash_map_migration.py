"""
Advisory HugeFlatHashMap migration (vmsdk/src/huge/flat_hash_map.h).

While a table grows or shrinks it keeps its old table (the background) and
moves entries out of it a few at a time. Besides each mutation's own moves,
server cron moves more, so backgrounds finish even when traffic stops:
main-thread tables migrate in cron itself, and an index's own tables in a
mutations-pool task inside the index's write phase. The controlled variable
HugeMapAdvisoryMigration turns cron's moves off, which lets a test leave
backgrounds in place and then turn them back on. The dev INFO field
huge_hash_maps_migrating counts the tables that still have a background.
"""

import json
import time

import pytest
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


def _advisory_migration(client: Valkey, value: str) -> None:
    assert client.execute_command("FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
                                  "HugeMapAdvisoryMigration", value) == b"OK"


def _create_and_ingest(client: Valkey, algorithm: str,
                       key_type: KeyDataType) -> Index:
    index = Index(
        "idx",
        [Vector("vec", DIM, type=algorithm, distance="L2")],
        prefixes=["doc:"],
        type=key_type,
    )
    index.create(client)
    pipe = client.pipeline(transaction=False)
    for i in range(NUM_KEYS):
        vec = [float(i)] * DIM
        if key_type == KeyDataType.HASH:
            pipe.hset(f"doc:{i}", mapping={"vec": float_to_bytes(vec)})
        else:
            pipe.execute_command("JSON.SET", f"doc:{i}", "$",
                                 json.dumps({"vec": vec}))
        if (i + 1) % PIPELINE == 0:
            pipe.execute()
    pipe.execute()
    waiters.wait_for_equal(lambda: index.info(client).num_docs, NUM_KEYS)
    return index


def _expect_frozen_then_drained(client: Valkey) -> None:
    frozen = _maps_migrating(client)
    assert frozen > 0, "no background was left behind"
    # Ten or more cron ticks at the default hz, with nothing draining.
    time.sleep(1.5)
    assert _maps_migrating(client) == frozen

    # From here on, no command touches search: whatever drains now is server
    # cron.
    _advisory_migration(client, "yes")
    waiters.wait_for_equal(lambda: _maps_migrating(client), 0, timeout=30)


class TestHugeHashMapMigration(ValkeySearchTestCaseDebugMode):

    @pytest.mark.parametrize("algorithm,key_type", [
        ("FLAT", KeyDataType.HASH),
        ("HNSW", KeyDataType.HASH),
        ("FLAT", KeyDataType.JSON),
    ], ids=["FLAT-HASH", "HNSW-HASH", "FLAT-JSON"])
    def test_server_cron_drains_all_maps(self, algorithm, key_type):
        """
        With cron's migration off, ingest leaves backgrounds behind and they
        stay. Turned back on with no traffic at all, server cron drains every
        table: the main-thread ones (the string intern store, the vector
        registry's tables, the schema's db_key_info_) and the index's own
        (index_key_info_, the vector index's key maps, the FLAT label map).
        """
        client: Valkey = self.server.get_new_client()
        _advisory_migration(client, "no")
        # A mutation moves the fewest entries it can, so a table that grows
        # keeps its background for nearly the whole of its next cycle.
        _config_set(client, "huge-hash-map-migrate-batch", "1")
        try:
            _create_and_ingest(client, algorithm, key_type)
            _expect_frozen_then_drained(client)
        finally:
            _advisory_migration(client, "yes")
            _config_set(client, "huge-hash-map-migrate-batch", "8")

    def test_server_cron_drains_shrinking_maps(self):
        """
        Deleting keys until the tables shrink also leaves backgrounds, and
        server cron drains those too. Deleting stops as soon as a shrink
        starts: further erases would empty the background themselves.
        """
        client: Valkey = self.server.get_new_client()
        index = _create_and_ingest(client, "FLAT", KeyDataType.HASH)
        waiters.wait_for_equal(lambda: _maps_migrating(client), 0, timeout=30)

        _advisory_migration(client, "no")
        _config_set(client, "huge-hash-map-migrate-batch", "1")
        try:
            chunk = 500
            for start in range(NUM_KEYS - chunk, 0, -chunk):
                keys = [f"doc:{i}" for i in range(start, start + chunk)]
                client.delete(*keys)
                waiters.wait_for_equal(
                    lambda: index.info(client).mutation_queue_size, 0)
                if _maps_migrating(client) > 0:
                    break
            _expect_frozen_then_drained(client)
        finally:
            _advisory_migration(client, "yes")
            _config_set(client, "huge-hash-map-migrate-batch", "8")
