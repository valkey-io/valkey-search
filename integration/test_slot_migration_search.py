"""
FT.SEARCH consistency during slot migration.

Every completion mode filters results by slot ownership, so while a target
has a migrating slot's keys indexed but not owned, NOCONTENT returns exactly
what content mode returns.
"""
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser


INDEX_NAME = "idx_sm_consistency"
TAG = "{sm}"  # slot 8193, second shard
OTHER_TAG = "{other}"  # Same shard as TAG, different slot (11361)
NUM_MIGRATING = 20
NUM_OTHER = 10
EXPECTED = NUM_MIGRATING + NUM_OTHER


class TestSearchSlotMigrationConsistency(ValkeySearchClusterTestCase):
    CLUSTER_SIZE = 2

    def _setup(self):
        """Two shards, all docs on the owner of TAG's slot. Returns the source
        and target primaries' clients, the target server and the slot."""
        clients = [self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)]
        clients[0].execute_command(
            "FT.CREATE", INDEX_NAME, "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "t", "TAG", "body", "TEXT")
        for client in clients:
            waiters.wait_for_true(
                lambda c=client: INDEX_NAME.encode() in c.execute_command("FT._LIST"))

        # Determine source and target
        ranges = self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)
        slot = clients[0].execute_command("CLUSTER", "KEYSLOT", TAG)
        src_idx = next(i for i, (start, end) in enumerate(ranges) if start <= slot < end)
        dst_idx = 1 - src_idx
        src, dst = clients[src_idx], clients[dst_idx]

        # Populate data
        other_slot = src.execute_command("CLUSTER", "KEYSLOT", OTHER_TAG)
        assert ranges[src_idx][0] <= other_slot < ranges[src_idx][1]
        for i in range(NUM_MIGRATING):
            src.hset(f"doc:{TAG}:{i}", mapping={"t": "x", "body": "hello"})
        for i in range(NUM_OTHER):
            src.hset(f"doc:{OTHER_TAG}:{i}", mapping={"t": "x", "body": "hello"})
        return src, dst, self.get_primary(dst_idx), slot

    def _num_docs(self, client):
        return FTInfoParser(client.execute_command("FT.INFO", INDEX_NAME)).num_docs

    def _search(self, client, nocontent):
        args = ["FT.SEARCH", INDEX_NAME, "@t:{x}", "LIMIT", "0", "1000"]
        if nocontent:
            args.append("NOCONTENT")
        reply = client.execute_command(*args)
        keys = reply[1::1 if nocontent else 2]
        return reply[0], [k.decode() for k in keys]

    def _hold_migration_before_transfer(self, src, dst, dst_server, slot):
        """Start a legacy migration and copy every key of the slot: the target
        has them indexed while the source still owns the slot."""
        src_id = src.execute_command("CLUSTER", "MYID").decode()
        dst_id = dst.execute_command("CLUSTER", "MYID").decode()
        dst.execute_command("CLUSTER", "SETSLOT", slot, "IMPORTING", src_id)
        src.execute_command("CLUSTER", "SETSLOT", slot, "MIGRATING", dst_id)
        keys = src.execute_command("CLUSTER", "GETKEYSINSLOT", slot, NUM_MIGRATING)
        src.execute_command("MIGRATE", dst_server.bind_ip, dst_server.port, "",
                            0, 5000, "COPY", "KEYS", *keys)
        assert self._num_docs(dst) == NUM_MIGRATING
        return dst_id

    def _transfer_ownership(self, src, dst, dst_id, slot):
        """The source still holds its copies, so it refuses SETSLOT NODE; it
        learns of the new owner through gossip and drops them."""
        dst.execute_command("CLUSTER", "SETSLOT", slot, "NODE", dst_id)
        waiters.wait_for_equal(lambda: self._num_docs(src), NUM_OTHER, timeout=30)
        # The target filters against its cached cluster map, refreshed by cron
        # (250ms expiry), so it drops the slot's keys until that catches up.
        waiters.wait_for_equal(lambda: self._search(dst, True)[0], EXPECTED, timeout=5)

    def _assert_nocontent_matches_content(self, coordinators):
        for coordinator in coordinators:
            count, keys = self._search(coordinator, False)
            assert count == EXPECTED
            assert len(keys) == len(set(keys)) == EXPECTED
            nc_count, nc_keys = self._search(coordinator, True)
            assert nc_count == EXPECTED
            assert sorted(nc_keys) == sorted(keys)

    def test_nocontent_matches_content_during_migration(self):
        """With the migration held, and again after the transfer, NOCONTENT
        returns the same count and keys as content mode from either primary."""
        src, dst, dst_server, slot = self._setup()
        dst_id = self._hold_migration_before_transfer(src, dst, dst_server, slot)
        self._assert_nocontent_matches_content((src, dst))
        self._transfer_ownership(src, dst, dst_id, slot)
        self._assert_nocontent_matches_content((src, dst))
