"""
Coordinator metadata received from outside the module is validated before it
is used. These tests force validation to fail on one node with the
ForceMetadataValidationFailure controlled variable and check, for each source,
that the node rejects the metadata, keeps running, and does not apply it:

  - from its primary: the FT.INTERNAL_UPDATE replication stream (replica)
  - over the network: GlobalMetadata fetched from a peer primary
  - from an RDB: a replica's full sync

The RDB case uses diskless replica load: that is the only RDB load path on
which the engine survives a failed module aux_load. Loading an RDB file from
disk (startup, DEBUG RELOAD, disk-based sync) exits the server on any load
error, whatever the module reports.
"""
import pytest
from valkey.exceptions import BusyLoadingError, ResponseError

from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters

FORCE_VAR = "ForceMetadataValidationFailure"
FAILURES = "search_metadata_validation_failures_cnt"


class TestMetadataValidation(ValkeySearchClusterTestCaseDebugMode):
    REPLICAS_COUNT = 1

    def get_config_file_lines(self, testdir, port):
        # Diskless sync on both ends, so a replica survives a failed RDB load
        # and retries the sync (see the module docstring).
        return super().get_config_file_lines(testdir, port) + [
            "repl-diskless-sync yes",
            "repl-diskless-sync-delay 0",
            "repl-diskless-load flush-before-load",
            "repl-timeout 10",
        ]

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        # FT.CREATE's consistency check waits for every node; keep it short
        # for the cases where a node rejects the new index.
        args["search.ft-info-timeout-ms"] = "1000"
        return args

    def _force_failure(self, client, enabled):
        client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", FORCE_VAR,
            "yes" if enabled else "no")

    def _failures(self, client):
        return int(client.info("search")[FAILURES])

    def _has_index(self, client, name):
        try:
            return name.encode() in client.execute_command("FT._LIST")
        except BusyLoadingError:
            return False

    def _create_index(self, client, name):
        try:
            client.execute_command("FT.CREATE", name, "SCHEMA", "n", "NUMERIC")
        except ResponseError as e:
            # The index is created locally first. When another node rejects
            # it, the cluster consistency check times out afterwards.
            if "already exists" in str(e):
                raise
        assert self._has_index(client, name)

    def test_replica_rejects_metadata_from_primary(self):
        rg = self.get_replication_group(0)
        primary, replica = rg.primary.client, rg.replicas[0]
        before = self._failures(replica.client)
        self._force_failure(replica.client, True)

        self._create_index(primary, "idx_repl")

        waiters.wait_for_true(
            lambda: self._failures(replica.client) > before, timeout=30)
        assert replica.does_logfile_contains(
            "CreateEntryOnReplica failed in FT.INTERNAL_UPDATE")
        assert replica.client.ping()
        assert replica.client.info("replication")["master_link_status"] == "up"
        assert not self._has_index(replica.client, "idx_repl")

    def test_primary_rejects_metadata_from_peer(self):
        source = self.get_replication_group(0).primary.client
        target = self.get_replication_group(1).primary
        before = self._failures(target.client)
        self._force_failure(target.client, True)

        self._create_index(source, "idx_peer")

        waiters.wait_for_true(
            lambda: self._failures(target.client) > before, timeout=30)
        assert target.does_logfile_contains("Rejecting GlobalMetadata from")
        assert target.client.ping()
        assert not self._has_index(target.client, "idx_peer")

        # The rejection is specific to validation: once it passes, the next
        # metadata broadcast is accepted.
        self._force_failure(target.client, False)
        waiters.wait_for_true(
            lambda: self._has_index(target.client, "idx_peer"), timeout=30)

    def test_replica_rejects_metadata_in_rdb(self):
        rg = self.get_replication_group(0)
        primary, replica = rg.primary.client, rg.replicas[0]
        self._create_index(primary, "idx_rdb")
        waiters.wait_for_true(
            lambda: self._has_index(replica.client, "idx_rdb"), timeout=30)

        before = self._failures(replica.client)
        self._force_failure(replica.client, True)
        # A new replication id makes the reconnect a full sync.
        primary.execute_command("DEBUG", "change-repl-id")
        primary.execute_command("CLIENT", "KILL", "TYPE", "replica")

        waiters.wait_for_true(
            lambda: self._failures(replica.client) > before, timeout=30)
        waiters.wait_for_true(
            lambda: replica.does_logfile_contains(
                "module AUX data for the module type"), timeout=30)
        assert replica.client.ping()

        # Once validation passes, the retried full sync completes.
        self._force_failure(replica.client, False)
        waiters.wait_for_true(
            lambda: self._has_index(replica.client, "idx_rdb"), timeout=60)
        waiters.wait_for_equal(
            lambda: replica.client.info("replication")["master_link_status"],
            "up", timeout=60)
