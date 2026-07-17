"""
RDB cross-version compatibility tests.

The module dynamically stamps each RDB with the minimum version required to load
it, based on the features used (e.g. English text → kRelease12, non-English text
→ kRelease14).  These tests verify that older module versions correctly reject
RDB files whose version stamp exceeds their own (forward-compatibility rejection), 
and that older RDB files can be loaded by the current module (backward compatibility).

Test scenarios are parametrized by (reader_version, index_setup, expected_outcome)
so adding a new version boundary is a single row.
"""

import os

import pytest
from valkey import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchClusterTestCase,
)
from indexes import Index, Text
from utils import (
    IndexingTestHelper,
    cleanup_module_binary,
    skip_if_san_build,
    start_server_with_old_module,
)


# ---------------------------------------------------------------------------
# Parametrized test data
# ---------------------------------------------------------------------------

# Each tuple: (description, reader_version, index_setup_fn, expected_outcome)
#   index_setup_fn: callable(client) that creates an index and ingests data
#   expected_outcome: "reject" (old module refuses RDB) or "accept" (loads fine)

_compat_index = Index("compat_idx", [Text("t")])


def _create_english_text_index(client):
    """Create an English text index — stamps kRelease12."""
    _compat_index.create(client, wait_for_backfill=True)
    _compat_index.load_data(client, 10)


CURRENT_RDB_ON_OLD_MODULE_CASES = [
    pytest.param(
        "1.0.0", _create_english_text_index, "reject",
        id="current_rdb_rejected_by_v1.0",
    ),
]


# ---------------------------------------------------------------------------
# Helper: produce an RDB with the current module, then try loading on old
# ---------------------------------------------------------------------------

def _do_current_rdb_on_old_module(test_case, client, server, reader_version,
                                  index_setup_fn, expected_outcome):
    """
    Phase 1: On the current module, create an index via index_setup_fn and SAVE.
    Phase 2: Start a server with `reader_version` module, loading the RDB.
    Assert the expected outcome.
    """
    skip_if_san_build()

    # Phase 1: create index, ingest data, save RDB
    index_setup_fn(client)
    client.execute_command("SAVE")

    testdir = server.cwd
    dbfilename = server.args["dbfilename"]
    rdb_path = os.path.join(testdir, dbfilename)
    assert os.path.exists(rdb_path), f"RDB file not found at {rdb_path}"

    server.exit(cleanup=False)

    # Phase 2: load RDB on old module
    module_path = None
    try:
        if expected_outcome == "reject":
            old_server, _, logfile, module_path = start_server_with_old_module(
                test_case, testdir, dbfilename, reader_version,
                wait_for_ping=False, connect_client=False,
            )
            test_case.wait_for_logfile(
                logfile, "Failed to load ValkeySearch aux section from RDB"
            )
            test_case.wait_for_logfile(logfile, "require minimum version")
            # Assert the server actually failed to start (process exited)
            old_server.wait_for_shutdown()
            assert old_server.server.returncode is not None, (
                "Expected server to exit after RDB rejection, but it is still running"
            )
        else:
            _, old_client, _, module_path = start_server_with_old_module(
                test_case, testdir, dbfilename, reader_version,
                wait_for_ping=True, connect_client=True,
            )
            assert old_client.ping()
            indexes = old_client.execute_command("FT._LIST")
            assert b"compat_idx" in indexes
    finally:
        cleanup_module_binary(module_path)


# ---------------------------------------------------------------------------
# Tests: current RDB → old module (parametrized)
# ---------------------------------------------------------------------------

class TestCurrentRDBOnOldModule_CMD(ValkeySearchTestCaseBase):
    """Current-version RDB loaded on older module versions (standalone)."""

    @pytest.mark.parametrize(
        "reader_version,index_setup_fn,expected_outcome",
        CURRENT_RDB_ON_OLD_MODULE_CASES,
    )
    def test_current_rdb_on_old_module(self, reader_version, index_setup_fn,
                                       expected_outcome):
        _do_current_rdb_on_old_module(
            self, self.client, self.server,
            reader_version, index_setup_fn, expected_outcome,
        )


class TestCurrentRDBOnOldModule_CME(ValkeySearchClusterTestCase):
    """Current-version RDB loaded on older module versions (cluster)."""

    @pytest.mark.parametrize(
        "reader_version,index_setup_fn,expected_outcome",
        CURRENT_RDB_ON_OLD_MODULE_CASES,
    )
    def test_current_rdb_on_old_module(self, reader_version, index_setup_fn,
                                       expected_outcome):
        primary = self.replication_groups[0].primary
        cluster_client = self.new_cluster_client()
        _do_current_rdb_on_old_module(
            self, cluster_client, primary.server,
            reader_version, index_setup_fn, expected_outcome,
        )


# ---------------------------------------------------------------------------
# Test: old RDB loads on latest (backward compatibility)
# ---------------------------------------------------------------------------

class TestOldRDBLoadsOnLatest(ValkeySearchTestCaseBase):
    """
    An RDB produced by the 1.2 module (English text index, version stamp 1.2.0)
    should load successfully on the current module (1.4+).
    """

    def test_old_rdb_loads_on_latest(self):
        skip_if_san_build()

        testdir = self.server.cwd
        dbfilename = self.server.args["dbfilename"]

        # Stop the current server to free the port/dir
        self.server.exit(cleanup=False)

        # Phase 1: Start with 1.2 module, create English text index, save
        module_path = None
        try:
            server_v1_2, client_v1_2, _, module_path = start_server_with_old_module(
                self, testdir, dbfilename, "1.2.0",
                wait_for_ping=True, connect_client=True,
            )

            client_v1_2.execute_command(
                "FT.CREATE", "old_idx", "ON", "HASH",
                "SCHEMA", "content", "TEXT"
            )
            for i in range(10):
                client_v1_2.execute_command(
                    "HSET", f"doc:{i}", "content", f"hello world document {i}"
                )
            IndexingTestHelper.wait_for_backfill_complete_on_node(
                client_v1_2, "old_idx"
            )
            client_v1_2.execute_command("SAVE")

            rdb_path = os.path.join(testdir, dbfilename)
            assert os.path.exists(rdb_path), f"RDB not found at {rdb_path}"

            server_v1_2.exit(cleanup=False)
        finally:
            cleanup_module_binary(module_path)

        # Phase 2: Start with current module, load the 1.2-produced RDB
        current_module_path = os.getenv("MODULE_PATH")
        server_path = os.getenv("VALKEY_SERVER_PATH")
        server_latest, client_latest = self.create_server(
            testdir=testdir,
            server_path=server_path,
            args={
                "appendonly": "no",
                "dbfilename": dbfilename,
                "loadmodule": current_module_path,
            },
            wait_for_ping=True,
            connect_client=True,
        )

        # Verify the index is present and searchable
        assert client_latest.ping()
        indexes = client_latest.execute_command("FT._LIST")
        assert b"old_idx" in indexes, (
            f"Expected 'old_idx' in index list, got {indexes}"
        )

        IndexingTestHelper.wait_for_backfill_complete_on_node(
            client_latest, "old_idx"
        )

        result = client_latest.execute_command(
            "FT.SEARCH", "old_idx", "@content:hello"
        )
        assert result[0] == 10, (
            f"Expected 10 results searching old index, got {result[0]}"
        )
