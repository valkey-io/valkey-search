"""
RDB version compatibility tests for multi-language index support.

Tests three scenarios around the version boundary introduced by multi-language
text search (kRelease14 = 1.4.0):

1. Old RDB (produced by 1.2 module) loads successfully on latest code (1.4).
2. Latest RDB with multi-language index (stamps 1.4) fails to load on old code (1.2).
3. Latest RDB without multi-language index (stamps 1.2) loads successfully on old code (1.2).

Requires a 1.2.0 module binary at integration/module/1.2.0-libsearch.so.zip.
"""

import os
import zipfile

import pytest
from valkey import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from indexes import Index, Text, Vector
from utils import IndexingTestHelper


MODULE_V1_2_ZIP = "1.2.0-libsearch.so.zip"
MODULE_V1_2_SO = "1.2.0-libsearch.so"


def _skip_if_san_build():
    if os.environ.get("SAN_BUILD", "no") != "no":
        pytest.skip("1.2.0 module binary is not ASAN/TSAN-compatible")


def _get_module_dir():
    return os.path.join(os.path.dirname(__file__), "module")


def _extract_v1_2_module():
    """Extract the 1.2.0 module binary and return its path."""
    module_dir = _get_module_dir()
    zip_path = os.path.join(module_dir, MODULE_V1_2_ZIP)
    if not os.path.exists(zip_path):
        pytest.skip(f"1.2.0 module binary not found at {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(module_dir)
    module_path = os.path.join(module_dir, MODULE_V1_2_SO)
    os.chmod(module_path, 0o755)
    return module_path


def _cleanup_module(module_path):
    """Remove the extracted module binary."""
    if module_path and os.path.exists(module_path):
        os.remove(module_path)


def _start_server_with_module_v1_2(test_case, testdir, dbfilename,
                                   wait_for_ping=False, connect_client=False):
    """Start a valkey server with the 1.2.0 search module, loading an existing RDB."""
    server_path = os.getenv("VALKEY_SERVER_PATH")
    module_path = _extract_v1_2_module()

    server, client = test_case.create_server(
        testdir=testdir,
        server_path=server_path,
        args={
            "appendonly": "no",
            "dbfilename": dbfilename,
            "loadmodule": module_path,
        },
        wait_for_ping=wait_for_ping,
        connect_client=connect_client,
    )

    logfile = os.path.join(server.cwd, server.args["logfile"])
    return server, client, logfile, module_path


# =============================================================================
# Scenario 1: Old RDB (from 1.2) loads on latest version — should work
# =============================================================================


class TestOldRDBLoadsOnLatest(ValkeySearchTestCaseBase):
    """
    An RDB produced by the 1.2 module (English text index, version stamp 1.2.0)
    should load successfully on the current module (1.4).
    """

    def test_old_rdb_loads_on_latest(self):
        _skip_if_san_build()

        # Phase 1: Start server with 1.2 module, create English text index, save
        module_path = _extract_v1_2_module()
        try:
            testdir = self.server.cwd
            dbfilename = self.server.args["dbfilename"]

            # Stop the current server first
            self.server.exit(cleanup=False)

            # Start with 1.2 module
            server_path = os.getenv("VALKEY_SERVER_PATH")
            server_v1_2, client_v1_2 = self.create_server(
                testdir=testdir,
                server_path=server_path,
                args={
                    "appendonly": "no",
                    "dbfilename": dbfilename,
                    "loadmodule": module_path,
                },
                wait_for_ping=True,
                connect_client=True,
            )

            # Create an English text index and ingest data on 1.2
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

            # Stop the 1.2 server
            server_v1_2.exit(cleanup=False)

            # Phase 2: Start with current module, load the 1.2-produced RDB
            current_module_path = os.getenv("MODULE_PATH")
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

            # Wait for backfill to complete after restore
            IndexingTestHelper.wait_for_backfill_complete_on_node(
                client_latest, "old_idx"
            )

            # Verify data is searchable
            result = client_latest.execute_command(
                "FT.SEARCH", "old_idx", "@content:hello"
            )
            assert result[0] == 10, (
                f"Expected 10 results searching old index, got {result[0]}"
            )
            print("Scenario 1 PASSED: Old RDB (1.2) loaded successfully on latest module.")
        finally:
            _cleanup_module(module_path)


# =============================================================================
# Scenario 2: Latest RDB with multi-language index fails on old version
# =============================================================================


class TestMultiLangRDBFailsOnOldVersion(ValkeySearchTestCaseBase):
    """
    An RDB produced by the current module (1.4) with a FRENCH text index
    (version stamp 1.4.0) should fail to load on the 1.2 module.
    """

    def test_multilang_rdb_fails_on_v1_2(self):
        _skip_if_san_build()

        client: Valkey = self.server.get_new_client()

        # Phase 1: Create a French text index on current module (1.4)
        client.execute_command(
            "FT.CREATE", "french_idx", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )
        for i in range(10):
            client.execute_command(
                "HSET", f"doc:{i}", "content", f"Les enfants jouent document {i}"
            )
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "french_idx")
        client.execute_command("SAVE")

        # Grab RDB location
        testdir = self.server.cwd
        dbfilename = self.server.args["dbfilename"]
        rdb_path = os.path.join(testdir, dbfilename)
        assert os.path.exists(rdb_path), f"RDB not found at {rdb_path}"

        # Stop the current server (keep the RDB)
        self.server.exit(cleanup=False)

        # Phase 2: Start server with 1.2 module, loading the 1.4-stamped RDB
        module_path = None
        try:
            _, _, logfile, module_path = _start_server_with_module_v1_2(
                self, testdir, dbfilename,
                wait_for_ping=False, connect_client=False,
            )

            # The 1.2 module should reject the RDB because version 1.4 > 1.2
            self.wait_for_logfile(
                logfile, "Failed to load ValkeySearch aux section from RDB"
            )
            self.wait_for_logfile(logfile, "require minimum version")
            print(
                "Scenario 2 PASSED: Multi-language RDB (1.4) correctly "
                "rejected by 1.2 module."
            )
        finally:
            _cleanup_module(module_path)


# =============================================================================
# Scenario 3: Latest RDB without multi-language loads on old version
# =============================================================================


class TestNonMultiLangRDBLoadsOnOldVersion(ValkeySearchTestCaseBase):
    """
    An RDB produced by the current module (1.4) with only an English text index
    (version stamp 1.2.0, since no multi-language features are used) should load
    successfully on the 1.2 module.
    """

    def test_non_multilang_rdb_loads_on_v1_2(self):
        _skip_if_san_build()

        client: Valkey = self.server.get_new_client()

        # Phase 1: Create an English-only text index on current module (1.4)
        # This produces a version stamp of 1.2.0 (not 1.4.0)
        client.execute_command(
            "FT.CREATE", "english_idx", "ON", "HASH",
            "SCHEMA", "content", "TEXT"
        )
        for i in range(10):
            client.execute_command(
                "HSET", f"doc:{i}", "content", f"hello world document {i}"
            )
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "english_idx")
        client.execute_command("SAVE")

        # Grab RDB location
        testdir = self.server.cwd
        dbfilename = self.server.args["dbfilename"]
        rdb_path = os.path.join(testdir, dbfilename)
        assert os.path.exists(rdb_path), f"RDB not found at {rdb_path}"

        # Stop the current server (keep the RDB)
        self.server.exit(cleanup=False)

        # Phase 2: Start server with 1.2 module, loading the RDB
        module_path = None
        try:
            server_v1_2, client_v1_2, logfile, module_path = (
                _start_server_with_module_v1_2(
                    self, testdir, dbfilename,
                    wait_for_ping=True, connect_client=True,
                )
            )

            # The 1.2 module should accept this RDB (stamp 1.2.0 <= module 1.2.0)
            assert client_v1_2.ping()

            # Verify the index exists
            indexes = client_v1_2.execute_command("FT._LIST")
            assert b"english_idx" in indexes, (
                f"Expected 'english_idx' in index list, got {indexes}"
            )

            # Wait for backfill to complete
            IndexingTestHelper.wait_for_backfill_complete_on_node(
                client_v1_2, "english_idx"
            )

            # Verify data is searchable on the old module
            result = client_v1_2.execute_command(
                "FT.SEARCH", "english_idx", "@content:hello"
            )
            assert result[0] == 10, (
                f"Expected 10 results on 1.2 module, got {result[0]}"
            )
            print(
                "Scenario 3 PASSED: English-only RDB from 1.4 loaded "
                "successfully on 1.2 module."
            )
        finally:
            _cleanup_module(module_path)
