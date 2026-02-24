from valkey import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchClusterTestCase,
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from indexes import Index, Text
import os
import zipfile
import subprocess
import pytest

index = Index(
    "test_rdb_load_v1_idx",
    [Text("t")],
)

def _start_server_with_search_v1(test_case, testdir, dbfilename):
    """Start a valkey server the valkey-search module v1.0, loading an existing RDB."""
    server_path = os.getenv("VALKEY_SERVER_PATH")
    
    # Unzip the v1.0 module binary, overwriting if it already exists
    module_dir = os.path.join(os.path.dirname(__file__), "module")
    zip_path = os.path.join(module_dir, "1.0.0-libsearch.so.zip")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(module_dir)
    module_path = os.path.join(module_dir, "1.0.0-libsearch.so")
    os.chmod(module_path, 0o755)

    # start the server using v1.0 search module
    server, _ = test_case.create_server(
        testdir=testdir,
        server_path=server_path,
        args={
            "appendonly": "no",
            "dbfilename": dbfilename,
            "loadmodule": module_path,
        },
        wait_for_ping=False,
        connect_client=False,
    )

    logfile = os.path.join(server.cwd, server.args["logfile"])
    return server, logfile, module_path

def do_rdb_load_v1(test_case, client, server):
    # skip ASAN test for now since binary takes too much space
    if os.environ.get('SAN_BUILD', 'no') != 'no':
        pytest.skip("v1 module binary is not ASAN-compatible")

    # Phase 1: Create a text index, then save
    index.create(client, wait_for_backfill=True)
    index.load_data(client, 10)
    client.execute_command("SAVE")

    # Grab the RDB location info before stopping
    testdir = server.cwd
    dbfilename = server.args["dbfilename"]
    rdb_path = os.path.join(testdir, dbfilename)
    assert os.path.exists(rdb_path), f"RDB file not found at {rdb_path}"

    # Stop the server (keep the RDB)
    server.exit(cleanup=False)

    # Phase 2: Start a new server WITHOUT valkey-search module
    _, logfile, module_path = _start_server_with_search_v1(
        test_case, testdir, dbfilename
    )

    # wait for server process to exit with time limit
    try:
        test_case.wait_for_logfile(logfile, "Failed to load ValkeySearch aux section from RDB")
        test_case.wait_for_logfile(logfile, "require minimum version")
        print("v1 server correctly failed to load RDB from newer version.")
    finally:
        if os.path.exists(module_path):
            os.remove(module_path)

class TestRDBLoadV1CMD(ValkeySearchTestCaseBase):
    def test_rdb_load_v1(self):
        do_rdb_load_v1(self, self.client, self.server)

class TestRDBLoadV1CME(ValkeySearchClusterTestCase):
    def test_rdb_load_v1(self):
        primary = self.replication_groups[0].primary
        cluster_client = self.new_cluster_client()
        do_rdb_load_v1(self, cluster_client, primary.server)
