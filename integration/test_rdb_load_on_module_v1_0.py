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
    "test_rdb_load_1_0_0_idx",
    [Text("t")],
)

def _start_server_with_search_module_v1_0(test_case, testdir, dbfilename):
    """Start a valkey server the valkey-search module 1.0.0, loading an existing RDB."""
    server_path = os.getenv("VALKEY_SERVER_PATH")
    
    # Unzip the 1.0.0 module binary, overwriting if it already exists
    module_dir = os.path.join(os.path.dirname(__file__), "module")
    zip_path = os.path.join(module_dir, "1.0.0-libsearch.so.zip")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(module_dir)
    module_path = os.path.join(module_dir, "1.0.0-libsearch.so")
    os.chmod(module_path, 0o755)

    # start the server using 1.0.0 search module
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

def do_rdb_load_on_module_v1_0(test_case, client, server):
    # skip ASAN test for now since binary takes too much space
    if os.environ.get('SAN_BUILD', 'no') != 'no':
        pytest.skip("1.0.0 module binary is not ASAN-compatible")

    # Phase 1: On the server with current search module, create a text index and save
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

    # Phase 2: Start a new server with 1.0 version valkey-search module
    _, logfile, module_path = _start_server_with_search_module_v1_0(
        test_case, testdir, dbfilename
    )

    # The server is expected to fail with certain error message
    # Wait for error message to appear in logs
    try:
        test_case.wait_for_logfile(logfile, "Failed to load ValkeySearch aux section from RDB")
        test_case.wait_for_logfile(logfile, "require minimum version")
        print("Server with search version 1.0.0 correctly failed to load RDB from newer version.")
    finally:
        if os.path.exists(module_path):
            os.remove(module_path)

class TestRDBLoadOnModuleV1_0_CMD(ValkeySearchTestCaseBase):
    # Saving an RDB from the current module version and loading it on the 1.0 module version 
    # Expecting the appropriate error message
    def test_rdb_load_on_module_v1_0(self):
        do_rdb_load_on_module_v1_0(self, self.client, self.server)

class TestRDBLoadOnModuleV1_0_CME(ValkeySearchClusterTestCase):
    # Saving an RDB from the current module version and loading it on the 1.0 module version
    # Expecting the appropriate error message
    def test_rdb_load_on_module_v1_0(self):
        primary = self.replication_groups[0].primary
        cluster_client = self.new_cluster_client()
        do_rdb_load_on_module_v1_0(self, cluster_client, primary.server)
