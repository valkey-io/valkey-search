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
import logging
import subprocess

index = Index(
    "test_rdb_load_v1_idx",
    [Text("t")],
)

def _log_contains(logfile, message):
    if not os.path.exists(logfile):
        return False
    with open(logfile, "r") as f:
        return message in f.read()

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

    # Write config file with loadmodule directive
    # Cannot use start_server from valkeytestframework since it wait for server to ready
    port = test_case.get_bind_port()
    conf_file = os.path.join(testdir, f"valkey_{port}.conf")
    logfile = os.path.join(testdir, f"logfile_{port}")
    with open(conf_file, "w") as f:
        f.write(f"appendonly no\n")
        f.write(f"dir {testdir}\n")
        f.write(f"dbfilename {dbfilename}\n")
        f.write(f"loadmodule {module_path}\n")
        f.write(f"port {port}\n")
        f.write(f"logfile {logfile}\n")

    proc = subprocess.Popen([server_path, conf_file])

    return proc, logfile, conf_file, module_path

def do_rdb_load_v1(test_case, client, server):
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
    proc, logfile, conf_file, module_path = _start_server_with_search_v1(
        test_case, testdir, dbfilename
    )

    # wait for server process to exit with time limit
    try:
        waiters.wait_for_true(
            lambda: _log_contains(logfile, "Failed to load ValkeySearch aux section from RDB"),
            timeout=30,
        )
        with open(logfile, "r") as f:
            log_contents = f.read()
        assert "require minimum version" in log_contents, (
            f"Expected version requirement message not found in log: {logfile}"
        )
        logging.info("v1 server correctly failed to load RDB from newer version.")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        for path in [module_path, conf_file, logfile]:
            if os.path.exists(path):
                os.remove(path)

class TestRDBLoadV1CMD(ValkeySearchTestCaseBase):
    def test_rdb_load_v1(self):
        do_rdb_load_v1(self, self.client, self.server)

class TestRDBLoadV1CME(ValkeySearchClusterTestCase):
    def test_rdb_load_v1(self):
        primary = self.replication_groups[0].primary
        cluster_client = self.new_cluster_client()
        do_rdb_load_v1(self, cluster_client, primary.server)
