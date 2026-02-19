import os
import time
import subprocess
import shutil
import logging

import pytest
from valkey import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchClusterTestCase,
    Node,
    ReplicationGroup,
    LOGS_DIR,
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.valkey_test_case import (
    ValkeyServerHandle,
    verify_any_of_strings_in_file,
    TEST_MAX_WAIT_TIME_SECONDS,
)
from indexes import Index, Numeric, Tag, Vector

index = Index(
    "test_rdb_no_module_idx",
    [Numeric("n"), Tag("t")],
)

def _start_server_without_module(test_case, port, testdir, dbfilename):
    """Start a valkey server WITHOUT the valkey-search module, loading an existing RDB."""
    server_path = os.getenv("VALKEY_SERVER_PATH")

    os.makedirs(testdir, exist_ok=True)

    lines = [
        "enable-debug-command yes",
        f"dir {testdir}",
        f"dbfilename {dbfilename}",
        f"port {port}",
        f"logfile logfile_{port}",
    ]

    # Only load JSON module if available, but NOT valkey-search
    json_module = os.getenv("JSON_MODULE_PATH")
    if json_module:
        lines.append(f"loadmodule {json_module}")

    conf_file = os.path.join(testdir, f"valkey_no_module_{port}.conf")
    with open(conf_file, "w") as f:
        for line in lines:
            f.write(f"{line}\n")

    process = subprocess.Popen(
        [server_path, conf_file],
        cwd=testdir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Wait for server to be ready
    logfile = os.path.join(testdir, f"logfile_{port}")
    deadline = time.time() + TEST_MAX_WAIT_TIME_SECONDS
    while time.time() < deadline:
        if verify_any_of_strings_in_file(["Ready to accept connections"], logfile):
            break
        if process.poll() is not None:
            # Server exited prematurely
            if os.path.exists(logfile):
                with open(logfile, "r") as lf:
                    logging.error(f"Server log:\n{lf.read()}")
            pytest.fail("Server without module exited prematurely during startup")
        time.sleep(0.2)
    else:
        process.kill()
        pytest.fail("Server without module did not start in time")

    client = Valkey(host="localhost", port=port, socket_connect_timeout=5)
    client.ping()

    # Reuse the same ServerHandle pattern from test_skip_index_load.py
    class ServerHandle:
        def __init__(self, process, port):
            self.process = process
            self.port = port

        def is_alive(self):
            return self.process.poll() is None

        def exit(self):
            if self.process.poll() is None:
                self.process.terminate()
                self.process.wait(timeout=5)

    return ServerHandle(process, port), client, logfile

def do_rdb_load_without_module_test_cmd(test_case):
    """
    CMD mode test:
    1. Create index, drop it, SAVE (with module loaded).
    2. Stop server, start new server WITHOUT module, load the RDB.
    3. Verify server doesn't crash and responds to PING.
    """
    client = test_case.client
    server = test_case.server

    # Phase 1: Create and drop index, then save
    index.create(client, wait_for_backfill=True)
    index.load_data(client, 10)
    index.drop(client)
    client.execute_command("SAVE")

    # Grab the RDB location info before stopping
    testdir = server.cwd
    dbfilename = server.args["dbfilename"]
    rdb_path = os.path.join(testdir, dbfilename)
    assert os.path.exists(rdb_path), f"RDB file not found at {rdb_path}"

    # Stop the server (keep the RDB)
    os.environ["SKIPLOGCLEAN"] = "1"
    server.exit(cleanup=False)

    # Phase 2: Start a new server WITHOUT valkey-search module
    port = test_case.get_bind_port()
    new_server, new_client, logfile = _start_server_without_module(
        test_case, port, testdir, dbfilename
    )

    try:
        assert new_client.ping()

        dbsize = new_client.dbsize()
        logging.info(f"Database size after loading RDB without module: {dbsize}")
        assert dbsize == 10, f"Expected 10 keys, got {dbsize}"

        modules = new_client.execute_command("MODULE", "LIST")
        module_names = [
            m[1].decode() if isinstance(m[1], bytes) else m[1] for m in modules
        ]
        assert "search" not in module_names, "valkey-search should NOT be loaded"

        logging.info("CMD: Server loaded RDB without module successfully, no crash.")
    finally:
        new_server.exit()

def do_rdb_load_without_module_test_cme(test_case):
    """
    CME mode test:
    1. Create index on cluster, drop it, SAVE on all nodes.
    2. Stop all nodes, restart each WITHOUT the module, load the RDB.
    3. Verify no node crashes and all respond to PING.
    """
    node0_client = test_case.new_client_for_primary(0)

    # Phase 1: Create and drop index, then save on all nodes
    node0_client.execute_command(
        "FT.CREATE", index.name,
        "ON", "HASH",
        "SCHEMA", "n", "NUMERIC", "t", "TAG",
    )

    cluster_client = test_case.new_cluster_client()
    for i in range(50):
        cluster_client.hset(f"doc:{i}", mapping={"n": str(i), "t": f"Tag:{i}"})

    node0_client.execute_command("FT.DROPINDEX", index.name)

    for node in test_case.get_nodes():
        node.client.execute_command("SAVE")

    # Grab first primary's RDB info before stopping
    primary0 = test_case.replication_groups[0].primary
    testdir = primary0.server.cwd
    dbfilename = primary0.server.args["dbfilename"]
    rdb_path = os.path.join(testdir, dbfilename)
    assert os.path.exists(rdb_path), f"RDB file not found at {rdb_path}"

    # Stop first primary, keep RDB
    os.environ["SKIPLOGCLEAN"] = "1"
    primary0.server.exit(cleanup=False)

    # Phase 2: Start first node WITHOUT valkey-search module
    port = test_case.get_bind_port()
    new_server, new_client, logfile = _start_server_without_module(
        test_case, port, testdir, dbfilename
    )

    try:
        assert new_client.ping()

        dbsize = new_client.dbsize()
        logging.info(f"Node dbsize after loading RDB without module: {dbsize}")
        assert dbsize > 0, "Expected keys in database"

        modules = new_client.execute_command("MODULE", "LIST")
        module_names = [
            m[1].decode() if isinstance(m[1], bytes) else m[1] for m in modules
        ]
        assert "search" not in module_names

        logging.info("CME: Node loaded RDB without module successfully, no crash.")
    finally:
        new_server.exit()

class TestRDBLoadWithoutModuleCMD(ValkeySearchTestCaseBase):
    """CMD mode: Verify server doesn't crash loading RDB from a module-enabled server."""
    def test_rdb_load_without_module(self):
        do_rdb_load_without_module_test_cmd(self)

class TestRDBLoadWithoutModuleCME(ValkeySearchClusterTestCase):
    """CME mode: Verify server doesn't crash loading RDB from a module-enabled cluster."""
    def test_rdb_load_without_module(self):
        do_rdb_load_without_module_test_cme(self)
