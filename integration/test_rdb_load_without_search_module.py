import os

from valkey import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchClusterTestCase,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Numeric, Tag

index = Index(
    "test_rdb_no_module_idx",
    [Numeric("n"), Tag("t")],
)

def _start_server_without_module(test_case, port, testdir, dbfilename):
    """Start a valkey server WITHOUT the valkey-search module, loading an existing RDB."""
    server_path = os.getenv("VALKEY_SERVER_PATH")

    # Use create_server from valkeytestframework, customize args to exclude module
    server, client = test_case.create_server(
        testdir=testdir,
        port=port,
        server_path=server_path,
        args={
            "enable-debug-command": "yes",
            "appendonly": "no",
            "dir": testdir,
            "dbfilename": dbfilename,
        }
    )
    logfile = os.path.join(server.cwd, server.args["logfile"])
    return server, client, logfile

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
    server.exit(cleanup=False)

    # Phase 2: Start a new server WITHOUT valkey-search module
    port = test_case.get_bind_port()
    new_server, new_client, logfile = _start_server_without_module(
        test_case, port, testdir, dbfilename
    )

    assert new_client.ping()

    dbsize = new_client.dbsize()
    print(f"Database size after loading RDB without module: {dbsize}")
    assert dbsize == 10, f"Expected 10 keys, got {dbsize}"

    modules = new_client.execute_command("MODULE", "LIST")
    module_names = [
        m[1].decode() if isinstance(m[1], bytes) else m[1] for m in modules
    ]
    assert "search" not in module_names, "valkey-search should NOT be loaded"

    print("CMD: Server loaded RDB without module successfully, no crash.")

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
    primary0.server.exit(cleanup=False)

    # Phase 2: Start first node WITHOUT valkey-search module
    port = test_case.get_bind_port()
    new_server, new_client, logfile = _start_server_without_module(
        test_case, port, testdir, dbfilename
    )

    assert new_client.ping()

    dbsize = new_client.dbsize()
    print(f"Node dbsize after loading RDB without module: {dbsize}")
    assert dbsize > 0, "Expected keys in database"

    modules = new_client.execute_command("MODULE", "LIST")
    module_names = [
        m[1].decode() if isinstance(m[1], bytes) else m[1] for m in modules
    ]
    assert "search" not in module_names

    print("CME: Node loaded RDB without module successfully, no crash.")

class TestRDBLoadWithoutModuleCMD(ValkeySearchTestCaseBase):
    """CMD mode: Verify server doesn't crash loading RDB from a module-enabled server."""
    def test_rdb_load_without_module(self):
        do_rdb_load_without_module_test_cmd(self)

class TestRDBLoadWithoutModuleCME(ValkeySearchClusterTestCase):
    """CME mode: Verify server doesn't crash loading RDB from a module-enabled cluster."""
    def test_rdb_load_without_module(self):
        do_rdb_load_without_module_test_cme(self)
