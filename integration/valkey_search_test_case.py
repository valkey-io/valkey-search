import os
import time
import pytest
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkeytestframework.valkey_test_case import ReplicationTestCase
from valkeytestframework.valkey_test_case import ValkeyServerHandle
from valkeytestframework.util import waiters
from valkey import ResponseError
from valkey.cluster import ValkeyCluster, ClusterNode
from valkey.client import Valkey
from valkey.connection import Connection
from typing import List, Tuple
import random
import string
import logging
import shutil

LOGS_DIR = "/tmp/valkey-test-framework-files"

if "LOGS_DIR" in os.environ:
    LOGS_DIR = os.environ["LOGS_DIR"]


class Node:
    """This class represents a valkey server instance, regardless of its role"""

    def __init__(
        self,
        client=None,
        server=None,
        logfile=None,
    ):
        self.client: Valkey = client
        self.server: ValkeyServerHandle = server
        self.logfile: str = logfile

    def does_logfile_contains(self, pattern: str) -> bool:
        try:
            logfile = open(self.logfile, "r")
            for line in logfile:
                if pattern in line:
                    return True
            return False
        except:
            return False


class ReplicationGroup:
    """This class represents a valkey server replication group. A replication group consists of a single primary node
    and 0 or more replication nodes."""

    def __init__(
        self,
        primary,
        replicas,
    ):
        self.primary: Node = primary
        self.replicas: List[Node] = replicas

    def setup_replications_cluster(self):
        node_id: bytes = self.primary.client.cluster("MYID")
        node_id = node_id.decode("utf-8")
        for replica in self.replicas:
            replica.client.execute_command(f"CLUSTER REPLICATE {node_id}")
        self._wait_for_replication()

    def setup_replications_cmd(self):
        primary_ip = self.primary.server.bind_ip
        primary_port = self.primary.server.port
        for replica in self.replicas:
            replica.client.execute_command(
                f"REPLICAOF {primary_ip} {primary_port}"
            )
        self._wait_for_replication()

    def _wait_for_replication(self):
        # connected_slaves:1
        # slave0:ip=127.0.0.1,port=14892,state=online,offset=98,lag=0,type=replica
        waiters.wait_for_true(
            lambda: self._check_all_replicas_are_connected(),
            timeout=30,
        )

        # Now that we see all the replicas are connected, wait for their status to change
        # into "online"
        for i in range(0, len(self.replicas)):
            name = f"slave{i}"
            # Wait for the replication to complete for this replica
            waiters.wait_for_true(
                lambda: self._check_is_replica_online(name),
                timeout=30,
            )

    def _check_all_replicas_are_connected(self):
        return self.primary.client.info("replication")[
            "connected_slaves"
        ] == len(self.replicas)

    def _check_is_replica_online(self, name) -> bool:
        replica_status = self.primary.client.info("replication")[name]
        return replica_status["state"] == "online"

    def _wait_for_meet(self, count: int) -> bool:
        d: dict = self.primary.client.cluster("NODES")
        if len(d) != count:
            return False
        for record in d.values():
            if record["connected"] == False:
                return False
        return True

    def _replication_lag(self, index) -> int:
        primary_offset = self.primary.client.info("REPLICATION")['master_repl_offset']
        replica_offset = self.replicas[index].client.info("REPLICATION")['slave_repl_offset']
        assert primary_offset >= replica_offset
        return primary_offset - replica_offset

    def replication_lag(self) -> int:
        '''
        The maximum replication lag
        '''
        return max([self._replication_lag(r) for r in range(len(self.replicas))])

    def get_replica_connection(self, index) -> Valkey:
        return self.replicas[index].client

    def get_primary_connection(self) -> Valkey:
        return self.primary.client

    @staticmethod
    def cleanup(rg):
        """Kill all the replication group nodes"""
        if not rg:
            return

        if rg.primary:
            if rg.primary.server is not None:
                rg.primary.server.exit()

        if not rg.replicas:
            return

        for replica in rg.replicas:
            if replica.server:
                replica.server.exit()


class ValkeySearchTestCaseCommon(ValkeyTestCase):
    """Common base class for the various Search test cases"""

    def normalize_dir_name(self, name: str) -> str:
        """Replace special chars from a string with an underscore"""
        chars_to_replace: str = "!@#$%^&*() -~[]{}><+"
        for char in chars_to_replace:
            name = name.replace(char, "_")
        return name

    def get_config_file_lines(self, testdir, port) -> List[str]:
        """A template method, must be implemented by subclasses
        See ValkeySearchTestCaseBase.get_config_file_lines & ValkeySearchClusterTestCase.get_config_file_lines
        for example usage."""
        raise NotImplementedError

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        return args

    def start_server(
        self,
        port: int,
        test_name: str,
        cluster_enabled=True,
        is_primary=True,
    ) -> (ValkeyServerHandle, Valkey, str):
        """Launch server node and return a tuple of the server handle, a client to the server
        and the log file path"""
        server_path = os.getenv("VALKEY_SERVER_PATH")
        testdir = f"{LOGS_DIR}/{test_name}"

        os.makedirs(testdir, exist_ok=True)
        curdir = os.getcwd()
        os.chdir(testdir)
        lines = self.get_config_file_lines(testdir, port)

        conf_file = f"{testdir}/valkey_{port}.conf"
        with open(conf_file, "w+") as f:
            for line in lines:
                f.write(f"{line}\n")
            f.write("\n")
            f.close()

        role = "primary" if is_primary else "replica"
        logfile = f"{testdir}/logfile-{role}-{port}.log"
        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            args=self.append_startup_args({"logfile": logfile}),
            port=port,
            conf_file=conf_file,
        )
        os.chdir(curdir)
        self.wait_for_logfile(logfile, "Ready to accept connections")
        client.ping()
        return server, client, logfile

    def verify_replicaof_succeeded(self, replica_client) -> bool:
        info = replica_client.execute_command("INFO", "replication")
        role = info.get("role", "")
        master_link_status = info.get("master_link_status", "")
        assert role == "slave" and master_link_status == "up"

    def get_nodes(self) -> List[Node]:
        return self.nodes

    def get_primaries(self) -> List[Node]:
        return self.primaries

    def get_replicas(self) -> List[Node]:
        return self.replicas

    def verify_modules_loaded(self, client: Valkey) -> None:
        """Verify that the required search and json modules are loaded."""
        module_list_data = client.execute_command("MODULE LIST")
        search_loaded = False
        json_loaded = False
        for module in module_list_data:
            if module[b"name"] == b"search":
                search_loaded = True
            elif module[b"name"] == b"json":
                json_loaded = True
        assert search_loaded, "search module not loaded"
        assert json_loaded, "json module not loaded"


class ValkeySearchTestCaseBase(ValkeySearchTestCaseCommon):

    @pytest.fixture(autouse=True)
    def setup_test(self, request):
        # Setup
        test_name = self.normalize_dir_name(request.node.name)
        self.test_name = test_name

        replica_count = 0
        if hasattr(request, "param") and request.param["replica_count"]:
            replica_count = request.param["replica_count"]

        primary = self.start_new_server(is_primary=True)
        replicas: List[Node] = []
        for _ in range(0, replica_count):
            replicas.append(self.start_new_server(is_primary=False))

        self.rg = ReplicationGroup(primary=primary, replicas=replicas)
        self.rg.setup_replications_cmd()
        self.server = self.rg.primary.server
        self.client = self.rg.primary.client

        self.nodes: List[Node] = [self.rg.primary]
        self.nodes += self.rg.replicas

        yield

        # Cleanup
        ReplicationGroup.cleanup(self.rg)

    def get_config_file_lines(self, testdir, port) -> List[str]:
        return [
            "enable-debug-command yes",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"dir {testdir}",
            f"loadmodule {os.getenv('MODULE_PATH')}",
        ]

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False
        except ResponseError as e:
            assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
            assert str(e) == expected_err_reply, assert_error_msg
            return str(e)

    def verify_server_key_count(self, client, expected_num_keys):
        actual_num_keys = self.server.num_keys()
        assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"
        assert actual_num_keys == expected_num_keys, assert_num_key_error_msg

    def generate_random_string(self, length=7):
        """Creates a random string with specified length."""
        characters = string.ascii_letters + string.digits
        random_string = "".join(
            random.choice(characters) for _ in range(length)
        )
        return random_string

    def parse_valkey_info(self, section):
        mem_info = self.client.execute_command("INFO " + section)
        lines = mem_info.decode("utf-8").split("\r\n")
        stats_dict = {}
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                stats_dict[key.strip()] = value.strip()
        return stats_dict

    def start_new_server(self, is_primary=True) -> Node:
        """Create a new Valkey server instance"""
        server, client, logfile = self.start_server(
            port=self.get_bind_port(),
            test_name=self.test_name,
            cluster_enabled=False,
            is_primary=is_primary,
        )
        return Node(client=client, server=server, logfile=logfile)

    def get_replica_connection(self, index) -> Valkey:
        return self.rg.get_replica_connection(index)

    def get_primary_connection(self) -> Valkey:
        return self.rg.get_primary_connection()

def EnableDebugMode(config: List[str]):
    # turn "loadmodule xx.so" into "loadmodule xx.so --debug-mode yes"
    load_module = f"loadmodule {os.getenv('MODULE_PATH')}"
    return [x.replace(load_module, load_module + " --debug-mode yes") for x in config]

class ValkeySearchTestCaseDebugMode(ValkeySearchTestCaseBase):
    '''
    Same as ValkeySearchTestCaseBase, except that "debug-mode" is enabled.
    '''
    def get_config_file_lines(self, testdir, port) -> List[str]:
        return EnableDebugMode(super(ValkeySearchTestCaseDebugMode, self).get_config_file_lines(testdir, port))
    
    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.info-developer-visible"] = "yes"
        return args

class ValkeySearchClusterTestCase(ValkeySearchTestCaseCommon):
    # Default cluster size
    CLUSTER_SIZE = 3
    # Default value for replication
    REPLICAS_COUNT = 0

    def _split_range_pairs(self, start, end, n):
        points = [start + i * (end - start) // n for i in range(n + 1)]
        return list(zip(points[:-1], points[1:]))

    def add_slots(self, node_idx, first_slot, last_slot):
        client: Valkey = self.client_for_primary(node_idx)
        slots_to_add = list(range(int(first_slot), int(last_slot)))
        client.execute_command("CLUSTER ADDSLOTS", *slots_to_add)

    def cluster_meet(self, node_idx, primaries_count):
        """We basically call meet for each node on all nodes in the cluster"""
        client: Valkey = self.client_for_primary(node_idx)
        current_node = self.replication_groups[node_idx]

        for node_to_meet in range(0, primaries_count):
            rg: ReplicationGroup = self.replication_groups[node_to_meet]

            # Prepare a list of instances (the primary + replicas if any)
            instances_arr: List[Node] = [rg.primary]
            for repl in rg.replicas:
                instances_arr.append(repl)

            for repl in current_node.replicas:
                instances_arr.append(repl)

            for inst in instances_arr:
                client.execute_command(
                    " ".join(
                        [
                            "CLUSTER",
                            "MEET",
                            f"{inst.server.bind_ip}",
                            f"{inst.server.port}",
                        ]
                    )
                )

    @pytest.fixture(autouse=True)
    def setup_test(self, request):
        replica_count = self.REPLICAS_COUNT
        if hasattr(request, "param") and request.param["replica_count"]:
            replica_count = request.param["replica_count"]

        self.replication_groups: list[ReplicationGroup] = list()
        ports = []
        for i in range(0, self.CLUSTER_SIZE):
            ports.append(self.get_bind_port())
            for _ in range(0, replica_count):
                ports.append(self.get_bind_port())

        test_name = self.normalize_dir_name(request.node.name)
        testdir_base = f"{LOGS_DIR}/{test_name}"

        if os.path.exists(testdir_base):
            shutil.rmtree(testdir_base)

        for i in range(0, len(ports), replica_count + 1):
            primary_port = ports[i]
            server, client, logfile = self.start_server(
                port=primary_port,
                test_name=test_name,
                cluster_enabled=True,
                is_primary=True,
            )

            replicas = []
            for _ in range(0, replica_count):
                # Start the replicas
                i = i + 1
                replica_port = ports[i]
                replica_server, replica_client, replica_logfile = (
                    self.start_server(
                        port=replica_port,
                        test_name=test_name,
                        cluster_enabled=True,
                        is_primary=False,
                    )
                )
                replicas.append(
                    Node(
                        server=replica_server,
                        client=replica_client,
                        logfile=replica_logfile,
                    )
                )

            primary_node = Node(
                server=server,
                client=client,
                logfile=logfile,
            )
            rg = ReplicationGroup(primary=primary_node, replicas=replicas)
            self.replication_groups.append(rg)

        self.nodes: List[Node] = list()
        for rg in self.replication_groups:
            self.nodes.append(rg.primary)
            self.nodes += rg.replicas

        # Split the slots
        ranges = self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)
        node_idx = 0
        for start, end in ranges:
            self.add_slots(node_idx, start, end)
            node_idx = node_idx + 1

        # Perform cluster meet
        for node_idx in range(0, self.CLUSTER_SIZE):
            self.cluster_meet(node_idx, self.CLUSTER_SIZE)

        waiters.wait_for_equal(
            lambda: self._wait_for_meet(
                self.CLUSTER_SIZE + (self.CLUSTER_SIZE * replica_count)
            ),
            True,
            timeout=10,
        )

        # Wait for the cluster to be up
        for rg in self.replication_groups:
            logging.info(
                f"Waiting for cluster to change state...{rg.primary.logfile}"
            )
            self.wait_for_logfile(
                rg.primary.logfile, "Cluster state changed: ok"
            )
            rg.setup_replications_cluster()
        logging.info("Cluster is up and running!")
        yield

        # Cleanup
        for rg in self.replication_groups:
            ReplicationGroup.cleanup(rg)

    def get_config_file_lines(self, testdir, port) -> List[str]:
        return [
            "enable-debug-command yes",
            "cluster-databases 16",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"dir {testdir}",
            "cluster-enabled yes",
            f"cluster-config-file nodes_{port}.conf",
            f"loadmodule {os.getenv('MODULE_PATH')} --use-coordinator",
        ]

    def _wait_for_meet(self, count: int) -> bool:
        for primary in self.replication_groups:
            if primary._wait_for_meet(count) == False:
                return False
        return True

    def get_primary(self, index) -> ValkeyServerHandle:
        return self.replication_groups[index].primary.server

    def get_primary_port(self, index) -> int:
        return self.replication_groups[index].primary.server.port

    def new_client_for_primary(self, index) -> Valkey:
        return self.replication_groups[index].primary.server.get_new_client()

    def client_for_primary(self, index) -> Valkey:
        return self.replication_groups[index].primary.client
    
    def get_all_primary_clients(self) -> List[Valkey]:
        return [rg.primary.client for rg in self.replication_groups]

    def get_replication_group(self, index) -> ReplicationGroup:
        return self.replication_groups[index]

    def new_cluster_client(self) -> ValkeyCluster:
        """Return a cluster client"""
        startup_nodes = []
        for index in range(0, self.CLUSTER_SIZE):
            startup_nodes.append(
                ClusterNode(
                    self.replication_groups[index].primary.server.bind_ip,
                    self.replication_groups[index].primary.server.port,
                )
            )

        valkey_conn = ValkeyCluster.from_url(
            url="valkey://{}:{}".format(
                startup_nodes[0].host, startup_nodes[0].port
            ),
            connection_class=Connection,
            startup_nodes=startup_nodes,
            require_full_coverage=True,
        )
        valkey_conn.ping()
        return valkey_conn
    
    def replication_lag(self) -> int:
        return max([rg.replication_lag() for rg in self.replication_groups])

class ValkeySearchClusterTestCaseDebugMode(ValkeySearchClusterTestCase):
    '''
    Same as ValkeySearchClusterTestCase, except that "debug-mode" is enabled.
    '''
    def get_config_file_lines(self, testdir, port) -> List[str]:
        return EnableDebugMode(super(ValkeySearchClusterTestCaseDebugMode, self).get_config_file_lines(testdir, port))
    
    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.info-developer-visible"] = "yes"
        return args
