import os
import pytest
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
from valkey.cluster import ValkeyCluster, ClusterNode
from valkey.client import Valkey
from valkey.connection import Connection
import random
import string
import logging
import shutil


class ValkeySearchTestCaseBase(ValkeyTestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        loadmodule = f"{os.getenv('MODULE_PATH')} --loadmodule {os.getenv('JSON_MODULE_PATH')}"
        args = {
            "enable-debug-command": "yes",
            "loadmodule": loadmodule,
        }
        server_path = os.getenv("VALKEY_SERVER_PATH")

        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=server_path, args=args
        )
        logging.info("startup args are: %s", args)

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


class ValkeySearchClusterTestCase(ValkeySearchTestCaseBase):
    def _start_server(self, port, test_name):
        server_path = os.getenv("VALKEY_SERVER_PATH")
        testdir_base = f"/tmp/valkey-search-clusters/{test_name}"
        testdir = f"{testdir_base}/{port}"

        os.makedirs(testdir, exist_ok=True)
        curdir = os.getcwd()
        os.chdir(testdir)
        lines = [
            "enable-debug-command yes",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"loadmodule {os.getenv('MODULE_PATH')} --use-coordinator",
            f"dir {testdir}",
            "cluster-enabled yes",
        ]

        conf_file = f"{testdir}/valkey.conf"
        with open(conf_file, "w+") as f:
            for line in lines:
                f.write(f"{line}\n")
            f.write("\n")
            f.close()

        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            args="",
            port=port,
            conf_file=conf_file,
        )
        os.chdir(curdir)
        logfile = f"{testdir}/logfile_{port}"
        return server, client, logfile

    def _split_range_pairs(self, start, end, n):
        points = [start + i * (end - start) // n for i in range(n + 1)]
        return list(zip(points[:-1], points[1:]))

    def add_slots(self, node_idx, first_slot, last_slot):
        client: Valkey = self.client_for_primary(node_idx)
        slots_to_add = list(range(int(first_slot), int(last_slot)))
        try:
            client.execute_command("CLUSTERADMIN ADDSLOTS", *slots_to_add)
        except Exception as e:
            client.execute_command("CLUSTER ADDSLOTS", *slots_to_add)

    def cluster_meet(self, node_idx, primaries_count):
        client: Valkey = self.client_for_primary(node_idx)

        for node_to_meet in range(0, primaries_count):
            if node_to_meet == node_idx:
                continue
            try:
                client.execute_command(
                    " ".join(
                        [
                            "CLUSTERADMIN",
                            "MEET",
                            "127.0.0.1",
                            f"{self.get_primary_port(node_to_meet)}",
                        ]
                    )
                )
            except Exception as e:
                client.execute_command(
                    " ".join(
                        [
                            "CLUSTER",
                            "MEET",
                            "127.0.0.1",
                            f"{self.get_primary_port(node_to_meet)}",
                        ]
                    )
                )

    @pytest.fixture(autouse=True)
    def setup_test(self, request):
        self.servers = []
        ports = [
            self.get_bind_port(),
            self.get_bind_port(),
            self.get_bind_port(),
        ]

        testdir_base = f"/tmp/valkey-search-clusters/{request.node.name}"
        if os.path.exists(testdir_base):
            shutil.rmtree(testdir_base)

        for port in ports:
            server, client, logfile = self._start_server(
                port, request.node.name
            )
            self.servers.append(
                {
                    "server": server,
                    "client": client,
                    "port": port,
                    "logfile": logfile,
                }
            )

        # Split the slots
        ranges = self._split_range_pairs(0, 16384, len(ports))
        node_idx = 0
        for start, end in ranges:
            self.add_slots(node_idx, start, end)
            node_idx = node_idx + 1

        # Perform cluster meet
        for node_idx in range(0, 3):
            self.cluster_meet(node_idx, 3)

        # Wait for the cluster to be up
        for server in self.servers:
            logging.info(f"Waiting for cluster to change state...{logfile}")
            self.wait_for_logfile(
                server["logfile"], "Cluster state changed: ok"
            )
            logging.info("Cluster is up and running!")

    def get_primary(self, index):
        return self.servers[index]["server"]

    def get_primary_port(self, index):
        return self.servers[index]["port"]

    def new_client_for_primary(self, index):
        return self.servers[index]["server"].get_new_client()

    def client_for_primary(self, index):
        return self.servers[index]["client"]

    def new_cluster_client(self):
        """Return a cluster client"""
        startup_nodes = [
            ClusterNode("127.0.0.1", self.get_primary_port(0)),
            ClusterNode("127.0.0.1", self.get_primary_port(1)),
            ClusterNode("127.0.0.1", self.get_primary_port(2)),
        ]

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
