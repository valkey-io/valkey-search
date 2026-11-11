"""Stress test for lazy expiration during Search commands (#831).

Runs concurrent HSET, FT.SEARCH and EXPIRE traffic against a cluster with
replicas. Before the fix, lazy expiration of keys opened by FT.SEARCH queued an
UNLINK for replication in the middle of the command, which crashed the primary
with:

  blocked.c: 'server.also_propagate.numops == 0' is not true

This test is long running, is skipped by default and is not part of the
default integration suite (run.sh ALL_FILES). To run it manually, remove the
skip decorator and run it with the environment run.sh sets up
(VALKEY_SERVER_PATH, VALKEY_CLI_PATH, VALKEY_SEARCH_PATH, TEST_TMPDIR,
TEST_UNDECLARED_OUTPUTS_DIR).
"""

import logging
import os
import struct
import threading
import time

import valkey
import valkey.cluster

from absl.testing import absltest
import utils

PORTS = [6379, 6380, 6381, 6382, 6383, 6384]
REPLICA_COUNT = 1
INDEX_NAME = "idx"
NUM_KEYS = 1000
NUM_WRITERS = 50
NUM_SEARCHERS = 75
NUM_EXPIRERS = 75
OPS_PER_CLIENT = int(os.environ.get("LAZY_EXPIRE_OPS_PER_CLIENT", "10000"))
QUERY_VECTOR = struct.pack("<3f", 0.1, 0.2, 0.3)


class LazyExpireCrashTest(absltest.TestCase):

    def setUp(self):
        super().setUp()
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(message)s",
            level=logging.INFO,
        )
        valkey_search_path = os.environ["VALKEY_SEARCH_PATH"]
        self.valkey_cluster_under_test = utils.start_valkey_cluster(
            os.environ["VALKEY_SERVER_PATH"],
            os.environ["VALKEY_CLI_PATH"],
            PORTS,
            os.environ["TEST_TMPDIR"],
            os.environ["TEST_UNDECLARED_OUTPUTS_DIR"],
            {
                "loglevel": "notice",
                "enable-debug-command": "yes",
                "repl-diskless-load": "swapdb",
                # tripled, for slow machines
                "cluster-node-timeout": "45000",
            },
            {
                f"{valkey_search_path}": "--reader-threads 2 --writer-threads 5 "
                "--log-level notice --use-coordinator",
            },
            REPLICA_COUNT,
        )

    def tearDown(self):
        if self.valkey_cluster_under_test:
            self.valkey_cluster_under_test.terminate()
        super().tearDown()

    def _cluster_client(self) -> valkey.ValkeyCluster:
        return utils.connect_to_valkey_cluster(
            [valkey.cluster.ClusterNode("localhost", port) for port in PORTS],
            True,
        )

    def _primary_port(self) -> int:
        for port in PORTS:
            if valkey.Valkey(port=port).execute_command("ROLE")[0] in (
                b"master",
                "master",
            ):
                return port
        self.fail("No primary found in cluster")

    def _wait_for_backfill(self, client: valkey.Valkey, timeout: int = 60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = client.execute_command("FT.INFO", INDEX_NAME)
            fields = dict(zip(info[::2], info[1::2]))
            in_progress = fields.get(b"backfill_in_progress",
                                     fields.get("backfill_in_progress"))
            if in_progress in (0, b"0", "0"):
                return
            time.sleep(0.5)
        self.fail("Timed out waiting for backfill to complete")

    @absltest.skip(
        "Long-running high-concurrency stress test for #831; run manually "
        "when needed."
    )
    def test_concurrent_workload(self):
        primary_port = self._primary_port()
        primary = valkey.Valkey(port=primary_port)
        primary.execute_command(
            "FT.CREATE", INDEX_NAME, "ON", "HASH", "SCHEMA",
            "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
            "price", "NUMERIC", "tags", "TAG",
        )
        self._wait_for_backfill(primary)

        write_clients = [self._cluster_client() for _ in range(NUM_WRITERS)]
        search_clients = [
            valkey.Valkey(port=primary_port) for _ in range(NUM_SEARCHERS)
        ]
        expire_clients = [self._cluster_client() for _ in range(NUM_EXPIRERS)]

        crash_detected = threading.Event()
        stop_watchdog = threading.Event()

        def watchdog():
            while not stop_watchdog.is_set():
                try:
                    self.valkey_cluster_under_test.ping_all()
                except Exception as e:  # pylint: disable=broad-except
                    logging.error("Watchdog detected crash: %s", e)
                    crash_detected.set()
                    return
                time.sleep(0.1)

        def writer(client_id):
            for i in range(OPS_PER_CLIENT):
                if crash_detected.is_set():
                    return
                write_clients[client_id].execute_command(
                    "HSET", f"doc:{i % NUM_KEYS}", "vec", QUERY_VECTOR,
                    "price", str(100 + i), "tags", f"tag{i % 5}",
                )

        def searcher(client_id):
            for i in range(OPS_PER_CLIENT):
                if crash_detected.is_set():
                    return
                search_clients[client_id].execute_command(
                    "FT.SEARCH", INDEX_NAME,
                    f"@price:[{90 + i} {110 + i}] =>[KNN 10 @vec $BLOB]",
                    "PARAMS", "2", "BLOB", QUERY_VECTOR, "DIALECT", "2",
                )
                search_clients[client_id].execute_command(
                    "FT.SEARCH", INDEX_NAME,
                    f"@tags:{{tag{i % 5}}} =>[KNN 10 @vec $BLOB]",
                    "PARAMS", "2", "BLOB", QUERY_VECTOR, "DIALECT", "2",
                )

        def expirer(client_id):
            for i in range(OPS_PER_CLIENT):
                if crash_detected.is_set():
                    return
                expire_clients[client_id].execute_command(
                    "EXPIRE", f"doc:{i % NUM_KEYS}", "1"
                )

        def run_safely(fn, client_id):
            try:
                fn(client_id)
            except (valkey.exceptions.ConnectionError,
                    valkey.exceptions.ClusterDownError) as e:
                logging.error("Client %s lost connection: %s", fn.__name__, e)
                crash_detected.set()

        watchdog_thread = threading.Thread(target=watchdog, daemon=True)
        watchdog_thread.start()

        threads = [
            threading.Thread(target=run_safely, args=(writer, i))
            for i in range(NUM_WRITERS)
        ]
        threads += [
            threading.Thread(target=run_safely, args=(searcher, i))
            for i in range(NUM_SEARCHERS)
        ]
        threads += [
            threading.Thread(target=run_safely, args=(expirer, i))
            for i in range(NUM_EXPIRERS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        stop_watchdog.set()
        watchdog_thread.join(timeout=1)

        terminated = self.valkey_cluster_under_test.get_terminated_servers()
        self.assertFalse(crash_detected.is_set(),
                         f"Server crash detected, terminated: {terminated}")
        self.assertEqual(terminated, [])
        self.valkey_cluster_under_test.ping_all()

        stats = primary.info("commandstats")
        self.assertGreater(stats.get("cmdstat_hset", {}).get("calls", 0), 0)
        self.assertGreater(stats.get("cmdstat_expire", {}).get("calls", 0), 0)
        self.assertGreater(
            stats.get("cmdstat_FT.SEARCH", {}).get("calls", 0), 0)


if __name__ == "__main__":
    absltest.main()
