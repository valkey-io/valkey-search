import logging
import os
import time

import valkey
import valkey.cluster

from absl import flags
from absl.testing import absltest
from absl.testing import parameterized
from testing.integration import utils
from testing.integration import stability_runner

FLAGS = flags.FLAGS
flags.DEFINE_string("valkey_server_path", None, "Path to the Valkey server")
flags.DEFINE_string("valkey_cli_path", None, "Path to the Valkey CLI")
flags.DEFINE_string("memtier_path", None, "Path to the Memtier binary")


class StabilityTests(parameterized.TestCase):

    def setUp(self):
        super().setUp()
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(message)s",
            level=logging.DEBUG,
        )
        self.valkey_server = None

    def tearDown(self):
        if self.valkey_server:
            for _, process in self.valkey_server.items():
                process.terminate()
        super().tearDown()

    @parameterized.named_parameters(
        dict(
            testcase_name="hnsw_no_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_no_backfill",
                ports=(8000, 8001, 8002),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=0,
                ftdropindex_interval_sec=0,
                flushdb_interval_sec=0,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=0,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="hnsw_with_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(8003, 8004, 8005),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=10,
                ftdropindex_interval_sec=10,
                flushdb_interval_sec=20,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=0,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="flat_no_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="flat_no_backfill",
                ports=(8006, 8007, 8008),
                index_type="FLAT",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=0,
                ftdropindex_interval_sec=0,
                flushdb_interval_sec=0,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=0,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="flat_with_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="flat_with_backfill",
                ports=(8009, 8010, 8011),
                index_type="FLAT",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=10,
                ftdropindex_interval_sec=10,
                flushdb_interval_sec=20,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=0,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="hnsw_with_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(8012, 8013, 8014),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=10,
                ftdropindex_interval_sec=10,
                flushdb_interval_sec=20,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=False,
                replica_count=0,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="hnsw_no_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_no_backfill",
                ports=(8015, 8016, 8017),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=0,
                ftdropindex_interval_sec=0,
                flushdb_interval_sec=0,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=False,
                replica_count=0,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="hnsw_with_backfill_coordinator_replica",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(8018, 8019, 8020, 8021, 8022, 8023),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=10,
                ftdropindex_interval_sec=10,
                flushdb_interval_sec=20,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=1,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="hnsw_with_backfill_no_coordinator_replica",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(8024, 8025, 8026, 8027, 8028, 8029),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=10,
                ftdropindex_interval_sec=10,
                flushdb_interval_sec=20,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=False,
                replica_count=1,
                repl_diskless_load="swapdb",
            ),
        ),
        dict(
            testcase_name="hnsw_with_backfill_coordinator_repl_diskless_disabled",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(8030, 8031, 8032, 8033, 8034, 8035),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=10,
                ftdropindex_interval_sec=10,
                flushdb_interval_sec=20,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=1,
                repl_diskless_load="disabled",
            ),
        ),
        dict(
            testcase_name=(
                "hnsw_with_backfill_no_coordinator_repl_diskless_disabled"
            ),
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(8036, 8037, 8038, 8039, 8040, 8041),
                index_type="HNSW",
                vector_dimensions=100,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=10,
                ftdropindex_interval_sec=10,
                flushdb_interval_sec=20,
                randomize_bg_job_intervals=True,
                num_memtier_threads=10,
                num_memtier_clients=10,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=60,
                test_timeout=120,
                keyspace_size=1000000,
                use_coordinator=False,
                replica_count=1,
                repl_diskless_load="disabled",
            ),
        ),
    )
    def test_valkeyquery_stability(self, config):
        valkey_server_stdout_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]

        if FLAGS.valkey_server_path is None:
            raise ValueError(
                "--test_arg=--valkey_server_path=/path/to/valkey_server "
                "is required"
            )
        if FLAGS.valkey_cli_path is None:
            raise ValueError(
                "--test_arg=--valkey_cli_path=/path/to/valkey_cli is required"
            )
        if FLAGS.memtier_path is None:
            raise ValueError(
                "--test_arg=--memtier_path=/path/to/memtier is required"
            )
        valkey_module_path = os.path.join(
            os.environ["PWD"],
            "src/libvalkeysearch.so",
        )
        config = config._replace(memtier_path=FLAGS.memtier_path)

        self.valkey_server = utils.start_valkey_cluster(
            FLAGS.valkey_server_path,
            FLAGS.valkey_cli_path,
            config.ports,
            os.environ["TEST_TMPDIR"],
            valkey_server_stdout_dir,
            {
                "loglevel": "debug",
                "enable-debug-command": "yes",
                "repl-diskless-load": config.repl_diskless_load,
                # tripled, for slow machines
                "cluster-node-timeout": "45000",
            },
            {
                f"{valkey_module_path}": "--threads 2 --log-level notice"
                + (" --use-coordinator" if config.use_coordinator else "")
            },
            config.replica_count,
        )
        connected = False
        for _ in range(10):
            try:
                valkey_conn = valkey.ValkeyCluster(
                    host="localhost",
                    port=config.ports[0],
                    startup_nodes=[
                        valkey.cluster.ClusterNode("localhost", port)
                        for port in config.ports
                    ],
                    require_full_coverage=True,
                )
                valkey_conn.ping()
                connected = True
                break
            except valkey.exceptions.ConnectionError:
                time.sleep(1)

        if not connected:
            self.fail("Failed to connect to valkey server")

        results = stability_runner.StabilityRunner(config).run()

        if results is None:
            self.fail("Failed to run stability test")

        for port, process in self.valkey_server.items():
            if process.poll():
                self.fail("a process died during test, port: %d", port)

        self.assertTrue(
            results.successful_run,
            msg="Expected stability test to be performed successfully",
        )
        for result in results.memtier_results:
            self.assertGreater(
                result.total_ops,
                0,
                msg=f"Expected positive total ops for memtier run {result.name}",
            )
            self.assertEqual(
                result.failures,
                0,
                f"Expected zero failures for memtier run {result.name}",
            )
            self.assertFalse(
                result.halted,
                msg=(
                    f"Expected memtier run {result.name} to not be halted (didn't "
                    "make progress for >10sec)"
                ),
            )
        for result in results.background_task_results:
            self.assertGreater(
                result.total_ops,
                0,
                msg=f"Expected positive total ops for background task {result.name}",
            )
            # BGSAVE will fail if another is ongoing.
            if result.name == "BGSAVE":
                pass
            else:
                self.assertEqual(
                    result.failures,
                    0,
                    f"Expected zero failures for background task {result.name}",
                )


if __name__ == "__main__":
    absltest.main()
