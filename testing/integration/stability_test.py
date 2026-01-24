import logging
import os
import time

import valkey
import valkey.cluster

from absl.testing import absltest
from absl.testing import parameterized
import utils
import stability_runner


class StabilityTests(parameterized.TestCase):

    def setUp(self):
        super().setUp()
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(message)s",
            level=logging.DEBUG,
        )
        self.valkey_cluster_under_test = None

    def tearDown(self):
        if self.valkey_cluster_under_test:
            self.valkey_cluster_under_test.terminate()
        super().tearDown()


    @parameterized.named_parameters(

        # dict(
        #     testcase_name="flat_with_backfill_coordinator",
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="flat_with_backfill",
        #         ports=(7009, 7010, 7011),
        #         index_type="FLAT",
        #         vector_dimensions=100,
        #         bgsave_interval_sec=15,
        #         ftcreate_interval_sec=10,
        #         ftdropindex_interval_sec=10,
        #         flushdb_interval_sec=20,
        #         randomize_bg_job_intervals=True,
        #         num_memtier_threads=10,
        #         num_memtier_clients=10,
        #         num_search_clients=10,
        #         insertion_mode="time_interval",
        #         test_time_sec=60,
        #         test_timeout=120,
        #         keyspace_size=1000000,
        #         use_coordinator=True,
        #         replica_count=0,
        #         repl_diskless_load="swapdb",
        #     ),
        # ),
        # dict(
        #     testcase_name="hnsw_with_backfill_no_coordinator",
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="hnsw_with_backfill",
        #         ports=(7012, 7013, 7014),
        #         index_type="HNSW",
        #         vector_dimensions=100,
        #         bgsave_interval_sec=15,
        #         ftcreate_interval_sec=10,
        #         ftdropindex_interval_sec=10,
        #         flushdb_interval_sec=20,
        #         randomize_bg_job_intervals=True,
        #         num_memtier_threads=10,
        #         num_memtier_clients=10,
        #         num_search_clients=10,
        #         insertion_mode="time_interval",
        #         test_time_sec=60,
        #         test_timeout=120,
        #         keyspace_size=1000000,
        #         use_coordinator=False,
        #         replica_count=0,
        #         repl_diskless_load="swapdb",
        #     ),
        # ),
        # dict(
        #     testcase_name="hnsw_no_backfill_no_coordinator",
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="hnsw_no_backfill",
        #         ports=(7015, 7016, 7017),
        #         index_type="HNSW",
        #         vector_dimensions=100,
        #         bgsave_interval_sec=15,
        #         ftcreate_interval_sec=0,
        #         ftdropindex_interval_sec=0,
        #         flushdb_interval_sec=0,
        #         randomize_bg_job_intervals=True,
        #         num_memtier_threads=10,
        #         num_memtier_clients=10,
        #         num_search_clients=10,
        #         insertion_mode="time_interval",
        #         test_time_sec=60,
        #         test_timeout=120,
        #         keyspace_size=1000000,
        #         use_coordinator=False,
        #         replica_count=0,
        #         repl_diskless_load="swapdb",
        #     ),
        # ),
        # dict(
        #     testcase_name="hnsw_with_backfill_coordinator_replica",
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="hnsw_with_backfill",
        #         ports=(7018, 7019, 7020, 7021, 7022, 7023),
        #         index_type="HNSW",
        #         vector_dimensions=100,
        #         bgsave_interval_sec=15,
        #         ftcreate_interval_sec=10,
        #         ftdropindex_interval_sec=10,
        #         flushdb_interval_sec=20,
        #         randomize_bg_job_intervals=True,
        #         num_memtier_threads=10,
        #         num_memtier_clients=10,
        #         num_search_clients=10,
        #         insertion_mode="time_interval",
        #         test_time_sec=60,
        #         test_timeout=120,
        #         keyspace_size=1000000,
        #         use_coordinator=True,
        #         replica_count=1,
        #         repl_diskless_load="swapdb",
        #         failover_interval_sec=30,
        #         test_failover_recovery=True,
        #     ),
        # ),
        dict(
            testcase_name="hnsw_with_backfill_no_coordinator_replica",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(7024, 7025, 7026, 7027, 7028, 7029),
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
                failover_interval_sec=30,
                test_failover_recovery=True,
            ),
        ),
        dict(
            testcase_name="hnsw_with_backfill_coordinator_repl_diskless_disabled",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(7030, 7031, 7032, 7033, 7034, 7035),
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
                test_time_sec=120,
                test_timeout=180,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=1,
                repl_diskless_load="disabled",
                failover_interval_sec=30,
                test_failover_recovery=True,
            ),
        ),
        # dict(
        #     testcase_name=(
        #         "hnsw_with_backfill_no_coordinator_repl_diskless_disabled"
        #     ),
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="hnsw_with_backfill",
        #         ports=(7036, 7037, 7038, 7039, 7040, 7041),
        #         index_type="HNSW",
        #         vector_dimensions=100,
        #         bgsave_interval_sec=15,
        #         ftcreate_interval_sec=10,
        #         ftdropindex_interval_sec=10,
        #         flushdb_interval_sec=20,
        #         randomize_bg_job_intervals=True,
        #         num_memtier_threads=10,
        #         num_memtier_clients=10,
        #         num_search_clients=10,
        #         insertion_mode="time_interval",
        #         test_time_sec=120,  # Total test time - failover happens early, leaving 100s for validation
        #         test_timeout=180,   # Increased timeout to accommodate full test duration
        #         keyspace_size=1000000,
        #         use_coordinator=False,
        #         replica_count=1,
        #         repl_diskless_load="disabled",
        #         failover_interval_sec=35,  # Failover at t=15s, completes by ~t=30s, leaves 90s for post-failover validation
        #         test_failover_recovery=True,
        #     ),
        # ),
    )
    def test_valkeyquery_stability(self, config):
        valkey_server_stdout_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]
        valkey_server_path = os.environ["VALKEY_SERVER_PATH"]
        valkey_cli_path = os.environ["VALKEY_CLI_PATH"]
        memtier_path = os.environ["MEMTIER_PATH"]
        valkey_search_path = os.environ["VALKEY_SEARCH_PATH"]

        config = config._replace(memtier_path=memtier_path)

        self.valkey_cluster_under_test = utils.start_valkey_cluster(
            valkey_server_path,
            valkey_cli_path,
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
                f"{valkey_search_path}": "--reader-threads 2 --writer-threads 5 --log-level notice"
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

        # Check for unexpectedly terminated servers
        # During failover testing, only allow servers that were intentionally shut down
        terminated = self.valkey_cluster_under_test.get_terminated_servers()
        if terminated:
            unexpected_terminations = [
                port for port in terminated 
                if port not in results.intentionally_failed_ports
            ]
            if unexpected_terminations:
                self.fail(
                    f"Valkey servers died unexpectedly during test, ports: {unexpected_terminations}. "
                    f"Intentionally failed ports: {results.intentionally_failed_ports}"
                )
            else:
                logging.info(
                    f"Nodes intentionally shut down during failover testing: {terminated}"
                )

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
            # During failover testing, some background tasks may fail transiently
            # This is expected behavior as nodes are being shut down and promoted
            elif result.name == "FAILOVER":
                # FAILOVER task itself is tracked but failures are acceptable
                # (some failovers may not complete if timing is unlucky)
                pass
            elif config.failover_interval_sec > 0 and result.name in ["FT.CREATE", "FLUSHDB", "FT.DROPINDEX"]:
                # During failover, these operations may fail transiently:
                # - FT.CREATE: coordinator/target node may be failing over
                # - FLUSHDB: cluster may be in transition state
                # - FT.DROPINDEX: index may be temporarily unavailable
                # Allow up to 3 failures per background task during failover testing
                # (1-2 failovers * ~1-2 operations during failover window)
                self.assertLessEqual(
                    result.failures,
                    3,
                    f"Expected at most 3 transient failures for background task {result.name} during failover, got {result.failures}",
                )
            else:
                self.assertEqual(
                    result.failures,
                    0,
                    f"Expected zero failures for background task {result.name}",
                )


if __name__ == "__main__":
    absltest.main()
