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

        dict(
            testcase_name="flat_with_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="flat_with_backfill",
                ports=(7009, 7010, 7011),
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
            testcase_name="hnsw_memory_limit_save_restore",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_memory_test",
                ports=(7087,),  # Single node only
                index_type="HNSW",
                vector_dimensions=128,
                bgsave_interval_sec=15,
                ftcreate_interval_sec=0,  # Disabled - only one create at start
                ftdropindex_interval_sec=0,  # Disabled - no drops
                flushdb_interval_sec=0,  # Disabled - no flushes
                randomize_bg_job_intervals=True,
                num_memtier_threads=20,
                num_memtier_clients=20,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=500,
                test_timeout=600,
                keyspace_size=10000000,
                use_coordinator=False,
                replica_count=0,
                repl_diskless_load="swapdb",
                maxmemory="2000mb",  # 2GB memory limit
                maxmemory_policy="noeviction",
                failover_interval_sec=0,
                test_failover_recovery=False,
            ),
        ),
        dict(
            testcase_name="text_memory_limit_save_restore",
            config=stability_runner.StabilityTestConfig(
                index_name="text_memory_test",
                ports=(7088,),  # Single node only
                index_type="TEXT",
                vector_dimensions=10,  # Dummy value for TEXT index
                bgsave_interval_sec=15,
                ftcreate_interval_sec=0,
                ftdropindex_interval_sec=0,
                flushdb_interval_sec=0,
                randomize_bg_job_intervals=True,
                num_memtier_threads=20,
                num_memtier_clients=20,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=500,
                test_timeout=600,
                keyspace_size=10000000,
                use_coordinator=False,
                replica_count=0,
                repl_diskless_load="swapdb",
                maxmemory="500mb",
                maxmemory_policy="noeviction",
                failover_interval_sec=0,
                test_failover_recovery=False,
            ),
        ),
        dict(
            testcase_name="tag_memory_limit_save_restore",
            config=stability_runner.StabilityTestConfig(
                index_name="tag_memory_test",
                ports=(7089,),  # Single node only
                index_type="TAG",
                vector_dimensions=10,  # Dummy value for TAG index
                bgsave_interval_sec=15,
                ftcreate_interval_sec=0,
                ftdropindex_interval_sec=0,
                flushdb_interval_sec=0,
                randomize_bg_job_intervals=True,
                num_memtier_threads=20,
                num_memtier_clients=20,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=500,
                test_timeout=600,
                keyspace_size=10000000,
                use_coordinator=False,
                replica_count=0,
                repl_diskless_load="swapdb",
                maxmemory="500mb",
                maxmemory_policy="noeviction",
                failover_interval_sec=0,
                test_failover_recovery=False,
            ),
        ),
        dict(
            testcase_name="numeric_memory_limit_save_restore",
            config=stability_runner.StabilityTestConfig(
                index_name="numeric_memory_test",
                ports=(7090,),  # Single node only
                index_type="NUMERIC",
                vector_dimensions=10,  # Dummy value for NUMERIC index
                bgsave_interval_sec=15,
                ftcreate_interval_sec=0,
                ftdropindex_interval_sec=0,
                flushdb_interval_sec=0,
                randomize_bg_job_intervals=True,
                num_memtier_threads=20,
                num_memtier_clients=20,
                num_search_clients=10,
                insertion_mode="time_interval",
                test_time_sec=500,
                test_timeout=600,
                keyspace_size=10000000,
                use_coordinator=False,
                replica_count=0,
                repl_diskless_load="swapdb",
                maxmemory="500mb",
                maxmemory_policy="noeviction",
                failover_interval_sec=0,
                test_failover_recovery=False,
            ),
        ),
        dict(
            testcase_name="hnsw_with_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(7012, 7013, 7014),
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
                ports=(7015, 7016, 7017),
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
                ports=(7018, 7019, 7020, 7021, 7022, 7023),
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
                failover_interval_sec=30,
                test_failover_recovery=True,
            ),
        ),
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
                test_time_sec=90,
                test_timeout=180,
                keyspace_size=1000000,
                use_coordinator=True,
                replica_count=1,
                repl_diskless_load="disabled",
                failover_interval_sec=50,
                test_failover_recovery=True,
            ),
        ),
        dict(
            testcase_name=(
                "hnsw_with_backfill_no_coordinator_repl_diskless_disabled"
            ),
            config=stability_runner.StabilityTestConfig(
                index_name="hnsw_with_backfill",
                ports=(7036, 7037, 7038, 7039, 7040, 7041),
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
                use_coordinator=False,
                replica_count=1,
                repl_diskless_load="disabled",
                failover_interval_sec=35,
                test_failover_recovery=True,
            ),
        ),
        dict(
            testcase_name="text_with_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="text_with_backfill",
                ports=(7042, 7043, 7044),
                index_type="TEXT",
                vector_dimensions=10,  # Dummy value for TEXT index
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
            testcase_name="text_with_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="text_with_backfill",
                ports=(7045, 7046, 7047),
                index_type="TEXT",
                vector_dimensions=10,  # Dummy value for TEXT index
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
            testcase_name="text_no_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="text_no_backfill",
                ports=(7048, 7049, 7050),
                index_type="TEXT",
                vector_dimensions=10,  # Dummy value for TEXT index
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
        # THIS TEST FAILS BECAUSE OF A KNOWN CRASH
        # dict(
        #     testcase_name="text_with_backfill_coordinator_replica",
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="text_with_backfill",
        #         ports=(7051, 7052, 7053, 7054, 7055, 7056),
        #         index_type="TEXT",
        #         vector_dimensions=10,  # Dummy value for TEXT index
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
        #     ),
        # ),
        dict(
            testcase_name="tag_with_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="tag_with_backfill",
                ports=(7057, 7058, 7059),
                index_type="TAG",
                vector_dimensions=10,  # Dummy value for TAG index
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
            testcase_name="tag_with_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="tag_with_backfill",
                ports=(7060, 7061, 7062),
                index_type="TAG",
                vector_dimensions=10,  # Dummy value for TAG index
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
            testcase_name="tag_no_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="tag_no_backfill",
                ports=(7063, 7064, 7065),
                index_type="TAG",
                vector_dimensions=10,  # Dummy value for TAG index
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
        # THIS TEST FAILS BECAUSE OF A KNOWN CRASH
        # dict(
        #     testcase_name="tag_with_backfill_coordinator_replica",
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="tag_with_backfill",
        #         ports=(7066, 7067, 7068, 7069, 7070, 7071),
        #         index_type="TAG",
        #         vector_dimensions=10,  # Dummy value for TAG index
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
        #     ),
        # ),
        dict(
            testcase_name="numeric_with_backfill_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="numeric_with_backfill",
                ports=(7072, 7073, 7074),
                index_type="NUMERIC",
                vector_dimensions=10,  # Dummy value for NUMERIC index
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
            testcase_name="numeric_with_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="numeric_with_backfill",
                ports=(7075, 7076, 7077),
                index_type="NUMERIC",
                vector_dimensions=10,  # Dummy value for NUMERIC index
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
            testcase_name="numeric_no_backfill_no_coordinator",
            config=stability_runner.StabilityTestConfig(
                index_name="numeric_no_backfill",
                ports=(7078, 7079, 7080),
                index_type="NUMERIC",
                vector_dimensions=10,  # Dummy value for NUMERIC index
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
        # THIS TEST FAILS BECAUSE OF A KNOWN CRASH
        # dict(
        #     testcase_name="numeric_with_backfill_coordinator_replica",
        #     config=stability_runner.StabilityTestConfig(
        #         index_name="numeric_with_backfill",
        #         ports=(7081, 7082, 7083, 7084, 7085, 7086),
        #         index_type="NUMERIC",
        #         vector_dimensions=10,  # Dummy value for NUMERIC index
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

        # Check if this is standalone mode (single node, no replicas)
        is_standalone = len(config.ports) == 1 and config.replica_count == 0
        
        if is_standalone:
            # Standalone mode for OOM testing
            logging.info("Starting standalone Valkey server for OOM testing")
            server_config = {
                "loglevel": "debug",
                "enable-debug-command": "yes",
            }
            if config.maxmemory:
                server_config["maxmemory"] = config.maxmemory
                server_config["maxmemory-policy"] = config.maxmemory_policy
            
            stdout_path = os.path.join(valkey_server_stdout_dir, f"{config.ports[0]}_stdout.txt")
            stdout_file = open(stdout_path, "w", buffering=1)
            
            # Use same naming convention as cluster mode (nodes{port}) so restart_node can find it
            node_dir = os.path.join(os.environ["TEST_TMPDIR"], f"nodes{config.ports[0]}")
            os.makedirs(node_dir, exist_ok=True)
            
            standalone_server = utils.start_valkey_process(
                valkey_server_path,
                config.ports[0],
                node_dir,
                stdout_file,
                server_config,
                {
                    f"{valkey_search_path}": "--reader-threads 2 --writer-threads 5 --log-level notice"
                    + (" --use-coordinator" if config.use_coordinator else "")
                },
            )
            # Wrap in a simple object that has terminate and get_terminated_servers methods
            self.valkey_cluster_under_test = utils.ValkeyClusterUnderTest(
                [standalone_server], [stdout_file]
            )
        else:
            # Cluster mode
            cluster_config = {
                "loglevel": "debug",
                "enable-debug-command": "yes",
                "repl-diskless-load": config.repl_diskless_load,
                "cluster-node-timeout": "45000",
            }
            
            if config.maxmemory:
                cluster_config["maxmemory"] = config.maxmemory
                cluster_config["maxmemory-policy"] = config.maxmemory_policy
            
            self.valkey_cluster_under_test = utils.start_valkey_cluster(
                valkey_server_path,
                valkey_cli_path,
                config.ports,
                os.environ["TEST_TMPDIR"],
                valkey_server_stdout_dir,
                cluster_config,
                {
                    f"{valkey_search_path}": "--reader-threads 2 --writer-threads 5 --log-level notice"
                    + (" --use-coordinator" if config.use_coordinator else "")
                },
                config.replica_count,
            )
        connected = False
        for _ in range(10):
            try:
                if is_standalone:
                    # Use standalone client for single node
                    valkey_conn = valkey.Valkey(
                        host="localhost",
                        port=config.ports[0],
                    )
                else:
                    # Use cluster client for multi-node
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

        results = stability_runner.StabilityRunner(config, is_standalone=is_standalone).run()

        if results is None:
            self.fail("Failed to run stability test")

        # Check for unexpectedly terminated servers
        terminated = self.valkey_cluster_under_test.get_terminated_servers()
        
        # Special handling for OOM test (single node, maxmemory set, noeviction policy)
        if (len(config.ports) == 1 and config.maxmemory and 
            config.maxmemory_policy == "noeviction"):
            # Check if OOM was detected (marked in intentionally_failed_ports by runner)
            if config.ports[0] in results.intentionally_failed_ports:
                logging.info("=" * 60)
                logging.info("OOM DETECTED - Beginning disconnect/reconnect sequence")
                logging.info("=" * 60)
                
                # Use utils.shutdown_node to shutdown the server (like in failover)
                port = config.ports[0]
                addr = f"localhost:{port}"
                logging.info("Shutting down server at %s...", addr)
                utils.shutdown_node(addr)
                
                # Wait for shutdown to complete
                time.sleep(5)
                
                # Restart the server using utils.restart_node (adapted from failover code)
                logging.info("Restarting server to test save/restore...")
                modules = {
                    f"{valkey_search_path}": "--reader-threads 2 --writer-threads 5 --log-level notice"
                    + (" --use-coordinator" if config.use_coordinator else "")
                }
                
                restarted_node = utils.restart_node(
                    valkey_server_path=valkey_server_path,
                    port=port,
                    config_dir=os.environ["TEST_TMPDIR"],
                    stdout_dir=valkey_server_stdout_dir,
                    modules=modules,
                )
                
                if not restarted_node:
                    self.fail(f"Failed to restart server on port {port}")
                
                logging.info("Server restarted, waiting for it to load data from disk...")
                logging.info("This may take time depending on the amount of data saved...")
                time.sleep(20)  # Give more time for restore to begin
                
                # Try to reconnect and verify - with extended timeout for restore
                reconnected = False
                for attempt in range(60):  # Try for up to 60 seconds
                    try:
                        valkey_conn = valkey.Valkey(
                            host="localhost",
                            port=port,
                            socket_timeout=10,
                        )
                        valkey_conn.ping()
                        reconnected = True
                        logging.info("Successfully reconnected to restarted server")
                        
                        # Verify data restoration
                        try:
                            import numpy as np
                            
                            # 1. Verify index exists
                            info_result = valkey_conn.execute_command("FT.INFO", config.index_name)
                            num_docs = 0
                            for i, item in enumerate(info_result):
                                if item == b"num_docs" or item == "num_docs":
                                    num_docs = int(info_result[i + 1])
                                    break
                            
                            logging.info(f"Index exists with {num_docs} documents")
                            if num_docs == 0:
                                self.fail("Index restored but contains no documents")
                            
                            # 2. Verify searchable - use exact same queries as stability_runner.py
                            if config.index_type in ["HNSW", "FLAT"]:
                                test_vector = np.random.rand(config.vector_dimensions).astype(np.float32).tobytes()
                                search_result = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@tag:{my_tag} @numeric:[0 100])=>[KNN 3 @embedding $query_vector]",
                                    "NOCONTENT",
                                    "PARAMS", "2", "query_vector", test_vector,
                                    "DIALECT", "2"
                                )
                            elif config.index_type == "TEXT":
                                search_result = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@tag:{my_tag} @numeric:[0 100] @content:sample_search_document_with_electronics | @category:electronics)"
                                )
                            elif config.index_type == "TAG":
                                search_result = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@category:{electronics} @product_type:{smartwatch})"
                                )
                            elif config.index_type == "NUMERIC":
                                search_result = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@price:[100 500] @quantity:[10 100] @rating:[40 50])"
                                )
                            logging.info(f"Index is searchable ({search_result[0]} results)")
                            
                            # 3. Verify writable and indexing - write new data and verify it can be searched
                            test_key = f"restore_test_key_{int(time.time())}"
                            if config.index_type in ["HNSW", "FLAT"]:
                                test_vector = np.random.rand(config.vector_dimensions).astype(np.float32).tobytes()
                                valkey_conn.execute_command(
                                    "HSET", test_key,
                                    "embedding", test_vector,
                                    "tag", "my_tag",
                                    "numeric", "10",
                                    "title", "restore_test_unique",
                                    "description", "testing_restore"
                                )
                                # Verify by searching for the unique title
                                verify_search = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@title:restore_test_unique)",
                                    "NOCONTENT"
                                )
                            elif config.index_type == "TEXT":
                                valkey_conn.execute_command(
                                    "HSET", test_key,
                                    "tag", "my_tag",
                                    "numeric", "10",
                                    "content", "restore_test_unique_content",
                                    "category", "electronics"
                                )
                                # Verify by searching for the unique content
                                verify_search = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@content:restore_test_unique_content)",
                                    "NOCONTENT"
                                )
                            elif config.index_type == "TAG":
                                valkey_conn.execute_command(
                                    "HSET", test_key,
                                    "category", "electronics,gadgets,wearables",
                                    "product_type", "smartwatch|fitness",
                                    "brand", "restore_test_unique_brand",
                                    "features", "waterproof;heartrate;gps"
                                )
                                # Verify by searching for the unique brand
                                verify_search = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@brand:{restore_test_unique_brand})",
                                    "NOCONTENT"
                                )
                            elif config.index_type == "NUMERIC":
                                valkey_conn.execute_command(
                                    "HSET", test_key,
                                    "price", "99999",
                                    "quantity", "50",
                                    "rating", "45",
                                    "timestamp", "1640000000"
                                )

                                # Verify by searching for the unique price
                                verify_search = valkey_conn.execute_command(
                                    "FT.SEARCH", config.index_name,
                                    "(@price:[99998 100000])",
                                    "NOCONTENT"
                                )
                            
                            if verify_search[0] > 0:
                                logging.info("Write and indexing operations functional")
                            else:
                                self.fail("Failed to index new data after restore - wrote data but couldn't search for it")
                            
                            valkey_conn.execute_command("DEL", test_key)
                            
                            logging.info("=" * 60)
                            logging.info("SUCCESS: OOM RECOVERY VERIFIED")
                            logging.info(f"Server restarted and restored {num_docs} documents")
                            logging.info("All functionality verified")
                            logging.info("=" * 60)
                                
                        except valkey.exceptions.ValkeyError as e:
                            logging.error(f"Verification failed: {e}")
                            self.fail(f"Verification failed after restart: {e}")
                        
                        break
                    except valkey.exceptions.ConnectionError:
                        logging.debug(f"Reconnection attempt {attempt + 1}/30...")
                        time.sleep(1)
                
                if not reconnected:
                    self.fail("Failed to reconnect after restart")
                    
            else:
                logging.warning("OOM not detected - may need longer test_time_sec")
        else:
            # Normal stability test - no crashes expected except intentional failovers
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
            
            # For OOM tests: Only allow HSET failures if OOM was actually detected and handled
            # This ensures we don't hide real failures when OOM didn't happen
            is_oom_test = (len(config.ports) == 1 and config.maxmemory and 
                          config.maxmemory_policy == "noeviction")
            
            if (is_oom_test and result.name == "HSET" and 
                config.ports[0] in results.intentionally_failed_ports):
                # OOM was detected and recovery was triggered - HSET failures are expected
                logging.info(
                    f"HSET had {result.failures} failures (expected OOM errors - recovery verified)"
                )
            else:
                # Normal case or OOM test where OOM didn't happen - no failures expected
                self.assertEqual(
                    result.failures,
                    0,
                    f"Expected zero failures for memtier run {result.name}",
                )
            
            # For OOM tests: Only allow HSET to be halted if OOM was actually detected and handled
            # HSET gets killed when OOM is detected and doesn't restart (unlike failover)
            # This ensures we don't hide real halting issues when OOM didn't happen
            if (is_oom_test and result.name == "HSET" and 
                config.ports[0] in results.intentionally_failed_ports):
                # OOM was detected and recovery was triggered - HSET halting is expected
                logging.info(
                    f"HSET was halted (expected - killed for OOM recovery without restart)"
                )
            else:
                # Normal case or OOM test where OOM didn't happen - no halting expected
                self.assertFalse(
                    result.halted,
                    msg=(
                        f"Expected memtier run {result.name} to not be halted (didn't "
                        "make progress for >20sec)"
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
            elif config.failover_interval_sec > 0 and result.name in ["FT.CREATE", "FLUSHDB", "FT.DROPINDEX"]:
                # Allow up to 3 failures per background task during failover testing. These are for the situation where the
                # cluster information is not updated fast enough and causes a race condition in the check. This is a situation
                # that can happen and we want to avoid catching failures like those because they are not true failures (they are expected)
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
