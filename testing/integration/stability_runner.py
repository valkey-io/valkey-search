"""ValkeyQuery stability test core."""

import logging
import os
import sys
import threading
import time
from typing import NamedTuple
import valkey
import utils


class MemtierProcessRunResult(NamedTuple):
    """Results for a single memtier process run."""

    name: str
    total_ops: int
    failures: int
    halted: bool
    runtime: float


class BackgroundTaskRunResult(NamedTuple):
    """Results for a single background thread run."""

    name: str
    total_ops: int
    failures: int


class StabilityRunResult(NamedTuple):
    """Results for a single stability test run."""

    # False if the test was unable to be performed.
    successful_run: bool
    memtier_results: list[MemtierProcessRunResult]
    background_task_results: list[BackgroundTaskRunResult]
    intentionally_failed_ports: set  # Ports that were intentionally shut down during failover


class StabilityTestConfig(NamedTuple):
    """Configuration for a single stability test run."""

    index_name: str
    ports: tuple[int, ...]
    index_type: str
    vector_dimensions: int
    bgsave_interval_sec: int
    ftcreate_interval_sec: int
    ftdropindex_interval_sec: int
    flushdb_interval_sec: int
    randomize_bg_job_intervals: bool
    num_memtier_threads: int
    num_memtier_clients: int
    num_search_clients: int
    insertion_mode: str
    test_time_sec: int
    test_timeout: int
    keyspace_size: int
    use_coordinator: bool
    replica_count: int
    repl_diskless_load: str
    memtier_path: str = ""
    failover_interval_sec: int = 0  # 0 means no failover testing
    test_failover_recovery: bool = True  # Whether to test node recovery after failover


class StabilityRunner:
    """Stability test runner.

    Attributes:
      config: The configuration for the test.
    """

    def __init__(self, config: StabilityTestConfig):
        self.config = config
        logging.basicConfig(
            handlers=[
                logging.StreamHandler(stream=sys.stdout),
            ],
            level="DEBUG",
            format=(
                "%(asctime)s [%(levelname)s] (%(name)s) %(funcName)s: %(message)s"
            ),
        )

    def run(self) -> StabilityRunResult:
        """Runs the stability test, sending memtier commands and running background threads that perform valkey operations.

        Returns:

        Raises:
          ValueError:
        """
        try:
            client = valkey.ValkeyCluster(
                host="localhost",
                port=self.config.ports[0],
                startup_nodes=[
                    valkey.cluster.ClusterNode("localhost", port)
                    for port in self.config.ports
                ],
                require_full_coverage=True,
                socket_timeout=10,
            )
        except valkey.exceptions.ConnectionError as e:
            logging.error("Unable to connect to valkey, %s", e)
            return StabilityRunResult(
                successful_run=False,
                memtier_results=[],
                background_task_results=[],
                intentionally_failed_ports=set(),
            )

        try:
            utils.drop_index(client=client, index_name=self.config.index_name)
        except valkey.exceptions.ValkeyError:
            pass

        attributes = {
            "tag": utils.TagDefinition(),
            "numeric": utils.NumericDefinition(),
        }
        if self.config.index_type == "HNSW":
            attributes.update({
                "embedding": utils.HNSWVectorDefinition(
                    vector_dimensions=self.config.vector_dimensions
                )
            })
        else:
            attributes.update({
                "embedding": utils.FlatVectorDefinition(
                    vector_dimensions=self.config.vector_dimensions
                ),
            })
        utils.create_index(
            client=client,
            index_name=self.config.index_name,
            store_data_type=utils.StoreDataType.HASH.name,
            attributes=attributes,
        )

        threads: list[utils.RandomIntervalTask] = []
        index_state = utils.IndexState(
            index_lock=threading.Lock(), ft_created=True
        )
        if self.config.bgsave_interval_sec != 0:
            threads.append(
                utils.periodic_bgsave(
                    client,
                    self.config.bgsave_interval_sec,
                    self.config.randomize_bg_job_intervals,
                )
            )

        if self.config.ftcreate_interval_sec != 0:
            threads.append(
                utils.periodic_ftcreate(
                    client,
                    self.config.ftcreate_interval_sec,
                    self.config.randomize_bg_job_intervals,
                    self.config.index_name,
                    attributes,
                    index_state,
                )
            )

        if self.config.ftdropindex_interval_sec != 0:
            threads.append(
                utils.periodic_ftdrop(
                    client,
                    self.config.ftdropindex_interval_sec,
                    self.config.randomize_bg_job_intervals,
                    self.config.index_name,
                    index_state,
                )
            )

        if self.config.flushdb_interval_sec != 0:
            threads.append(
                utils.periodic_flushdb(
                    client,
                    self.config.flushdb_interval_sec,
                    self.config.randomize_bg_job_intervals,
                    index_state,
                    self.config.use_coordinator,
                )
            )

        # Start failover testing if configured and replicas exist
        if self.config.failover_interval_sec != 0 and self.config.replica_count > 0:
            valkey_server_path = os.environ["VALKEY_SERVER_PATH"]
            config_dir = os.environ["TEST_TMPDIR"]
            stdout_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]
            
            logging.info(
                "Failover testing enabled: interval=%ds, recovery=%s",
                self.config.failover_interval_sec,
                self.config.test_failover_recovery
            )
            
            threads.append(
                utils.periodic_failover(
                    client=client,
                    interval_sec=self.config.failover_interval_sec,
                    randomize=self.config.randomize_bg_job_intervals,
                    valkey_server_path=valkey_server_path,
                    config_dir=config_dir,
                    stdout_dir=stdout_dir,
                    test_recovery=self.config.test_failover_recovery,
                )
            )
        elif self.config.failover_interval_sec != 0 and self.config.replica_count == 0:
            logging.warning(
                "Failover testing requested but replica_count=0 - skipping failover"
            )

        memtier_output_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]

        insert_command = (
            f"{self.config.memtier_path}"
            " --cluster-mode"
            " -s localhost"
            f" -p {self.config.ports[0]}"
            f" -t {self.config.num_memtier_threads}"
            f" -c {self.config.num_memtier_clients}"
            " --random-data"
            " -"
            " --command='HSET __key__ embedding __data__ tag my_tag numeric 10'"
            " --command-key-pattern=P"
            f" -d {self.config.vector_dimensions*4}"
            " --json-out-file"
            f" {memtier_output_dir}/{self.config.index_name}_memtier_insert.json"
        )
        delete_command = (
            f"{self.config.memtier_path}"
            " --cluster-mode"
            " -s localhost"
            f" -p {self.config.ports[0]}"
            f" -t {self.config.num_memtier_threads}"
            f" -c {self.config.num_memtier_clients}"
            " --random-data"
            " -"
            " --command='DEL __key__'"
            " --command-key-pattern=P"
            f" -d {self.config.vector_dimensions*4}"
            " --json-out-file"
            f" {memtier_output_dir}/{self.config.index_name}_memtier_del.json"
        )
        expire_command = (
            f"{self.config.memtier_path}"
            " --cluster-mode"
            " -s localhost"
            f" -p {self.config.ports[0]}"
            f" -t {self.config.num_memtier_threads}"
            f" -c {self.config.num_memtier_clients}"
            " --random-data"
            " -"
            " --command='EXPIRE __key__ 1'"
            " --command-key-pattern=P"
            f" -d {self.config.vector_dimensions*4}"
            " --json-out-file"
            f" {memtier_output_dir}/{self.config.index_name}_memtier_expire.json"
        )

        if self.config.insertion_mode == "request_count":
            keys_per_client = int(
                self.config.keyspace_size
                / self.config.num_memtier_clients
                / self.config.num_memtier_threads
            )
            logging.debug("%d keys per client needed", keys_per_client)
            insert_command += f" -n {keys_per_client}"
            delete_command += f" -n {keys_per_client}"
            expire_command += f" -n {keys_per_client}"
        elif self.config.insertion_mode == "time_interval":
            insert_command += f" --test-time {self.config.test_time_sec}"
            delete_command += f" --test-time {self.config.test_time_sec}"
            expire_command += f" --test-time {self.config.test_time_sec}"
        else:
            raise ValueError(
                f"Unknown insertion mode: {self.config.insertion_mode}"
            )

        search_command = (
            f"{self.config.memtier_path}"
            " --cluster-mode"
            " -s localhost"
            f" -p {self.config.ports[0]}"
            f" -t {self.config.num_memtier_threads}"
            f" -c {self.config.num_search_clients}"
            " -"
            " --command='FT.SEARCH"
            f" {self.config.index_name} "
            '"(@tag:{my_tag} @numeric:[0 100])=>[KNN 3 @embedding $query_vector]"'
            ' NOCONTENT PARAMS 2 "query_vector" __data__ DIALECT 2\' '
            f" --test-time={self.config.test_time_sec}"
            f" -d {self.config.vector_dimensions*4}"
            " --json-out-file"
            f" {memtier_output_dir}/{self.config.index_name}_memtier_search.json"
        )

        ft_info_command = (
            f"{self.config.memtier_path}"
            " --cluster-mode"
            " -s localhost"
            f" -p {self.config.ports[0]}"
            f" -t {self.config.num_memtier_threads}"
            f" -c {self.config.num_search_clients}"
            " -"
            f" --command='FT.INFO {self.config.index_name}'"
            f" --test-time={self.config.test_time_sec}"
            f" -d {self.config.vector_dimensions*4}"
            f" --json-out-file"
            f" {memtier_output_dir}/{self.config.index_name}_memtier_ftinfo.json"
        )

        ft_list_command = (
            f"{self.config.memtier_path}"
            " --cluster-mode"
            " -s localhost"
            f" -p {self.config.ports[0]}"
            f" -t {self.config.num_memtier_threads}"
            f" -c {self.config.num_search_clients}"
            " -"
            " --command='FT._LIST'"
            f" --test-time={self.config.test_time_sec}"
            f" -d {self.config.vector_dimensions*4}"
            " --json-out-file"
            f" {memtier_output_dir}/{self.config.index_name}_memtier_ftlist.json"
        )

        logging.debug("insert_command: %s", insert_command)
        logging.debug("delete_command: %s", delete_command)
        logging.debug("expire_command: %s", expire_command)
        logging.debug("search_command: %s", search_command)
        logging.debug("ft_info_command: %s", ft_info_command)
        logging.debug("ft_list_command: %s", ft_list_command)

        processes: list[utils.MemtierProcess] = []
        processes.append(
            utils.MemtierProcess(command=insert_command, name="HSET")
        )
        processes.append(
            utils.MemtierProcess(command=delete_command, name="DEL")
        )
        processes.append(
            utils.MemtierProcess(command=expire_command, name="EXPIRE")
        )
        processes.append(
            utils.MemtierProcess(
                command=search_command,
                name="FT.SEARCH",
                error_predicate=lambda err: err
                != f"-Index with name '{self.config.index_name}' not found",
            )
        )
        processes.append(
            utils.MemtierProcess(
                command=ft_info_command,
                name="FT.INFO",
                error_predicate=lambda err: err
                != f"-Index with name '{self.config.index_name}' not found",
            )
        )
        processes.append(
            utils.MemtierProcess(command=ft_list_command, name="FT._LIST")
        )

        timeout_start = time.time()
        while time.time() - timeout_start < self.config.test_timeout:
            if all(p.done for p in processes):
                logging.info("---===All processes finished===---")
                break
            for process in processes:
                process.process_logs()
                process.print_status()
            time.sleep(1)
        else:
            logging.error("Timed out waiting for processes to finish")
            logging.info("killing processes...")
            for process in processes:
                process.process.kill()
            logging.error("Processes killed")

        # Collect intentionally failed ports from failover task BEFORE stopping threads
        intentionally_failed_ports = set()
        for thread in threads:
            if thread.name == "FAILOVER":
                intentionally_failed_ports = thread.failed_ports.copy()
                logging.info("Collected intentionally failed ports: %s", intentionally_failed_ports)
                break
        
        for thread in threads:
            thread.stop()
        
        return StabilityRunResult(
            successful_run=True,
            memtier_results=[
                MemtierProcessRunResult(
                    name=process.name,
                    total_ops=process.total_ops,
                    failures=process.failures,
                    halted=process.halted,
                    runtime=process.runtime,
                )
                for process in processes
            ],
            background_task_results=[
                BackgroundTaskRunResult(
                    name=thread.name,
                    total_ops=thread.ops,
                    failures=thread.failures,
                )
                for thread in threads
            ],
            intentionally_failed_ports=intentionally_failed_ports,
        )
