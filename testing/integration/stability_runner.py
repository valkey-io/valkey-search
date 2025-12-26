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
            )

        # Drop existing index
        try:
            utils.drop_index(client=client, index_name=self.config.index_name)
        except valkey.exceptions.ValkeyError:
            pass

        # Create index attributes based on index type
        if self.config.index_type == "HNSW":
            attributes = {
                "tag": utils.TagDefinition(),
                "numeric": utils.NumericDefinition(),
                "title": utils.TextDefinition(nostem=True),
                "description": utils.TextDefinition(),
                "embedding": utils.HNSWVectorDefinition(
                    vector_dimensions=self.config.vector_dimensions
                )
            }
        elif self.config.index_type == "FLAT":
            attributes = {
                "tag": utils.TagDefinition(),
                "numeric": utils.NumericDefinition(),
                "title": utils.TextDefinition(nostem=True),
                "description": utils.TextDefinition(),
                "embedding": utils.FlatVectorDefinition(
                    vector_dimensions=self.config.vector_dimensions
                ),
            }
        elif self.config.index_type == "TEXT":
            attributes = {
                "tag": utils.TagDefinition(),
                "numeric": utils.NumericDefinition(),
                "content": utils.TextDefinition(),
                "category": utils.TextDefinition(nostem=True),
            }
        elif self.config.index_type == "TAG":
            attributes = {
                "category": utils.TagDefinition(separator=","),
                "product_type": utils.TagDefinition(separator="|"),
                "brand": utils.TagDefinition(separator=","),
                "features": utils.TagDefinition(separator=";"),
            }
        elif self.config.index_type == "NUMERIC":
            attributes = {
                "price": utils.NumericDefinition(),
                "quantity": utils.NumericDefinition(),
                "rating": utils.NumericDefinition(),
                "timestamp": utils.NumericDefinition(),
            }
        else:
            raise ValueError(f"Unknown index type: {self.config.index_type}")
        
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

        memtier_output_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]

        # Build HSET command based on index type
        if self.config.index_type in ["HNSW", "FLAT"]:
            # Vector-based index: include embedding field with text fields
            hset_fields = "embedding __data__ tag my_tag numeric 10 title __data__ description __data__"
        elif self.config.index_type == "TEXT":
            # Text-only index: use simple text without spaces to avoid quote escaping issues
            hset_fields = 'tag my_tag numeric 10 content sample_search_document_with_electronics category electronics'
        elif self.config.index_type == "TAG":
            # Tag-only index: multiple tag fields with different separators
            hset_fields = 'category electronics,gadgets,wearables product_type smartwatch|fitness brand apple,premium features waterproof;heartrate;gps'
        elif self.config.index_type == "NUMERIC":
            # Numeric-only index: multiple numeric fields with positive integer values
            hset_fields = 'price 299 quantity 50 rating 45 timestamp 1640000000'
        
        if self.config.index_type in ["TEXT", "TAG", "NUMERIC"]:
            insert_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_memtier_clients}"
                " --reconnect-on-error"
                " --max-reconnect-attempts=3"
                " --command='HSET __key__ "
                f"{hset_fields}'"
                " --command-key-pattern=P"
                " --json-out-file "
                f"{memtier_output_dir}/{self.config.index_name}_memtier_insert.json"
            )
        else:
            insert_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_memtier_clients}"
                " --random-data"
                " --reconnect-on-error"
                " --max-reconnect-attempts=3"
                " -"
                f" --command='HSET __key__ {hset_fields}'"
                " --command-key-pattern=P"
                f" -d {self.config.vector_dimensions*4}"
                " --json-out-file"
                f" {memtier_output_dir}/{self.config.index_name}_memtier_insert.json"
            )
        if self.config.index_type in ["TEXT", "TAG", "NUMERIC"]:
            delete_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_memtier_clients}"
                " --reconnect-on-error"
                " --max-reconnect-attempts=3"
                " -"
                " --command='DEL __key__'"
                " --command-key-pattern=P"
                " --json-out-file"
                f" {memtier_output_dir}/{self.config.index_name}_memtier_del.json"
            )
        else:
            delete_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_memtier_clients}"
                " --random-data"
                " --reconnect-on-error"
                " --max-reconnect-attempts=3"
                " -"
                " --command='DEL __key__'"
                " --command-key-pattern=P"
                f" -d {self.config.vector_dimensions*4}"
                " --json-out-file"
                f" {memtier_output_dir}/{self.config.index_name}_memtier_del.json"
            )
        if self.config.index_type in ["TEXT", "TAG", "NUMERIC"]:
            expire_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_memtier_clients}"
                " --reconnect-on-error"
                " --max-reconnect-attempts=3"
                " -"
                " --command='EXPIRE __key__ 1'"
                " --command-key-pattern=P"
                " --json-out-file"
                f" {memtier_output_dir}/{self.config.index_name}_memtier_expire.json"
            )
        else:
            expire_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_memtier_clients}"
                " --random-data"
                " --reconnect-on-error"
                " --max-reconnect-attempts=3"
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

        # Build search query based on index type
        if self.config.index_type in ["HNSW", "FLAT"]:
            # Vector KNN search
            search_query = '"(@tag:{my_tag} @numeric:[0 100])=>[KNN 3 @embedding $query_vector]" NOCONTENT PARAMS 2 "query_vector" __data__ DIALECT 2'
        elif self.config.index_type == "TEXT":
            # Text search - updated to match the new content format without spaces
            search_query = '"(@tag:{my_tag} @numeric:[0 100] @content:sample_search_document_with_electronics | @category:electronics)"'
        elif self.config.index_type == "TAG":
            # Tag search - exact match on multiple tag fields
            search_query = '"(@category:{electronics} @product_type:{smartwatch})"'
        elif self.config.index_type == "NUMERIC":
            # Numeric range search - multiple numeric range filters
            search_query = '"(@price:[100 500] @quantity:[10 100] @rating:[40 50])"'
        
        search_command = (
            f"{self.config.memtier_path}"
            " --cluster-mode"
            " -s localhost"
            f" -p {self.config.ports[0]}"
            f" -t {self.config.num_memtier_threads}"
            f" -c {self.config.num_search_clients}"
            " -"
            f" --command='FT.SEARCH {self.config.index_name} {search_query}'"
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
                error_predicate=lambda err: f"-Index with name '{self.config.index_name}' not found" not in err,
            )
        )
        processes.append(
            utils.MemtierProcess(
                command=ft_info_command,
                name="FT.INFO",
                error_predicate=lambda err: f"-Index with name '{self.config.index_name}' not found" not in err,
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
        )
