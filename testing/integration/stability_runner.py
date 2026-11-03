"""ValkeyQuery stability test core."""

import logging
import os
import sys
import threading
import time
from typing import NamedTuple
import valkey
import utils


# How long to leave memtier alone before the first failover. memtier's
# --connection-stage-timeout aborts the process (exit code 2) if connections
# fail before it reaches steady state, so a failover must not land during
# start-up.
MEMTIER_STARTUP_GRACE_SEC = 20

# Upper bound on how much paused time is added back to the test_timeout budget,
# so a failover stuck with in_progress set cannot extend the deadline forever.
MAX_PAUSE_COMPENSATION_SEC = 300

# How long a single pause may last before the poll loop force-resumes memtier.
MAX_SINGLE_PAUSE_SEC = 120


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
    # Unhandled exceptions raised by the task. Always a defect, and checked
    # separately from `failures` because some tasks tolerate failures.
    crashes: int = 0


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
      failover_state: Shared state for coordinating process pausing during failover
    """

    def __init__(self, config: StabilityTestConfig, cluster=None):
        self.config = config
        # Cluster under test, so nodes restarted by failover recovery come back
        # with the same server and module args the cluster was created with.
        self.cluster = cluster
        # Background tasks and memtier processes, tracked so cleanup() can
        # release them if run() raises before its own cleanup.
        self._threads = []
        self._processes = []
        # Shared state for failover coordination
        self.failover_state = {
            'in_progress': False,
            'failed_ports': set(),  # Ports that are currently down due to failover
            'new_primary_connected': False,  # Whether the new primary is fully operational
            # Monotonic timestamp of the last failover state change. Used to
            # recognise the window "during and shortly after a failover", in
            # which the module legitimately reports that it cannot reach every
            # cluster member. See utils.failover_recently_active().
            'last_failover_activity': 0.0,
            'lock': threading.Lock(),
        }
        # Memtier pause bookkeeping. Touched from two threads: the failover task
        # pauses via the before_shutdown hook, and the poll loop resumes once
        # failover_state['in_progress'] clears, so the lock is what keeps the
        # "since" timestamp and the running total consistent.
        self._memtier_pause_lock = threading.Lock()
        self._memtier_paused_since: float | None = None
        self._memtier_paused_total = 0.0
        logging.basicConfig(
            handlers=[
                logging.StreamHandler(stream=sys.stdout),
            ],
            level="DEBUG",
            format=(
                "%(asctime)s [%(levelname)s] (%(name)s) %(funcName)s: %(message)s"
            ),
        )

    def pause_memtier(self, reason: str) -> None:
        """Freeze every memtier process. Idempotent, safe from any thread.

        Wired into periodic_failover as before_shutdown so the load is quiet
        before the primary is shut down rather than a poll interval afterwards.
        """
        with self._memtier_pause_lock:
            if self._memtier_paused_since is not None:
                return
            paused = [p.name for p in self._processes if p.pause()]
            self._memtier_paused_since = time.time()
        logging.info(
            "Pausing memtier (%s): %s", reason, ", ".join(paused) or "none"
        )

    def resume_memtier(self, reason: str) -> None:
        """Let every paused memtier process continue. Idempotent."""
        with self._memtier_pause_lock:
            if self._memtier_paused_since is None:
                return
            resumed = [p.name for p in self._processes if p.resume()]
            paused_for = time.time() - self._memtier_paused_since
            self._memtier_paused_since = None
            self._memtier_paused_total += paused_for
        logging.info(
            "Resuming memtier after %.1fs (%s): %s",
            paused_for,
            reason,
            ", ".join(resumed) or "none",
        )

    def memtier_paused(self) -> bool:
        with self._memtier_pause_lock:
            return self._memtier_paused_since is not None

    def current_pause_seconds(self) -> float:
        """How long the current pause has lasted, or 0 if not paused."""
        with self._memtier_pause_lock:
            if self._memtier_paused_since is None:
                return 0.0
            return time.time() - self._memtier_paused_since

    def paused_seconds(self) -> float:
        """Total time memtier has spent frozen, including any current pause."""
        with self._memtier_pause_lock:
            total = self._memtier_paused_total
            if self._memtier_paused_since is not None:
                total += time.time() - self._memtier_paused_since
        return min(total, MAX_PAUSE_COMPENSATION_SEC)

    def cleanup(self):
        """Stop background tasks and release memtier resources.

        Idempotent, so tearDown can call it even after run() cleaned up. Needed
        because run() raising skips its own cleanup.
        """
        for thread in self._threads:
            try:
                thread.stop()
            except Exception as e:  # pylint: disable=broad-except
                logging.warning("Failed to stop task %s: %s", thread.name, e)
        for process in self._processes:
            try:
                process.close()
            except Exception as e:  # pylint: disable=broad-except
                logging.warning(
                    "Failed to close memtier %s: %s", process.name, e
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

        # Create index attributes based on index type
        if self.config.index_type == "HNSW":
            attributes = {
                "tag": utils.TagDefinition(),
                "numeric": utils.NumericDefinition(),
                "title": utils.TextDefinition(nostem=False),
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
                "title": utils.TextDefinition(nostem=False, with_suffix_trie=True),
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
        
        # Setup reuses the background tasks' create/drop path, which also seeds
        # the per-node index state they rely on. Every node starts as unknown:
        # replicas are never targeted but do hold the index (it is replicated),
        # so seeding them as "absent" would turn a promoted replica's correct
        # "already exists" into a false failure.
        index_state = utils.IndexState(
            index_lock=threading.Lock(),
            ft_created=False,
        )

        # Clean up an index left over from an earlier run. A "not found" reply is
        # expected on a fresh cluster and is not treated as a failure.
        utils.periodic_ftdrop_task(
            client,
            self.config.index_name,
            index_state,
            use_coordinator=self.config.use_coordinator,
        )

        if not utils.periodic_ftcreate_task(
            client,
            self.config.index_name,
            attributes,
            index_state,
            use_coordinator=self.config.use_coordinator,
        ):
            logging.error("Unable to create the index on the cluster")
            return StabilityRunResult(
                successful_run=False,
                memtier_results=[],
                background_task_results=[],
                intentionally_failed_ports=set(),
            )

        threads: list[utils.RandomIntervalTask] = []
        # Every background task sends its commands to the whole cluster. They get
        # failover_state for two reasons: RandomIntervalTask uses it to pause
        # while a failover is running, and the tasks themselves use it to leave
        # the node that is down out of the fan-out until it has rejoined.
        if self.config.bgsave_interval_sec != 0:
            task = utils.RandomIntervalTask(
                "BGSAVE",
                self.config.bgsave_interval_sec,
                self.config.randomize_bg_job_intervals,
                lambda: utils.periodic_bgsave_task(
                    client, self.failover_state
                ),
                failover_state=self.failover_state,
            )
            task.run()
            threads.append(task)

        if self.config.ftcreate_interval_sec != 0:
            task = utils.RandomIntervalTask(
                "FT.CREATE",
                self.config.ftcreate_interval_sec,
                self.config.randomize_bg_job_intervals,
                lambda: utils.periodic_ftcreate_task(
                    client,
                    self.config.index_name,
                    attributes,
                    index_state,
                    self.failover_state,
                    use_coordinator=self.config.use_coordinator,
                ),
                failover_state=self.failover_state,
            )
            task.run()
            threads.append(task)

        if self.config.ftdropindex_interval_sec != 0:
            task = utils.RandomIntervalTask(
                "FT.DROPINDEX",
                self.config.ftdropindex_interval_sec,
                self.config.randomize_bg_job_intervals,
                lambda: utils.periodic_ftdrop_task(
                    client,
                    self.config.index_name,
                    index_state,
                    self.failover_state,
                    use_coordinator=self.config.use_coordinator,
                ),
                failover_state=self.failover_state,
            )
            task.run()
            threads.append(task)

        if self.config.flushdb_interval_sec != 0:
            task = utils.RandomIntervalTask(
                "FLUSHDB",
                self.config.flushdb_interval_sec,
                self.config.randomize_bg_job_intervals,
                lambda: utils.periodic_flushdb_task(
                    client,
                    index_state,
                    self.config.use_coordinator,
                    self.failover_state,
                ),
                failover_state=self.failover_state,
            )
            task.run()
            threads.append(task)

        # Start failover testing if configured and replicas exist
        if self.config.failover_interval_sec != 0 and self.config.replica_count > 0:
            valkey_server_path = os.environ["VALKEY_SERVER_PATH"]
            config_dir = os.environ["TEST_TMPDIR"]
            stdout_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]
            
            # Fallback only: restart_node prefers cluster.node_modules, so a
            # restarted node loads the module the same way its peers did.
            modules = {}
            if "VALKEY_SEARCH_PATH" in os.environ:
                modules[os.environ["VALKEY_SEARCH_PATH"]] = (
                    "--reader-threads 2 --writer-threads 5 --log-level notice"
                    + (" --use-coordinator" if self.config.use_coordinator else "")
                )
            
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
                    modules=modules,
                    test_recovery=self.config.test_failover_recovery,
                    failover_state=self.failover_state,
                    entry_point_port=self.config.ports[0],  # Protect entry point from failover
                    cluster=self.cluster,
                    # Let memtier finish connecting before the first failover.
                    initial_delay_sec=MEMTIER_STARTUP_GRACE_SEC,
                    # Quiesce the load before the node is shut down. The
                    # processes are spawned later, so self._processes is read at
                    # call time.
                    before_shutdown=lambda: self.pause_memtier(
                        "failover about to shut a node down"
                    ),
                )
            )
        elif self.config.failover_interval_sec != 0 and self.config.replica_count == 0:
            logging.warning(
                "Failover testing requested but replica_count=0 - skipping failover"
            )

        memtier_output_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]

        # For failover testing the memtier processes are paused (SIGSTOP) for the
        # duration of the failover and resumed afterwards, so each one keeps its
        # counters, its position in the key range and its connections. See the
        # poll loop below.
        # Build HSET command based on index type
        if self.config.index_type in ["HNSW", "FLAT"]:
            # Vector-based index: include embedding field with text fields
            hset_fields = "embedding __data__ tag my_tag numeric 10 title __data__ description __data__"
        elif self.config.index_type == "TEXT":
            # Text-only index: Multiple document types with extensive content for diverse search results
            # Document 1: Multiple prefix words, fuzzy terms, comprehensive content
            hset_fields_1 = 'tag my_tag numeric 10 content "The quick brown fox jumps over fuzzy lazy dogs in a fuzzy meadow. Fuzzy search algorithms help find fuzzy matches in text. Understanding fuzzy logic requires fuzzy thinking and fuzzy concepts." title "prefix_smartwatch wearable prefix_device prefix_tracker"'
            # Document 2: Multiple device endings, fuzzy terms, rich content
            hset_fields_2 = 'tag my_tag numeric 15 content "Amazing fuzzy search capabilities enable fuzzy matching. Modern fuzzy systems use fuzzy logic for fuzzy results. Implementing fuzzy algorithms creates fuzzy patterns for better fuzzy detection." title "electronic device medical device smart device mobile device"'
            # Document 3: Exact smartwatch matches, fuzzy variations, detailed text
            hset_fields_3 = 'tag my_tag numeric 20 content "Fuzziness detection in text using fuzzy methods. Fuzzy matching improves fuzzy search results. Advanced fuzzy techniques enhance fuzzy precision and fuzzy recall in fuzzy systems." title "smartwatch fitness smartwatch luxury smartwatch"'
            # Document 4: Multiple prefix terms, fuzzy content, extensive documentation
            hset_fields_4 = 'tag my_tag numeric 25 content "Search through fuzzy matching algorithms with fuzzy scoring. Fuzzy search implementations use fuzzy distance metrics. Understanding fuzzy boundaries helps optimize fuzzy performance in fuzzy applications." title "prefix_electronics guide prefix_computing manual prefix_technology documentation"'
            # Document 5: Random data using __data__ for content and title fields
            hset_fields_5 = 'tag my_tag numeric 30 content __data__ title __data__'
        elif self.config.index_type == "TAG":
            # Tag-only index: multiple tag fields with different separators
            hset_fields = 'category electronics,gadgets,wearables product_type smartwatch|fitness brand apple,premium features waterproof;heartrate;gps'
        elif self.config.index_type == "NUMERIC":
            # Numeric-only index: multiple numeric fields with positive integer values
            hset_fields = 'price 299 quantity 50 rating 45 timestamp 1640000000'
        
        if self.config.index_type == "TEXT":
            # Multiple HSET commands for TEXT indexes with different documents
            insert_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_memtier_clients}"
                " --random-data"
                " -d 100"
                " --command='HSET __key__ "
                f"{hset_fields_1}'"
                " --command-ratio=1"
                " --command-key-pattern=P"
                " --command='HSET __key__ "
                f"{hset_fields_2}'"
                " --command-ratio=1"
                " --command-key-pattern=P"
                " --command='HSET __key__ "
                f"{hset_fields_3}'"
                " --command-ratio=1"
                " --command-key-pattern=P"
                " --command='HSET __key__ "
                f"{hset_fields_4}'"
                " --command-ratio=1"
                " --command-key-pattern=P"
                " --command='HSET __key__ "
                f"{hset_fields_5}'"
                " --command-ratio=1"
                " --command-key-pattern=P"
                " --pipeline=1"
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
                " -"
                f" --command='HSET __key__ {hset_fields}'"
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

        # Build search query based on index type
        if self.config.index_type == "TEXT":
            # Text search - Multiple search types
            # 1. Prefix wildcard: matches words starting with "prefix"
            # 2. Fuzzy search: matches words similar to "fuzzy" (using % for edit distance)
            # 3. Suffix wildcard: matches words ending with "device"
            # 4. Exact match: matches exact word "smartwatch"
            # 5. Phrase search with SLOP 0 INORDER: exact ordered phrase match
            # 6. SLOP without order: matches "systems", "matching", "enable" within SLOP 3, any order
            search_command = (
                f"{self.config.memtier_path}"
                " --cluster-mode"
                " -s localhost"
                f" -p {self.config.ports[0]}"
                f" -t {self.config.num_memtier_threads}"
                f" -c {self.config.num_search_clients}"
                f" --command='FT.SEARCH {self.config.index_name} \"@title:prefix*\"'"
                " --command-ratio=1"
                f" --command='FT.SEARCH {self.config.index_name} \"@content:%fuzzy%\"'"
                " --command-ratio=1"
                f" --command='FT.SEARCH {self.config.index_name} \"@title:*device\"'"
                " --command-ratio=1"
                f" --command='FT.SEARCH {self.config.index_name} \"@title:smartwatch\"'"
                " --command-ratio=1"
                f" --command='FT.SEARCH {self.config.index_name} \"@title:\\\"fitness smartwatch\\\"\" SLOP 0 INORDER'"
                " --command-ratio=1"
                f" --command='FT.SEARCH {self.config.index_name} \"@content:systems matching enable\" SLOP 3'"
                " --command-ratio=1"
                " --pipeline=1"
                " --json-out-file"
                f" {memtier_output_dir}/{self.config.index_name}_memtier_search.json"
            )
        else:
            # Vector KNN search
            # Tag search - exact match on multiple tag fields
            # Numeric range search - multiple numeric range filters
            if self.config.index_type in ["HNSW", "FLAT"]:
                search_query = '"(@tag:{my_tag} @numeric:[0 100])=>[KNN 3 @embedding $query_vector]" NOCONTENT PARAMS 2 "query_vector" __data__ DIALECT 2'
            elif self.config.index_type == "TAG":
                search_query = '"(@category:{electronics} @product_type:{smartwatch})"'
            else:  # NUMERIC
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
            f" -d {self.config.vector_dimensions*4}"
            " --json-out-file"
            f" {memtier_output_dir}/{self.config.index_name}_memtier_ftlist.json"
        )

        # How long each process runs, applied uniformly to all six.
        #
        # request_count is what makes pausing safe. --test-time is a wall-clock
        # deadline, so time spent stopped by SIGSTOP is deducted from the test.
        # -n counts requests, so a paused run continues where it left off.
        if self.config.insertion_mode == "request_count":
            requests_per_client = int(
                self.config.keyspace_size
                / self.config.num_memtier_clients
                / self.config.num_memtier_threads
            )
            logging.debug("%d requests per client", requests_per_client)
            duration_args = f" -n {requests_per_client}"
        elif self.config.insertion_mode == "time_interval":
            duration_args = f" --test-time {self.config.test_time_sec}"
        else:
            raise ValueError(
                f"Unknown insertion mode: {self.config.insertion_mode}"
            )

        # Deliberately NO --reconnect-on-error. On memtier 2.3.0 the reconnect
        # path trips an assert in cluster_client::connect()
        # (`m_connections.size() == m_key_index_pools.size()`) and aborts the
        # process. Newer memtier fixes this, so it can come back once the
        # memtier used here includes that fix. Until then connections to the
        # failed node stay down for the rest of the run.
        insert_command += duration_args
        delete_command += duration_args
        expire_command += duration_args
        search_command += duration_args
        ft_info_command += duration_args
        ft_list_command += duration_args

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

        # Published so cleanup() can release them if run() raises.
        self._threads = threads
        self._processes = processes

        timeout_start = time.time()

        # The deadline is pushed out by however long the processes were frozen, so
        # a failover does not consume the budget meant for the test itself.
        while (
            time.time() - timeout_start - self.paused_seconds()
            < self.config.test_timeout
        ):
            # Check if failover is in progress
            with self.failover_state['lock']:
                failover_in_progress = self.failover_state['in_progress']

            if failover_in_progress:
                # Normally already paused by periodic_failover's before_shutdown
                # hook; this is the fallback and is a no-op otherwise. SIGSTOP
                # keeps memtier's counters, key position and connections intact.
                self.pause_memtier("failover in progress")

                paused_for = self.current_pause_seconds()
                if paused_for > MAX_SINGLE_PAUSE_SEC:
                    logging.error(
                        "Memtier has been paused for %.0fs (limit %ds) and"
                        " failover still reports in_progress - resuming anyway",
                        paused_for,
                        MAX_SINGLE_PAUSE_SEC,
                    )
                    self.resume_memtier("pause exceeded its limit")
            else:
                self.resume_memtier("failover complete")

            currently_paused = self.memtier_paused()

            if all(p.done for p in processes):
                logging.info("---===All processes finished===---")
                break

            for process in processes:
                # Drain the pipes even while paused, but skip print_status: it
                # marks a process done, which a stopped process is not.
                process.process_logs()
                if not currently_paused:
                    process.print_status()
            time.sleep(1)
        else:
            # For request_count runs this is the normal end of the run:
            # test_timeout (plus paused time) sets the run length and
            # test_time_sec is not used. time_interval runs should end on their
            # own through --test-time, so reaching this there means something
            # stalled. Either way, pass/fail comes from the failure, halted and
            # total_ops checks in the test.
            log = (
                logging.info
                if self.config.insertion_mode == "request_count"
                else logging.error
            )
            log("Reached test_timeout with processes still running, ending the run")
            for process in processes:
                if not process.done:
                    # Read what was printed since the last poll, so errors
                    # from the final second are still counted.
                    process.process_logs()
                    process.close()

        for thread in threads:
            thread.stop()

        # Snapshot AFTER stopping: once stop() returns no new failover can start,
        # so no intentionally shut down port can be missing from this set.
        intentionally_failed_ports = set()
        for thread in threads:
            if thread.name == "FAILOVER":
                intentionally_failed_ports = thread.failed_ports.copy()
                logging.info("Collected intentionally failed ports: %s", intentionally_failed_ports)
                break

        # Release the memtier pipes and reap the processes. close() does not
        # drain the pipes, so the stats below are what the poll loop observed.
        for process in processes:
            process.close()

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
                    crashes=thread.crashes,
                )
                for thread in threads
            ],
            intentionally_failed_ports=intentionally_failed_ports,
        )
