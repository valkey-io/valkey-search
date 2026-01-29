"""Utilities for ValkeySearch testing."""

from abc import abstractmethod
import fcntl
import logging
import os
import random
import re
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, NamedTuple, TextIO, Union
import json
import numpy as np
from enum import Enum
import valkey
import valkey.exceptions


class StoreDataType(Enum):
    HASH = 1
    JSON = 2


class VectorIndexType(Enum):
    FLAT = 1
    HNSW = 2


def to_str(val):
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8")
        except UnicodeDecodeError:
            return val.hex()  # fallback: show as hex
    return str(val)


class ValkeyServerUnderTest:
    def __init__(self, process_handle: subprocess.Popen[Any], port: int):
        self.process_handle = process_handle
        self.port = port

    def terminate(self):
        self.process_handle.terminate()

    def terminated(self):
        return self.process_handle.poll()

    def ping(self) -> Any:
        return valkey.Valkey(port=self.port).ping()


def start_valkey_process(
    valkey_server_path: str,
    port: int,
    directory: str,
    stdout_file: TextIO,
    args: dict[str, str],
    modules: dict[str, str],
    password: str | None = None,
) -> ValkeyServerUnderTest:
    command = f"{valkey_server_path} --port {port} --dir {directory}"
    modules_args = [f'"--loadmodule {k} {v}"' for k, v in modules.items()]
    args_str = " ".join([f"--{k} {v}" for k, v in args.items()] + modules_args)
    command += " --loadmodule " + os.environ["VALKEY_JSON_PATH"]
    command += " " + args_str
    command = "ulimit -c unlimited && " + command
    logging.info("Starting valkey process with command: %s", command)

    process = subprocess.Popen(
        command, shell=True, stdout=stdout_file, stderr=stdout_file
    )

    connected = False
    for i in range(10):
        logging.info(
            "Attempting to connect to Valkey @ port %d (try #%d)", port, i
        )
        try:
            valkey_conn = valkey.Valkey(
                host="localhost",
                port=port,
                password=password,
                socket_timeout=1000,
            )
            valkey_conn.ping()
            connected = True
            break
        except (
            valkey.exceptions.ConnectionError,
            valkey.exceptions.ResponseError,
            valkey.exceptions.TimeoutError,
        ):
            time.sleep(1)
    if not connected:
        raise valkey.exceptions.ConnectionError(
            f"Failed to connect to valkey server on port {port}"
        )
    logging.info("Attempting to connect to Valkey: OK")

    return ValkeyServerUnderTest(process, port)


class ValkeyClusterUnderTest:
    def __init__(self, servers: List[ValkeyServerUnderTest], stdout_files: List[TextIO] = None):
        self.servers = servers
        self.stdout_files = stdout_files or []

    def terminate(self):
        for server in self.servers:
            server.terminate()
        # Close all stdout files
        for stdout_file in self.stdout_files:
            try:
                stdout_file.close()
            except Exception as e:
                logging.warning("Failed to close stdout file: %s", e)

    def get_terminated_servers(self) -> List[int]:
        result = []
        for server in self.servers:
            if server.terminated():
                result.append(server.port)
        return result

    def ping_all(self):
        result = []
        for server in self.servers:
            result.append(server.ping())
        return result


def start_valkey_cluster(
    valkey_server_path: str,
    valkey_cli_path: str,
    ports: List[int],
    directory: str,
    stdout_directory: str,
    args: Dict[str, str],
    modules: Dict[str, str],
    replica_count: int = 0,
    password: str | None = None,
) -> Dict[int, subprocess.Popen[Any]]:
    """Starts a valkey cluster.

    Starts a valkey cluster with the given ports and arguments, with zero replicas.

    Args:
      valkey_server_path:
      valkey_cli_path:
      ports:
      directory:
      stdout_directory:
      args:

    Returns:
      Dictionary of port to valkey process.
    """
    cluster_args = dict(args)
    processes = []
    stdout_files = []

    for port in ports:
        stdout_path = os.path.join(stdout_directory, f"{port}_stdout.txt")
        # Open file without buffering - will be closed when cluster terminates
        stdout_file = open(stdout_path, "w", buffering=1)
        stdout_files.append(stdout_file)
        node_dir = os.path.join(directory, f"nodes{port}")
        cluster_args["cluster-enabled"] = "yes"
        cluster_args["cluster-config-file"] = os.path.join(
            node_dir, "nodes.conf"
        )
        cluster_args["cluster-node-timeout"] = "10000"
        os.mkdir(node_dir)
        processes.append(start_valkey_process(
            valkey_server_path,
            port,
            node_dir,
            stdout_file,
            cluster_args,
            modules,
            password,
        ))

    cli_stdout_path = os.path.join(stdout_directory, "valkey_cli_stdout.txt")
    # Close file after subprocess completes
    with open(cli_stdout_path, "w") as cli_stdout_file:
        valkey_cli_args = [valkey_cli_path, "--cluster-yes", "--cluster", "create"]
        for port in ports:
            valkey_cli_args.append(f"127.0.0.1:{port}")
        valkey_cli_args.extend(["--cluster-replicas", str(replica_count)])
        if password:
            valkey_cli_args.extend(["-a", password])

        logging.info("Creating valkey cluster with command: %s", valkey_cli_args)

        timeout = 60
        now = time.time()
        while time.time() - now < timeout:
            try:
                subprocess.run(
                    valkey_cli_args,
                    check=True,
                    stdout=cli_stdout_file,
                    stderr=cli_stdout_file,
                )
                break
            except subprocess.CalledProcessError:
                time.sleep(1)

    # This is also ugly, but we need to wait for the cluster to be ready. There
    # doesn't seem to be a way to do that with the valkey-server, since it seems to
    # be ready immediately, but returns an CLUSTERDOWN error when we try to search
    # too early, even after checking with ping.
    time.sleep(10)

    return ValkeyClusterUnderTest(processes, stdout_files)


class AttributeDefinition:
    @abstractmethod
    def to_arguments(self) -> List[Any]:
        pass


class HNSWVectorDefinition(AttributeDefinition):
    def __init__(
        self,
        vector_dimensions: int,
        m=10,
        vector_type="FLOAT32",
        distance_metric="COSINE",
        ef_construction=5,
        ef_runtime=10,
    ):
        self.vector_dimensions = vector_dimensions
        self.m = m
        self.vector_type = vector_type
        self.distance_metric = distance_metric
        self.ef_construction = ef_construction
        self.ef_runtime = ef_runtime

    def to_arguments(self) -> List[Any]:
        return [
            "VECTOR",
            "HNSW",
            12,
            "M",
            self.m,
            "TYPE",
            self.vector_type,
            "DIM",
            self.vector_dimensions,
            "DISTANCE_METRIC",
            self.distance_metric,
            "EF_CONSTRUCTION",
            self.ef_construction,
            "EF_RUNTIME",
            self.ef_runtime,
        ]


class FlatVectorDefinition(AttributeDefinition):
    def __init__(
        self,
        vector_dimensions: int,
        vector_type="FLOAT32",
        distance_metric="COSINE",
    ):
        self.vector_dimensions = vector_dimensions
        self.vector_type = vector_type
        self.distance_metric = distance_metric

    def to_arguments(self) -> List[Any]:
        return [
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            self.vector_type,
            "DIM",
            self.vector_dimensions,
            "DISTANCE_METRIC",
            self.distance_metric,
        ]


class TagDefinition(AttributeDefinition):
    def __init__(self, separator=",", alias=None):
        self.separator = separator
        self.alias = alias

    def to_arguments(self) -> List[Any]:
        args = []
        if self.alias:
            args += ["AS", self.alias]
        args += [
            "TAG",
            "SEPARATOR",
            self.separator,
        ]
        return args


class NumericDefinition(AttributeDefinition):
     def __init__(self, alias=None):
        self.alias = alias

     def to_arguments(self) -> List[Any]:
        args = []
        if self.alias:
            args += ["AS", self.alias]
        args += ["NUMERIC"]
        return args


class TextDefinition(AttributeDefinition):
    def __init__(self, nostem=False, with_suffix_trie=False, min_stem_size=None, alias=None):
        self.nostem = nostem
        self.with_suffix_trie = with_suffix_trie
        self.min_stem_size = min_stem_size
        self.alias = alias

    def to_arguments(self) -> List[Any]:
        args = []
        if self.alias:
            args += ["AS", self.alias]
        args += ["TEXT"]
        if self.nostem:
            args += ["NOSTEM"]
        if self.with_suffix_trie:
            args += ["WITHSUFFIXTRIE"]
        if self.min_stem_size is not None:
            args += ["MINSTEMSIZE", str(self.min_stem_size)]
        return args


def create_index(
    client: valkey.ValkeyCluster,
    index_name: str,
    store_data_type: str,
    attributes: Dict[str, AttributeDefinition],
    target_nodes=valkey.ValkeyCluster.DEFAULT_NODE,
):
    """Creates a new Vector index.

    Args:
      client:
      index_name:
      store_data_type:
      attributes:
      target_nodes:
    """
    args = [
        "FT.CREATE",
        index_name,
        "ON",
        store_data_type,
        "SCHEMA",
    ]
    for name, definition in attributes.items():
        def_args = definition.to_arguments()
        if store_data_type == StoreDataType.JSON.name:
            args.append("$." + name)
            if def_args[0] != "AS":
                args.append("AS")
                args.append(name)
        else:          
            args.append(name)
        
        args.extend(def_args)

    return client.execute_command(*args, target_nodes=target_nodes)


def convert_bytes(value):
    if isinstance(value, np.ndarray):
        return value.tobytes().decode('latin1')
    return value


def to_json_string(my_dict):
    converted_dict = {key: convert_bytes(value) for key, value in my_dict.items()}
    return json.dumps(converted_dict)


def store_entry(
    client: valkey.ValkeyCluster,
    store_data_type: str,
    key: str,
    mapping
):
    """Store entry.

    Args:
      client:
      store_data_type:
      key:
      mapping:
    """
    if store_data_type == StoreDataType.HASH.name:
        return client.hset(key, mapping=mapping)
    
    args = [
        "JSON.SET",
        key,
        "$",
        to_json_string(mapping),
    ]
    response = client.execute_command(*args)
    if response == 'OK' or response == b'OK':
        return 4
    return response


def drop_index(client: valkey.ValkeyCluster, index_name: str):
    args = [
        "FT.DROPINDEX",
        index_name,
    ]
    client.execute_command(*args)


def fetch_ft_info(client: valkey.ValkeyCluster, index_name: str):
    args = [
        "FT.INFO",
        index_name,
    ]
    return client.execute_command(*args, target_nodes=client.ALL_NODES)


def generate_deterministic_data(vector_dimensions: int, seed: int):
    # Set a fixed seed value for reproducibility
    np.random.seed(seed)
    # Generate deterministic random data
    data = np.random.rand(vector_dimensions).astype(np.float32).tobytes()
    return data


def insert_vector(
    client: valkey.ValkeyCluster, key: str, vector_dimensions: int, seed: int
):
    vector = generate_deterministic_data(vector_dimensions, seed)
    return client.hset(
        key,
        {
            "embedding": vector,
            "some_hash_key": "some_hash_key_value_" + key,
        },
    )


def insert_vectors_thread(
    key_prefix: str,
    num_vectors: int,
    vector_dimensions: int,
    host: str,
    port: int,
    seed: int,
):
    client = valkey.Valkey(host=host, port=port)
    for i in range(1, num_vectors):
        insert_vector(
            client=client,
            key=(key_prefix + "_" + str(seed) + "_" + str(i)),
            vector_dimensions=vector_dimensions,
            seed=(i + seed * num_vectors),
        )


def insert_vectors(
    host: str,
    port: int,
    num_threads: int,
    vector_dimensions: int,
    num_vectors: int,
):
    """Inserts vectors into the index.

    Args:
      host:
      port:
      num_threads:
      vector_dimensions:
      num_vectors:

    Returns:
    """
    threads = []
    for i in range(1, num_threads):
        thread = threading.Thread(
            target=insert_vectors_thread,
            args=(
                "Thread-" + str(i),
                num_vectors,
                vector_dimensions,
                host,
                port,
                i,
            ),
        )
        threads.append(thread)
    return threads


def delete_vector(client: valkey.ValkeyCluster, key: str):
    return client.delete(key)


def knn_search(
    client: valkey.ValkeyCluster,
    index_name: str,
    vector_dimensions: int,
    seed: int,
):
    """KNN searches the index.

    Args:
      client:
      index_name:
      vector_dimensions:
      seed:

    Returns:
    """
    vector = generate_deterministic_data(vector_dimensions, seed)
    args = [
        "FT.SEARCH",
        index_name,
        "*=>[KNN 3 @embedding $vec EF_RUNTIME 1 AS score]",
        "params",
        2,
        "vec",
        vector,
        "DIALECT",
        2,
    ]
    return client.execute_command(*args, target_nodes=client.RANDOM)


def writer_queue_size(client: valkey.ValkeyCluster, index_name: str):
    out = fetch_ft_info(client, index_name)
    for index, item in enumerate(out):
        if "mutation_queue_size" in str(item):
            return int(str(out[index + 1])[2:-1])
    logging.error("Couldn't find mutation_queue_size")
    exit(1)


def wait_for_empty_writer_queue_size(
    client: valkey.ValkeyCluster, index_name: str, timeout=0
):
    """Wait for the writer queue size to hit zero.

    Args:
      client:
      index_name:
      timeout:
    """
    start = time.time()
    while True:
        try:
            queue_size = writer_queue_size(client=client, index_name=index_name)
            if queue_size == 0:
                return
            logging.info(
                "Waiting for queue size to hit zero, current size: %d",
                queue_size,
            )
        except (
            valkey.exceptions.ConnectionError,
            valkey.exceptions.ResponseError,
        ) as e:
            logging.error("Error fetching FT.INFO: %s", e)
        if timeout > 0 and time.time() - start > timeout:
            logging.error("Timed out waiting for queue size to hit zero")
            return
        time.sleep(1)


class RandomIntervalTask:
    """Randomly executes a task at a random interval.

    Used to inject (faulty) background operations into the test.

    Attributes:
      stopped:
      interval:
      randomize:
      stop_condition:
      task:
      ops:
      failures:
      name:
      thread:
      failed_ports: Set of ports that were intentionally shut down (for failover tasks)
      failover_state: Optional shared state to pause during failovers
    """

    def __init__(
        self,
        name: str,
        interval: int,
        randomize: bool,
        work_func: Callable[[], bool],
        failover_state: dict | None = None,
    ):
        stop_condition = threading.Condition()
        self.stopped = False
        self.interval = interval
        self.randomize = randomize
        self.stop_condition = stop_condition
        self.task = work_func
        self.ops = 0
        self.failures = 0
        self.name = name
        self.failed_ports = set()  # Track intentionally failed ports (for failover)
        self.failover_state = failover_state

    def stop(self):
        if not self.thread:
            logging.error("Thread not running")
            return
        with self.stop_condition:
            self.stopped = True
            self.stop_condition.notify()
        self.thread.join()

    def run(self):
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

    def loop(self):
        """Main loop that executes the task at intervals, pausing during failovers."""
        with self.stop_condition:
            while True:
                modifier = 1
                if self.randomize:
                    modifier = random.random()
                self.stop_condition.wait_for(
                    lambda: self.stopped, timeout=self.interval * modifier
                )
                if self.stopped:
                    return
                
                # Check if failover is in progress - skip execution if so
                if self.failover_state is not None:
                    with self.failover_state['lock']:
                        failover_in_progress = self.failover_state['in_progress']
                    
                    if failover_in_progress:
                        logging.debug("<%s> Skipping execution - failover in progress", self.name)
                        continue  # Skip this iteration, wait for next interval
                
                # Execute the task
                if not self.task():
                    self.failures += 1
                self.ops += 1


def periodic_bgsave_task(
    client: valkey.ValkeyCluster,
) -> bool:
    try:
        logging.info("<BGSAVE> Invoking background save")
        client.bgsave(target_nodes=client.ALL_NODES)
    except (
        valkey.exceptions.ConnectionError,
        valkey.exceptions.ResponseError,
    ) as e:
        logging.error("<BGSAVE> encountered error: %s", e)
        return False
    return True


def periodic_bgsave(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    randomize: bool,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "BGSAVE", interval_sec, randomize, lambda: periodic_bgsave_task(client)
    )
    thread.run()
    return thread


class IndexState:

    def __init__(self, index_lock: threading.Lock, ft_created: bool):
        self.index_lock = index_lock
        self.ft_created = ft_created


def get_available_nodes_excluding_failed(
    client: valkey.ValkeyCluster,
    failed_ports: set
) -> list:
    """Build a list of cluster node objects excluding failed ports.
    
    Returns:
        List of ClusterNode objects that are not in the failed_ports set
    """
    if not failed_ports:
        # No failed ports, return all nodes
        return client.ALL_NODES
    
    available_nodes = []
    try:
        # Get all nodes from the cluster
        nodes = client.get_nodes()
        for node in nodes:
            # Extract port from the node
            node_port = node.port
            if node_port not in failed_ports:
                available_nodes.append(node)
        
        if not available_nodes:
            logging.warning("No available nodes found after excluding failed ports: %s", failed_ports)
            # Fallback to ALL_NODES if somehow no nodes are available
            return client.ALL_NODES
        
        logging.debug("Available nodes (excluding failed ports %s): %d nodes", failed_ports, len(available_nodes))
        return available_nodes
    except Exception as e:
        logging.warning("Error building available nodes list: %s, falling back to ALL_NODES", e)
        return client.ALL_NODES


def periodic_ftdrop_task(
    client: valkey.ValkeyCluster,
    index_name: str,
    index_state: IndexState,
    failover_state: dict | None = None,
    entry_point_port: int | None = None,
) -> bool:
    
    client.nodes_manager.initialize()
    with index_state.index_lock:
        logging.info("<FT.DROPINDEX> Invoking index drop")
        try:
            # Always use the entry point node if specified (protected from failover)
            if entry_point_port is not None:
                entry_point_node = client.get_node(host="localhost", port=entry_point_port)
                if entry_point_node is not None:
                    # Execute FT.DROPINDEX on the entry point node
                    args = ["FT.DROPINDEX", index_name]
                    client.execute_command(*args, target_nodes=[entry_point_node])
                    logging.info("<FT.DROPINDEX> Successfully dropped index")
                else:
                    logging.warning("<FT.DROPINDEX> Entry point node at port %d not found, using default", entry_point_port)
                    drop_index(client, index_name)
            else:
                # Fallback to original logic if entry_point_port not provided
                drop_index(client, index_name)
                logging.info("<FT.DROPINDEX> Successfully dropped index")
            index_state.ft_created = False
        except (
            valkey.exceptions.ConnectionError,
            valkey.exceptions.ResponseError,
        ) as e:
            error_str = str(e)
            if not index_state.ft_created and "not found" in error_str:
                logging.debug("<FT.DROPINDEX> got expected error: %s", e)
            else:
                logging.error("<FT.DROPINDEX> got unexpected error: %s", e)
                return False
    return True


def periodic_ftdrop(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    random_interval: bool,
    index_name: str,
    index_state: IndexState,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "FT.DROPINDEX",
        interval_sec,
        random_interval,
        lambda: periodic_ftdrop_task(client, index_name, index_state),
    )
    thread.run()
    return thread

def periodic_ftcreate_task(
    client: valkey.ValkeyCluster,
    index_name: str,
    attributes: Dict[str, AttributeDefinition],
    index_state: IndexState,
    failover_state: dict | None = None,
    entry_point_port: int | None = None,
) -> bool:
    with index_state.index_lock:
        try:
            logging.info("<FT.CREATE> Invoking index creation")
            
            # Always use the entry point node if specified (protected from failover)
            if entry_point_port is not None:
                entry_point_node = client.get_node(host="localhost", port=entry_point_port)
                if entry_point_node is not None:
                    target_nodes = [entry_point_node]
                    logging.info("<FT.CREATE> Using entry point node at port %d (always available)", entry_point_port)
                else:
                    logging.warning("<FT.CREATE> Entry point node at port %d not found, falling back to DEFAULT_NODE", entry_point_port)
                    target_nodes = client.DEFAULT_NODE
            else:
                # Fallback to original logic if entry_point_port not provided
                target_nodes = client.DEFAULT_NODE
                if failover_state is not None:
                    with failover_state['lock']:
                        failed_ports = failover_state['failed_ports'].copy()
                    
                    if failed_ports:
                        target_nodes = get_available_nodes_excluding_failed(client, failed_ports)
                        logging.debug("<FT.CREATE> Using filtered nodes (excluding failed ports: %s)", failed_ports)
                
            create_index(
                client=client, 
                store_data_type=StoreDataType.HASH.name, 
                index_name=index_name, 
                attributes=attributes,
                target_nodes=target_nodes
            )
            logging.info("<FT.CREATE> Successfully created index")
            index_state.ft_created = True
        except (
            valkey.exceptions.ConnectionError,
            valkey.exceptions.ResponseError,
        ) as e:
            if index_state.ft_created and "already exists" in str(e):
                logging.debug("<FT.CREATE> got expected error: %s", e)
            else:
                logging.error("<FT.CREATE> got unexpected error: %s", e)
                return False
    return True


def periodic_ftcreate(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    random_interval: bool,
    index_name: str,
    attributes: Dict[str, AttributeDefinition],
    index_state: IndexState,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "FT.CREATE",
        interval_sec,
        random_interval,
        lambda: periodic_ftcreate_task(
            client, index_name, attributes, index_state
        ),
    )
    thread.run()
    return thread


def periodic_flushdb_task(
    client: valkey.ValkeyCluster,
    index_state: IndexState,
    use_coordinator: bool,
) -> bool:
    with index_state.index_lock:
        logging.info("<FLUSHDB> Invoking flush DB")
        try:
            client.flushdb()
            if not use_coordinator:
                index_state.ft_created = False
        except (
            valkey.exceptions.ConnectionError,
            valkey.exceptions.ResponseError,
        ) as e:
            logging.error(
                "<FLUSHDB> got unexpected error during FLUSHDB: %s", e
            )
            return False
    return True


def periodic_flushdb(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    random_interval: bool,
    index_state: IndexState,
    use_coordinator: bool,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "FLUSHDB",
        interval_sec,
        random_interval,
        lambda: periodic_flushdb_task(client, index_state, use_coordinator),
    )
    thread.run()
    return thread


def set_non_blocking(fd) -> None:
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def spawn_memtier_process(command: str) -> subprocess.Popen[Any]:
    memtier_process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if memtier_process.stdout is not None:
        set_non_blocking(memtier_process.stdout.fileno())
    if memtier_process.stderr is not None:
        set_non_blocking(memtier_process.stderr.fileno())
    return memtier_process


class MemtierErrorLineInfo(NamedTuple):
    run_number: int
    percent_complete: float
    runtime: float
    threads: int
    ops: int
    ops_sec: float
    avg_ops_sec: float
    b_sec: int
    avg_b_sec: int
    latency: float
    avg_latency: float
    error: str | None


class MemtierProcess:

    def __init__(
        self,
        command: str,
        name: str,
        trailing_secs: int = 10,
        error_predicate: Callable[[str], bool] | None = None,
    ):
        self.name = name
        self.runtime = 0
        self.trailing_ops_sec = []
        self.failures = 0
        self.trailing_secs = trailing_secs
        self.halted = False
        self.process = spawn_memtier_process(command)
        self.done = False
        self.error_predicate = error_predicate
        self.total_ops = 0
        self.avg_ops_sec = 0

    def process_logs(self):
        for line in self._process_memtier_subprocess_output():
            is_acceptable_error = False
            if line.error is not None:
                # If error_predicate is provided and returns True, this is an acceptable error
                if self.error_predicate is not None and self.error_predicate(line.error):
                    logging.debug(
                        "<%s> encountered expected error (ignored): %s", self.name, line.error
                    )
                    is_acceptable_error = True
                else:
                    # This is an unexpected error - log it
                    logging.error(
                        "<%s> encountered error: %s", self.name, line.error
                    )
            self._add_line_to_stats(line, is_acceptable_error=is_acceptable_error)

    def _add_line_to_stats(self, line: MemtierErrorLineInfo, is_acceptable_error: bool = False):
        if line.error is not None:
            # Only count as failure if this is not an acceptable error
            if not is_acceptable_error:
                self.failures += 1
        else:
            self.runtime = line.runtime
            self.trailing_ops_sec.insert(0, line.ops_sec)
            if len(self.trailing_ops_sec) > self.trailing_secs:
                self.trailing_ops_sec.pop()
            # Only update total_ops and avg_ops_sec for non-error lines
            self.total_ops = line.ops
            self.avg_ops_sec = line.avg_ops_sec
        if self.trailing_ops_sec:
            trailing_ops_sec = sum(self.trailing_ops_sec) / len(
                self.trailing_ops_sec
            )
            if (
                trailing_ops_sec == 0
                and len(self.trailing_ops_sec) == self.trailing_secs
            ):
                self.halted = True

    def print_status(self):
        if self.process.poll() is not None and not self.done:
            logging.info(
                "<%s> - \tState: Exit Code %d,\tRuntime: %d,\ttotal ops:"
                " %d,\tops/s(latest): %d,\tavg ops/s(lifetime): %d",
                self.name,
                self.process.returncode,
                self.runtime,
                self.total_ops,
                self.trailing_ops_sec[0] if self.trailing_ops_sec else 0,
                self.avg_ops_sec,
            )
            self.done = True
        if self.done:
            return
        if self.trailing_ops_sec:
            trailing_ops_sec = sum(self.trailing_ops_sec) / len(
                self.trailing_ops_sec
            )
            logging.info(
                "<%s> - \tState: Running,\tRuntime: %d,\ttotal ops:"
                " %d,\tops/s(latest): %d,\tavg ops/s(lifetime): %d,\tavg"
                " ops/s(10s): %d",
                self.name,
                self.runtime,
                self.total_ops,
                self.trailing_ops_sec[0],
                self.avg_ops_sec,
                trailing_ops_sec,
            )
            return
        logging.info("<%s> - \tState: Waiting for output", self.name)

    def _process_memtier_subprocess_output(self):
        try:
            parsed_lines = []
            while True:
                if self.process.stderr is None:
                    break
                stderr = self.process.stderr.readline()
                if stderr:
                    stderr = stderr.decode("utf-8")
                    error_line_info = parse_memtier_error_line(stderr)
                    if error_line_info is not None:
                        parsed_lines.append(error_line_info)
                    else:
                        logging.info(
                            "<%s> stderr: %s", self.name, stderr.strip()
                        )
                else:
                    break
            while True:
                if self.process.stdout is None:
                    break
                stdout = self.process.stdout.readline()
                if stdout:
                    stdout = stdout.decode("utf-8")
                    logging.info("<%s> stdout: %s", self.name, stdout.strip())
                else:
                    break
            return parsed_lines
        except IOError:
            pass


def parse_memtier_error_line(line: str):
    # Actual memtier format: [RUN #1 1%,   0 secs] 10 threads 10 conns:        4408 ops,    8807 (avg:    8807) ops/sec, 4.24MB/sec (avg: 4.24MB/sec), 30.95 (avg: 30.95) msec latency
    progress_pattern = (
        r"\[RUN #(\d+)"
        r"\s+([\d\.]+)%?,\s+([\d\.]+)\s+secs\]\s+(\d+)\s+threads\s+\d+\s+conns:\s+(\d+)\s+ops,\s+([\d\.]+)\s+\(avg:\s+([\d\.]+)\)\s+ops\/sec,\s+([\d\.]+[KMG]?B\/sec)\s+\(avg:\s+([\d\.]+[KMG]?B\/sec)\),\s+(-nan|[\d\.]+)\s+\(avg:\s+([\d\.]+)\)\s+msec\s+latency"
    )
    match = re.search(progress_pattern, line)

    if match:
        run_number = int(match.group(1))
        percent_complete = float(match.group(2))
        runtime = float(match.group(3))
        threads = int(match.group(4))
        ops = int(match.group(5))
        ops_sec = float(match.group(6))
        avg_ops_sec = float(match.group(7))
        b_sec = match.group(8)
        avg_b_sec = match.group(9)
        latency = match.group(10)
        if latency == '-nan':
            latency = 0.0
        else:
            latency = float(latency)
        avg_latency = float(match.group(11))
        return MemtierErrorLineInfo(
            run_number=run_number,
            percent_complete=percent_complete,
            runtime=runtime,
            threads=threads,
            ops=ops,
            ops_sec=ops_sec,
            avg_ops_sec=avg_ops_sec,
            b_sec=b_sec,
            avg_b_sec=avg_b_sec,
            latency=latency,
            avg_latency=avg_latency,
            error=None,
        )
    else:
        # See if it matches the error pattern
        error_pattern = r"server [\d\.]+:\d+ handle error response: (.*)"
        match = re.search(error_pattern, line)
        if match:
            return MemtierErrorLineInfo(
                run_number=0,
                percent_complete=0,
                runtime=0,
                threads=0,
                ops=0,
                ops_sec=0,
                avg_ops_sec=0,
                b_sec=0,
                avg_b_sec=0,
                latency=0,
                avg_latency=0,
                error=match.group(1),
            )
        return None


def connect_to_valkey_cluster(
    startup_nodes: List[valkey.cluster.ClusterNode],
    require_full_coverage: bool = True,
    password: str | None = None,
    attempts: int = 10,
    connection_class=valkey.connection.Connection,
) -> valkey.ValkeyCluster:
    """Connects to a valkey cluster, retrying if necessary.

    Args:
      startup_nodes: List of cluster nodes to connect to.
      require_full_coverage: Whether to require full coverage of the cluster.

    Returns:
      Valkey cluster connection or None if connection failed.
    """
    if attempts <= 0:
        raise ValueError("attempts must be > 0")

    while attempts > 0:
        attempts -= 1
        try:
            valkey_conn = valkey.cluster.ValkeyCluster.from_url(
                url="valkey://{}:{}".format(
                    startup_nodes[0].host, startup_nodes[0].port
                ),
                password=password,
                connection_class=connection_class,
                startup_nodes=startup_nodes,
                require_full_coverage=require_full_coverage,
            )
            valkey_conn.ping()
            return valkey_conn
        except valkey.exceptions.ConnectionError as e:
            if attempts == 0:
                raise e
            logging.info("Failed to connect to valkey cluster, retrying...")
            time.sleep(1)

    assert False

# Cluster Failover Functions
class ClusterNode(NamedTuple):
    """Represents a node in the cluster topology."""
    node_id: str
    addr: str  # host:port
    is_primary: bool
    primary_id: str | None  # For replicas, the ID of their primary


def get_cluster_nodes(client: valkey.ValkeyCluster) -> tuple[List[ClusterNode], List[ClusterNode]]:
    """Discover cluster topology by parsing CLUSTER NODES output.
    
    This function queries the cluster to get the current topology, separating
    primary and replica nodes. It ignores nodes that are in a failed state.
    
    Returns:
        Tuple of (primarys, replicas) where each is a list of ClusterNode objects
        
    """
    try:
        nodes_output = client.execute_command("CLUSTER", "NODES").decode().splitlines()
    except (valkey.exceptions.ConnectionError, valkey.exceptions.ResponseError) as e:
        logging.error("Failed to get cluster nodes: %s", e)
        return [], []
    
    primarys = []
    replicas = []
    
    for line in nodes_output:
        if not line.strip():
            continue
            
        parts = line.split()
        if len(parts) < 8:
            continue
            
        node_id = parts[0]
        addr = parts[1].split("@")[0]  # Remove cluster bus port
        flags = parts[2]
        primary_id = parts[3] if len(parts) > 3 else "-"
        
        # Check if this is a primary node (and not failed)
        if "master" in flags and "fail" not in flags:
            primarys.append(ClusterNode(
                node_id=node_id,
                addr=addr,
                is_primary=True,
                primary_id=None
            ))
        # Check if this is a replica node
        elif "slave" in flags and "fail" not in flags:
            replicas.append(ClusterNode(
                node_id=node_id,
                addr=addr,
                is_primary=False,
                primary_id=primary_id
            ))
    
    return primarys, replicas


def pick_primary_to_fail(primarys: List[ClusterNode], replicas: List[ClusterNode], exclude_port: int | None = None) -> ClusterNode | None:
    """Randomly select a primary node to fail, ensuring it has replicas.
    
    This function implements a random selection strategy to increase test coverage
    and avoid bias. It only selects primarys that have at least one replica to
    ensure the cluster can perform automatic failover.
    
    Args:
        primarys: List of primary nodes in the cluster
        replicas: List of replica nodes in the cluster
        exclude_port: Port number to exclude from selection (typically the entry point)
        
    Returns:
        Selected ClusterNode to fail, or None if no suitable primary found
    """
    if not primarys:
        logging.warning("No primary nodes available to fail")
        return None
    
    # Find primarys that have at least one replica AND are not the excluded port
    primarys_with_replicas = []
    for primary in primarys:
        has_replica = any(r.primary_id == primary.node_id for r in replicas)
        if has_replica:
            # Check if this primary is the excluded port
            if exclude_port is not None:
                try:
                    primary_port = int(primary.addr.split(":")[1])
                    if primary_port == exclude_port:
                        logging.info(
                            "Skipping primary at port %d (entry point - excluded from failover)",
                            primary_port
                        )
                        continue
                except Exception as e:
                    logging.warning("Could not parse port from address %s: %s", primary.addr, e)
            
            primarys_with_replicas.append(primary)
    
    if not primarys_with_replicas:
        logging.warning("No suitable primarys found (all either lack replicas or are excluded)")
        return None
    
    # Randomly select one primary to fail
    selected = random.choice(primarys_with_replicas)
    logging.info(
        "Selected primary to fail: node_id=%s, addr=%s (out of %d candidates)",
        selected.node_id,
        selected.addr,
        len(primarys_with_replicas)
    )
    return selected


def shutdown_node(addr: str, password: str | None = None) -> bool:
    """Shut down a specific cluster node using SHUTDOWN NOSAVE.
    
    This simulates a real crash/failure scenario:
    - SHUTDOWN NOSAVE immediately terminates the process without saving to disk
    - Mimics network partition, process crash, or power failure
    - Triggers automatic replica promotion by the cluster
    - No persistence side effects that could interfere with the test     
    Returns:
        True if shutdown command was sent successfully, False otherwise
    """
    try:
        host, port = addr.split(":")
        node_client = valkey.Valkey(
            host=host,
            port=int(port),
            password=password,
            socket_timeout=2,
        )
        logging.info("Sending SHUTDOWN NOSAVE to node %s", addr)
        node_client.execute_command("SHUTDOWN", "NOSAVE")
    except Exception as e:
        # Connection drop is EXPECTED and means shutdown succeeded
        logging.info("Node %s connection dropped (expected after SHUTDOWN): %s", addr, e)
        return True
    
    # If we reach here without exception, something unexpected happened
    logging.warning("SHUTDOWN command completed without connection drop - unexpected")
    return True


def wait_for_new_primary(
    client: valkey.ValkeyCluster,
    old_primary_id: str,
    old_primary_addr: str,
    timeout: int = 30
) -> tuple[bool, str | None]:
    """Wait for a replica to be promoted to primary after the old primary fails.
    
    This function polls the cluster topology until it detects that:
    1. The old primary node ID is no longer present as a primary
    2. A new primary has taken over its slots
    Returns:
        Tuple of (success: bool, new_primary_addr: str | None)
        - success: True if new primary detected within timeout
        - new_primary_addr: Address of the newly promoted primary, or None if failed
    """
    start = time.time()
    logging.info("Waiting for replica promotion (old primary: %s at %s)", old_primary_id, old_primary_addr)
    
    # Track which replicas were under the old primary
    initial_primarys, initial_replicas = get_cluster_nodes(client)
    old_primary_replicas = [r for r in initial_replicas if r.primary_id == old_primary_id]
    
    if old_primary_replicas:
        logging.info(
            "Old primary had %d replica(s): %s",
            len(old_primary_replicas),
            [r.addr for r in old_primary_replicas]
        )
    
    while time.time() - start < timeout:
        primarys, replicas = get_cluster_nodes(client)
        
        # Check if old primary is gone from primary list
        old_primary_still_present = any(m.node_id == old_primary_id for m in primarys)
        
        if not old_primary_still_present and primarys:
            # Find which of the old replicas became the new primary
            new_primary_addr = None
            for old_replica in old_primary_replicas:
                # Check if this replica is now a primary
                if any(m.node_id == old_replica.node_id for m in primarys):
                    new_primary_addr = old_replica.addr
                    logging.info(
                        "REPLICA PROMOTED: %s (node_id: %s) promoted to primary after %.1fs",
                        new_primary_addr,
                        old_replica.node_id,
                        time.time() - start
                    )
                    break
            
            if not new_primary_addr:
                # Couldn't identify which replica was promoted, but promotion happened
                logging.warning(
                    "Replica promotion detected but couldn't identify which replica (old primary: %s)",
                    old_primary_id
                )
            
            return True, new_primary_addr
        
        time.sleep(1)
    
    logging.error(
        "Timeout waiting for replica promotion after %d seconds (old primary: %s)",
        timeout,
        old_primary_id
    )
    return False, None


def wait_for_cluster_ok(client: valkey.ValkeyCluster, timeout: int = 30) -> bool:
    """Wait for the cluster to reach a healthy state with full slot coverage.
    
    After failover, this function ensures:
    1. All 16384 hash slots are assigned to available primarys
    2. No slots are in "migrating" or "importing" state
    3. Cluster state is reported as "ok"
    Returns:
        True if cluster reaches OK state within timeout, False otherwise
    """
    start = time.time()
    logging.info("Waiting for cluster to reach OK state")
    
    while time.time() - start < timeout:
        try:
            info = client.execute_command("CLUSTER", "INFO").decode()
            if "cluster_state:ok" in info:
                logging.info("Cluster reached OK state after %.1fs", time.time() - start)
                return True
            else:
                # Log the current state for debugging
                for line in info.split("\r\n"):
                    if "cluster_state" in line:
                        logging.debug("Current cluster state: %s", line)
        except (valkey.exceptions.ConnectionError, valkey.exceptions.ResponseError) as e:
            logging.debug("Error checking cluster state (will retry): %s", e)
        
        time.sleep(1)
    
    logging.error("Timeout waiting for cluster OK state after %d seconds", timeout)
    return False


def wait_for_node_topology_convergence(
    client: valkey.ValkeyCluster,
    rejoined_node_id: str,
    timeout: int = 30
) -> bool:
    """Wait for all cluster nodes to recognize a rejoined node in their topology.
    
    Returns:
        True if all nodes recognize the rejoined node within timeout, False otherwise
    """
    start = time.time()
    logging.info(
        "Waiting for cluster topology to converge (checking for node %s)...",
        rejoined_node_id
    )
    
    while time.time() - start < timeout:
        try:
            # Get CLUSTER NODES from all active nodes
            all_nodes = client.get_nodes()
            convergence_achieved = True
            nodes_checked = 0
            nodes_see_rejoined = 0
            
            for node in all_nodes:
                try:
                    nodes_checked += 1
                    # Get this node's view of the cluster
                    node_client = valkey.Valkey(
                        host=node.host,
                        port=node.port,
                        socket_timeout=2,
                    )
                    nodes_output = node_client.execute_command("CLUSTER", "NODES").decode()
                    
                    # Check if this node sees the rejoined node
                    rejoined_node_found = False
                    for line in nodes_output.splitlines():
                        if not line.strip():
                            continue
                        
                        parts = line.split()
                        if len(parts) < 3:
                            continue
                        
                        node_id = parts[0]
                        flags = parts[2]
                        
                        if node_id == rejoined_node_id:
                            rejoined_node_found = True
                            
                            # Check if the rejoined node is in a bad state
                            if "handshake" in flags or "noaddr" in flags or "fail" in flags:
                                logging.debug(
                                    "Node %s:%d sees rejoined node %s but it's in state: %s",
                                    node.host, node.port, rejoined_node_id, flags
                                )
                                convergence_achieved = False
                                break
                            else:
                                nodes_see_rejoined += 1
                                logging.debug(
                                    "Node %s:%d has converged view of rejoined node %s (flags: %s)",
                                    node.host, node.port, rejoined_node_id, flags
                                )
                            break
                    
                    if not rejoined_node_found:
                        logging.debug(
                            "Node %s:%d does not see rejoined node %s yet",
                            node.host, node.port, rejoined_node_id
                        )
                        convergence_achieved = False
                        
                except Exception as e:
                    logging.debug("Error checking node %s:%d: %s", node.host, node.port, e)
                    convergence_achieved = False
            
            if convergence_achieved and nodes_checked > 0 and nodes_see_rejoined == nodes_checked:
                logging.info(
                    "Cluster topology converged after %.1fs - all %d nodes recognize rejoined node %s",
                    time.time() - start,
                    nodes_checked,
                    rejoined_node_id
                )
                return True
            else:
                logging.debug(
                    "Topology convergence in progress: %d/%d nodes see rejoined node",
                    nodes_see_rejoined,
                    nodes_checked
                )
                
        except Exception as e:
            logging.debug("Error checking cluster topology convergence: %s", e)
        
        time.sleep(1)
    
    logging.error(
        "Timeout waiting for topology convergence after %d seconds (rejoined node: %s)",
        timeout,
        rejoined_node_id
    )
    return False


def restart_node(
    valkey_server_path: str,
    port: int,
    config_dir: str,
    stdout_dir: str,
    modules: Dict[str, str],
    password: str | None = None
) -> ValkeyServerUnderTest | None:
    """Restart a previously failed node to test recovery and rejoin behavior.
    
    Args:
        valkey_server_path: Path to valkey-server binary
        port: Port number of the node to restart
        config_dir: Directory containing node configurations  
        stdout_dir: Directory for stdout logs
        modules: Dictionary of module paths to their arguments (must match initial startup)
        password: Optional password for authentication
        
    Returns:
        ValkeyServerUnderTest object if restart succeeds, None otherwise
    """
    try:
        node_dir = os.path.join(config_dir, f"nodes{port}")
        stdout_path = os.path.join(stdout_dir, f"{port}_restart_stdout.txt")
        
        if not os.path.exists(node_dir):
            logging.error("Node directory %s does not exist", node_dir)
            return None
        
        logging.info("Restarting node on port %d using start_valkey_process", port)
        
        stdout_file = open(stdout_path, "w", buffering=1)
        
        # Build cluster args exactly as in start_valkey_cluster
        cluster_args = {
            "cluster-enabled": "yes",
            "cluster-config-file": os.path.join(node_dir, "nodes.conf"),
            "cluster-node-timeout": "10000"  # Use same timeout as initial startup (not 45000)
        }
        
        # Reuse start_valkey_process to ensure identical configuration
        # This guarantees same module loading order, argument parsing, etc.
        return start_valkey_process(
            valkey_server_path=valkey_server_path,
            port=port,
            directory=node_dir,
            stdout_file=stdout_file,
            args=cluster_args,
            modules=modules,
            password=password
        )
        
    except Exception as e:
        logging.error("Error restarting node on port %d: %s", port, e)
        return None


def periodic_failover_task(
    client: valkey.ValkeyCluster,
    valkey_server_path: str,
    config_dir: str,
    stdout_dir: str,
    modules: Dict[str, str],
    test_recovery: bool,
    password: str | None = None,
    failed_ports_tracker: set | None = None,
    failover_state: dict | None = None,
    entry_point_port: int | None = None,
) -> bool:
    """Execute a single cluster failover operation.
    
    This performs the complete failover sequence:
    1. Discover current cluster topology
    2. Select a primary node to fail (one with replicas)
    3. Shut down the selected primary
    4. Wait for replica promotion
    5. Wait for cluster to reach OK state
    6. Optionally restart the failed node to test recovery
    Returns:
        True if failover sequence completed successfully, False otherwise
    """
    logging.info("<FAILOVER> Starting cluster failover sequence")
    
    # Signal that failover is starting - this must happen BEFORE shutdown
    if failover_state is not None:
        with failover_state['lock']:
            failover_state['in_progress'] = True
        logging.info("<FAILOVER> Set failover_state['in_progress'] = True")
    
    # Step 1: Get cluster topology
    primarys, replicas = get_cluster_nodes(client)
    if not primarys:
        logging.error("<FAILOVER> No primarys found in cluster")
        return False
    
    logging.info("<FAILOVER> Found %d primarys and %d replicas", len(primarys), len(replicas))
    
    # Step 2: Pick a primary to fail (excluding entry point)
    victim = pick_primary_to_fail(primarys, replicas, exclude_port=entry_point_port)
    if not victim:
        logging.error("<FAILOVER> No suitable primary found to fail")
        return False
    
    logging.info("<FAILOVER> Selected victim: %s (node_id: %s)", victim.addr, victim.node_id)
    
    # Extract and track the port that we're shutting down
    try:
        victim_port = int(victim.addr.split(":")[1])
        if failed_ports_tracker is not None:
            failed_ports_tracker.add(victim_port)
            logging.info("<FAILOVER> Tracking failed port in thread: %d", victim_port)
        
        # Also add to shared failover_state for FT.CREATE/FT.DROPINDEX to see
        if failover_state is not None:
            with failover_state['lock']:
                failover_state['failed_ports'].add(victim_port)
                failover_state['new_primary_connected'] = False
            logging.info("<FAILOVER> Added port %d to shared failover_state", victim_port)
    except Exception as e:
        logging.warning("<FAILOVER> Could not extract port from address %s: %s", victim.addr, e)
    
    # Step 3: Shut down the primary
    if not shutdown_node(victim.addr, password):
        logging.error("<FAILOVER> Failed to shutdown node %s", victim.addr)
        return False
    
    # Give the node a moment to fully shut down
    time.sleep(2)
    
    # Step 4: Wait for replica promotion
    promotion_success, new_primary_addr = wait_for_new_primary(
        client, victim.node_id, victim.addr, timeout=30
    )
    if not promotion_success:
        logging.error("<FAILOVER> Replica promotion did not complete in time Replica not promoted")
        return False
    
    # Step 5: Wait for cluster OK state
    if not wait_for_cluster_ok(client, timeout=30):
        logging.error("<FAILOVER> Cluster did not reach OK state in time")
        return False
    
    logging.info("<FAILOVER> Failover completed successfully - new primary: %s", new_primary_addr or "unknown")
    
    # Signal that failover has completed - cluster is stable again
    # This allows memtier processes to restart and redirect traffic to the new primary
    if failover_state is not None:
        with failover_state['lock']:
            failover_state['in_progress'] = False
            if new_primary_addr:
                failover_state['new_primary_addr'] = new_primary_addr
        logging.info("<FAILOVER> Set failover_state['in_progress'] = False - memtier processes can restart now")
    
    # Step 6: Wait for traffic redirection before bringing old primary back
    if test_recovery:
        recovery_delay_sec = 20
        logging.info(
            "<FAILOVER> Waiting %d seconds for traffic to redirect to new primary (%s) before reconnecting old primary...",
            recovery_delay_sec,
            new_primary_addr or "unknown"
        )
        time.sleep(recovery_delay_sec)
        
        # Step 7: Restart the old primary as a replica
        logging.info("<FAILOVER> Now reconnecting old primary %s as replica", victim.addr)
        try:
            port = int(victim.addr.split(":")[1])
            restarted_node = restart_node(
                valkey_server_path=valkey_server_path,
                port=port,
                config_dir=config_dir,
                stdout_dir=stdout_dir,
                modules=modules,
                password=password
            )
            if restarted_node:
                logging.info("<FAILOVER> Old primary successfully reconnected to cluster")
                
                # Wait for cluster topology to converge before removing from failed_ports
                # This ensures all nodes recognize the rejoined node, preventing
                # "Unable to contact all cluster members" errors
                topology_converged = wait_for_node_topology_convergence(
                    client=client,
                    rejoined_node_id=victim.node_id,
                    timeout=30
                )
                
                if not topology_converged:
                    logging.warning(
                        "<FAILOVER> Topology convergence timeout for node %d (id: %s) - proceeding anyway",
                        port,
                        victim.node_id
                    )
                
                # NOW it's safe to remove from failed_ports - all nodes see the rejoined node
                if failover_state is not None:
                    with failover_state['lock']:
                        if port in failover_state['failed_ports']:
                            failover_state['failed_ports'].remove(port)
                            logging.info("<FAILOVER> Removed port %d from failed_ports (topology converged)", port)
                        # Mark new primary as connected
                        failover_state['new_primary_connected'] = True
                        logging.info("<FAILOVER> Set new_primary_connected = True")
                
                # Force client to refresh its topology to see the rejoined node
                try:
                    logging.info("<FAILOVER> Forcing client topology refresh...")
                    
                    # Log the raw CLUSTER NODES output to understand cluster state
                    cluster_nodes_output = client.execute_command("CLUSTER", "NODES")
                    cluster_nodes_str = cluster_nodes_output.decode() if isinstance(cluster_nodes_output, bytes) else str(cluster_nodes_output)
                    logging.info("<FAILOVER> CLUSTER NODES output: %s", cluster_nodes_str)
                    
                except Exception as e:
                    logging.warning("<FAILOVER> Error refreshing client topology: %s", e)
                time.sleep(5)
                
                # Verify the node is now a replica
                try:
                    node_client = valkey.Valkey(
                        host="localhost",
                        port=port,
                        password=password,
                        socket_timeout=2,
                    )
                    role_info = node_client.execute_command("ROLE")
                    if role_info[0].decode() == "slave":
                        primary_host = role_info[1].decode()
                        primary_port = int(role_info[2])
                        logging.info(
                            "<FAILOVER> OLD primary NOW REPLICA: Port %d is now replicating from %s:%d",
                            port,
                            primary_host,
                            primary_port
                        )
                    else:
                        logging.warning(
                            "Old primary at port %d has role: %s (expected: slave)",
                            port,
                            role_info[0].decode()
                        )
                except Exception as e:
                    logging.warning(
                        "Could not verify replica role for port %d: %s",
                        port,
                        e
                    )
            else:
                logging.warning("<FAILOVER> Failed to restart old primary, but failover was successful")
        except Exception as e:
            logging.warning("<FAILOVER> Error during recovery testing: %s", e)
    
    return True


def periodic_failover(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    randomize: bool,
    valkey_server_path: str,
    config_dir: str,
    stdout_dir: str,
    modules: Dict[str, str],
    test_recovery: bool = True,
    password: str | None = None,
    failover_state: dict | None = None,
    entry_point_port: int | None = None,
) -> RandomIntervalTask:
    """Create a background task that periodically triggers cluster failovers.
    
    This creates a RandomIntervalTask that runs failover operations at the
    specified interval, with optional randomization to make timing less predictable.
        
    Returns:
        RandomIntervalTask that can be started and stopped
    """
    # Create the thread first so we can pass it to the work function
    # to allow tracking of failed ports
    thread = RandomIntervalTask(
        "FAILOVER",
        interval_sec,
        randomize,
        lambda: False,  # Temporary placeholder
    )
    
    # Now set the actual work function that has access to the thread
    thread.task = lambda: periodic_failover_task(
        client=client,
        valkey_server_path=valkey_server_path,
        config_dir=config_dir,
        stdout_dir=stdout_dir,
        modules=modules,
        test_recovery=test_recovery,
        password=password,
        failed_ports_tracker=thread.failed_ports,
        failover_state=failover_state,
        entry_point_port=entry_point_port,
    )
    
    thread.run()
    return thread
