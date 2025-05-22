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
    def __init__(self, servers: List[ValkeyServerUnderTest]):
        self.servers = servers

    def terminate(self):
        for server in self.servers:
            server.terminate()

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

    for port in ports:
        stdout_path = os.path.join(stdout_directory, f"{port}_stdout.txt")
        stdout_file = open(stdout_path, "w")
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
    cli_stdout_file = open(cli_stdout_path, "w")
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

    return ValkeyClusterUnderTest(processes)


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
    def __init__(self, separator=","):
        self.separator = separator

    def to_arguments(self) -> List[Any]:
        return [
            "TAG",
            "SEPARATOR",
            self.separator,
        ]


class NumericDefinition(AttributeDefinition):
    def to_arguments(self) -> List[Any]:
        return [
            "NUMERIC",
        ]


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
        
        if store_data_type == StoreDataType.JSON.name:
            args.append("$." + name)
            args.append("AS")
          
        args.append(name)
        args.extend(definition.to_arguments())

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
    """

    def __init__(
        self,
        name: str,
        interval: int,
        randomize: bool,
        work_func: Callable[[], bool],
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
        """ """
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


def periodic_ftdrop_task(
    client: valkey.ValkeyCluster,
    index_name: str,
    index_state: IndexState,
) -> bool:
    with index_state.index_lock:
        logging.info("<FT.DROPINDEX> Invoking index drop")
        try:
            drop_index(client, index_name)
            index_state.ft_created = False
        except (
            valkey.exceptions.ConnectionError,
            valkey.exceptions.ResponseError,
        ) as e:
            if not index_state.ft_created and "not found" in str(e):
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
) -> bool:
    with index_state.index_lock:
        try:
            logging.info("<FT.CREATE> Invoking index creation")
            create_index(
                client=client, store_data_type=StoreDataType.HASH.name, index_name=index_name, attributes=attributes
            )
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
            if (
                line.error is not None
                and self.error_predicate is not None
                and not self.error_predicate(line.error)
            ):
                continue
            if line.error is not None:
                logging.error(
                    "<%s> encountered error: %s", self.name, line.error
                )
            self._add_line_to_stats(line)

    def _add_line_to_stats(self, line: MemtierErrorLineInfo):
        if line.error is not None:
            self.failures += 1
        else:
            self.runtime = line.runtime
            self.trailing_ops_sec.insert(0, line.ops_sec)
            if len(self.trailing_ops_sec) > self.trailing_secs:
                self.trailing_ops_sec.pop()
        if self.trailing_ops_sec:
            trailing_ops_sec = sum(self.trailing_ops_sec) / len(
                self.trailing_ops_sec
            )
            if (
                trailing_ops_sec == 0
                and len(self.trailing_ops_sec) == self.trailing_secs
            ):
                self.halted = True
        self.total_ops = line.ops
        self.avg_ops_sec = line.avg_ops_sec

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
    progress_pattern = (
        r"\[RUN #(\d+)"
        r" ([\d\.]+)%?,\s+([\d\.]+)\s+secs\]\s+([\d\.]+)\s+threads:\s+(\d+)\s+ops,\s+([\d\.]+)\s+\(avg:\s+([\d\.]+)\)\s+ops\/sec,\s+([\d\.]+[KMG]B\/sec)\s+\(avg:\s+(\d+\.\d+[KMG]?B\/sec)\),\s+(-nan|[\d\.]+)\s+\(avg:\s+(\d+\.\d+)\)\s+msec\s+latency"
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
        latency = float(match.group(10))
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
