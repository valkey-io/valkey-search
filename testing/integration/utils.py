"""Utilities for ValkeySearch testing."""

from abc import abstractmethod
import atexit
import fcntl
import logging
import os
import random
import re
import shutil
import subprocess
import threading
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    TextIO,
    Union,
)
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


def is_sanitizer_enabled() -> bool:
    san_build = os.getenv("SAN_BUILD", "").lower()
    return bool(
        os.getenv("ASAN_BUILD")
        or os.getenv("TSAN_BUILD")
        or (san_build and san_build != "no")
        or "-asan" in os.getenv("VALKEY_SEARCH_PATH", "")
        or "-tsan" in os.getenv("VALKEY_SEARCH_PATH", "")
        or "-asan" in os.getenv("MODULE_PATH", "")
        or "-tsan" in os.getenv("MODULE_PATH", "")
    )


class ValkeyServerUnderTest:
    def __init__(self, process_handle: subprocess.Popen[Any], port: int):
        self.process_handle = process_handle
        self.port = port

    def terminate(self):
        try:
            self.process_handle.terminate()
            self.process_handle.wait(timeout=5)
            return
        except ProcessLookupError:
            return
        except subprocess.TimeoutExpired:
            logging.warning(
                "Process on port %d did not exit within 5s of SIGTERM; escalating to SIGKILL",
                self.port,
            )
        except Exception as e:
            logging.warning(
                "Unexpected error during SIGTERM for port %d: %s; escalating to SIGKILL",
                self.port,
                e,
            )

        try:
            self.process_handle.kill()
            self.process_handle.wait(timeout=5)
        except ProcessLookupError:
            pass
        except subprocess.TimeoutExpired:
            logging.error(
                "CRITICAL: Process on port %d failed to terminate even after SIGKILL!",
                self.port,
            )
        except Exception as e:
            logging.error(
                "Error during SIGKILL for process on port %d: %s", self.port, e
            )

    def terminated(self):
        return self.process_handle.poll() is not None

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
    conf_path = os.path.join(directory, f"valkey_{port}.conf")
    with open(conf_path, "w") as f:
        f.write(f"port {port}\n")
        f.write(f"dir {directory}\n")
        if password:
            f.write(f"requirepass {password}\n")
        if "save" not in args:
            # Setting 'save 99999999 1' overrides default periodic snapshot triggers (e.g.
            # 'save 60 10000', which causes fork failures and MISCONF errors under ASan
            # during heavy ingest) while keeping saveparamslen > 0 so Valkey still saves
            # final RDB on graceful shutdown/restart (e.g. DEBUG RESTART).
            f.write("save 99999999 1\n")
        for k, v in args.items():
            f.write(f"{k} {v}\n")
        f.write(f"loadmodule {os.environ['VALKEY_JSON_PATH']}\n")
        for k, v in modules.items():
            mod_args = v
            # Under sanitizers (ASan/TSan), cap reader and writer thread pools to 4 to avoid
            # resource exhaustion and test timeouts while preserving concurrency coverage.
            if is_sanitizer_enabled() and (
                "libsearch" in k or k == os.environ.get("VALKEY_SEARCH_PATH")
            ):
                if (
                    "--reader-threads" not in mod_args
                    and "search.reader-threads" not in args
                ):
                    mod_args = f"{mod_args} --reader-threads 4".strip()
                if (
                    "--writer-threads" not in mod_args
                    and "search.writer-threads" not in args
                ):
                    mod_args = f"{mod_args} --writer-threads 4".strip()
            f.write(f"loadmodule {k} {mod_args}\n")

    command = f"ulimit -c unlimited && exec {valkey_server_path} {conf_path}"
    logging.info("Starting valkey process with config: %s", conf_path)

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
        try:
            process.terminate()
            process.wait(timeout=5)
        except ProcessLookupError:
            pass
        except subprocess.TimeoutExpired:
            logging.warning(
                "Process on port %d did not exit within 5s of SIGTERM; escalating to SIGKILL",
                port,
            )
            try:
                process.kill()
                process.wait(timeout=5)
            except ProcessLookupError:
                pass
            except subprocess.TimeoutExpired:
                logging.error(
                    "CRITICAL: Process on port %d failed to terminate even after SIGKILL!",
                    port,
                )
            except Exception as e:
                logging.error(
                    "Error during SIGKILL for process on port %d: %s", port, e
                )
        except Exception as e:
            logging.warning(
                "Unexpected error terminating process on port %d: %s", port, e
            )
        raise valkey.exceptions.ConnectionError(
            f"Failed to connect to valkey server on port {port}"
        )
    logging.info("Attempting to connect to Valkey: OK")

    return ValkeyServerUnderTest(process, port)


class ValkeyClusterUnderTest:
    active_clusters = set()
    active_cluster = None

    def __init__(
        self,
        servers: List[ValkeyServerUnderTest],
        stdout_files: List[TextIO] = None,
        node_dirs: List[str] = None,
    ):
        self.servers = list(servers)
        self.stdout_files = list(stdout_files or [])
        self.node_dirs = list(node_dirs or [])
        ValkeyClusterUnderTest.active_clusters.add(self)
        ValkeyClusterUnderTest.active_cluster = self

    def register_server(
        self, server: ValkeyServerUnderTest, stdout_file: TextIO | None = None
    ):
        """Register a replacement or restarted server (e.g. during failover)."""
        self.servers = [s for s in self.servers if s.port != server.port] + [server]
        if stdout_file is not None and stdout_file not in self.stdout_files:
            self.stdout_files.append(stdout_file)

    def terminate(self):
        ValkeyClusterUnderTest.active_clusters.discard(self)
        if ValkeyClusterUnderTest.active_cluster is self:
            ValkeyClusterUnderTest.active_cluster = next(
                iter(ValkeyClusterUnderTest.active_clusters), None
            )
        for server in self.servers:
            server.terminate()
        # Close all stdout files
        for stdout_file in self.stdout_files:
            try:
                stdout_file.close()
            except Exception as e:
                logging.warning("Failed to close stdout file: %s", e)
        # Clean up node directories
        for node_dir in self.node_dirs:
            if os.path.exists(node_dir):
                shutil.rmtree(node_dir, ignore_errors=True)

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


def _cleanup_active_clusters():
    for cluster in list(ValkeyClusterUnderTest.active_clusters):
        try:
            cluster.terminate()
        except Exception:
            pass


atexit.register(_cleanup_active_clusters)


_worker_port_cycle = 0


def get_worker_cluster_ports(
    num_nodes: int = 3,
    default_ports: Union[List[int], tuple, None] = None,
) -> List[int]:
    """Return collision-free ports partitioned by worker ID (e.g. pytest-xdist)."""
    global _worker_port_cycle
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if not worker_id and default_ports:
        return list(default_ports)
    worker_idx = 0
    if worker_id and worker_id.startswith("gw"):
        try:
            worker_idx = int(worker_id[2:])
        except ValueError:
            worker_idx = 0
    if worker_idx >= 100:
        raise RuntimeError(
            f"Worker index {worker_idx} exceeds maximum supported workers (100)"
        )
    # Base ports for testing/integration: 7000 + worker_idx * 50
    # Client ports: [7000, 11999] (stride = 50 ports per worker, up to 100 workers)
    # Cluster bus:  [17000, 21999]
    # Coordinator:  [27294, 32294]
    # Cycle across tests on the same worker to prevent immediate port reuse.
    # Dynamically compute slot_size to prevent overlapping port ranges.
    slot_size = max(num_nodes * 2, 12)
    max_slots = 48 // slot_size
    if max_slots < 1:
        raise ValueError(
            f"num_nodes {num_nodes} exceeds maximum supported cluster size for worker port allocation"
        )
    cycle_offset = (_worker_port_cycle % max_slots) * slot_size
    _worker_port_cycle += 1
    base = 7000 + worker_idx * 50 + cycle_offset
    return [base + i * 2 for i in range(num_nodes)]


def get_worker_tmpdir(base_dir: str | None = None) -> str:
    """Return isolated temporary directory for the current worker process."""
    if not base_dir:
        base_dir = os.environ.get("TEST_TMPDIR", "/tmp")
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id and os.path.basename(os.path.normpath(base_dir)) != worker_id:
        path = os.path.join(base_dir, worker_id)
        os.makedirs(path, exist_ok=True)
        return path
    return base_dir


def get_worker_stdoutdir(base_dir: str | None = None) -> str:
    """Return isolated stdout directory for the current worker process."""
    if not base_dir:
        base_dir = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR", "/tmp")
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id and os.path.basename(os.path.normpath(base_dir)) != worker_id:
        path = os.path.join(base_dir, worker_id)
        os.makedirs(path, exist_ok=True)
        return path
    return base_dir


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
) -> ValkeyClusterUnderTest:
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
    directory = get_worker_tmpdir(directory)
    stdout_directory = get_worker_stdoutdir(stdout_directory)
    cluster_args = dict(args)
    processes = []
    stdout_files = []
    node_dirs = []

    try:
        for port in ports:
            stdout_path = os.path.join(stdout_directory, f"{port}_stdout.txt")
            # Open file without buffering - will be closed when cluster terminates
            stdout_file = open(stdout_path, "w", buffering=1)
            stdout_files.append(stdout_file)
            node_dir = os.path.join(directory, f"nodes{port}")
            node_dirs.append(node_dir)
            cluster_args["cluster-enabled"] = "yes"
            cluster_args["cluster-config-file"] = os.path.join(
                node_dir, "nodes.conf"
            )
            cluster_args["cluster-node-timeout"] = "10000"
            if os.path.exists(node_dir):
                shutil.rmtree(node_dir, ignore_errors=True)
            os.makedirs(node_dir, exist_ok=True)
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

            display_args = [
                "***" if i > 0 and valkey_cli_args[i - 1] == "-a" else arg
                for i, arg in enumerate(valkey_cli_args)
            ]
            logging.info("Creating valkey cluster with command: %s", display_args)

            timeout = 60
            now = time.time()
            while time.time() - now < timeout:
                try:
                    subprocess.run(
                        valkey_cli_args,
                        check=True,
                        stdout=cli_stdout_file,
                        stderr=cli_stdout_file,
                        timeout=10,
                    )
                    break
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    time.sleep(1)
            else:
                raise RuntimeError(
                    f"Timed out creating Valkey cluster on ports {ports} after {timeout} seconds. Check {cli_stdout_path}"
                )

        # This is also ugly, but we need to wait for the cluster to be ready. There
        # doesn't seem to be a way to do that with the valkey-server, since it seems to
        # be ready immediately, but returns an CLUSTERDOWN error when we try to search
        # too early, even after checking with ping.
        time.sleep(10)

        return ValkeyClusterUnderTest(processes, stdout_files, node_dirs)
    except Exception:
        for p in processes:
            try:
                p.terminate()
            except Exception as e:
                logging.warning(
                    "Error terminating server process during startup failure cleanup: %s",
                    e,
                )
        for f in stdout_files:
            try:
                f.close()
            except Exception as e:
                logging.warning(
                    "Error closing stdout file during startup failure cleanup: %s",
                    e,
                )
        for d in node_dirs:
            if os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)
        raise


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
    def __init__(self, nostem=False, with_suffix_trie=False, alias=None):
        self.nostem = nostem
        self.with_suffix_trie = with_suffix_trie
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


def drop_index(
    client: valkey.ValkeyCluster,
    index_name: str,
    target_nodes=None,
):
    """Drops an index.

    Args:
      client:
      index_name:
      target_nodes: nodes to send FT.DROPINDEX to. Defaults to valkey-py's own
        routing for search commands, which is the client's default node.
    """
    args = [
        "FT.DROPINDEX",
        index_name,
    ]
    if target_nodes is None:
        return client.execute_command(*args)
    return client.execute_command(*args, target_nodes=target_nodes)


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


def wait_for_search_count(
    client: valkey.ValkeyCluster,
    index_name: str,
    query: str,
    expected: int,
    timeout: int = 30,
):
    """Poll FT.SEARCH until it reports the expected total match count.

    Works in cluster mode: the count comes back from the coordinator's merged
    result, so this waits until every shard has indexed its writes. Preferred
    over a fixed sleep, which is racy under load.
    """
    start = time.time()
    last = None
    while True:
        try:
            got = client.execute_command(
                "FT.SEARCH", index_name, query, "NOCONTENT", "LIMIT", "0", "0",
                target_nodes=client.RANDOM,
            )
            last = got[0]
            if last == expected:
                return
        except valkey.exceptions.ResponseError as e:
            last = str(e)
        if time.time() - start > timeout:
            raise AssertionError(
                f"Timed out waiting for {expected} matches of {query!r} in "
                f"{index_name}; last saw {last}"
            )
        time.sleep(0.1)


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
        # Unhandled exceptions, tracked separately from `failures`. A task
        # reporting False is an expected outcome the test may tolerate; an
        # exception escaping the work function is always a defect, so it must not
        # end up in a bucket that some tasks are allowed to ignore.
        self.crashes = 0
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
                
                # Execute the task. An unexpected exception is recorded and the
                # loop carries on, rather than being allowed to kill the thread:
                # a dead thread silently stops generating load for the rest of
                # the run, and whether the test notices depends only on how many
                # iterations happened to complete first. It is counted in
                # `crashes` rather than `failures` so that it stays visible even
                # for tasks whose ordinary failures are tolerated.
                try:
                    if not self.task():
                        self.failures += 1
                except Exception as e:  # pylint: disable=broad-except
                    logging.exception(
                        "<%s> Unhandled exception in background task: %s",
                        self.name,
                        e,
                    )
                    self.crashes += 1
                self.ops += 1


# ---------------------------------------------------------------------------
# Failover-aware command fan-out
# ---------------------------------------------------------------------------
#
# Background tasks send their commands to every node of the cluster, so the test
# exercises the whole cluster rather than a single node. The only nodes left out
# are the ones the failover task has deliberately taken down: those are tracked
# in failover_state['failed_ports'] from just before the SHUTDOWN until the
# restarted node has rejoined and the topology has converged. A skipped node is
# therefore always a node that genuinely cannot answer.
#
# The node memtier is connected to (the entry point) is never selected as a
# failover victim - see pick_primary_to_fail's exclude_port - so it always stays
# part of the fan-out.

# valkey-py's ValkeyCluster is not thread safe with respect to topology changes:
# nodes_manager.initialize() rebuilds the internal node maps while other threads
# may be iterating them, which surfaces as "RuntimeError: dictionary changed
# size during iteration". All background tasks share one client, so topology
# refreshes and node listings happen under this lock and the result is
# snapshotted into a plain list before use.
_TOPOLOGY_LOCK = threading.RLock()

# The failed-port set observed by the last topology check. Used to re-discover
# the topology only when a failover has actually changed it, instead of on every
# task invocation.
_last_seen_failed_ports: set = set()

# Everything a per-node command can raise: unreachable nodes
# (ConnectionError/TimeoutError/OSError), cluster-level problems
# (ClusterDownError, SlotNotCoveredError, retry exhaustion) and plain command
# errors. ValkeyClusterException is listed explicitly because it derives from
# Exception, not from ValkeyError, so it would otherwise escape these handlers
# and be recorded as a crash rather than as a task failure. Anything outside this
# tuple really is unexpected and is meant to reach RandomIntervalTask's crash
# counter.
_NODE_ERRORS = (
    valkey.exceptions.ValkeyError,
    valkey.exceptions.ValkeyClusterException,
    OSError,
)


def get_failed_ports(failover_state: dict | None) -> set:
    """Snapshot the ports currently down because of a failover."""
    if failover_state is None:
        return set()
    with failover_state["lock"]:
        return set(failover_state["failed_ports"])


def refresh_cluster_topology(
    client: valkey.ValkeyCluster, task_name: str = ""
) -> bool:
    """Re-discover the cluster topology, tolerating transient failures.

    Returns True if the refresh succeeded. A failure is not fatal: the client
    keeps its cached topology, which is usually still usable.
    """
    try:
        with _TOPOLOGY_LOCK:
            client.nodes_manager.initialize()
        return True
    except (RuntimeError, *_NODE_ERRORS) as e:
        # Includes the "all slots are not covered" case that a cluster in the
        # middle of a failover can report, which is why this is not fatal.
        logging.warning(
            "<%s> Failed to refresh cluster topology: %s", task_name, e
        )
        return False


def refresh_topology_if_failover_changed(
    client: valkey.ValkeyCluster,
    failover_state: dict | None,
    task_name: str = "",
) -> None:
    """Refresh the cached topology when the set of failed ports changed.

    A failover changes which nodes are primaries, so the cached view has to be
    re-read when a node goes down and again once it has rejoined. Outside those
    transitions the topology is stable and refreshing it would only add
    contention on the shared client.
    """
    global _last_seen_failed_ports
    failed_ports = get_failed_ports(failover_state)
    with _TOPOLOGY_LOCK:
        changed = failed_ports != _last_seen_failed_ports
        _last_seen_failed_ports = failed_ports
    if changed:
        logging.info(
            "<%s> Failed ports changed to %s, re-discovering topology",
            task_name,
            failed_ports or "{}",
        )
        refresh_cluster_topology(client, task_name)


def get_healthy_nodes(
    client: valkey.ValkeyCluster,
    failover_state: dict | None = None,
    primaries_only: bool = True,
    task_name: str = "",
) -> list:
    """Nodes that commands may be sent to right now.

    Everything except the ports currently down for a failover. Always returns a
    concrete list of ClusterNode - never one of valkey-py's ALL_NODES/PRIMARIES
    sentinels - so callers can address the nodes one at a time and attribute
    successes and errors per node.
    """
    failed_ports = get_failed_ports(failover_state)
    try:
        with _TOPOLOGY_LOCK:
            nodes = list(
                client.get_primaries() if primaries_only else client.get_nodes()
            )
    except (RuntimeError, *_NODE_ERRORS) as e:
        logging.error("<%s> Unable to read cluster topology: %s", task_name, e)
        return []

    healthy = [node for node in nodes if node.port not in failed_ports]
    if failed_ports:
        logging.debug(
            "<%s> Targeting %d node(s), skipping failed ports %s",
            task_name,
            len(healthy),
            failed_ports,
        )
    return healthy


def describe_node(node) -> str:
    return f"{node.host}:{node.port}"


class IndexState:
    """Whether the index is expected to exist, tracked per node.

    A single cluster-wide boolean is not sufficient once index commands fan out:
    a node that was down during a failover can legitimately disagree with the
    rest of the cluster about whether the index exists. `ft_created` is kept as
    the cluster-wide intent (what the last successful create/drop/flush asked
    for) while `_index_on_node` records what was actually observed per port. A
    port with no entry is "unknown" - typically one that just rejoined after a
    restart, whose index set depends on what its RDB happened to hold - and both
    "already exists" and "not found" are tolerated for it.

    Every access happens while holding index_lock: FT.CREATE, FT.DROPINDEX and
    FLUSHDB all serialize on it.
    """

    def __init__(
        self,
        index_lock: threading.Lock,
        ft_created: bool,
        ports: Iterable[int] = (),
    ):
        self.index_lock = index_lock
        self.ft_created = ft_created
        self._index_on_node: Dict[int, bool] = {
            int(port): bool(ft_created) for port in ports
        }
        self._rotation = 0

    def observe(self, port: int, present: bool) -> None:
        """Record whether the node at `port` now holds the index."""
        self._index_on_node[int(port)] = bool(present)

    def observe_all(self, ports: Iterable[int], present: bool) -> None:
        for port in ports:
            self.observe(port, present)

    def forget(self, port: int) -> None:
        """Forget what we knew about a node, making its state unknown."""
        self._index_on_node.pop(int(port), None)

    def forget_all(self, ports: Iterable[int]) -> None:
        for port in ports:
            self.forget(port)

    def has_index(self, port: int) -> bool | None:
        """True/False when known for this port, None when unknown."""
        return self._index_on_node.get(int(port))

    def next_target(self, nodes: list):
        """Pick the next node in round-robin order.

        Used when one node is enough for the whole cluster (coordinator mode);
        rotating keeps every node in the rotation instead of always loading the
        same one.
        """
        node = nodes[self._rotation % len(nodes)]
        self._rotation += 1
        return node


def index_command_targets(
    client: valkey.ValkeyCluster,
    index_state: IndexState,
    failover_state: dict | None,
    task_name: str,
) -> list:
    """Primaries an index command should be sent to.

    Also drops the remembered index state of every node that is currently down,
    because a node that restarts comes back with whatever its RDB held, which
    may or may not include the index.
    """
    refresh_topology_if_failover_changed(client, failover_state, task_name)
    failed_ports = get_failed_ports(failover_state)
    if failed_ports:
        index_state.forget_all(failed_ports)
    return get_healthy_nodes(
        client,
        failover_state,
        primaries_only=True,
        task_name=task_name,
    )


def drop_index_on_node(
    client: valkey.ValkeyCluster,
    node,
    index_name: str,
    index_state: IndexState,
) -> bool:
    """FT.DROPINDEX against one node. Returns False on an unexpected error."""
    try:
        drop_index(client, index_name, target_nodes=[node])
        logging.info("<FT.DROPINDEX> Dropped index on %s", describe_node(node))
        index_state.observe(node.port, False)
        return True
    except valkey.exceptions.ResponseError as e:
        if "not found" in str(e):
            # Expected while this node is not known to hold the index, which
            # covers a node whose state is unknown because it just rejoined.
            was_created = index_state.has_index(node.port) is True
            index_state.observe(node.port, False)
            if was_created:
                logging.error(
                    "<FT.DROPINDEX> %s reports the index is missing although it"
                    " was created there: %s",
                    describe_node(node),
                    e,
                )
                return False
            logging.debug(
                "<FT.DROPINDEX> got expected error from %s: %s",
                describe_node(node),
                e,
            )
            return True
        logging.error(
            "<FT.DROPINDEX> got unexpected error from %s: %s",
            describe_node(node),
            e,
        )
        return False
    except _NODE_ERRORS as e:
        logging.error(
            "<FT.DROPINDEX> %s failed: %s", describe_node(node), e
        )
        return False


def periodic_ftdrop_task(
    client: valkey.ValkeyCluster,
    index_name: str,
    index_state: IndexState,
    failover_state: dict | None = None,
    use_coordinator: bool = False,
) -> bool:
    with index_state.index_lock:
        logging.info("<FT.DROPINDEX> Invoking index drop")
        targets = index_command_targets(
            client, index_state, failover_state, "FT.DROPINDEX"
        )
        if not targets:
            logging.error(
                "<FT.DROPINDEX> No reachable primary to drop the index on"
            )
            return False

        healthy_ports = [node.port for node in targets]
        if use_coordinator:
            # The coordinator's metadata manager removes the index cluster-wide
            # and FT.DROPINDEX only returns OK once the other reachable nodes
            # agree, so one primary covers the whole cluster; the remaining ones
            # would simply answer "not found". The target rotates so the load
            # does not always land on the same node.
            targets = [index_state.next_target(targets)]

        succeeded = True
        for node in targets:
            if not drop_index_on_node(client, node, index_name, index_state):
                succeeded = False

        if succeeded:
            if use_coordinator:
                index_state.observe_all(healthy_ports, False)
            index_state.ft_created = False
        return succeeded


def periodic_ftdrop(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    random_interval: bool,
    index_name: str,
    index_state: IndexState,
    failover_state: dict | None = None,
    use_coordinator: bool = False,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "FT.DROPINDEX",
        interval_sec,
        random_interval,
        lambda: periodic_ftdrop_task(
            client,
            index_name,
            index_state,
            failover_state,
            use_coordinator,
        ),
        failover_state=failover_state,
    )
    thread.run()
    return thread


def create_index_on_node(
    client: valkey.ValkeyCluster,
    node,
    index_name: str,
    attributes: Dict[str, AttributeDefinition],
    index_state: IndexState,
) -> bool:
    """FT.CREATE against one node. Returns False on an unexpected error."""
    try:
        create_index(
            client=client,
            store_data_type=StoreDataType.HASH.name,
            index_name=index_name,
            attributes=attributes,
            target_nodes=[node],
        )
        logging.info("<FT.CREATE> Created index on %s", describe_node(node))
        index_state.observe(node.port, True)
        return True
    except valkey.exceptions.ResponseError as e:
        if "already exists" in str(e):
            # Expected while this node is known to hold the index, and also
            # while its state is unknown: a node that just rejoined may have
            # reloaded the index from its RDB, and in coordinator mode the
            # cluster may have re-created it there.
            was_dropped = index_state.has_index(node.port) is False
            index_state.observe(node.port, True)
            if was_dropped:
                logging.error(
                    "<FT.CREATE> %s reports the index already exists although"
                    " it was dropped there: %s",
                    describe_node(node),
                    e,
                )
                return False
            logging.debug(
                "<FT.CREATE> got expected error from %s: %s",
                describe_node(node),
                e,
            )
            return True
        logging.error(
            "<FT.CREATE> got unexpected error from %s: %s",
            describe_node(node),
            e,
        )
        return False
    except _NODE_ERRORS as e:
        logging.error(
            "<FT.CREATE> %s failed: %s", describe_node(node), e
        )
        return False


def periodic_ftcreate_task(
    client: valkey.ValkeyCluster,
    index_name: str,
    attributes: Dict[str, AttributeDefinition],
    index_state: IndexState,
    failover_state: dict | None = None,
    use_coordinator: bool = False,
) -> bool:
    with index_state.index_lock:
        logging.info("<FT.CREATE> Invoking index creation")
        targets = index_command_targets(
            client, index_state, failover_state, "FT.CREATE"
        )
        if not targets:
            logging.error(
                "<FT.CREATE> No reachable primary to create the index on"
            )
            return False

        healthy_ports = [node.port for node in targets]
        if use_coordinator:
            # In coordinator mode the schema is a cluster-level object: the
            # metadata manager replicates it to every node and FT.CREATE only
            # returns OK once the other reachable nodes have converged. Sending
            # it to the remaining primaries as well would only produce "already
            # exists" from each of them, so one rotating target is used instead.
            targets = [index_state.next_target(targets)]

        succeeded = True
        for node in targets:
            if not create_index_on_node(
                client, node, index_name, attributes, index_state
            ):
                succeeded = False

        if succeeded:
            if use_coordinator:
                index_state.observe_all(healthy_ports, True)
            index_state.ft_created = True
        return succeeded


def periodic_ftcreate(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    random_interval: bool,
    index_name: str,
    attributes: Dict[str, AttributeDefinition],
    index_state: IndexState,
    failover_state: dict | None = None,
    use_coordinator: bool = False,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "FT.CREATE",
        interval_sec,
        random_interval,
        lambda: periodic_ftcreate_task(
            client,
            index_name,
            attributes,
            index_state,
            failover_state,
            use_coordinator,
        ),
        failover_state=failover_state,
    )
    thread.run()
    return thread


def periodic_flushdb_task(
    client: valkey.ValkeyCluster,
    index_state: IndexState,
    use_coordinator: bool,
    failover_state: dict | None = None,
) -> bool:
    with index_state.index_lock:
        logging.info("<FLUSHDB> Invoking flush DB")
        # FLUSHDB is a per-node operation on every primary (that is also
        # valkey-py's default routing for it), minus the ones that are down.
        targets = index_command_targets(
            client, index_state, failover_state, "FLUSHDB"
        )
        if not targets:
            logging.error("<FLUSHDB> No reachable primary to flush")
            return False

        succeeded = True
        for node in targets:
            try:
                client.flushdb(target_nodes=[node])
                logging.info("<FLUSHDB> Flushed %s", describe_node(node))
                if not use_coordinator:
                    # Without the coordinator, flushing a node also deletes its
                    # index schemas. With the coordinator the module re-creates
                    # them, since the schema is a cluster-level object.
                    index_state.observe(node.port, False)
            except _NODE_ERRORS as e:
                logging.error(
                    "<FLUSHDB> got unexpected error from %s: %s",
                    describe_node(node),
                    e,
                )
                succeeded = False

        if succeeded and not use_coordinator:
            index_state.ft_created = False
        return succeeded


def periodic_flushdb(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    random_interval: bool,
    index_state: IndexState,
    use_coordinator: bool,
    failover_state: dict | None = None,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "FLUSHDB",
        interval_sec,
        random_interval,
        lambda: periodic_flushdb_task(
            client, index_state, use_coordinator, failover_state
        ),
        failover_state=failover_state,
    )
    thread.run()
    return thread


def periodic_bgsave_task(
    client: valkey.ValkeyCluster,
    failover_state: dict | None = None,
) -> bool:
    """BGSAVE on every reachable node, primaries and replicas alike."""
    logging.info("<BGSAVE> Invoking background save")
    refresh_topology_if_failover_changed(client, failover_state, "BGSAVE")
    targets = get_healthy_nodes(
        client, failover_state, primaries_only=False, task_name="BGSAVE"
    )
    if not targets:
        logging.error("<BGSAVE> No reachable node to save")
        return False

    succeeded = True
    for node in targets:
        try:
            client.bgsave(target_nodes=[node])
        except valkey.exceptions.ResponseError as e:
            # A save still running from a previous round is expected, not a
            # defect: the interval is shorter than a save of a large keyspace.
            # valkey-py sends BGSAVE SCHEDULE, and SCHEDULE only defers when the
            # busy child is an AOF rewrite - with an RDB child already running
            # the server answers "Background save already in progress". Any other
            # response is counted, so the failure total stays meaningful.
            if "already in progress" in str(e).lower():
                logging.debug(
                    "<BGSAVE> save already running on %s: %s",
                    describe_node(node),
                    e,
                )
                continue
            logging.error(
                "<BGSAVE> unexpected error on %s: %s", describe_node(node), e
            )
            succeeded = False
        except _NODE_ERRORS as e:
            logging.error(
                "<BGSAVE> %s failed: %s", describe_node(node), e
            )
            succeeded = False
    return succeeded


def periodic_bgsave(
    client: valkey.ValkeyCluster,
    interval_sec: int,
    randomize: bool,
    failover_state: dict | None = None,
) -> RandomIntervalTask:
    thread = RandomIntervalTask(
        "BGSAVE",
        interval_sec,
        randomize,
        lambda: periodic_bgsave_task(client, failover_state),
        failover_state=failover_state,
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
        except (
            valkey.exceptions.ConnectionError,
            valkey.exceptions.ValkeyClusterException,
        ) as e:
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
    stdout_file = None
    try:
        config_dir = get_worker_tmpdir(config_dir)
        stdout_dir = get_worker_stdoutdir(stdout_dir)
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
        server = start_valkey_process(
            valkey_server_path=valkey_server_path,
            port=port,
            directory=node_dir,
            stdout_file=stdout_file,
            args=cluster_args,
            modules=modules,
            password=password
        )
        for cluster in ValkeyClusterUnderTest.active_clusters:
            if any(s.port == port for s in cluster.servers) or any(
                d == node_dir for d in cluster.node_dirs
            ):
                cluster.register_server(server, stdout_file)
                break
        else:
            if ValkeyClusterUnderTest.active_cluster is not None:
                ValkeyClusterUnderTest.active_cluster.register_server(
                    server, stdout_file
                )
        return server
        
    except Exception as e:
        if stdout_file is not None:
            try:
                stdout_file.close()
            except Exception:
                pass
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

    def abort(reason: str) -> bool:
        """Clear in_progress on an aborted failover and report the failure.

        Leaving in_progress set would pause every background task and keep the
        memtier processes killed for the remainder of the run, turning one
        aborted failover into a silent stall. Ports already recorded in
        failed_ports stay there: that node really is down, so the fan-out has to
        keep skipping it.
        """
        logging.error("<FAILOVER> Aborting failover: %s", reason)
        if failover_state is not None:
            with failover_state['lock']:
                failover_state['in_progress'] = False
            logging.info(
                "<FAILOVER> Cleared failover_state['in_progress'] after abort"
            )
        return False

    # Signal that failover is starting - this must happen BEFORE shutdown
    if failover_state is not None:
        with failover_state['lock']:
            failover_state['in_progress'] = True
        logging.info("<FAILOVER> Set failover_state['in_progress'] = True")
    
    # Step 1: Get cluster topology
    primarys, replicas = get_cluster_nodes(client)
    if not primarys:
        return abort("no primarys found in cluster")
    
    logging.info("<FAILOVER> Found %d primarys and %d replicas", len(primarys), len(replicas))
    
    # Step 2: Pick a primary to fail (excluding entry point)
    victim = pick_primary_to_fail(primarys, replicas, exclude_port=entry_point_port)
    if not victim:
        return abort("no suitable primary found to fail")
    
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
        return abort(f"failed to shutdown node {victim.addr}")
    
    # Give the node a moment to fully shut down
    time.sleep(2)
    
    # Step 4: Wait for replica promotion
    promotion_success, new_primary_addr = wait_for_new_primary(
        client, victim.node_id, victim.addr, timeout=30
    )
    if not promotion_success:
        return abort("replica promotion did not complete in time")
    
    # Step 5: Wait for cluster OK state
    if not wait_for_cluster_ok(client, timeout=30):
        return abort("cluster did not reach OK state in time")
    
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
