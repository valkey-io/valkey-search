"""
Pytest configuration and collision-free port tracking for Valkey Search integration tests.
Ensures concurrent workers allocated via pytest-xdist never collide on client ports,
cluster bus ports, or search coordinator ports.
"""

import fcntl
import logging
import os
import socket
import tempfile
from pathlib import Path
import pytest

# If running under pytest-xdist, isolate the LOGS_DIR per worker process
worker_id = os.environ.get("PYTEST_XDIST_WORKER")
if worker_id and "LOGS_DIR" in os.environ:
    base_logs_dir = os.environ["LOGS_DIR"]
    if not base_logs_dir.endswith(f"/{worker_id}"):
        worker_logs_dir = os.path.join(base_logs_dir, worker_id)
        os.environ["LOGS_DIR"] = worker_logs_dir
        os.makedirs(worker_logs_dir, exist_ok=True)


class SafePortTracker(object):
    """Provides collision-free ports for valkey-server across concurrent pytest workers.

    Port ranges are partitioned by xdist worker ID into disjoint blocks.
    Within each worker's block, ports are allocated monotonically with wraparound
    to prevent reusing sockets in TIME_WAIT.
    Persistent file locks (flock) without unlinking protect against cross-process
    and external collisions without inode recreation races.
    """

    CLUSTER_BUS_PORT_OFFSET = 10000
    SEARCH_COORDINATOR_PORT_OFFSET = 20294
    SEARCH_COORDINATOR_TLS_PORT_OFFSET = 20295

    # Worker base ports: [10000, 19999] (stride = 100 ports per worker, up to 100 workers)
    # Cluster bus ports: [20000, 29999]
    # Coordinator ports: [30294, 40294]
    # None of the three bands overlap with each other.
    BASE_PORT = 10000
    PORTS_PER_WORKER = 100
    MAX_WORKERS = 100

    LOCKS_DIR = os.path.join(
        tempfile.gettempdir(),
        f"valkey_port_locks_{os.getuid() if hasattr(os, 'getuid') else 0}",
    )

    # Worker-local monotonic offset for round-robin port cycling
    _worker_port_offset = 0

    def __init__(self, node_id=None):
        self.node_id = node_id
        os.makedirs(self.LOCKS_DIR, exist_ok=True)
        self.locked_fds = {}  # port -> fd

        w_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
        if w_id.startswith("gw"):
            try:
                self.worker_idx = int(w_id[2:])
            except ValueError:
                self.worker_idx = 0
        else:
            self.worker_idx = 0

        if self.worker_idx >= self.MAX_WORKERS:
            raise RuntimeError(
                f"Worker index {self.worker_idx} exceeds maximum supported workers ({self.MAX_WORKERS})"
            )

        self.worker_start_port = (
            self.BASE_PORT + self.worker_idx * self.PORTS_PER_WORKER
        )
        self.worker_end_port = self.worker_start_port + self.PORTS_PER_WORKER

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for port, fd in list(self.locked_fds.items()):
            self._unlock_fd(fd)
        self.locked_fds.clear()

    @classmethod
    def _unlock_fd(cls, fd):
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    def _try_lock_port(self, port):
        lock_path = os.path.join(self.LOCKS_DIR, f"port_{port}.lock")
        fd = None
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            return None

        # Verify the port can actually be bound
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("0.0.0.0", port))
            except OSError:
                self._unlock_fd(fd)
                return None

        return fd

    def get_unused_port(self):
        num_candidates = self.PORTS_PER_WORKER
        for _ in range(num_candidates):
            candidate_port = (
                self.worker_start_port + SafePortTracker._worker_port_offset
            )
            SafePortTracker._worker_port_offset = (
                SafePortTracker._worker_port_offset + 1
            ) % self.PORTS_PER_WORKER

            ports_to_lock = [
                candidate_port,
                candidate_port + self.CLUSTER_BUS_PORT_OFFSET,
                candidate_port + self.SEARCH_COORDINATOR_PORT_OFFSET,
            ]
            if candidate_port == 6378:
                ports_to_lock.append(
                    candidate_port + self.SEARCH_COORDINATOR_TLS_PORT_OFFSET
                )

            attempt_fds = {}
            success = True
            for p in ports_to_lock:
                fd = self._try_lock_port(p)
                if fd is None:
                    success = False
                    break
                attempt_fds[p] = fd

            if success:
                self.locked_fds.update(attempt_fds)
                return candidate_port

            # Release locks acquired in this failed attempt
            for fd in attempt_fds.values():
                self._unlock_fd(fd)

        raise RuntimeError(
            f"Failed to find an available port in worker {self.worker_idx} range "
            f"[{self.worker_start_port}, {self.worker_end_port}) after {num_candidates} attempts"
        )


# Monkey-patch valkeytestframework.conftest to use SafePortTracker
try:
    import valkeytestframework.conftest
    valkeytestframework.conftest.PortTracker = SafePortTracker
except (ImportError, AttributeError):
    pass


@pytest.fixture(scope="function", autouse=True)
def resource_port_tracker(request):
    """Fixture providing collision-free port tracking per test."""
    with SafePortTracker(request.node.nodeid) as tracker:
        yield tracker


try:
    import valkeytestframework.conftest
    valkeytestframework.conftest.resource_port_tracker = resource_port_tracker
except (ImportError, AttributeError):
    pass
