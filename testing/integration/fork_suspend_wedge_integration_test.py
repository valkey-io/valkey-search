"""Reproducer: an HNSW vector write never returns, which freezes the node.

What this test asserts
----------------------
Two specific defects, each with its own signature. Neither assertion can be
satisfied by a slow machine, a slow command, or a client timeout, which is the
whole point: the original stability-test failures were reported as "Expected zero
failures for memtier run HSET" and as socket timeouts, and those messages are
true of any overloaded node.

  1. A writer task that never returns. Signature: the index mutation queue is
     non-empty and the module's ingest counters do not move for several seconds,
     while a write-worker thread burns close to a full core. Progress stopped
     but the CPU did not, so the task is spinning rather than blocked or merely
     slow. Checked by _assert_writer_pool_progresses.

  2. The node freezing inside fork(). Signature: PING stops being answered while
     the process is alive, and the server log shows it reached
     "Strategy for next backup" but never logged the writer-suspend result from
     valkey_search.cc:1054. Checked by _assert_survives_bgsave.

Where the defect is
-------------------
Sampling a spinning write-worker and resolving the instruction pointers with
eu-addr2line puts every sample on one line:

    third_party/hnswlib/hnswalg.h:1863
      dist_t d = EvaluateDistance(data_point, GetDataByInternalId(cand));

which is the body of the greedy descent at hnswalg.h:1850, inside the insert
path of hnswlib addPoint:

    for (int level = maxlevelcopy; level > curlevel; level--) {
      bool changed = true;
      while (changed) {
        changed = false;
        std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
        int size = getListCount(get_linklist(currObj, level));
        for (int i = 0; i < size; i++) {
          tableint cand = datal[i];
          dist_t d = EvaluateDistance(data_point, GetDataByInternalId(cand));
          if (d < curdist) { curdist = d; currObj = cand; changed = true; }
        }
      }
    }

There is no visited set and no iteration cap, so the walk is only bounded by
curdist decreasing. hnswlib::updatePoint -> repairConnectionsForUpdate
(hnswalg.h:1686) contains the identical construct and is the other way in; a
capture from a 3-node cluster run landed there instead of here.

Observed with 31 elements in the index, one record in the queue, one writer at
100% of a core and the other four writers at zero. So it needs neither a large
index nor concurrency between writers.

Why the workload looks like this
-------------------------------
stability_runner.py builds memtier with

    --random-data --command='HSET __key__ ...' --command-key-pattern=P -d 400

so it rewrites the same key set forever with fresh random bytes. Random bytes
reinterpreted as float32 span the entire range, including non-finite values, and
overwriting an indexed key routes addPoint into the update path. This test does
the same thing deliberately: a fixed key set, payloads from os.urandom, and
several client threads.

FLAT is the control for both assertions. It has no graph to descend, so if a
FLAT case ever fails, suspect the harness or the machine rather than the index.
"""

import logging
import os
import random
import shutil
import subprocess
import threading
import time
from typing import Dict, List, Optional, Tuple

import numpy as np
import valkey
from absl.testing import absltest
from absl.testing import parameterized

import utils

_PORT = 7300
_INDEX_NAME = "fork_wedge_repro"
_VECTOR_FIELD = "embedding"
_TAG_FIELD = "tag"
_VECTOR_DIMENSIONS = 100

# Small and fixed, so writes land on labels that already exist and the writers
# concentrate on the same graph nodes. A single record is enough to hang, so this
# is about reaching the state quickly, not about volume.
_KEY_COUNT = 2000

_WRITER_THREADS = 8
_QUERY_THREADS = 2
_WRITE_PIPELINE_DEPTH = 20

# Matches the stability suite, where this was found.
_MODULE_ARGS = "--reader-threads 2 --writer-threads 5 --log-level notice"

_BGSAVE_INTERVAL_SEC = 1.0

# Generous, because a client timeout must never be what fails this test. The
# assertions below look at server-side progress counters instead.
_CLIENT_TIMEOUT_SEC = 120.0

# How long the ingest counters may stay frozen with work outstanding before the
# writer pool is declared stuck. Well beyond any single legitimate vector insert.
_STALL_SECONDS = 10.0
# A thread above this share of one core is spinning, not blocked. A worker doing
# real work on a backlog also moves the counters, which is checked separately.
_SPIN_CPU_PERCENT = 70.0

_PROGRESS_POLL_SEC = 1.0
_SEED_DRAIN_TIMEOUT_SEC = 180.0
_LOAD_DURATION_SEC = 120.0

# A wedged node still completes the TCP handshake -- the kernel backlog does
# that -- and then never replies, so the liveness probe must be bounded even
# though the load clients are not.
_PING_TIMEOUT_SEC = 2.0
_MISSED_PINGS_FOR_WEDGE = 3

# Ordered stages of the fork-prepare handshake and the log line proving the
# server finished each one.
_FORK_STAGES: Tuple[Tuple[str, str], ...] = (
    ("bgsave_requested", "Strategy for next backup"),
    ("writer_pool_suspended", "suspend writer worker thread pool returned"),
    ("reader_pool_suspended", "suspend reader worker thread pool returned"),
    ("utility_pool_suspended", "suspend utility worker thread pool returned"),
    ("grpc_suspended", "suspend gRPC returned"),
    ("fork_completed", "Background saving started."),
)

_STAGE_DIAGNOSIS = {
    "bgsave_requested": (
        "inside fork(), in writer_thread_pool_->SuspendWorkers() "
        "(src/valkey_search.cc:1053), waiting for a writer that never returns. "
        "AtForkPrepare is a pthread_atfork prepare handler, so the process "
        "cannot serve anything while this waits, and the "
        "MaxWorkerSuspensionSecs valve in OnServerCronCallback is unreachable "
        "because it runs on this same thread"
    ),
    "writer_pool_suspended": (
        "in reader_thread_pool_->SuspendWorkers() (src/valkey_search.cc:1057): "
        "a reader worker never returned from its task"
    ),
    "reader_pool_suspended": (
        "in utility_thread_pool_->SuspendWorkers() (src/valkey_search.cc:1061)"
    ),
    "utility_pool_suspended": (
        "in coordinator::GRPCSuspender::Instance().Suspend() "
        "(src/valkey_search.cc:1065)"
    ),
    "grpc_suspended": (
        "past every pool suspension, so the stall is in the engine's fork path "
        "rather than in the module"
    ),
    "fork_completed": (
        "past the fork handshake, so the stall is somewhere other than "
        "AtForkPrepare"
    ),
}

# Frames that identify the defect in a captured stack. Used to enrich the
# failure message when elfutils is present; never used to decide pass or fail,
# so the test does not depend on the tool being installed.
_CULPRIT_FRAMES = (
    "repairConnectionsForUpdate",
    "addPoint",
    "AddRecordImpl",
    "EvaluateDistance",
)


# Which kind of vector payload to write. This is the variable that decides
# whether the defect reproduces, so it is switchable in order to demonstrate
# that rather than just assert it.
#
#   "urandom" (default) - dim*4 bytes straight from urandom, which is what
#                         memtier's --random-data produces. Reinterpreted as
#                         float32 these span the entire representable range,
#                         and roughly a third of 100-dimension vectors contain
#                         at least one NaN or infinity (a random 32-bit pattern
#                         is non-finite once every 256 values).
#   "float"             - ordinary unit-norm float32 vectors, the shape a real
#                         embedding model emits.
_PAYLOAD_MODE = os.environ.get("FORK_WEDGE_PAYLOAD", "urandom")


def _random_vector() -> bytes:
    """A dim*4 byte payload in whichever mode is selected."""
    if _PAYLOAD_MODE == "float":
        vector = np.random.rand(_VECTOR_DIMENSIONS).astype(np.float32)
        norm = float(np.linalg.norm(vector))
        if norm > 0:
            vector = vector / norm
        return vector.astype(np.float32).tobytes()
    return os.urandom(_VECTOR_DIMENSIONS * 4)


def _client(timeout_sec: float = _CLIENT_TIMEOUT_SEC) -> valkey.Valkey:
    return valkey.Valkey(
        host="127.0.0.1",
        port=_PORT,
        socket_timeout=timeout_sec,
        socket_connect_timeout=timeout_sec,
    )


def _create_index(client: valkey.Valkey, index_type: str):
    if index_type == "HNSW":
        vector_definition = utils.HNSWVectorDefinition(
            vector_dimensions=_VECTOR_DIMENSIONS,
            m=10,
            ef_construction=10,
            ef_runtime=10,
        )
    else:
        vector_definition = utils.FlatVectorDefinition(
            vector_dimensions=_VECTOR_DIMENSIONS
        )
    return utils.create_index(
        client,
        _INDEX_NAME,
        utils.StoreDataType.HASH.name,
        {
            _VECTOR_FIELD: vector_definition,
            _TAG_FIELD: utils.TagDefinition(),
        },
        # Not a ValkeyCluster client, so there is no node to target.
        target_nodes=None,
    )


def _decode(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class _Progress:
    """Server-side indexing progress, as the module itself reports it.

    Everything here is read from the server rather than inferred from client
    latency, so a stall verdict cannot be produced by a slow client.
    """

    __slots__ = ("queue_size", "vectors_ingested", "upserts", "at")

    def __init__(
        self,
        queue_size: Optional[int],
        vectors_ingested: Optional[int],
        upserts: Optional[int],
    ):
        self.queue_size = queue_size
        self.vectors_ingested = vectors_ingested
        self.upserts = upserts
        self.at = time.monotonic()

    @property
    def usable(self) -> bool:
        return self.queue_size is not None and self.upserts is not None

    @property
    def work_outstanding(self) -> bool:
        return bool(self.queue_size)

    def counters(self) -> Tuple[Optional[int], Optional[int]]:
        return (self.vectors_ingested, self.upserts)

    def __str__(self) -> str:
        return (
            f"queue={self.queue_size} vectors_ingested={self.vectors_ingested} "
            f"upserts={self.upserts}"
        )


def _read_progress(client: valkey.Valkey) -> _Progress:
    """Sample FT.INFO and INFO. Missing values come back as None, never raise.

    A wedged node answers neither, and that is a legitimate observation rather
    than an error, so every failure mode here degrades to None.
    """
    queue_size = None
    vectors = None
    upserts = None
    try:
        info = client.execute_command("FT.INFO", _INDEX_NAME)
        flat = [_decode(v) for v in info]
        if "mutation_queue_size" in flat:
            queue_size = int(flat[flat.index("mutation_queue_size") + 1])
    except Exception:  # pylint: disable=broad-except
        pass
    try:
        raw = client.execute_command("INFO", "everything")
        # valkey-py registers a response callback for INFO that parses the
        # payload into a dict, so this is usually already parsed. Handle the raw
        # form too rather than depending on that callback staying registered.
        if isinstance(raw, dict):
            fields = {
                _decode(k): _decode(v) for k, v in raw.items()
            }
        else:
            fields = {}
            for line in _decode(raw).splitlines():
                if ":" in line and not line.startswith("#"):
                    key, _, value = line.partition(":")
                    fields[key.strip()] = value.strip()
        if "search_ingest_field_vector" in fields:
            vectors = int(fields["search_ingest_field_vector"])
        if "search_time_slice_upserts" in fields:
            upserts = int(fields["search_time_slice_upserts"])
    except Exception:  # pylint: disable=broad-except
        pass
    return _Progress(queue_size, vectors, upserts)


def _worker_cpu(pid: int) -> Dict[str, Tuple[str, int]]:
    """tid -> (thread name, cpu ticks) for the module's worker threads.

    From procfs, so it works on a process that has stopped answering commands.
    """
    out: Dict[str, Tuple[str, int]] = {}
    task_dir = f"/proc/{pid}/task"
    try:
        tids = os.listdir(task_dir)
    except OSError:
        return out
    for tid in tids:
        try:
            with open(f"{task_dir}/{tid}/stat", "r") as f:
                stat = f.read()
        except OSError:
            continue
        # comm is parenthesised and may contain spaces, so split around it.
        start = stat.find("(")
        end = stat.rfind(")")
        if start < 0 or end < 0:
            continue
        name = stat[start + 1 : end]
        fields = stat[end + 2 :].split()
        # utime and stime are fields 14 and 15 of /proc/pid/stat, which are
        # indexes 11 and 12 once pid, comm and state are removed.
        try:
            ticks = int(fields[11]) + int(fields[12])
        except (IndexError, ValueError):
            continue
        out[tid] = (name, ticks)
    return out


def _cpu_percentages(
    before: Dict[str, Tuple[str, int]],
    after: Dict[str, Tuple[str, int]],
    elapsed_sec: float,
    name_prefix: str = "",
) -> List[Tuple[float, str]]:
    """(percent of one core, label) per thread, busiest first."""
    if elapsed_sec <= 0:
        return []
    ticks_per_sec = os.sysconf("SC_CLK_TCK")
    rows = []
    for tid, (name, end_ticks) in after.items():
        if tid not in before or not name.startswith(name_prefix):
            continue
        delta = end_ticks - before[tid][1]
        if delta <= 0:
            continue
        percent = 100.0 * delta / ticks_per_sec / elapsed_sec
        rows.append((percent, f"{name}(tid {tid}) {percent:.0f}% of a core"))
    rows.sort(reverse=True)
    return rows


def _capture_stack(pid: int) -> str:
    """Best-effort stack capture, for the failure message only.

    eu-stack rather than gdb: libsearch.so carries hundreds of MB of debug info
    and gdb spends minutes loading it, long enough to lose the process.
    """
    tool = shutil.which("eu-stack")
    if not tool:
        return "eu-stack not installed, no stack captured"
    try:
        result = subprocess.run(
            [tool, "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return f"eu-stack failed: {e}"
    interesting = [
        line.strip()
        for line in result.stdout.splitlines()
        if any(frame in line for frame in _CULPRIT_FRAMES)
    ]
    if not interesting:
        return "stack captured, no hnswlib frames in it"
    # Dedupe while keeping order; the same frames repeat across threads.
    seen = set()
    unique = []
    for line in interesting:
        if line not in seen:
            seen.add(line)
            unique.append(line)
    return "\n      ".join(unique[:8])


def _last_fork_stage(log_text: str) -> Optional[str]:
    """Furthest fork-prepare stage the server is known to have completed.

    Scans the whole log: reaching a stage even once proves the handshake can get
    that far, which is what narrows down where it is stuck now.
    """
    reached = None
    for stage, marker in _FORK_STAGES:
        if marker in log_text:
            reached = stage
        else:
            break
    return reached


class _LoadThread(threading.Thread):
    """A stoppable client loop that never lets its own errors fail the test.

    Every assertion in this file is made from server-side state, so client
    exceptions are counted and swallowed. The stop flag is deliberately not
    named _stop: that attribute belongs to threading.Thread and shadowing it
    breaks join().
    """

    def __init__(self, name: str):
        super().__init__(name=name, daemon=True)
        self._stop_requested = threading.Event()
        self.operations = 0
        self.errors = 0

    def stop(self) -> None:
        self._stop_requested.set()

    def _step(self, client: valkey.Valkey) -> None:
        raise NotImplementedError

    def run(self) -> None:
        client = _client()
        while not self._stop_requested.is_set():
            try:
                self._step(client)
                self.operations += 1
            except (
                valkey.exceptions.ConnectionError,
                valkey.exceptions.TimeoutError,
            ):
                self.errors += 1
                try:
                    client.close()
                except Exception:  # pylint: disable=broad-except
                    pass
                client = _client()
            except valkey.exceptions.ResponseError:
                self.errors += 1
        try:
            client.close()
        except Exception:  # pylint: disable=broad-except
            pass


class _SeedLoad(_LoadThread):
    """Writes the key set once, on its own thread.

    On its own thread so the monitor below keeps sampling even when these
    clients block: an HSET blocks its client until the mutation is processed, so
    a stuck writer stalls the seeding client too, and seeding inline would make
    the client stall pre-empt the diagnosis.
    """

    def __init__(self, name: str):
        super().__init__(name)
        self.finished = threading.Event()

    def run(self) -> None:
        client = _client()
        try:
            pipe = client.pipeline(transaction=False)
            for i in range(_KEY_COUNT):
                if self._stop_requested.is_set():
                    break
                pipe.hset(
                    f"doc:{i}",
                    mapping={
                        _VECTOR_FIELD: _random_vector(),
                        _TAG_FIELD: f"t{i % 16}",
                    },
                )
                if i % 200 == 0:
                    pipe.execute()
                    self.operations += 1
            pipe.execute()
            self.operations += 1
        except Exception:  # pylint: disable=broad-except
            self.errors += 1
        finally:
            self.finished.set()
            try:
                client.close()
            except Exception:  # pylint: disable=broad-except
                pass


class _OverwriteLoad(_LoadThread):
    """Rewrites already-indexed keys with fresh vectors.

    Keys come only from the seeded range, so hnswlib always finds the label
    present and takes the in-place update path rather than appending.
    """

    def _step(self, client: valkey.Valkey) -> None:
        pipe = client.pipeline(transaction=False)
        for _ in range(_WRITE_PIPELINE_DEPTH):
            pipe.hset(
                f"doc:{random.randrange(_KEY_COUNT)}",
                mapping={
                    _VECTOR_FIELD: _random_vector(),
                    _TAG_FIELD: f"t{random.randrange(16)}",
                },
            )
        pipe.execute()


class _QueryLoad(_LoadThread):
    def _step(self, client: valkey.Valkey) -> None:
        client.execute_command(
            "FT.SEARCH",
            _INDEX_NAME,
            f"*=>[KNN 10 @{_VECTOR_FIELD} $vec]",
            "PARAMS",
            2,
            "vec",
            _random_vector(),
            "DIALECT",
            2,
        )


class _BgsaveLoad(_LoadThread):
    """Fires BGSAVE, which is what makes the main thread run AtForkPrepare."""

    def _step(self, client: valkey.Valkey) -> None:
        try:
            # Deliberately not valkey-py's bgsave(), which defaults to
            # BGSAVE SCHEDULE and would defer instead of forking now.
            client.execute_command("BGSAVE")
        except valkey.exceptions.ResponseError as e:
            if "in progress" not in str(e).lower():
                raise
        self._stop_requested.wait(_BGSAVE_INTERVAL_SEC)


class ForkSuspendWedgeTest(parameterized.TestCase):

    def setUp(self):
        super().setUp()
        self.server: Optional[utils.ValkeyServerUnderTest] = None
        self.load_threads: List[_LoadThread] = []
        # Per test case, not just per port: every case reuses _PORT, and the
        # BGSAVEs leave a dump.rdb behind that the next case's node would load,
        # bringing the previous case's index with it.
        case = self._testMethodName
        self.stdout_path = os.path.join(
            os.environ["TEST_UNDECLARED_OUTPUTS_DIR"],
            f"{case}_{_PORT}_stdout.txt",
        )
        self.stdout_file = open(self.stdout_path, "w")
        node_dir = os.path.join(os.environ["TEST_TMPDIR"], f"{case}_{_PORT}")
        shutil.rmtree(node_dir, ignore_errors=True)
        os.makedirs(node_dir)

        self.server = utils.start_valkey_process(
            os.environ["VALKEY_SERVER_PATH"],
            _PORT,
            node_dir,
            self.stdout_file,
            {
                "loglevel": "notice",
                "enable-debug-command": "yes",
                # Only the explicit BGSAVEs below should fork, so a freeze is
                # attributable to them.
                "save": '""',
            },
            {os.environ["VALKEY_SEARCH_PATH"]: _MODULE_ARGS},
        )

    def tearDown(self):
        for thread in self.load_threads:
            thread.stop()
        for thread in self.load_threads:
            # Bounded: against a stuck node these threads are parked on a
            # socket, and a hung join would hide the real assertion.
            thread.join(timeout=30)
        if self.server is not None:
            self.server.terminate()
            if self.server.needed_sigkill:
                logging.error(
                    "Port %d ignored SIGTERM and had to be SIGKILLed, so its "
                    "main thread was still blocked at teardown",
                    self.server.port,
                )
        try:
            self.stdout_file.close()
        except OSError:
            pass
        super().tearDown()

    def _read_log(self) -> str:
        self.stdout_file.flush()
        with open(self.stdout_path, "r", errors="replace") as f:
            return f.read()

    def _fail_stuck_writer(
        self,
        index_type: str,
        phase: str,
        stalled_for: float,
        frozen: _Progress,
        spinning: List[Tuple[float, str]],
    ) -> None:
        """Report a writer task that stopped returning, with the evidence."""
        assert self.server is not None
        stack = _capture_stack(self.server.process_handle.pid)
        self.fail(
            f"A {index_type} writer task stopped returning during {phase}.\n"
            f"  The index made no progress for {stalled_for:.0f}s while a "
            "worker burned CPU, so this is a spin, not slowness and not a "
            "client timeout.\n"
            f"  frozen server-side counters: {frozen}\n"
            f"  threads burning CPU meanwhile: "
            f"{[label for _, label in spinning]}\n"
            f"  captured frames:\n      {stack}\n"
            "  Expected location: the greedy descent at "
            "third_party/hnswlib/hnswalg.h:1850, whose body is "
            "hnswalg.h:1863 EvaluateDistance(data_point, "
            "GetDataByInternalId(cand)). It has no visited set and no "
            "iteration cap, so it only terminates while curdist keeps "
            "decreasing. Reached from VectorHNSW::AddRecordImpl "
            "(src/indexes/vector_hnsw.cc:154) via hnswlib addPoint on insert, "
            "or via updatePoint -> repairConnectionsForUpdate "
            "(hnswalg.h:1686) when an already-indexed key is overwritten.\n"
            f"  node log: {self.stdout_path}"
        )

    def _assert_writer_pool_progresses(
        self, client: valkey.Valkey, index_type: str, phase: str, until
    ) -> None:
        """Fail if indexing stalls while a writer spins, else return.

        `until` is a predicate; polling stops when it returns True. The stall
        verdict needs both halves to hold at once:
          - the module's own ingest counters do not move while the mutation
            queue is non-empty, which rules out "slow but working", and
          - a write-worker is consuming most of a core, which rules out a
            blocked worker and rules out an idle node.
        """
        assert self.server is not None
        pid = self.server.process_handle.pid
        deadline = time.monotonic() + _SEED_DRAIN_TIMEOUT_SEC
        last_counters = None
        frozen_since = None
        frozen_sample = None
        cpu_at_freeze = None

        while time.monotonic() < deadline:
            if until():
                return
            if self.server.terminated():
                return
            sample = _read_progress(client)
            cpu_now = _worker_cpu(pid)

            if not sample.usable:
                # The node is not answering. That is the other defect, and the
                # BGSAVE assertion is what reports it.
                time.sleep(_PROGRESS_POLL_SEC)
                continue

            counters = sample.counters()
            if counters != last_counters or not sample.work_outstanding:
                last_counters = counters
                frozen_since = None
                frozen_sample = None
                cpu_at_freeze = None
            else:
                if frozen_since is None:
                    frozen_since = sample.at
                    frozen_sample = sample
                    cpu_at_freeze = cpu_now
                stalled_for = sample.at - frozen_since
                if stalled_for >= _STALL_SECONDS:
                    spinning = [
                        row
                        for row in _cpu_percentages(
                            cpu_at_freeze or {},
                            cpu_now,
                            stalled_for,
                            name_prefix="write-worker",
                        )
                        if row[0] >= _SPIN_CPU_PERCENT
                    ]
                    if spinning:
                        self._fail_stuck_writer(
                            index_type,
                            phase,
                            stalled_for,
                            frozen_sample or sample,
                            spinning,
                        )
                    # Frozen but nothing is spinning: a different defect, so do
                    # not claim this one. Keep watching.
            time.sleep(_PROGRESS_POLL_SEC)

        self.fail(
            f"{index_type} indexing did not finish {phase} within "
            f"{_SEED_DRAIN_TIMEOUT_SEC:.0f}s and no writer was spinning, so "
            "this is neither the hnswlib descent hang nor a healthy run. Last "
            f"sample: {_read_progress(client)}"
        )

    def _assert_survives_bgsave(self, index_type: str) -> None:
        """Fail if the node stops answering PING while still running."""
        assert self.server is not None
        started = time.monotonic()
        deadline = started + _LOAD_DURATION_SEC
        missed = 0
        first_miss_at: Optional[float] = None

        while time.monotonic() < deadline:
            if self.server.terminated():
                break
            if self.server.is_responsive(timeout_sec=_PING_TIMEOUT_SEC):
                missed = 0
                first_miss_at = None
            else:
                missed += 1
                if first_miss_at is None:
                    first_miss_at = time.monotonic() - started
                logging.warning(
                    "Port %d missed PING %d/%d",
                    self.server.port,
                    missed,
                    _MISSED_PINGS_FOR_WEDGE,
                )
                if missed >= _MISSED_PINGS_FOR_WEDGE:
                    self._fail_wedged(index_type, first_miss_at or 0.0)
            time.sleep(1.0)

        exit_code = self.server.exit_code()
        if exit_code is not None and exit_code < 0:
            self.fail(
                f"Port {_PORT} was killed by signal {-exit_code} during the "
                f"{index_type} load instead of staying up. See the bug report "
                f"in {self.stdout_path}"
            )

    def _fail_wedged(self, index_type: str, wedged_after: float) -> None:
        assert self.server is not None
        spinning_before = _worker_cpu(self.server.process_handle.pid)
        time.sleep(2.0)
        spinning = _cpu_percentages(
            spinning_before,
            _worker_cpu(self.server.process_handle.pid),
            2.0,
        )
        stack = _capture_stack(self.server.process_handle.pid)
        log_text = self._read_log()
        stage = _last_fork_stage(log_text)
        diagnosis = _STAGE_DIAGNOSIS.get(
            stage, "never reached the module's fork-prepare callback at all"
        )
        self.fail(
            f"Port {_PORT} stopped answering PING {wedged_after:.0f}s into the "
            f"{index_type} load while its process stayed alive. A hang, so "
            "there is no crash report.\n"
            f"  BGSAVEs attempted: "
            f"{log_text.count('Strategy for next backup')}\n"
            f"  furthest fork-prepare stage completed: {stage}\n"
            f"  so the main thread is {diagnosis}\n"
            f"  threads burning CPU meanwhile: "
            f"{[label for _, label in spinning] or 'none above 0%'}\n"
            f"  captured frames:\n      {stack}\n"
            f"  node log: {self.stdout_path}"
        )

    @parameterized.named_parameters(
        # Expected to fail until the defect is fixed: this is the reproducer.
        dict(testcase_name="hnsw", index_type="HNSW"),
        # Control: identical load, no graph to descend.
        dict(testcase_name="flat", index_type="FLAT"),
    )
    def test_writes_keep_progressing_and_node_survives_bgsave(
        self, index_type: str
    ):
        monitor = _client()
        _create_index(monitor, index_type)

        # Phase 1: seed. The stall already shows up here, because every insert
        # goes through the same descent.
        seed = _SeedLoad("seed")
        self.load_threads.append(seed)
        seed.start()
        self._assert_writer_pool_progresses(
            monitor,
            index_type,
            "the initial load",
            until=lambda: seed.finished.is_set()
            and not _read_progress(monitor).work_outstanding,
        )

        # Phase 2: overwrite the same keys, which is the update path, with
        # BGSAVE running so a stuck writer becomes a frozen node.
        for i in range(_WRITER_THREADS):
            self.load_threads.append(_OverwriteLoad(f"overwrite-{i}"))
        for i in range(_QUERY_THREADS):
            self.load_threads.append(_QueryLoad(f"query-{i}"))
        self.load_threads.append(_BgsaveLoad("bgsave"))
        for thread in self.load_threads:
            if thread is not seed and not thread.is_alive():
                thread.start()

        self._assert_survives_bgsave(index_type)

        # Only meaningful if load actually reached the server.
        self.assertGreater(
            sum(t.operations for t in self.load_threads),
            0,
            msg=(
                "No load reached the server, so this run proves nothing about "
                "the indexing path"
            ),
        )
        monitor.close()


if __name__ == "__main__":
    absltest.main()
