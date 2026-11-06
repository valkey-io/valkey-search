"""Issue #8 integration checks: SVS_VAMANA creates no threads (the installed
pool has size() == 1), search latency under load stays in HNSW's regime, and
8 writer threads ingesting one index concurrently leave it complete."""

import random
import struct
import threading
import time

from valkey.client import Valkey

from ft_info_parser import FTInfoParser
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


DIM = 4
NUM_DOCS = 200
NUM_CLIENTS = 8
LOAD_SECONDS = 2.0
KNN_K = 10
LATENCY_BOUND = 10.0
LAT_DOCS = 2000      # latency test only; the thread-count tests keep NUM_DOCS
LAT_ROUNDS = 3


def _fp32(values):
    return struct.pack(f"<{len(values)}f", *values)


def _doc_vector(i):
    return [float(i)] + [0.0] * (DIM - 1)


def _threads(pid: int) -> int:
    """Thread count of the server process, from /proc/<pid>/status."""
    with open(f"/proc/{pid}/status") as status:
        for line in status:
            if line.startswith("Threads:"):
                return int(line.split()[1])
    raise AssertionError(f"no Threads: line in /proc/{pid}/status")


def _wait_indexed(client: Valkey, index: str, n: int, timeout: float = 60.0):
    """Block until FT.INFO shows n docs, an empty mutation queue and no
    backfill in progress."""
    deadline = time.monotonic() + timeout
    while True:
        info = FTInfoParser(client.execute_command("FT.INFO", index))
        if (info.num_docs >= n and info.mutation_queue_size == 0
                and not info.backfill_in_progress):
            return
        if time.monotonic() > deadline:
            raise AssertionError(
                f"{index}: {info.num_docs}/{n} docs indexed after {timeout}s "
                f"(mutation_queue_size={info.mutation_queue_size})")
        time.sleep(0.05)


def _knn(client: Valkey, index: str, query, k: int = KNN_K):
    return client.execute_command(
        "FT.SEARCH", index, f"*=>[KNN {k} @v $q]",
        "PARAMS", "2", "q", _fp32(query), "DIALECT", "2", "LIMIT", "0", str(k))


def _search_load(clients, index: str, seconds: float, sampler=None,
                 query=None):
    """Run one FT.SEARCH loop per client for `seconds`. Returns the list of
    per-call latencies (seconds) across all clients. `sampler`, if given, is
    called every 100 ms on the main thread while the load runs."""
    stop = threading.Event()
    latencies = [[] for _ in clients]
    errors = []

    def worker(i, client):
        q = query if query is not None else [NUM_DOCS / 2 + 0.5] + [0.0] * (DIM - 1)
        try:
            while not stop.is_set():
                t0 = time.perf_counter()
                reply = _knn(client, index, q)
                latencies[i].append(time.perf_counter() - t0)
                assert reply[0] == KNN_K, reply[0]
        except Exception as exc:  # surfaced on the main thread below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i, c))
               for i, c in enumerate(clients)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        time.sleep(0.1)
        if sampler is not None:
            sampler()
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors[0]
    return [lat for per_client in latencies for lat in per_client]


def _p99(samples):
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(0.99 * len(ordered)))]


class TestSVSThreadpool(ValkeySearchTestCaseBase):

    def _create(self, client: Valkey, index: str, algorithm: str, prefix: str):
        client.execute_command(
            "FT.CREATE", index, "PREFIX", "1", prefix,
            "SCHEMA", "v", "VECTOR", algorithm,
            "6", "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2")

    def _populate(self, client: Valkey, prefix: str):
        for i in range(NUM_DOCS):
            client.hset(f"{prefix}{i}", mapping={"v": _fp32(_doc_vector(i))})

    def _clients(self):
        return [self.server.get_new_client() for _ in range(NUM_CLIENTS)]

    def test_thread_count_unchanged_by_svs_build_ingest_and_search(self):
        """No thread is created at FT.CREATE, at bootstrap, while adding
        points, or under 8 concurrent searches."""
        client: Valkey = self.server.get_new_client()
        pid = self.server.pid()

        baseline = _threads(pid)
        self._create(client, "thr", "SVS_VAMANA", "thr:")
        after_create = _threads(pid)
        self._populate(client, "thr:")
        _wait_indexed(client, "thr", NUM_DOCS)
        after_ingest = _threads(pid)

        during_search = []
        _search_load(self._clients(), "thr", LOAD_SECONDS,
                     sampler=lambda: during_search.append(_threads(pid)))
        assert during_search, "no samples taken during the search load"

        observed = {
            "baseline": baseline,
            "after_create": after_create,
            "after_ingest": after_ingest,
            "during_search_min": min(during_search),
            "during_search_max": max(during_search),
        }
        print(f"threads: {observed}")
        assert all(v == baseline for v in observed.values()), observed

    def test_p99_search_latency_under_8_clients_within_10x_of_hnsw(self):
        """One search occupies one reader thread, like HNSW. Same docs in both
        indexes; three interleaved samples per index so drift on the runner
        hits both alike, compared on the median p99. Client scheduling
        dominates both numbers on a small index, hence the loose bound; the
        thread-count test above is the sharp check."""
        client: Valkey = self.server.get_new_client()
        self._create(client, "lat_svs", "SVS_VAMANA", "lat:")
        self._create(client, "lat_hnsw", "HNSW", "lat:")
        pipe = client.pipeline(transaction=False)
        for i in range(LAT_DOCS):
            pipe.hset(f"lat:{i}", mapping={"v": _fp32(_doc_vector(i))})
            if i % 200 == 199:
                pipe.execute()
        pipe.execute()
        _wait_indexed(client, "lat_svs", LAT_DOCS)
        _wait_indexed(client, "lat_hnsw", LAT_DOCS)

        query = [LAT_DOCS / 2 + 0.5] + [0.0] * (DIM - 1)
        for index in ("lat_svs", "lat_hnsw"):
            for _ in range(50):  # warm up
                _knn(client, index, query)

        clients = self._clients()
        p99s = {"lat_hnsw": [], "lat_svs": []}
        samples = {"lat_hnsw": 0, "lat_svs": 0}
        for _ in range(LAT_ROUNDS):
            for index in ("lat_hnsw", "lat_svs"):
                lat = _search_load(clients, index, LOAD_SECONDS, query=query)
                assert len(lat) >= 100, f"{index}: only {len(lat)} searches"
                p99s[index].append(_p99(lat))
                samples[index] += len(lat)
        p99 = {index: sorted(v)[len(v) // 2] for index, v in p99s.items()}

        detail = (f"median p99 over {LAT_ROUNDS} rounds: "
                  f"svs={p99['lat_svs'] * 1e3:.3f} ms "
                  f"(n={samples['lat_svs']}) vs "
                  f"hnsw={p99['lat_hnsw'] * 1e3:.3f} ms "
                  f"(n={samples['lat_hnsw']}), bound {LATENCY_BOUND}x")
        print(detail)
        assert p99["lat_svs"] <= LATENCY_BOUND * p99["lat_hnsw"], detail


class TestSVSConcurrentWriters(ValkeySearchTestCaseBase):
    """Writers on one SVS_VAMANA index run concurrently; SVS guards the index."""

    NUM_WRITERS = 8
    DOCS_PER_WRITER = 500
    DIM = 8

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.writer-threads"] = str(self.NUM_WRITERS)
        return args

    def test_eight_writers_ingest_one_index_completely(self):
        client: Valkey = self.server.get_new_client()
        pid = self.server.pid()
        n = self.NUM_WRITERS * self.DOCS_PER_WRITER
        rng = random.Random(8)
        vectors = [[rng.uniform(-1.0, 1.0) for _ in range(self.DIM)]
                   for _ in range(n)]

        baseline = _threads(pid)
        client.execute_command(
            "FT.CREATE", "cw", "PREFIX", "1", "cw:",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "6", "TYPE", "FLOAT32", "DIM", str(self.DIM),
            "DISTANCE_METRIC", "L2")

        # Each HSET blocks its client until a writer applies it: 8 clients keep
        # 8 mutations in flight.
        errors = []

        def ingest(w, c):
            try:
                for i in range(w * self.DOCS_PER_WRITER,
                               (w + 1) * self.DOCS_PER_WRITER):
                    c.hset(f"cw:{i}", mapping={"v": _fp32(vectors[i])})
            except Exception as exc:  # surfaced on the main thread below
                errors.append(exc)

        clients = [self.server.get_new_client()
                   for _ in range(self.NUM_WRITERS)]
        threads = [threading.Thread(target=ingest, args=(w, c))
                   for w, c in enumerate(clients)]
        for t in threads:
            t.start()
        during = []
        while any(t.is_alive() for t in threads):
            during.append(_threads(pid))
            time.sleep(0.05)
        for t in threads:
            t.join()
        assert not errors, errors[0]
        _wait_indexed(client, "cw", n)

        info = FTInfoParser(client.execute_command("FT.INFO", "cw"))
        assert info.num_docs == n
        assert info.hash_indexing_failures == 0
        assert info.attributes[0]["index"]["size"] == n
        assert _threads(pid) == baseline and max(during) == baseline, (
            f"threads moved: baseline {baseline}, during {set(during)}, "
            f"after {_threads(pid)}")

        # A KNN with the window widened to the index size returns all n keys.
        reply = client.execute_command(
            "FT.SEARCH", "cw", f"*=>[KNN {n} @v $q EF_RUNTIME {n}]",
            "PARAMS", "2", "q", _fp32(vectors[0]),
            "DIALECT", "2", "NOCONTENT", "LIMIT", "0", str(n))
        assert reply[0] == n, reply[0]
        assert set(reply[1:]) == {f"cw:{i}".encode() for i in range(n)}

        # And every sampled key is its own nearest neighbour.
        for i in range(0, n, n // 100):
            reply = client.execute_command(
                "FT.SEARCH", "cw", f"*=>[KNN 1 @v $q EF_RUNTIME {n}]",
                "PARAMS", "2", "q", _fp32(vectors[i]),
                "DIALECT", "2", "NOCONTENT")
            assert reply[0] == 1 and reply[1] == f"cw:{i}".encode(), (i, reply)
