"""
Integration tests for FLAT VECTOR_RANGE scans running concurrently with each
other and with index writes.
"""

import random
import struct
import threading
import time
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper


def float_to_bytes(floats):
    """Pack a list of floats into a little-endian binary blob."""
    return struct.pack(f"<{len(floats)}f", *floats)


def l2_distance(a, b):
    """Compute L2 (squared Euclidean) distance between two float lists."""
    return sum((x - y) ** 2 for x, y in zip(a, b, strict=True))


class TestVectorRangeFlatConcurrency(ValkeySearchTestCaseDebugMode):
    """FLAT VECTOR_RANGE scans running concurrently with each other and with
    index writes."""

    DIM = 4

    def _create_flat(self, client, prefixes):
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", str(len(prefixes)), *prefixes,
            "SCHEMA", "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", str(self.DIM), "DISTANCE_METRIC", "L2",
        ) == b"OK"

    def _range(self, client, radius, blob):
        res = client.execute_command(
            "FT.SEARCH", "idx", "@vec:[VECTOR_RANGE $r $b]", "NOCONTENT",
            "LIMIT", "0", "10000", "PARAMS", "4", "r", str(radius), "b", blob,
        )
        keys = {k.decode() for k in res[1:]}
        assert res[0] == len(keys)
        return keys

    def test_flat_vector_range_scans_run_concurrently(self):
        """
        With one FLAT VECTOR_RANGE scan parked inside the scan (at the
        cancellation poll every 100 vectors), a second scan on the same index
        reaches the same point instead of waiting for the first to finish.
        """
        client = self.server.get_new_client()
        self._create_flat(client, ["doc:"])
        for i in range(500):
            client.hset(f"doc:{i}", "vec", float_to_bytes([float(i)] * self.DIM))
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        blob = float_to_bytes([0.0] * self.DIM)

        results = [None, None]

        def scan(slot):
            results[slot] = self._range(self.server.get_new_client(), 1e12, blob)

        assert client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "Cancel") == b"OK"
        threads = [threading.Thread(target=scan, args=(i,)) for i in range(2)]
        try:
            for t in threads:
                t.start()
            waiters.wait_for_true(lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "Cancel") >= 2)
        finally:
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "Cancel")
            for t in threads:
                t.join()
        expected = {f"doc:{i}" for i in range(500)}
        assert results == [expected, expected]

    def test_flat_vector_range_exact_under_writes(self):
        """
        Concurrent FLAT VECTOR_RANGE readers stay exact while writers update
        and delete other keys of the index: keys that are never written are
        always reported exactly, and once writes stop the result matches a
        brute-force scan of the data.
        """
        client = self.server.get_new_client()
        self._create_flat(client, ["s:", "w:"])
        rng = random.Random(1234)

        def rand_vec():
            return [rng.uniform(-1.0, 1.0) for _ in range(self.DIM)]

        query = [0.0] * self.DIM
        blob = float_to_bytes(query)
        radius = 1.0
        stable = {f"s:{i}": rand_vec() for i in range(600)}
        for key, vec in stable.items():
            client.hset(key, "vec", float_to_bytes(vec))
        # float32 distances, as the index stores and compares them
        f32 = lambda v: list(struct.unpack(f"<{self.DIM}f", float_to_bytes(v)))
        stable_inside = {
            k for k, v in stable.items() if l2_distance(f32(v), query) <= radius
        }
        assert 0 < len(stable_inside) < len(stable)
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")

        stop = threading.Event()
        errors = []
        scans = [0]

        def reader():
            c = self.server.get_new_client()
            try:
                while not stop.is_set():
                    keys = self._range(c, radius, blob)
                    got = {k for k in keys if k.startswith("s:")}
                    if got != stable_inside:
                        errors.append(("stable keys differ", got ^ stable_inside))
                    scans[0] += 1
            except Exception as e:  # noqa: BLE001 - reported below
                errors.append(repr(e))

        def writer(seed):
            c = self.server.get_new_client()
            wrng = random.Random(seed)
            try:
                while not stop.is_set():
                    key = f"w:{wrng.randrange(200)}"
                    if wrng.random() < 0.2:
                        c.delete(key)
                    else:
                        vec = [wrng.uniform(-1.5, 1.5) for _ in range(self.DIM)]
                        c.hset(key, "vec", float_to_bytes(vec))
            except Exception as e:  # noqa: BLE001 - reported below
                errors.append(repr(e))

        threads = [threading.Thread(target=reader) for _ in range(4)]
        threads += [threading.Thread(target=writer, args=(s,)) for s in range(2)]
        for t in threads:
            t.start()
        time.sleep(2)
        stop.set()
        for t in threads:
            t.join()
        assert errors == []
        assert scans[0] > 0

        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        expected = set(stable_inside)
        for i in range(200):
            raw = client.hget(f"w:{i}", "vec")
            if raw is not None:
                vec = list(struct.unpack(f"<{self.DIM}f", raw))
                if l2_distance(vec, query) <= radius:
                    expected.add(f"w:{i}")
        assert self._range(client, radius, blob) == expected
