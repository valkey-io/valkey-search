"""FT.CREATE + HSET + FT.SEARCH for every accepted SVS_VAMANA TYPE x
COMPRESSION x DISTANCE_METRIC. BFLOAT16 and SQ8 are rejected at FT.CREATE
(test_svs_smoke.py)."""

import struct
import time

import pytest
from valkey.client import Valkey

from ft_info_parser import FTInfoParser
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


DIM = 4
VECTOR = [1.0, 2.0, 3.0, 4.0]
# IP scores 1 - dot, so a self-match scores 0 only for a unit vector.
UNIT_VECTOR = [0.5, 0.5, 0.5, 0.5]


def _fp32(values):
    return struct.pack(f"<{len(values)}f", *values)


def _fp16(values):
    return struct.pack(f"<{len(values)}e", *values)


ENCODERS = {"FLOAT32": _fp32, "FLOAT16": _fp16}
COMPRESSIONS = ["NONE", "FP16"]
METRICS = ["L2", "IP", "COSINE"]
COMBOS = [(t, c, m) for t in ENCODERS for c in COMPRESSIONS for m in METRICS]


def _threads(pid: int) -> int:
    with open(f"/proc/{pid}/status") as status:
        for line in status:
            if line.startswith("Threads:"):
                return int(line.split()[1])
    raise AssertionError(f"no Threads: line in /proc/{pid}/status")


def _wait_indexed(client: Valkey, index: str, n: int, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while True:
        info = FTInfoParser(client.execute_command("FT.INFO", index))
        if (info.num_docs >= n and info.mutation_queue_size == 0
                and not info.backfill_in_progress):
            return info
        if time.monotonic() > deadline:
            raise AssertionError(
                f"{index}: {info.num_docs}/{n} docs indexed after {timeout}s")
        time.sleep(0.05)


class TestSVSTypeCompressionCombos(ValkeySearchTestCaseBase):

    @pytest.mark.parametrize("vtype,compression,metric", COMBOS,
                             ids=[f"{t}-{c}-{m}" for t, c, m in COMBOS])
    def test_create_hset_search_round_trip(self, vtype, compression, metric):
        client: Valkey = self.server.get_new_client()
        pid = self.server.pid()
        encode = ENCODERS[vtype]
        vector = UNIT_VECTOR if metric == "IP" else VECTOR
        index = f"combo_{vtype.lower()}_{compression.lower()}_{metric.lower()}"
        key = f"{index}:1"

        threads_before = _threads(pid)
        client.execute_command(
            "FT.CREATE", index, "PREFIX", "1", f"{index}:",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "8", "TYPE", vtype, "DIM", str(DIM),
            "DISTANCE_METRIC", metric, "COMPRESSION", compression)
        client.hset(key, mapping={"v": encode(vector)})
        info = _wait_indexed(client, index, 1)

        attribute = info.attributes[0]["index"]
        assert attribute["data_type"] == vtype
        assert attribute["algorithm"]["compression"] == \
            f"SVS_COMPRESSION_{compression}"
        assert attribute["size"] == 1

        reply = client.execute_command(
            "FT.SEARCH", index, "*=>[KNN 1 @v $q]",
            "PARAMS", "2", "q", encode(vector), "DIALECT", "2")
        assert reply[0] == 1, reply
        assert reply[1] == key.encode(), reply
        # The query is the stored vector, so the distance is 0. The test
        # vectors are exact in FP16; COSINE's normalized copy is not, so FP16
        # storage (COMPRESSION FP16, or TYPE FLOAT16 with NONE) rounds its
        # self-match slightly away from 0.
        fp16_storage = compression == "FP16" or vtype == "FLOAT16"
        tolerance = 2e-3 if metric == "COSINE" and fp16_storage else 1e-6
        payload = dict(zip(reply[2][::2], reply[2][1::2]))
        assert float(payload[b"__v_score"]) == pytest.approx(0.0, abs=tolerance)

        assert _threads(pid) == threads_before, (
            f"{index}: thread count moved from {threads_before} to "
            f"{_threads(pid)} across FT.CREATE + HSET + FT.SEARCH")
