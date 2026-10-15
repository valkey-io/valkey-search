"""FT.CREATE + HSET + FT.SEARCH for every accepted SVS_VAMANA TYPE x
COMPRESSION. BFLOAT16 and SQ8 are rejected at FT.CREATE (test_svs_smoke.py)."""

import struct
import time

import pytest
from valkey.client import Valkey

from ft_info_parser import FTInfoParser
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


DIM = 4
VECTOR = [1.0, 2.0, 3.0, 4.0]


def _fp32(values):
    return struct.pack(f"<{len(values)}f", *values)


def _fp16(values):
    return struct.pack(f"<{len(values)}e", *values)


ENCODERS = {"FLOAT32": _fp32, "FLOAT16": _fp16}
COMPRESSIONS = ["NONE", "FP16"]
COMBOS = [(t, c) for t in ENCODERS for c in COMPRESSIONS]


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

    @pytest.mark.parametrize("vtype,compression", COMBOS,
                             ids=[f"{t}-{c}" for t, c in COMBOS])
    def test_create_hset_search_round_trip(self, vtype, compression):
        client: Valkey = self.server.get_new_client()
        pid = self.server.pid()
        encode = ENCODERS[vtype]
        index = f"combo_{vtype.lower()}_{compression.lower()}"
        key = f"{index}:1"

        threads_before = _threads(pid)
        client.execute_command(
            "FT.CREATE", index, "PREFIX", "1", f"{index}:",
            "SCHEMA", "v", "VECTOR", "SVS_VAMANA",
            "8", "TYPE", vtype, "DIM", str(DIM),
            "DISTANCE_METRIC", "L2", "COMPRESSION", compression)
        client.hset(key, mapping={"v": encode(VECTOR)})
        info = _wait_indexed(client, index, 1)

        attribute = info.attributes[0]["index"]
        assert attribute["data_type"] == vtype
        assert attribute["algorithm"]["compression"] == \
            f"SVS_COMPRESSION_{compression}"
        assert attribute["size"] == 1

        reply = client.execute_command(
            "FT.SEARCH", index, "*=>[KNN 1 @v $q]",
            "PARAMS", "2", "q", encode(VECTOR), "DIALECT", "2")
        assert reply[0] == 1, reply
        assert reply[1] == key.encode(), reply
        # The query is the stored vector, so the distance is 0 (FP16 exact).
        payload = dict(zip(reply[2][::2], reply[2][1::2]))
        assert float(payload[b"__v_score"]) == pytest.approx(0.0, abs=1e-6)

        assert _threads(pid) == threads_before, (
            f"{index}: thread count moved from {threads_before} to "
            f"{_threads(pid)} across FT.CREATE + HSET + FT.SEARCH")
