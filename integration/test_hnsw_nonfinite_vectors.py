"""
Inserting a vector with NaN or Inf components into an HNSW index must not hang
the writer thread.

The HNSW greedy descent keeps moving while `d < curdist`. Every distance from a
NaN vector is NaN, and under -ffast-math (finite-math-only) GCC on aarch64
compiled that compare so a NaN counted as "closer". The descent then walked the
upper-level neighbours forever, holding the writer. This test inserts such
vectors and requires the mutation queue to drain.
"""

import math
import random
import struct

import pytest
from valkey import Valkey

from ft_info_parser import FTInfoParser
from indexes import Index, Vector
from util import waiters
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

DIM = 4
# Enough finite vectors with a small M that the graph has upper levels, which
# is where the descent loop runs.
NUM_FINITE = 300
DRAIN_TIMEOUT_SEC = 30

index = Index("nonfinite_idx", [Vector("v", DIM, type="HNSW", m=2, efc=10)])


def _vec(values) -> bytes:
    return struct.pack(f"<{DIM}f", *values)


class TestHnswNonFiniteVectors(ValkeySearchTestCaseBase):

    def _queue_drained(self, client: Valkey) -> bool:
        info = FTInfoParser(client.execute_command("FT.INFO", index.name))
        return info.mutation_queue_size == 0

    @pytest.mark.parametrize(
        "bad_value", [math.nan, math.inf, -math.inf], ids=["nan", "inf", "neg_inf"]
    )
    def test_non_finite_vector_does_not_hang_writer(self, bad_value):
        client: Valkey = self.server.get_new_client()
        index.create(client, True)

        rng = random.Random(1)
        for i in range(NUM_FINITE):
            client.hset(f"k:{i}", mapping={"v": _vec([rng.random() for _ in range(DIM)])})
        waiters.wait_for_true(lambda: self._queue_drained(client), timeout=DRAIN_TIMEOUT_SEC)

        # Several, because the new element's level is random and the descent
        # only runs when it is below the current top level.
        for i in range(10):
            client.hset(
                f"bad:{i}",
                mapping={"v": _vec([bad_value] + [rng.random() for _ in range(DIM - 1)])},
            )
        # A spinning writer leaves the queue non-empty and this times out.
        waiters.wait_for_true(lambda: self._queue_drained(client), timeout=DRAIN_TIMEOUT_SEC)

        # The non-finite vectors must actually be indexed, otherwise the
        # descent was never exercised.
        info = FTInfoParser(client.execute_command("FT.INFO", index.name))
        assert info.num_docs == NUM_FINITE + 10
        assert client.ping()
        client.execute_command(
            "FT.SEARCH", index.name, "*=>[KNN 3 @v $q]",
            "PARAMS", "2", "q", _vec([0.5] * DIM), "DIALECT", "2",
        )
