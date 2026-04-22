"""
Integration tests for FT.SEARCH INFIELDS.

Happy-path coverage lives in `TestInfieldsCompatibility` (compatibility suite,
parametrized over hash + json), including error paths and NOCONTENT+SORTBY.

Kept here: the pure-KNN interaction — INFIELDS is silently ignored for
KNN-only queries. Redis Stack has the same behavior, but the compatibility
suite does not exercise vector queries in the text schema.
"""

import struct

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFTSearchInfields(ValkeySearchTestCaseBase):
    # ---- module-specific: INFIELDS + pure-KNN ----

    def test_infields_with_pure_vector_query(self):
        """INFIELDS parsed but inert for pure KNN vector queries."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "vec_idx",
            "ON", "HASH",
            "PREFIX", "1", "vec:",
            "SCHEMA",
            "title", "TEXT",
            "emb", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
        )
        client.execute_command("HSET", "vec:1", "title", "hello",
                               "emb", struct.pack("2f", 1.0, 0.0))
        client.execute_command("HSET", "vec:2", "title", "world",
                               "emb", struct.pack("2f", 0.0, 1.0))

        query_vec = struct.pack("2f", 1.0, 0.0)
        result = client.execute_command(
            "FT.SEARCH", "vec_idx",
            "*=>[KNN 2 @emb $v]",
            "PARAMS", "2", "v", query_vec,
            "INFIELDS", "1", "title",
            "NOCONTENT", "DIALECT", "2",
        )
        assert result[0] == 2
