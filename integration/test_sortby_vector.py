"""
Tests for SORTBY ordering of vector (KNN) queries.

A KNN clause selects WHICH documents are returned (the k nearest); SORTBY
independently selects HOW they are ordered. Sorting by a stored field reads
that field from the fetched document content, so it must be available even
when the RETURN clause does not include it.
"""

import struct

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


class TestKnnSortByStoredField(ValkeySearchTestCaseBase):
    """
        A KNN query sorting by a stored (non-score) field must order by that
        field even when RETURN omits it: the sort field is fetched for
        sorting without being serialized (issue #1353 item 8 review finding).
        Previously the KNN path pre-populated neighbor content with only the
        RETURN attributes, so the comparator saw every document as missing
        the sort field and fell back to ordering by key.
    """

    def _setup(self, client, idx, prefix):
        assert client.execute_command(
            "FT.CREATE", idx, "ON", "HASH", "PREFIX", "1", prefix,
            "SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2",
            "cat", "TAG", "p", "NUMERIC") == b"OK"
        # Query vector is the origin, so distance order is {prefix}3,1,2 while
        # price ASC order is {prefix}2,3,1 and key order is {prefix}1,2,3 --
        # all distinct, so a pass cannot come from the KNN retrieval order
        # nor from the key tie-break.
        docs = [(f"{prefix}1", (2.0, 2.0), "30"),
                (f"{prefix}2", (3.0, 3.0), "10"),
                (f"{prefix}3", (1.0, 1.0), "20")]
        for key, vec, price in docs:
            assert client.execute_command(
                "HSET", key, "vec", struct.pack("<2f", *vec),
                "cat", "a", "p", price) == 3
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", idx, "@cat:{a}", "NOCONTENT",
                "DIALECT", "2")[0] == 3
        )
        return struct.pack("<2f", 0.0, 0.0)

    def test_knn_sortby_stored_field_not_in_return(self):
        client: Valkey = self.server.get_new_client()
        blob = self._setup(client, "vsort_idx", "vsort:")
        query = "*=>[KNN 3 @vec $B AS dist]"

        def rows(*keys):
            return [3] + [e for k in keys
                          for e in (k.encode(), [b"cat", b"a"])]

        result = client.execute_command(
            "FT.SEARCH", "vsort_idx", query, "PARAMS", "2", "B", blob,
            "SORTBY", "p", "ASC", "RETURN", "1", "cat", "DIALECT", "2")
        assert result == rows("vsort:2", "vsort:3", "vsort:1"), "ASC"
        result = client.execute_command(
            "FT.SEARCH", "vsort_idx", query, "PARAMS", "2", "B", blob,
            "SORTBY", "p", "DESC", "RETURN", "1", "cat", "DIALECT", "2")
        assert result == rows("vsort:1", "vsort:3", "vsort:2"), "DESC"
        # The truncating LIMIT path must yield a prefix of the full reply.
        result = client.execute_command(
            "FT.SEARCH", "vsort_idx", query, "PARAMS", "2", "B", blob,
            "SORTBY", "p", "ASC", "LIMIT", "0", "2",
            "RETURN", "1", "cat", "DIALECT", "2")
        assert result == [3, b"vsort:2", [b"cat", b"a"],
                          b"vsort:3", [b"cat", b"a"]], "ASC LIMIT 2"

    def test_knn_sortby_distance_alias_with_return(self):
        client: Valkey = self.server.get_new_client()
        blob = self._setup(client, "vsorta_idx", "vsorta:")
        # SORTBY on the distance alias orders by Neighbor.distance and needs
        # no stored field, so index-served content remains sufficient.
        result = client.execute_command(
            "FT.SEARCH", "vsorta_idx", "*=>[KNN 3 @vec $B AS dist]",
            "PARAMS", "2", "B", blob,
            "SORTBY", "dist", "ASC", "RETURN", "1", "cat", "DIALECT", "2")
        assert result == [3, b"vsorta:3", [b"cat", b"a"],
                          b"vsorta:1", [b"cat", b"a"],
                          b"vsorta:2", [b"cat", b"a"]], "dist ASC"
