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

    def test_knn_sortby_stored_field_also_in_return(self):
        client: Valkey = self.server.get_new_client()
        blob = self._setup(client, "vsret_idx", "vsret:")
        # The sort field appearing in RETURN too must not change the order,
        # and each row carries both fields.
        result = client.execute_command(
            "FT.SEARCH", "vsret_idx", "*=>[KNN 3 @vec $B AS dist]",
            "PARAMS", "2", "B", blob,
            "SORTBY", "p", "ASC", "RETURN", "2", "cat", "p", "DIALECT", "2")
        assert result == [3, b"vsret:2", [b"cat", b"a", b"p", b"10"],
                          b"vsret:3", [b"cat", b"a", b"p", b"20"],
                          b"vsret:1", [b"cat", b"a", b"p", b"30"]], "ASC"

    def test_knn_sortby_schema_aliased_field(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "FT.CREATE", "vsal_idx", "ON", "HASH", "PREFIX", "1", "vsal:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2",
            "cat", "TAG", "price_raw", "AS", "p", "NUMERIC") == b"OK"
        docs = [("vsal:1", (2.0, 2.0), "30"), ("vsal:2", (3.0, 3.0), "10"),
                ("vsal:3", (1.0, 1.0), "20")]
        for key, vec, price in docs:
            assert client.execute_command(
                "HSET", key, "vec", struct.pack("<2f", *vec),
                "cat", "a", "price_raw", price) == 3
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", "vsal_idx", "@cat:{a}", "NOCONTENT",
                "DIALECT", "2")[0] == 3
        )
        blob = struct.pack("<2f", 0.0, 0.0)
        # SORTBY names the schema alias while the hash stores price_raw; the
        # sort value must be reachable under the name the comparator uses.
        result = client.execute_command(
            "FT.SEARCH", "vsal_idx", "*=>[KNN 3 @vec $B AS dist]",
            "PARAMS", "2", "B", blob,
            "SORTBY", "p", "ASC", "RETURN", "1", "cat", "DIALECT", "2")
        assert result == [3, b"vsal:2", [b"cat", b"a"],
                          b"vsal:3", [b"cat", b"a"],
                          b"vsal:1", [b"cat", b"a"]], "alias ASC"

    def test_knn_sortby_text_field_falls_back_to_fetch(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "FT.CREATE", "vstxt_idx", "ON", "HASH", "PREFIX", "1", "vstxt:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2",
            "cat", "TAG", "name", "TEXT") == b"OK"
        docs = [("vstxt:1", (2.0, 2.0), "cc"), ("vstxt:2", (3.0, 3.0), "aa"),
                ("vstxt:3", (1.0, 1.0), "bb")]
        for key, vec, name in docs:
            assert client.execute_command(
                "HSET", key, "vec", struct.pack("<2f", *vec),
                "cat", "a", "name", name) == 3
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", "vstxt_idx", "@cat:{a}", "NOCONTENT",
                "DIALECT", "2")[0] == 3
        )
        blob = struct.pack("<2f", 0.0, 0.0)
        # TEXT has no index-served raw value, so content must fall back to
        # the main-thread fetch; ordering still follows the field bytes.
        result = client.execute_command(
            "FT.SEARCH", "vstxt_idx", "*=>[KNN 3 @vec $B AS dist]",
            "PARAMS", "2", "B", blob,
            "SORTBY", "name", "ASC", "RETURN", "1", "cat", "DIALECT", "2")
        assert result == [3, b"vstxt:2", [b"cat", b"a"],
                          b"vstxt:3", [b"cat", b"a"],
                          b"vstxt:1", [b"cat", b"a"]], "text ASC"

    def test_knn_sortby_doc_missing_sort_field(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "FT.CREATE", "vsmiss_idx", "ON", "HASH", "PREFIX", "1", "vsmiss:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2",
            "cat", "TAG", "p", "NUMERIC") == b"OK"
        # vsmiss:2 lacks p entirely; distance and key orders both place it
        # mid/early, so "missing sorts last" is the only way it ends up last.
        assert client.execute_command(
            "HSET", "vsmiss:1", "vec", struct.pack("<2f", 1.0, 1.0),
            "cat", "a", "p", "20") == 3
        assert client.execute_command(
            "HSET", "vsmiss:2", "vec", struct.pack("<2f", 2.0, 2.0),
            "cat", "a") == 2
        assert client.execute_command(
            "HSET", "vsmiss:3", "vec", struct.pack("<2f", 3.0, 3.0),
            "cat", "a", "p", "10") == 3
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", "vsmiss_idx", "@cat:{a}", "NOCONTENT",
                "DIALECT", "2")[0] == 3
        )
        blob = struct.pack("<2f", 0.0, 0.0)
        query = "*=>[KNN 3 @vec $B AS dist]"
        result = client.execute_command(
            "FT.SEARCH", "vsmiss_idx", query, "PARAMS", "2", "B", blob,
            "SORTBY", "p", "ASC", "RETURN", "1", "cat", "DIALECT", "2")
        assert result == [3, b"vsmiss:3", [b"cat", b"a"],
                          b"vsmiss:1", [b"cat", b"a"],
                          b"vsmiss:2", [b"cat", b"a"]], "missing ASC"
        result = client.execute_command(
            "FT.SEARCH", "vsmiss_idx", query, "PARAMS", "2", "B", blob,
            "SORTBY", "p", "DESC", "RETURN", "1", "cat", "DIALECT", "2")
        assert result == [3, b"vsmiss:1", [b"cat", b"a"],
                          b"vsmiss:3", [b"cat", b"a"],
                          b"vsmiss:2", [b"cat", b"a"]], "missing DESC"


class TestKnnSortByNanValues(ValkeySearchTestCaseBase):
    """
        KNN-query variant of the NaN fold (issue #1353 item 8 hardening):
        with NOCONTENT the index-served shortcut is skipped, so the sort
        value is the raw hash bytes -- "-nan" parses to a real NaN and must
        fold to 0.0 rather than reach the comparator unordered. See
        TestSortByNanValues for the filter-path variant and rationale.
    """

    def test_knn_sortby_nan_folds_to_zero(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "FT.CREATE", "vsnan_idx", "ON", "HASH", "PREFIX", "1", "vsnan:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2", "p", "NUMERIC") == b"OK"
        # Distance order (3,5,9,1), key order (1,3,5,9), and the asserted
        # sort orders are all pairwise different; key 9 (-nan) sits between
        # 0 and 5 only if NaN folds to 0.0.
        docs = [("vsnan:1", "-3", (4.0, 4.0)), ("vsnan:3", "0", (1.0, 1.0)),
                ("vsnan:9", "-nan", (3.0, 3.0)), ("vsnan:5", "5", (2.0, 2.0))]
        for key, p, vec in docs:
            assert client.execute_command(
                "HSET", key, "p", p, "vec", struct.pack("<2f", *vec)) == 2
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", "vsnan_idx", "*", "NOCONTENT",
                "DIALECT", "2")[0] == 4
        )
        blob = struct.pack("<2f", 0.0, 0.0)
        query = "*=>[KNN 4 @vec $B AS dist]"
        result = client.execute_command(
            "FT.SEARCH", "vsnan_idx", query, "PARAMS", "2", "B", blob,
            "SORTBY", "p", "ASC", "NOCONTENT", "DIALECT", "2")
        assert result == [4, b"vsnan:1", b"vsnan:3", b"vsnan:9",
                          b"vsnan:5"], "nan ASC"
        result = client.execute_command(
            "FT.SEARCH", "vsnan_idx", query, "PARAMS", "2", "B", blob,
            "SORTBY", "p", "DESC", "NOCONTENT", "DIALECT", "2")
        assert result == [4, b"vsnan:5", b"vsnan:9", b"vsnan:3",
                          b"vsnan:1"], "nan DESC"
