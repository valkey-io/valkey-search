import struct

import pytest

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def _vec(*values) -> bytes:
    return struct.pack(f"<{len(values)}f", *values)


@pytest.mark.skip(reason=(
    "https://github.com/valkey-io/valkey-search/issues/1410"
    " -- FT.AGGREGATE reports the BM25 relevance in the `AS <alias>` column"
    " for a hybrid `text=>[KNN ...]` query, where the equivalent FT.SEARCH and"
    " the pure-vector aggregate both report the KNN distance. The test is"
    " written to fail against that defect; unskip it when the issue is fixed."))
class TestAggregateHybridDistance(ValkeySearchTestCaseBase):
    """
    FT.AGGREGATE must report the KNN distance in the `AS <alias>` column.

    For a hybrid `text=>[KNN ...]` query the alias column currently carries the
    BM25 text relevance instead of the vector distance, while the equivalent
    FT.SEARCH and the pure-vector `*=>[KNN ...]` aggregate both report the
    distance. The corpus below puts distance order and relevance order in
    opposition so the two values can never be confused.
    """

    # doc key -> (title, x coordinate, squared L2 distance from the query)
    DOCS = [
        ("doc:1", "hello hello hello hello", 0.0, 0.0),
        ("doc:2", "hello world of many other words here to dilute", 1.0, 1.0),
        ("doc:3", "hello hello there", 2.0, 4.0),
        ("doc:4", "hello a b c d e f g h i j k l m n o p", 3.0, 9.0),
    ]

    QUERY_VECTOR = _vec(0.0, 0.0, 0.0)

    def _populate(self, client: Valkey):
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
        ) == b"OK"

        for key, title, x, _distance in self.DOCS:
            assert client.execute_command(
                "HSET", key, "title", title, "vec", _vec(x, 0.0, 0.0),
            ) == 2

    @staticmethod
    def _distances_by_key(reply) -> dict:
        """Map __key -> float(mydist) from an FT.AGGREGATE reply."""
        by_key = {}
        for row in reply[1:]:
            fields = dict(zip(row[::2], row[1::2]))
            by_key[fields[b"__key"].decode()] = float(fields[b"mydist"])
        return by_key

    def test_hybrid_text_knn_aggregate_reports_distance(self):
        client: Valkey = self.server.get_new_client()
        self._populate(client)

        expected = {key: distance for key, _t, _x, distance in self.DOCS}

        # Control: a pure-vector aggregate reports the true distances. This is
        # the boundary of the defect -- only the hybrid text shape is wrong.
        pure_vector = client.execute_command(
            "FT.AGGREGATE", "idx", "*=>[KNN 4 @vec $q AS mydist]",
            "LOAD", "2", "@__key", "@mydist",
            "PARAMS", "2", "q", self.QUERY_VECTOR,
            "DIALECT", "2",
        )
        assert self._distances_by_key(pure_vector) == expected

        # Control: FT.SEARCH over the hybrid query reports the true distances.
        search = client.execute_command(
            "FT.SEARCH", "idx", "@title:hello=>[KNN 4 @vec $q AS mydist]",
            "RETURN", "1", "mydist",
            "PARAMS", "2", "q", self.QUERY_VECTOR,
            "DIALECT", "2",
        )
        search_distances = {
            search[i].decode(): float(dict(
                zip(search[i + 1][::2], search[i + 1][1::2])
            )[b"mydist"])
            for i in range(1, len(search), 2)
        }
        assert search_distances == expected

        # The defect: the same query through FT.AGGREGATE puts the BM25 text
        # relevance in the distance alias.
        hybrid = client.execute_command(
            "FT.AGGREGATE", "idx", "@title:hello=>[KNN 4 @vec $q AS mydist]",
            "LOAD", "2", "@__key", "@mydist",
            "PARAMS", "2", "q", self.QUERY_VECTOR,
            "DIALECT", "2",
        )
        assert self._distances_by_key(hybrid) == expected
