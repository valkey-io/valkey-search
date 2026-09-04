"""Pins the CURRENT (broken) behavior of FT.AGGREGATE ADDSCORES.

ADDSCORES should expose each document's relevance score to the aggregation
pipeline, as FT.SEARCH WITHSCORES does for the reply. It parses into
AggregateParameters::addscores_ and that field is read nowhere, so the keyword
silently does nothing. CreateRecordsFromNeighbors also only writes Neighbor.score
into a record under IsVectorQuery(), so a non-vector score has no route to output
at all.

TODO: implement ADDSCORES — surface the score as a pipeline field (Redis names it
@__score) so stages can reference it. Until then these tests assert the no-op so
the change is caught when implemented; each carries the assertion that should
replace it.
"""

import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters

IDX = "idxAddScores"
DOCS = {
    "d:1": {"desc": "hello world", "n": "1"},
    "d:2": {"desc": "hello hello world", "n": "2"},
    "d:3": {"desc": "world only", "n": "3"},
}


def _rows(result):
    """FT.AGGREGATE replies [count, row, row, ...] where each row is a flat
    field/value list. Returns a list of dicts with bytes decoded."""
    rows = []
    for row in result[1:]:
        d = {}
        for i in range(0, len(row), 2):
            key = row[i].decode() if isinstance(row[i], bytes) else row[i]
            val = row[i + 1]
            d[key] = val.decode() if isinstance(val, bytes) else val
        rows.append(d)
    return rows


class TestAggregateAddScores(ValkeySearchTestCaseBase):

    def _load(self) -> Valkey:
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", IDX, "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "desc", "TEXT", "n", "NUMERIC",
        )
        for key, mapping in DOCS.items():
            client.hset(key, mapping=mapping)
        waiters.wait_for_true(
            lambda: client.execute_command("FT.AGGREGATE", IDX, "hello",
                                           "LOAD", "1", "@n")[0] == 2
        )
        return client

    def test_addscores_is_accepted(self):
        """ADDSCORES parses. This is the only part of the keyword that works."""
        client = self._load()
        res = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "ADDSCORES", "LOAD", "1", "@n",
        )
        assert res[0] == 2

    def test_addscores_does_not_add_a_score_field(self):
        """Currently a no-op: the reply is byte-identical with and without it.

        Once ADDSCORES is implemented the ADDSCORES rows must carry a score
        field that the plain rows do not, i.e.
            assert all("__score" in r for r in _rows(with_scores))
            assert with_scores != without
        """
        client = self._load()
        with_scores = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "ADDSCORES", "LOAD", "1", "@n",
        )
        without = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "LOAD", "1", "@n",
        )

        assert with_scores == without
        for row in _rows(with_scores):
            assert set(row) == {"n"}

    def test_addscores_score_is_not_referenceable_by_a_stage(self):
        """@__score is resolved as an ordinary schema field, so referencing it
        is rejected even with ADDSCORES given.

        Once implemented both commands must succeed and the SORTBY must order by
        relevance — d:2 repeats "hello", so it outranks d:1:
            assert [r["n"] for r in _rows(sortby_res)] == ["2", "1"]
        """
        client = self._load()

        with pytest.raises(Exception, match="__score"):
            client.execute_command(
                "FT.AGGREGATE", IDX, "hello", "ADDSCORES",
                "LOAD", "2", "@n", "@__score",
            )

        with pytest.raises(Exception, match="__score"):
            client.execute_command(
                "FT.AGGREGATE", IDX, "hello", "ADDSCORES",
                "LOAD", "1", "@n", "SORTBY", "2", "@__score", "DESC",
            )
