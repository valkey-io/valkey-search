"""FT.AGGREGATE ADDSCORES: exposes each document's relevance score to the
aggregation pipeline as the field __score (matching Redis), the way FT.SEARCH
WITHSCORES exposes it in the reply.

The score written is the carried search-time score (Neighbor.score). With no
LOAD the query is still no_content, so the main-thread content fetch stays
skipped and the score is emitted without revalidation or recompute; this is the
same accepted semantics as FT.SEARCH NOCONTENT WITHSCORES.
"""

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
        client = self._load()
        res = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "ADDSCORES", "LOAD", "1", "@n",
        )
        assert res[0] == 2

    def test_addscores_adds_score_field(self):
        """ADDSCORES rows carry __score; plain rows do not."""
        client = self._load()
        with_scores = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "ADDSCORES", "LOAD", "1", "@n",
        )
        without = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "LOAD", "1", "@n",
        )

        assert with_scores != without
        rows = _rows(with_scores)
        assert all(set(r) == {"n", "__score"} for r in rows)
        assert all(float(r["__score"]) > 0.0 for r in rows)
        # d:2 repeats "hello" (tf=2), so BM25 ranks it above d:1.
        by_n = {r["n"]: float(r["__score"]) for r in rows}
        assert by_n["2"] > by_n["1"]
        for row in _rows(without):
            assert set(row) == {"n"}

    def test_addscores_score_is_referenceable_by_stages(self):
        """@__score can be named in LOAD and used by SORTBY."""
        client = self._load()

        res = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "ADDSCORES",
            "LOAD", "2", "@n", "@__score",
        )
        assert all("__score" in r for r in _rows(res))

        sortby_res = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "ADDSCORES",
            "LOAD", "1", "@n", "SORTBY", "2", "@__score", "DESC",
        )
        assert [r["n"] for r in _rows(sortby_res)] == ["2", "1"]

    def test_addscores_without_load(self):
        """No LOAD keeps the query no_content (main-thread fetch skipped); the
        score still flows because it comes from the neighbor, not the fetch."""
        client = self._load()
        res = client.execute_command(
            "FT.AGGREGATE", IDX, "hello", "ADDSCORES",
        )
        assert res[0] == 2
        rows = _rows(res)
        assert all(set(r) == {"__score"} for r in rows)
        assert all(float(r["__score"]) > 0.0 for r in rows)
