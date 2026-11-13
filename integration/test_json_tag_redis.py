import json

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper

# JSON TAG indexing compared with Redis 8.10.2. EXPECTED and SEPARATORS were
# recorded by running the same indexes, documents and queries there; each query
# lists the numbers of the d:NN keys it returns. Every document also has
# n = its number, so `@n:[-inf +inf]` lists the documents the index kept.
INDEXES = {
    # name: (path, alias, tag arguments, FT.CREATE arguments)
    "plain": ("$.t", "t", [], []),
    "sep": ("$.t", "t", ["SEPARATOR", ","], []),
    "each": ("$.t[*]", "t", [], []),
    "legacy_path": (".t", "t", [], []),
    "city": ("$.items[*].city", "city", [], []),
    "pipe": ('$["tags"][*]', "tags", ["SEPARATOR", "|"], []),
    "legacy_filter": (".t", "t", [], ["FILTER", "exists(@t)"]),
}
DOCS = [
    {"t": "a"},
    {"t": "a,b"},
    {"t": " a "},
    {"t": ["a", "c"]},
    {"t": True},
    {"t": False},
    {"t": None},
    {"t": []},
    {"t": 5},
    {"t": ["x", 1]},
    {"t": [None, "z"]},
    {"t": ""},
    {},
    {"t": {"k": "v"}},
    {"t": ["a b", "C"]},
    {"t": [["n"]]},
    {"t": "q\"r\\s"},
    {"items": [{"city": "Seoul"}, {"city": "New York"}]},
    {"items": [{"city": "Busan"}, {"zip": 1}]},
    {"tags": ["p|q", " r "]},
]
EXPECTED = {
    'legacy_filter @n:[-inf +inf]': [],
    'city @city:{Busan}': [19],
    'city @city:{New*}': [18],
    'city @city:{New\\ York}': [18],
    'city @city:{Seoul}': [18],
    'city @n:[-inf +inf]': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'each -@t:{a}': [1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'each @n:[-inf +inf]': [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'each @t:{C}': [4, 15],
    'each @t:{\\ a\\ }': [],
    'each @t:{a\\ b}': [15],
    'each @t:{a\\,b}': [],
    'each @t:{a}': [4],
    'each @t:{b}': [],
    'each @t:{c}': [4, 15],
    'each @t:{fa*}': [],
    'each @t:{false}': [],
    'each @t:{n}': [16],
    'each @t:{q\\"r\\\\s}': [],
    'each @t:{true}': [],
    'each @t:{x}': [],
    'each @t:{z}': [11],
    'legacy_path -@t:{a}': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'legacy_path @n:[-inf +inf]': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'legacy_path @t:{C}': [],
    'legacy_path @t:{\\ a\\ }': [],
    'legacy_path @t:{a\\ b}': [],
    'legacy_path @t:{a\\,b}': [],
    'legacy_path @t:{a}': [],
    'legacy_path @t:{b}': [],
    'legacy_path @t:{c}': [],
    'legacy_path @t:{fa*}': [],
    'legacy_path @t:{false}': [],
    'legacy_path @t:{n}': [],
    'legacy_path @t:{q\\"r\\\\s}': [],
    'legacy_path @t:{true}': [],
    'legacy_path @t:{x}': [],
    'legacy_path @t:{z}': [],
    'pipe @n:[-inf +inf]': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'pipe @tags:{p\\|q}': [],
    'pipe @tags:{p}': [20],
    'pipe @tags:{q}': [20],
    'pipe @tags:{r}': [20],
    'plain -@t:{a}': [2, 3, 5, 6, 7, 8, 11, 12, 13, 15, 17, 18, 19, 20],
    'plain @n:[-inf +inf]': [1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13, 15, 17, 18, 19, 20],
    'plain @t:{C}': [4, 15],
    'plain @t:{\\ a\\ }': [3],
    'plain @t:{a\\ b}': [15],
    'plain @t:{a\\,b}': [2],
    'plain @t:{a}': [1, 4],
    'plain @t:{b}': [],
    'plain @t:{c}': [4, 15],
    'plain @t:{fa*}': [6],
    'plain @t:{false}': [6],
    'plain @t:{n}': [],
    'plain @t:{q\\"r\\\\s}': [17],
    'plain @t:{true}': [5],
    'plain @t:{x}': [],
    'plain @t:{z}': [11],
    'sep -@t:{a}': [5, 6, 7, 8, 11, 12, 13, 15, 17, 18, 19, 20],
    'sep @n:[-inf +inf]': [1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13, 15, 17, 18, 19, 20],
    'sep @t:{C}': [4, 15],
    'sep @t:{\\ a\\ }': [],
    'sep @t:{a\\ b}': [15],
    'sep @t:{a\\,b}': [],
    'sep @t:{a}': [1, 2, 3, 4],
    'sep @t:{b}': [2],
    'sep @t:{c}': [4, 15],
    'sep @t:{fa*}': [6],
    'sep @t:{false}': [6],
    'sep @t:{n}': [],
    'sep @t:{q\\"r\\\\s}': [17],
    'sep @t:{true}': [5],
    'sep @t:{x}': [],
    'sep @t:{z}': [11],
}
SEPARATORS = {'legacy_filter': '', 'city': '', 'each': '', 'legacy_path': '', 'pipe': '|', 'plain': '', 'sep': ','}


class TestJsonTagRedisCompat(ValkeySearchTestCaseBase):

    def test_matches_redis(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", "1.3.0") == b"OK"
        for name, (path, alias, args, create_args) in INDEXES.items():
            assert client.execute_command(
                "FT.CREATE", name, "ON", "JSON", "PREFIX", "1", "d:",
                *create_args, "SCHEMA", path, "AS", alias, "TAG", *args,
                "$.n", "AS", "n", "NUMERIC") == b"OK"
        for i, doc in enumerate(DOCS, start=1):
            client.execute_command(
                "JSON.SET", f"d:{i:02d}", "$", json.dumps(dict(doc, n=i - 1)))
        for name in INDEXES:
            IndexingTestHelper.wait_for_backfill_complete_on_node(client, name)
            IndexingTestHelper.wait_for_indexing_complete_on_node(client, name)

        mismatches = {}
        for case, expected in EXPECTED.items():
            name, query = case.split(" ", 1)
            res = client.execute_command(
                "FT.SEARCH", name, query, "NOCONTENT", "LIMIT", "0", "100",
                "DIALECT", "2")
            actual = sorted(int(k.decode().split(":")[1]) for k in res[1:])
            if actual != expected:
                mismatches[case] = (actual, expected)
        for name, expected in SEPARATORS.items():
            attrs = client.execute_command("FT.INFO", name)
            attrs = attrs[attrs.index(b"attributes") + 1]
            tag = next(a for a in attrs if b"TAG" in a)
            actual = tag[tag.index(b"SEPARATOR") + 1].decode()
            if actual != expected:
                mismatches[f"{name} SEPARATOR"] = (actual, expected)
        assert not mismatches, mismatches
