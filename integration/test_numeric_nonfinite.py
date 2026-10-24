"""NUMERIC fields holding NaN, and FT.AGGREGATE SORTBY over NaN values.

Every spelling that parses to NaN ("-nan", "+nan", "nan(1)", " nan", ...) is
invalid NUMERIC data, as "nan" is. A NaN in the numeric B-tree is never
ordered against other values, so it could never be erased: after the key is
deleted its entry would stay behind, range counts would include it, and a
query returning it would hit a missing-key CHECK.

Infinities are ordered and stay valid.

In FT.AGGREGATE, an APPLY that yields NaN (sqrt of a negative) sorts after
every number in both directions, as a missing value does.
"""

import math

from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

NAN_SPELLINGS = ("nan", "NaN", "-nan", "+nan", "-NaN", "nan(1)", " nan", "nan ")


def _count(client, index, query):
    return client.execute_command(
        "FT.SEARCH", index, query, "LIMIT", "0", "0")[0]


def _keys(client, index, query):
    result = client.execute_command(
        "FT.SEARCH", index, query, "NOCONTENT", "LIMIT", "0", "1000")
    return {key.decode() for key in result[1:]}


def _create_numeric_index(client, index):
    client.execute_command(
        "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", index + ":",
        "SCHEMA", "n", "NUMERIC")


class TestNumericNonFinite(ValkeySearchTestCaseBase):

    def test_nan_is_invalid_and_leaves_nothing_behind(self):
        client = self.server.get_new_client()
        for i, spelling in enumerate(NAN_SPELLINGS):
            index = f"idx{i}"
            _create_numeric_index(client, index)
            valid = set()
            for v in list(range(20)) + list(range(100, 120)):
                client.hset(f"{index}:{v}", "n", v)
                valid.add(f"{index}:{v}")
            client.hset(f"{index}:bad", "n", spelling)
            client.delete(f"{index}:bad")
            # Fetching keys, not just counting them, is what would reach a
            # stale entry.
            assert _keys(client, index, "@n:[100 119]") == {
                f"{index}:{v}" for v in range(100, 120)}, spelling
            assert _keys(client, index, "@n:[-inf +inf]") == valid, spelling
            assert _count(client, index, "@n:[-inf +inf]") == 40, spelling
            assert _count(client, index, "@n:[0 19]") == 20, spelling
            assert client.ping()

    def test_nan_overwrite_drops_the_field(self):
        """The modify path: a valid value overwritten with NaN."""
        client = self.server.get_new_client()
        _create_numeric_index(client, "ow")
        valid = set()
        for v in range(40):
            client.hset(f"ow:{v}", "n", v)
            valid.add(f"ow:{v}")
        client.hset("ow:a", "n", 5)
        client.hset("ow:a", "n", "-nan")
        assert _keys(client, "ow", "@n:[5 5]") == {"ow:5"}
        assert _count(client, "ow", "@n:[-inf +inf]") == 40
        client.delete("ow:a")
        assert _keys(client, "ow", "@n:[-inf +inf]") == valid
        client.hset("ow:b", "n", 7)
        assert _keys(client, "ow", "@n:[7 7]") == {"ow:7", "ow:b"}
        assert client.ping()

    def test_infinities_stay_valid(self):
        client = self.server.get_new_client()
        _create_numeric_index(client, "inf")
        client.hset("inf:p", "n", "inf")
        client.hset("inf:m", "n", "-inf")
        client.hset("inf:one", "n", 1)
        assert _keys(client, "inf", "@n:[-inf +inf]") == {
            "inf:p", "inf:m", "inf:one"}
        assert _keys(client, "inf", "@n:[0 2]") == {"inf:one"}


class TestSearchSortByNaN(ValkeySearchTestCaseBase):

    def test_sortby_numeric_ignores_nan(self):
        """FT.SEARCH SORTBY on a NUMERIC field whose stored value is a NaN.

        Before 1.3.0 an invalid field is treated as missing and the key stays
        in its other indexes, so the tag query still returns it and SORTBY
        reads the raw NaN. Parsing must reject it, or it compares as a tie
        with every value and the numbers are not sorted.
        """
        client = self.server.get_new_client()
        client.execute_command(
            "CONFIG", "SET", "search.emulate-release", "1.2.0")
        client.execute_command(
            "FT.CREATE", "s", "ON", "HASH", "PREFIX", "1", "s:",
            "SCHEMA", "n", "NUMERIC", "t", "TAG")
        numbers = [(i * 37) % 101 for i in range(60)]
        for i, n in enumerate(numbers):
            client.hset(f"s:{i}", mapping={"n": n, "t": "x"})
            if i % 3 == 1:
                client.hset(f"s:nan{i}", mapping={"n": "-nan", "t": "x"})
        for direction in ("ASC", "DESC"):
            result = client.execute_command(
                "FT.SEARCH", "s", "@t:{x}", "SORTBY", "n", direction,
                "RETURN", "1", "n", "LIMIT", "0", "200")
            got = []
            for key, fields in zip(result[1::2], result[2::2]):
                if key.decode().startswith("s:nan"):
                    continue
                got.append(float(dict(zip(fields[::2], fields[1::2]))[b"n"]))
            expected = sorted(numbers, reverse=(direction == "DESC"))
            assert got == expected, (direction, got[:10])
        assert client.ping()


class TestAggregateSortNaN(ValkeySearchTestCaseBase):

    def test_sortby_puts_nan_last(self):
        client = self.server.get_new_client()
        _create_numeric_index(client, "agg")
        values = list(range(-60, 61))
        for v in values:
            client.hset(f"agg:{v}", "n", v)
        roots = sorted(math.sqrt(v) for v in values if v >= 0)
        nan_count = sum(1 for v in values if v < 0)
        # (MAX, rows returned): MAX equal to the row count is the full sort,
        # and MAX 30 the top-30 heap path. Without MAX, SORTBY keeps 10 rows.
        for max_arg, rows in ((str(len(values)), len(values)), ("30", 30)):
            for direction in ("ASC", "DESC"):
                extra = ["MAX", max_arg]
                result = client.execute_command(
                    "FT.AGGREGATE", "agg", "*", "LOAD", "1", "@n",
                    "APPLY", "sqrt(@n)", "AS", "x",
                    "SORTBY", "2", "@x", direction, *extra,
                    "LIMIT", "0", str(rows))
                xs = [dict(zip(row[::2], row[1::2]))[b"x"].decode()
                      for row in result[1:]]
                assert len(xs) == rows, (direction, max_arg)
                expected = roots if direction == "ASC" else roots[::-1]
                numbers = min(rows, len(roots))
                got = [float(x) for x in xs[:numbers]]
                assert got == [float(f"{r:.12g}") for r in expected[:numbers]], (
                    direction, max_arg, got[:10])
                tail = xs[numbers:]
                assert all(x.lower().endswith("nan") for x in tail), (
                    direction, max_arg, tail[:5])
                if rows == len(values):
                    assert len(tail) == nan_count
