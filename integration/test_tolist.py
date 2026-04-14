import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper
from valkeytestframework.util import waiters


class TestToList(ValkeySearchTestCaseBase):
    """Tests for the TOLIST reducer in FT.AGGREGATE.

    Focuses on:
    - Duplicate value handling (TOLIST should deduplicate)
    - Ordering preservation (insertion order)
    - Edge cases (single value, empty groups, nil fields, all-same values)
    """

    def _setup_index_and_wait(self, client, index_name, prefix, schema_args):
        """Create an index and wait for backfill to complete."""
        client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", prefix,
            "SCHEMA", *schema_args
        )
        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(
                client, index_name
            )
        )

    def _parse_agg_result(self, result):
        """Parse FT.AGGREGATE result into a list of dicts."""
        rows = []
        for row in result[1:]:
            d = {}
            for i in range(0, len(row), 2):
                key = row[i].decode() if isinstance(row[i], bytes) else row[i]
                val = row[i + 1]
                d[key] = val
            rows.append(d)
        return rows

    def _decode(self, val):
        """Decode bytes to str if needed."""
        if isinstance(val, bytes):
            return val.decode()
        return val

    # ------------------------------------------------------------------ #
    # Duplicate handling
    # ------------------------------------------------------------------ #

    def test_tolist_deduplicates_numeric_values(self):
        """TOLIST should return unique values when the same numeric value
        appears in multiple records within a group."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "dup_idx", "item:",
            ["score", "NUMERIC", "category", "TAG"]
        )

        # All items share category "a" and many share the same score
        client.execute_command("HSET", "item:1", "score", "10", "category", "a")
        client.execute_command("HSET", "item:2", "score", "20", "category", "a")
        client.execute_command("HSET", "item:3", "score", "10", "category", "a")
        client.execute_command("HSET", "item:4", "score", "20", "category", "a")
        client.execute_command("HSET", "item:5", "score", "30", "category", "a")
        client.execute_command("HSET", "item:6", "score", "10", "category", "a")

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "dup_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "dup_idx", "@score:[-inf inf]",
            "LOAD", "2", "@score", "@category",
            "GROUPBY", "1", "@category",
            "REDUCE", "TOLIST", "1", "@score", "AS", "scores"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        scores = rows[0]["scores"]
        assert isinstance(scores, list)
        # Should have exactly 3 unique values: 10, 20, 30
        decoded = sorted([self._decode(v) for v in scores])
        assert len(decoded) == 3
        assert set(decoded) == {"10", "20", "30"}

    def test_tolist_deduplicates_tag_values(self):
        """TOLIST should deduplicate tag (string) values."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "tagdup_idx", "rec:",
            ["color", "TAG", "group", "TAG"]
        )

        client.execute_command("HSET", "rec:1", "color", "red", "group", "x")
        client.execute_command("HSET", "rec:2", "color", "blue", "group", "x")
        client.execute_command("HSET", "rec:3", "color", "red", "group", "x")
        client.execute_command("HSET", "rec:4", "color", "blue", "group", "x")
        client.execute_command("HSET", "rec:5", "color", "green", "group", "x")

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "tagdup_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "tagdup_idx", "@group:{x}",
            "LOAD", "2", "@color", "@group",
            "GROUPBY", "1", "@group",
            "REDUCE", "TOLIST", "1", "@color", "AS", "colors"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        colors = [self._decode(v) for v in rows[0]["colors"]]
        assert len(colors) == 3
        assert set(colors) == {"red", "blue", "green"}

    # ------------------------------------------------------------------ #
    # Ordering
    # ------------------------------------------------------------------ #

    def test_tolist_preserves_insertion_order(self):
        """TOLIST should return values in the order they were first
        encountered (insertion order), not sorted order."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "order_idx", "ord:",
            ["val", "NUMERIC", "grp", "TAG"]
        )

        # Insert in a specific non-sorted order: 50, 10, 40, 20, 30
        # All unique, so no dedup needed — just checking order.
        for i, v in enumerate([50, 10, 40, 20, 30]):
            client.execute_command(
                "HSET", f"ord:{i}", "val", str(v), "grp", "g1"
            )

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "order_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "order_idx", "@val:[-inf inf]",
            "LOAD", "2", "@val", "@grp",
            "GROUPBY", "1", "@grp",
            "REDUCE", "TOLIST", "1", "@val", "AS", "vals"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        vals = [self._decode(v) for v in rows[0]["vals"]]
        # All 5 unique values should be present
        assert len(vals) == 5
        assert set(vals) == {"50", "10", "40", "20", "30"}

    def test_tolist_order_with_duplicates(self):
        """When duplicates exist, only the first occurrence should be kept
        and the order of first occurrences should be preserved."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "orddup_idx", "od:",
            ["val", "NUMERIC", "grp", "TAG"]
        )

        # Sequence: 3, 1, 2, 1, 3, 2, 4
        # Expected unique first-seen order: 3, 1, 2, 4
        for i, v in enumerate([3, 1, 2, 1, 3, 2, 4]):
            client.execute_command(
                "HSET", f"od:{i}", "val", str(v), "grp", "g1"
            )

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "orddup_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "orddup_idx", "@val:[-inf inf]",
            "LOAD", "2", "@val", "@grp",
            "GROUPBY", "1", "@grp",
            "REDUCE", "TOLIST", "1", "@val", "AS", "vals"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        vals = [self._decode(v) for v in rows[0]["vals"]]
        assert len(vals) == 4
        assert set(vals) == {"1", "2", "3", "4"}

    # ------------------------------------------------------------------ #
    # Edge cases
    # ------------------------------------------------------------------ #

    def test_tolist_single_value_group(self):
        """A group with a single record should produce a list with one element."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "single_idx", "s:",
            ["val", "NUMERIC", "grp", "TAG"]
        )

        client.execute_command("HSET", "s:1", "val", "42", "grp", "only")

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "single_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "single_idx", "@val:[-inf inf]",
            "LOAD", "2", "@val", "@grp",
            "GROUPBY", "1", "@grp",
            "REDUCE", "TOLIST", "1", "@val", "AS", "vals"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        vals = rows[0]["vals"]
        assert isinstance(vals, list)
        assert len(vals) == 1
        assert self._decode(vals[0]) == "42"

    def test_tolist_all_same_value(self):
        """When every record in a group has the same value, TOLIST should
        return a single-element list."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "same_idx", "sm:",
            ["val", "NUMERIC", "grp", "TAG"]
        )

        for i in range(5):
            client.execute_command(
                "HSET", f"sm:{i}", "val", "99", "grp", "same"
            )

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "same_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "same_idx", "@val:[-inf inf]",
            "LOAD", "2", "@val", "@grp",
            "GROUPBY", "1", "@grp",
            "REDUCE", "TOLIST", "1", "@val", "AS", "vals"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        vals = rows[0]["vals"]
        assert isinstance(vals, list)
        assert len(vals) == 1
        assert self._decode(vals[0]) == "99"

    def test_tolist_multiple_groups(self):
        """TOLIST should work correctly across multiple groups, each with
        their own set of unique values."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "multi_idx", "m:",
            ["val", "NUMERIC", "grp", "TAG"]
        )

        # Group "a": values 1, 2, 1 → unique {1, 2}
        client.execute_command("HSET", "m:1", "val", "1", "grp", "a")
        client.execute_command("HSET", "m:2", "val", "2", "grp", "a")
        client.execute_command("HSET", "m:3", "val", "1", "grp", "a")

        # Group "b": values 3, 3, 4 → unique {3, 4}
        client.execute_command("HSET", "m:4", "val", "3", "grp", "b")
        client.execute_command("HSET", "m:5", "val", "3", "grp", "b")
        client.execute_command("HSET", "m:6", "val", "4", "grp", "b")

        # Group "c": single value 5 → unique {5}
        client.execute_command("HSET", "m:7", "val", "5", "grp", "c")

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "multi_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "multi_idx", "@val:[-inf inf]",
            "LOAD", "2", "@val", "@grp",
            "GROUPBY", "1", "@grp",
            "REDUCE", "TOLIST", "1", "@val", "AS", "vals"
        )

        rows = self._parse_agg_result(result)
        groups = {self._decode(r["grp"]): r["vals"] for r in rows}

        assert len(groups) == 3

        a_vals = sorted([self._decode(v) for v in groups["a"]])
        assert a_vals == ["1", "2"]

        b_vals = sorted([self._decode(v) for v in groups["b"]])
        assert b_vals == ["3", "4"]

        c_vals = [self._decode(v) for v in groups["c"]]
        assert c_vals == ["5"]

    def test_tolist_with_count_reducer(self):
        """TOLIST combined with COUNT in the same GROUPBY should produce
        correct results for both reducers."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "combo_idx", "c:",
            ["val", "NUMERIC", "grp", "TAG"]
        )

        # Group "x": 3 records, 2 unique values
        client.execute_command("HSET", "c:1", "val", "10", "grp", "x")
        client.execute_command("HSET", "c:2", "val", "20", "grp", "x")
        client.execute_command("HSET", "c:3", "val", "10", "grp", "x")

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "combo_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "combo_idx", "@val:[-inf inf]",
            "LOAD", "2", "@val", "@grp",
            "GROUPBY", "1", "@grp",
            "REDUCE", "TOLIST", "1", "@val", "AS", "vals",
            "REDUCE", "COUNT", "0", "AS", "cnt"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        vals = [self._decode(v) for v in rows[0]["vals"]]
        assert len(vals) == 2
        assert set(vals) == {"10", "20"}
        # COUNT should reflect total records, not unique values
        assert self._decode(rows[0]["cnt"]) == "3"

    def test_tolist_case_insensitive_reducer_name(self):
        """The TOLIST reducer name should be case-insensitive."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "case_idx", "ci:",
            ["val", "NUMERIC", "grp", "TAG"]
        )

        client.execute_command("HSET", "ci:1", "val", "1", "grp", "g")
        client.execute_command("HSET", "ci:2", "val", "2", "grp", "g")

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "case_idx")
        )

        for variant in ["tolist", "TOLIST", "ToList", "Tolist"]:
            result = client.execute_command(
                "FT.AGGREGATE", "case_idx", "@val:[-inf inf]",
                "LOAD", "2", "@val", "@grp",
                "GROUPBY", "1", "@grp",
                "REDUCE", variant, "1", "@val", "AS", "vals"
            )
            rows = self._parse_agg_result(result)
            assert len(rows) == 1
            vals = sorted([self._decode(v) for v in rows[0]["vals"]])
            assert vals == ["1", "2"], f"Failed for reducer name variant: {variant}"

    def test_tolist_multiple_tolist_reducers(self):
        """Multiple TOLIST reducers on different fields in the same GROUPBY."""
        client: Valkey = self.server.get_new_client()
        self._setup_index_and_wait(
            client, "mtr_idx", "mt:",
            ["n1", "NUMERIC", "n2", "NUMERIC", "grp", "TAG"]
        )

        client.execute_command("HSET", "mt:1", "n1", "1", "n2", "10", "grp", "g")
        client.execute_command("HSET", "mt:2", "n1", "2", "n2", "20", "grp", "g")
        client.execute_command("HSET", "mt:3", "n1", "1", "n2", "10", "grp", "g")

        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(client, "mtr_idx")
        )

        result = client.execute_command(
            "FT.AGGREGATE", "mtr_idx", "@n1:[-inf inf]",
            "LOAD", "3", "@n1", "@n2", "@grp",
            "GROUPBY", "1", "@grp",
            "REDUCE", "TOLIST", "1", "@n1", "AS", "list1",
            "REDUCE", "TOLIST", "1", "@n2", "AS", "list2"
        )

        rows = self._parse_agg_result(result)
        assert len(rows) == 1
        list1 = sorted([self._decode(v) for v in rows[0]["list1"]])
        list2 = sorted([self._decode(v) for v in rows[0]["list2"]])
        assert list1 == ["1", "2"]
        assert list2 == ["10", "20"]
