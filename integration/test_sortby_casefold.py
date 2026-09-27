"""Integration tests for SORTBY on a field declared SORTABLE without UNF.

Redis case-folds a sortable field's sort value unless UNF was given, so all
uppercase no longer sorts before all lowercase. Folding only reorders results a
client may already depend on, so it is gated behind `search.emulate-release` >=
1.3.0. These tests run under debug-mode so the ceiling can be lifted to the (as
yet unreleased) fix version. Every expectation below was measured against
redis:8, search module 81000.
"""

import pytest
import valkey
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

FIX_RELEASE = "1.3.0"
LEGACY_RELEASE = "1.0.0"

# Chosen so folded and raw-byte orderings disagree, and so the accent case is
# covered: 'Á' folds to 'á', which still compares above every ASCII letter.
VALUES = ["apple", "Apple", "banana", "Banana", "Zebra", "zebra", "Ápple"]


class TestSortByCaseFold(ValkeySearchTestCaseDebugMode):
    def _client(self, emulate_release=FIX_RELEASE) -> Valkey:
        client: Valkey = self.server.get_new_client()
        assert (
            client.execute_command(
                f"CONFIG SET search.emulate-release {emulate_release}"
            )
            == b"OK"
        )
        return client

    def _create(self, client):
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA",
            "c", "TAG",
            "folded", "TEXT", "SORTABLE",
            "raw", "TEXT", "SORTABLE", "UNF",
            "plain", "TEXT",
            "tag_folded", "TAG", "SORTABLE",
        ) == b"OK"
        for i, value in enumerate(VALUES):
            assert client.execute_command(
                "HSET", f"d:{i}", "c", "x",
                "folded", value, "raw", value, "plain", value,
                "tag_folded", value,
            ) == 5

    def _order(self, client, field):
        result = client.execute_command(
            "FT.SEARCH", "idx", "@c:{x}", "SORTBY", field, "ASC",
            "LIMIT", "0", "20", "RETURN", "1", field,
        )
        return [result[i + 1][1].decode() for i in range(1, len(result), 2)]

    def test_bare_sortable_is_case_folded(self):
        client = self._client()
        self._create(client)
        order = self._order(client, "folded")
        # Case-insensitive grouping: apple with Apple, banana with Banana.
        assert order.index("Apple") - order.index("apple") in (-1, 1)
        assert order.index("Banana") - order.index("banana") in (-1, 1)
        assert order.index("zebra") - order.index("Zebra") in (-1, 1)
        # Folded order across groups, and the accent sorts last.
        assert [v.lower() for v in order] == [
            "apple", "apple", "banana", "banana", "zebra", "zebra", "ápple",
        ]

    def test_unf_still_compares_raw_bytes(self):
        client = self._client()
        self._create(client)
        # 'A'(0x41) < 'B' < 'Z' < 'a' < 'b' < 'z' < 'Á'(0xC3)
        assert self._order(client, "raw") == [
            "Apple", "Banana", "Zebra", "apple", "banana", "zebra", "Ápple",
        ]

    def test_field_without_sortable_compares_raw_bytes(self):
        """Folding is opt-in via SORTABLE, matching Redis."""
        client = self._client()
        self._create(client)
        assert self._order(client, "plain") == [
            "Apple", "Banana", "Zebra", "apple", "banana", "zebra", "Ápple",
        ]

    def test_tag_sortable_is_also_folded(self):
        client = self._client()
        self._create(client)
        assert [v.lower() for v in self._order(client, "tag_folded")] == [
            "apple", "apple", "banana", "banana", "zebra", "zebra", "ápple",
        ]

    def test_raw_bytes_before_fix_release(self):
        """Below the gate every field keeps the order it has always had."""
        client = self._client(LEGACY_RELEASE)
        self._create(client)
        for field in ("folded", "raw", "plain", "tag_folded"):
            assert self._order(client, field) == [
                "Apple", "Banana", "Zebra", "apple", "banana", "zebra", "Ápple",
            ], field

    def test_descending_folded(self):
        client = self._client()
        self._create(client)
        result = client.execute_command(
            "FT.SEARCH", "idx", "@c:{x}", "SORTBY", "folded", "DESC",
            "LIMIT", "0", "20", "RETURN", "1", "folded",
        )
        order = [result[i + 1][1].decode() for i in range(1, len(result), 2)]
        assert [v.lower() for v in order] == [
            "ápple", "zebra", "zebra", "banana", "banana", "apple", "apple",
        ]
