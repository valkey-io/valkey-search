"""Integration tests for SORTABLE / UNF in the FT.INFO attributes reply.

Redis reports SORTABLE and UNF as bare tokens on each attribute, present only
when declared. Adding them changes the shape of the attributes array, so the
fix is gated behind `search.emulate-release` >= 1.3.0 (see COMPATIBILITY.md).
These tests run under debug-mode so the ceiling can be lifted to the (as yet
unreleased) fix version.
"""

import pytest
import valkey
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

FIX_RELEASE = "1.3.0"
LEGACY_RELEASE = "1.0.0"


def index_options(client, index_name):
    """Return the index_options array from FT.INFO."""
    info = client.execute_command("FT.INFO", index_name)
    return info[info.index(b"index_options") + 1]


def attribute_flags(client, index_name, alias):
    """Return the flat attribute entry from FT.INFO for one alias."""
    info = client.execute_command("FT.INFO", index_name)
    attributes = info[info.index(b"attributes") + 1]
    for attribute in attributes:
        if attribute[attribute.index(b"attribute") + 1] == alias.encode():
            return [element for element in attribute if isinstance(element, bytes)]
    raise AssertionError(f"attribute {alias} not found in FT.INFO")


class TestFtInfoSortable(ValkeySearchTestCaseDebugMode):
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
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "p:",
            "SCHEMA",
            "plain", "TAG",
            "sorted", "TAG", "SORTABLE",
            "unsorted_form", "TAG", "SORTABLE", "UNF",
        ) == b"OK"

    def test_flags_reported_when_declared(self):
        client = self._client()
        self._create(client)

        assert b"SORTABLE" not in attribute_flags(client, "idx", "plain")
        assert b"UNF" not in attribute_flags(client, "idx", "plain")

        sorted_flags = attribute_flags(client, "idx", "sorted")
        assert b"SORTABLE" in sorted_flags
        assert b"UNF" not in sorted_flags

        unf_flags = attribute_flags(client, "idx", "unsorted_form")
        assert b"SORTABLE" in unf_flags
        assert b"UNF" in unf_flags
        # UNF follows SORTABLE, as in Redis.
        assert unf_flags.index(b"UNF") == unf_flags.index(b"SORTABLE") + 1

    def test_flags_absent_before_fix_release(self):
        client = self._client(LEGACY_RELEASE)
        self._create(client)

        for alias in ("plain", "sorted", "unsorted_form"):
            flags = attribute_flags(client, "idx", alias)
            assert b"SORTABLE" not in flags
            assert b"UNF" not in flags

    def test_index_options_reports_nohl(self):
        """Redis reports NOHL as a bare token in index_options."""
        client = self._client()
        assert client.execute_command(
            "FT.CREATE", "idxnohl", "ON", "HASH", "PREFIX", "1", "n:",
            "NOHL", "SCHEMA", "t", "TEXT",
        ) == b"OK"
        assert index_options(client, "idxnohl") == [b"NOHL"]

    def test_index_options_nooffsets_implies_nohl(self):
        """NOOFFSETS implies NOHL, as measured against RediSearch 2.10.20."""
        client = self._client()
        assert client.execute_command(
            "FT.CREATE", "idxnooff", "ON", "HASH", "PREFIX", "1", "o:",
            "NOOFFSETS", "SCHEMA", "t", "TEXT",
        ) == b"OK"
        assert index_options(client, "idxnooff") == [b"NOOFFSETS", b"NOHL"]

    def test_index_options_empty_without_flags(self):
        client = self._client()
        self._create(client)
        assert index_options(client, "idx") == []

    def test_index_options_absent_before_fix_release(self):
        client = self._client(LEGACY_RELEASE)
        self._create(client)
        info = client.execute_command("FT.INFO", "idx")
        assert b"index_options" not in info

    def test_flags_survive_a_reload(self):
        """The flags are persisted on the attribute, not recomputed from argv."""
        client = self._client()
        self._create(client)
        client.execute_command("DEBUG", "RELOAD")

        unf_flags = attribute_flags(client, "idx", "unsorted_form")
        assert b"SORTABLE" in unf_flags
        assert b"UNF" in unf_flags
        assert b"SORTABLE" in attribute_flags(client, "idx", "sorted")
        assert b"UNF" not in attribute_flags(client, "idx", "sorted")
