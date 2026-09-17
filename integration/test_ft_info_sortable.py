"""Integration tests for SORTABLE / UNF in the FT.INFO attributes reply.

Redis reports these as bare tokens with no value, which no generic key/value
parser can read, so they are reported here as `sortable` / `unf` pairs. That
changes the reply shape, so it is gated behind `search.emulate-release` >= 1.3.0
(see COMPATIBILITY.md). These tests run under debug-mode so the ceiling can be
lifted to the (as yet unreleased) fix version.
"""

import pytest
import valkey
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

FIX_RELEASE = "1.3.0"
LEGACY_RELEASE = "1.0.0"


def attribute_of(client, index_name, alias):
    """Return one attribute entry from FT.INFO as a {key: value} dict."""
    info = client.execute_command("FT.INFO", index_name)
    attributes = info[info.index(b"attributes") + 1]
    for attribute in attributes:
        pairs = dict(zip(attribute[::2], attribute[1::2]))
        if pairs.get(b"attribute") == alias.encode():
            return pairs
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
            "NOHL",
            "SCHEMA",
            "plain", "TAG",
            "sorted", "TAG", "SORTABLE",
            "unsorted_form", "TAG", "SORTABLE", "UNF",
        ) == b"OK"

    def test_attribute_pairs_reported_when_declared(self):
        client = self._client()
        self._create(client)

        plain = attribute_of(client, "idx", "plain")
        assert b"SORTABLE" not in plain
        assert b"UNF" not in plain

        sorted_attr = attribute_of(client, "idx", "sorted")
        assert sorted_attr[b"SORTABLE"] == b"1"
        assert b"UNF" not in sorted_attr

        unf_attr = attribute_of(client, "idx", "unsorted_form")
        assert unf_attr[b"SORTABLE"] == b"1"
        assert unf_attr[b"UNF"] == b"1"

    def test_every_attribute_entry_stays_pairwise(self):
        """No bare tokens: each attribute entry must have an even length."""
        client = self._client()
        self._create(client)
        info = client.execute_command("FT.INFO", "idx")
        attributes = info[info.index(b"attributes") + 1]
        for attribute in attributes:
            assert len(attribute) % 2 == 0, attribute

    def test_fields_absent_before_fix_release(self):
        client = self._client(LEGACY_RELEASE)
        self._create(client)

        for alias in ("plain", "sorted", "unsorted_form"):
            attribute = attribute_of(client, "idx", alias)
            assert b"SORTABLE" not in attribute
            assert b"UNF" not in attribute

    def test_attribute_pairs_survive_a_reload(self):
        """The flags are persisted on the attribute, not recomputed from argv."""
        client = self._client()
        self._create(client)
        client.execute_command("DEBUG", "RELOAD")

        unf_attr = attribute_of(client, "idx", "unsorted_form")
        assert unf_attr[b"SORTABLE"] == b"1"
        assert unf_attr[b"UNF"] == b"1"

        sorted_attr = attribute_of(client, "idx", "sorted")
        assert sorted_attr[b"SORTABLE"] == b"1"
        assert b"UNF" not in sorted_attr
