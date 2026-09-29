"""Integration tests for SORTABLE / UNF in the FT.INFO attributes reply.

Redis reports these as bare tokens with no value, which no generic key/value
parser can read, so they are reported here as pairs like CASESENSITIVE. SORTABLE
is reported for every non-vector attribute and UNF only for TAG and TEXT, since
Redis rejects SORTABLE on a vector and UNF suppresses a normalization a number
never has.
"""

import pytest
import valkey
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def attribute_of(client, index_name, alias):
    """Return one attribute entry from FT.INFO as a {key: value} dict."""
    info = client.execute_command("FT.INFO", index_name)
    attributes = info[info.index(b"attributes") + 1]
    for attribute in attributes:
        pairs = dict(zip(attribute[::2], attribute[1::2]))
        if pairs.get(b"attribute") == alias.encode():
            return pairs
    raise AssertionError(f"attribute {alias} not found in FT.INFO")


class TestFtInfoSortable(ValkeySearchTestCaseBase):
    def _client(self) -> Valkey:
        return self.server.get_new_client()

    def _create(self, client):
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "p:",
            "NOHL",
            "SCHEMA",
            "plain", "TAG",
            "sorted", "TAG", "SORTABLE",
            "unsorted_form", "TAG", "SORTABLE", "UNF",
            "amount", "NUMERIC", "SORTABLE",
            "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2",
        ) == b"OK"

    def test_pairs_report_what_was_declared(self):
        client = self._client()
        self._create(client)

        plain = attribute_of(client, "idx", "plain")
        assert plain[b"SORTABLE"] == b"0"
        assert plain[b"UNF"] == b"0"

        sorted_attr = attribute_of(client, "idx", "sorted")
        assert sorted_attr[b"SORTABLE"] == b"1"
        assert sorted_attr[b"UNF"] == b"0"

        unf_attr = attribute_of(client, "idx", "unsorted_form")
        assert unf_attr[b"SORTABLE"] == b"1"
        assert unf_attr[b"UNF"] == b"1"

    def test_unf_omitted_for_numeric_and_both_for_vector(self):
        client = self._client()
        self._create(client)

        amount = attribute_of(client, "idx", "amount")
        assert amount[b"SORTABLE"] == b"1"
        assert b"UNF" not in amount

        vec = attribute_of(client, "idx", "vec")
        assert b"SORTABLE" not in vec
        assert b"UNF" not in vec

    def test_every_attribute_entry_stays_pairwise(self):
        """No bare tokens: each attribute entry must have an even length."""
        client = self._client()
        self._create(client)
        info = client.execute_command("FT.INFO", "idx")
        attributes = info[info.index(b"attributes") + 1]
        for attribute in attributes:
            assert len(attribute) % 2 == 0, attribute

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
        assert sorted_attr[b"UNF"] == b"0"
