"""FT.HYBRID returns the columns the LOAD clause asked for, and no others.

The standalone path narrows the fused content fetch with the aggregate's
resolved LOAD clause. The cluster path has no such fetch -- each shard read its
own keys during its own arm search, because the coordinator cannot read keys it
does not own -- so the projection has to reach the shard instead. These pin the
two paths to the same answer for each of the three LOAD states, on HASH and
JSON indexes.
"""

import json
import struct

import pytest

from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters

INDEX = "idx"
DOCS = 8
# The origin, so the KNN arm ranks by the first coordinate.
Q = struct.pack("4f", 0.0, 0.0, 0.0, 0.0)
# Indexed under no attribute, so nothing but `LOAD *` has a reason to return it.
UNASKED = b"junk"


def _vec(*xs: float) -> bytes:
    return struct.pack(f"{len(xs)}f", *xs)


class _LoadProjectionTests:
    """Shared body. Subclasses provide _setup()."""

    def _seed(self, client: Valkey, writer, key_type: str) -> None:
        vec = ("VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4",
               "DISTANCE_METRIC", "L2")
        if key_type == "HASH":
            client.execute_command(
                "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA",
                "title", "TEXT", "colour", "TAG", "price", "NUMERIC",
                "vec", *vec)
        else:
            client.execute_command(
                "FT.CREATE", INDEX, "ON", "JSON", "PREFIX", "1", "d:", "SCHEMA",
                "$.title", "AS", "title", "TEXT",
                "$.colour", "AS", "colour", "TAG",
                "$.price", "AS", "price", "NUMERIC",
                "$.vec", "AS", "vec", *vec)
        for i in range(1, DOCS + 1):
            doc = {"title": "hello world", "colour": "red", "price": i,
                   "junk": "SHOULD_NOT_APPEAR"}
            if key_type == "HASH":
                writer.hset(f"d:{i}", mapping={
                    **doc, "vec": _vec(float(i), 0.0, 0.0, 0.0)})
            else:
                writer.execute_command(
                    "JSON.SET", f"d:{i}", "$",
                    json.dumps({**doc, "vec": [float(i), 0.0, 0.0, 0.0]}))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", INDEX, "@title:hello", "NOCONTENT",
                "LIMIT", "0", "0")[0] == DOCS,
            timeout=10)

    def _rows(self, client: Valkey, *tail) -> list:
        reply = client.execute_command(
            "FT.HYBRID", INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", str(DOCS),
            *tail,
            "LIMIT", "0", "100",
            "PARAMS", "2", "q", Q)
        rows = []
        for rec in reply[1:]:
            names = rec[0::2]
            # A dict would hide a column the reply carries twice.
            assert len(set(names)) == len(names), rec
            rows.append(dict(zip(names, rec[1::2])))
        return rows

    def _fields(self, client: Valkey, *tail) -> set:
        """Every column name the reply carried, across all rows."""
        return set().union(*(row.keys() for row in self._rows(client, *tail)))

    @pytest.mark.parametrize("key_type", ["HASH", "JSON"])
    def test_named_load_returns_only_that_field(self, key_type):
        """`LOAD 1 @title` names one column, so one column comes back."""
        client = self._setup(key_type)
        assert self._fields(client, "LOAD", "1", "@title") == {b"title"}

    @pytest.mark.parametrize("key_type", ["HASH", "JSON"])
    def test_no_load_returns_no_database_field(self, key_type):
        """With no LOAD clause the reply is the key and the fused score. The
        shards still fetch -- they need the content to revalidate against
        concurrent mutations -- but none of it is the caller's business."""
        client = self._setup(key_type)
        assert self._fields(client) == {b"__key", b"__score"}

    def test_load_all_returns_every_field(self):
        """`LOAD *` is the one state that does want the whole record."""
        client = self._setup("HASH")
        fields = self._fields(client, "LOAD", "*")
        assert UNASKED in fields, fields
        assert {b"title", b"colour", b"price"} <= fields, fields

    def test_load_all_on_json_returns_the_whole_document(self):
        """On JSON the whole record is one `$` column, as in Redisearch."""
        client = self._setup("JSON")
        rows = self._rows(client, "LOAD", "*")
        assert len(rows) == DOCS
        for row in rows:
            assert set(row) == {b"$"}, row
            assert json.loads(row[b"$"])["junk"] == "SHOULD_NOT_APPEAR"

    @pytest.mark.parametrize("key_type", ["HASH", "JSON"])
    @pytest.mark.parametrize("load", [[], ["LOAD", "1", "@title"],
                                      ["LOAD", "*"]],
                             ids=["no-load", "named", "load-all"])
    def test_stage_reads_the_field_in_every_load_state(self, key_type, load):
        """A field left out of the fetch reaches the stage as a missing
        value, so the stage answers wrong instead of the command failing."""
        client = self._setup(key_type)
        rows = self._rows(client, *load, "GROUPBY", "1", "@colour",
                          "REDUCE", "SUM", "1", "@price", "AS", "s")
        assert rows == [{b"colour": b"red", b"s": b"36"}], rows

    @pytest.mark.parametrize("key_type", ["HASH", "JSON"])
    def test_load_all_with_a_stage_keeps_the_whole_record(self, key_type):
        """A stage adds its field to the shard's fetch list, which must not
        replace the whole record `LOAD *` asked for."""
        client = self._setup(key_type)
        rows = self._rows(client, "LOAD", "*", "SORTBY", "2", "@price", "DESC")
        assert [row.get(b"price") for row in rows] == [
            str(i).encode() for i in range(DOCS, 0, -1)], rows
        for row in rows:
            if key_type == "HASH":
                assert UNASKED in row, row
            else:
                assert set(row) == {b"$", b"price"}, row
                document = json.loads(row[b"$"])
                assert document["junk"] == "SHOULD_NOT_APPEAR", row
                assert document["price"] == int(row[b"price"]), row


class TestFtHybridLoadProjection(_LoadProjectionTests,
                                 ValkeySearchTestCaseBase):
    def _setup(self, key_type: str) -> Valkey:
        client: Valkey = self.server.get_new_client()
        self._seed(client, client, key_type)
        return client


class TestFtHybridLoadProjectionCluster(_LoadProjectionTests,
                                        ValkeySearchClusterTestCase):
    def _setup(self, key_type: str) -> Valkey:
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        self._seed(client, cluster, key_type)
        return client
