"""FT.HYBRID returns the columns the LOAD clause asked for, and no others.

The standalone path narrows the fused content fetch with the aggregate's
resolved LOAD clause. The cluster path has no such fetch -- each shard read its
own keys during its own arm search, because the coordinator cannot read keys it
does not own -- so the projection has to reach the shard instead. These pin the
two paths to the same answer for each of the three LOAD states.
"""

import json
import struct

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

    def _seed(self, client: Valkey, writer) -> None:
        client.execute_command(
            "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA",
            "title", "TEXT",
            "colour", "TAG",
            "price", "NUMERIC",
            "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4",
            "DISTANCE_METRIC", "L2")
        for i in range(1, DOCS + 1):
            writer.hset(f"d:{i}", mapping={
                "title": "hello world",
                "colour": "red",
                "price": i,
                "junk": "SHOULD_NOT_APPEAR",
                "vec": _vec(float(i), 0.0, 0.0, 0.0)})
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", INDEX, "@title:hello", "NOCONTENT",
                "LIMIT", "0", "0")[0] == DOCS,
            timeout=10)

    def _fields(self, client: Valkey, *tail) -> set:
        """Every column name the reply carried, across all rows."""
        reply = client.execute_command(
            "FT.HYBRID", INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", str(DOCS),
            *tail,
            "LIMIT", "0", "100",
            "PARAMS", "2", "q", Q)
        names = set()
        for rec in reply[1:]:
            names.update(rec[i] for i in range(0, len(rec), 2))
        return names

    def test_named_load_returns_only_that_field(self):
        """`LOAD 1 @title` names one column, so one column comes back."""
        client = self._setup()
        assert self._fields(client, "LOAD", "1", "@title") == {b"title"}

    def test_no_load_returns_no_database_field(self):
        """With no LOAD clause the reply is the key and the fused score. The
        shards still fetch -- they need the content to revalidate against
        concurrent mutations -- but none of it is the caller's business."""
        client = self._setup()
        assert self._fields(client) == {b"__key", b"__score"}

    def test_load_all_returns_every_field(self):
        """`LOAD *` is the one state that does want the whole record."""
        client = self._setup()
        fields = self._fields(client, "LOAD", "*")
        assert UNASKED in fields, fields
        assert {b"title", b"colour", b"price"} <= fields, fields


class TestFtHybridLoadProjection(_LoadProjectionTests,
                                 ValkeySearchTestCaseBase):
    def _setup(self) -> Valkey:
        client: Valkey = self.server.get_new_client()
        self._seed(client, client)
        return client

    def test_json_named_load_returns_value(self):
        """The reply reads each LOAD column after the request's own copy of
        the identifier is gone, so the content must own its keys."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "jidx", "ON", "JSON", "PREFIX", "1", "j:", "SCHEMA",
            "$.title", "AS", "title", "TEXT",
            "$.price", "AS", "price", "NUMERIC",
            "$.vec", "AS", "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
            "DIM", "4", "DISTANCE_METRIC", "L2")
        for i in range(1, 4):
            client.execute_command("JSON.SET", f"j:{i}", "$", json.dumps({
                "title": "alpha", "price": i,
                "vec": [float(i), 0.0, 0.0, 0.0]}))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", "jidx", "@title:alpha", "NOCONTENT",
                "LIMIT", "0", "0")[0] == 3,
            timeout=10)
        reply = client.execute_command(
            "FT.HYBRID", "jidx", "SEARCH", "@title:alpha", "VSIM", "@vec",
            "$q", "COMBINE", "RRF", "2", "CONSTANT", "60",
            "LOAD", "1", "@price", "PARAMS", "2", "q", Q)
        assert len(reply) == 4, reply
        for rec in reply[1:]:
            fields = dict(zip(rec[::2], rec[1::2]))
            loaded = [v for k, v in fields.items() if not k.startswith(b"__")]
            assert loaded == [fields[b"__key"][len(b"j:"):]], rec


class TestFtHybridLoadProjectionCluster(_LoadProjectionTests,
                                        ValkeySearchClusterTestCase):
    def _setup(self) -> Valkey:
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        self._seed(client, cluster)
        return client
