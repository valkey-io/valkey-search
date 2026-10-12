"""Redis Search compatible RESP3 envelopes, including synchronous replies."""

import json
import struct
import time

import pytest
from valkey import ConnectionPool, ResponseError
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker

from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)


def envelope(total, results):
    return {
        b"attributes": [],
        b"warning": [],
        b"format": b"STRING",
        b"total_results": total,
        b"results": results,
    }


def row(key, attributes=None, **metadata):
    result = {b"id": key, b"values": []}
    if attributes is not None:
        result[b"extra_attributes"] = attributes
    result.update({key.encode(): value for key, value in metadata.items()})
    return result


class TestResp3(ValkeySearchTestCaseBase):
    @pytest.fixture
    def resp3(self):
        kwargs = dict(self.client.connection_pool.connection_kwargs)
        kwargs["protocol"] = 3
        pool = ConnectionPool(**kwargs)
        try:
            with Valkey(connection_pool=pool) as client:
                yield client
        finally:
            pool.disconnect()

    def populate(self, client, algorithm="FLAT"):
        client.execute_command(
            "FT.CREATE",
            "idx",
            "PREFIX",
            1,
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "n",
            "NUMERIC",
            "tag",
            "TAG",
            "v",
            "VECTOR",
            algorithm,
            6,
            "TYPE",
            "FLOAT32",
            "DIM",
            2,
            "DISTANCE_METRIC",
            "L2",
        )
        for i in range(1, 4):
            client.hset(
                f"doc:{i}",
                mapping={
                    "title": "hello world",
                    "n": str(i),
                    "tag": "group",
                    "v": struct.pack("ff", i, 0),
                    "binary": b"\x00\xff\r\n",
                },
            )
        self.wait_for_count(client, "idx", 3)

    @pytest.mark.parametrize("transaction", [False, True])
    def test_search_fields_and_resp2(self, resp3, transaction):
        self.populate(resp3)
        args = (
            "FT.SEARCH",
            "idx",
            "hello @n:[2 2]",
            "SORTBY",
            "n",
            "ASC",
            "LIMIT",
            0,
            1,
            "RETURN",
            5,
            "title",
            "AS",
            "name",
            "binary",
            "missing",
        )
        expected = envelope(
            1,
            [
                row(
                    b"doc:2",
                    {
                        b"name": b"hello world",
                        b"binary": b"\x00\xff\r\n",
                    },
                )
            ],
        )
        with resp3.pipeline(transaction=transaction) as pipe:
            pipe.execute_command(*args)
            pipe.ping()
            assert pipe.execute() == [expected, True]

        # A RESP2 client still receives the original alternating array.
        result = self.client.execute_command(*args)
        assert result[:2] == [1, b"doc:2"]
        assert (
            dict(zip(result[2][::2], result[2][1::2]))
            == expected[b"results"][0][b"extra_attributes"]
        )

    @pytest.mark.parametrize(
        "options",
        [
            ("NOCONTENT",),
            ("RETURN", 0),
            ("RETURN", 1, "missing"),
            ("LIMIT", 0, 0),
            ("LIMIT", 10, 2),
        ],
    )
    def test_empty_and_no_content(self, resp3, options):
        self.populate(resp3)
        result = resp3.execute_command("FT.SEARCH", "idx", "*", *options)
        if options[0] == "LIMIT":
            assert result == envelope(3, [])
        else:
            attrs = {} if options == ("RETURN", 1, "missing") else None
            result[b"results"].sort(key=lambda item: item[b"id"])
            assert result == envelope(
                3, [row(f"doc:{i}".encode(), attrs) for i in range(1, 4)]
            )
        assert resp3.execute_command(
            "FT.SEARCH", "idx", "@n:[10 20]", *options
        ) == envelope(0, [])

    @pytest.mark.parametrize("no_content", [False, True])
    def test_sort_keys(self, resp3, no_content):
        self.populate(resp3)
        options = ("NOCONTENT",) if no_content else ("RETURN", 1, "title")
        for field, value in [("n", b"#1"), ("tag", b"$group")]:
            result = resp3.execute_command(
                "FT.SEARCH",
                "idx",
                "@n:[1 1]",
                "SORTBY",
                field,
                "WITHSORTKEYS",
                *options,
            )
            attrs = None if no_content else {b"title": b"hello world"}
            assert result == envelope(1, [row(b"doc:1", attrs, sortkey=value)])
        # RESP2 keeps the pre-1.3.0 sort keys unless emulate-release opts in;
        # RESP3 always replies like Redis Search.
        if not no_content:
            assert self.client.execute_command(
                "FT.SEARCH", "idx", "@n:[1 1]", "SORTBY", "tag",
                "WITHSORTKEYS", *options,
            )[2] == b"#group"
        result = resp3.execute_command(
            "FT.SEARCH",
            "idx",
            "@n:[1 1]",
            "WITHSORTKEYS",
            *options,
        )
        assert result == envelope(1, [row(b"doc:1", attrs, sortkey=None)])

    @pytest.mark.parametrize(
        "options",
        [
            (),
            ("RETURN", 1, "distance"),
            ("NOCONTENT",),
            ("LIMIT", 0, 0),
            ("LIMIT", 2, 1),
            ("LIMIT", 1, 1),
        ],
    )
    def test_vector(self, resp3, options):
        self.populate(resp3)
        result = resp3.execute_command(
            "FT.SEARCH",
            "idx",
            "*=>[KNN 2 @v $blob AS distance]",
            "PARAMS",
            2,
            "blob",
            struct.pack("ff", 0, 0),
            *options,
            "DIALECT",
            2,
        )
        assert result[b"total_results"] == 2
        if options in [("LIMIT", 0, 0), ("LIMIT", 2, 1)]:
            assert result == envelope(2, [])
            return
        ids = [2] if options == ("LIMIT", 1, 1) else [1, 2]
        expected = []
        for i in ids:
            attrs = {
                b"distance": str(i * i).encode(),
                b"title": b"hello world",
                b"n": str(i).encode(),
                b"tag": b"group",
                b"v": struct.pack("ff", i, 0),
                b"binary": b"\x00\xff\r\n",
            }
            if options and options[0] == "RETURN":
                attrs = {b"distance": str(i * i).encode()}
            elif options == ("NOCONTENT",):
                attrs = None
            expected.append(row(f"doc:{i}".encode(), attrs))
        assert result == envelope(2, expected)

    @pytest.mark.parametrize("algorithm", ["FLAT", "HNSW"])
    @pytest.mark.parametrize(
        "predicate,ids",
        [
            ("@tag:{group}", [1, 2]),
            ("@n:[1 +inf]", [1, 2]),
            ("@title:hello", [1, 2]),
            ("@tag:{group} @n:[2 3] @title:hello", [2, 3]),
            ("@tag:{missing}", []),
        ],
    )
    @pytest.mark.parametrize("mode", ["content", "nocontent", "page", "count"])
    def test_hybrid(self, resp3, algorithm, predicate, ids, mode):
        self.populate(resp3, algorithm)
        # This document is nearest to the query vector but must be filtered out.
        resp3.hset(
            "doc:0",
            mapping={
                "title": "excluded",
                "tag": "excluded",
                "n": "0",
                "v": struct.pack("ff", 0, 0),
            },
        )
        self.wait_for_count(resp3, "idx", 4)
        options = ["RETURN", 4, "distance", "n", "AS", "number"]
        selected = ids
        if mode == "nocontent":
            options = ["NOCONTENT"]
        elif mode == "page":
            options += ["LIMIT", 1, 1]
            selected = ids[1:2]
        elif mode == "count":
            options += ["LIMIT", 0, 0]
            selected = []
        args = (
            "FT.SEARCH",
            "idx",
            f"({predicate})=>[KNN 2 @v $blob AS distance]",
            "PARAMS",
            2,
            "blob",
            struct.pack("ff", 0, 0),
            *options,
            "DIALECT",
            2,
        )
        expected_rows = [
            row(
                f"doc:{i}".encode(),
                (
                    None
                    if mode == "nocontent"
                    else {
                        b"distance": str(i * i).encode(),
                        b"number": str(i).encode(),
                    }
                ),
            )
            for i in selected
        ]
        assert resp3.execute_command(*args) == envelope(len(ids), expected_rows)

        resp2 = self.client.execute_command(*args)
        assert resp2[0] == len(ids)
        if mode == "nocontent":
            assert resp2[1:] == [item[b"id"] for item in expected_rows]
        else:
            assert len(resp2) == 1 + 2 * len(expected_rows)
            for offset, expected in enumerate(expected_rows):
                assert resp2[1 + 2 * offset] == expected[b"id"]
                fields = resp2[2 + 2 * offset]
                assert (
                    dict(zip(fields[::2], fields[1::2]))
                    == expected[b"extra_attributes"]
                )

    @pytest.mark.parametrize("transaction", [False, True])
    def test_aggregate(self, resp3, transaction):
        self.populate(resp3)
        with resp3.pipeline(transaction=transaction) as pipe:
            pipe.execute_command(
                "FT.AGGREGATE",
                "idx",
                "*",
                "LOAD",
                1,
                "tag",
                "GROUPBY",
                1,
                "@tag",
                "REDUCE",
                "COUNT",
                0,
                "AS",
                "count",
            )
            pipe.execute_command("FT.AGGREGATE", "idx", "@n:[10 20]", "LOAD", 1, "n")
            pipe.ping()
            assert pipe.execute() == [
                envelope(
                    1,
                    [
                        {
                            b"extra_attributes": {b"tag": b"group", b"count": b"3"},
                            b"values": [],
                        }
                    ],
                ),
                envelope(0, []),
                True,
            ]

    @pytest.mark.parametrize("dialect", [2, 3])
    def test_json(self, resp3, dialect):
        resp3.execute_command(
            "FT.CREATE",
            "jsonidx",
            "ON",
            "JSON",
            "PREFIX",
            1,
            "json:",
            "SCHEMA",
            "$.n",
            "AS",
            "n",
            "NUMERIC",
        )
        resp3.execute_command("JSON.SET", "json:1", "$", '{"n":7,"title":"hello"}')
        # MULTI/EXEC also verifies synchronous reply protocol detection.
        with resp3.pipeline() as pipe:
            pipe.execute_command("FT.SEARCH", "jsonidx", "*", "DIALECT", dialect)
            pipe.execute_command(
                "FT.SEARCH",
                "jsonidx",
                "*",
                "RETURN",
                3,
                "$.n",
                "AS",
                "number",
                "DIALECT",
                dialect,
            )
            full, projected = pipe.execute()
        assert full[b"total_results"] == 1
        document = json.loads(full[b"results"][0][b"extra_attributes"][b"$"])
        # Protocol negotiation preserves the existing JSON payload semantics.
        resp2 = self.client.execute_command(
            "FT.SEARCH", "jsonidx", "*", "DIALECT", dialect
        )
        attributes = dict(zip(resp2[2][::2], resp2[2][1::2]))
        assert document == json.loads(attributes[b"$"])
        if dialect == 2:
            assert document == {"n": 7, "title": "hello"}
        resp2_projected = self.client.execute_command(
            "FT.SEARCH",
            "jsonidx",
            "*",
            "RETURN",
            3,
            "$.n",
            "AS",
            "number",
            "DIALECT",
            dialect,
        )
        attributes = dict(zip(resp2_projected[2][::2], resp2_projected[2][1::2]))
        assert projected == envelope(1, [row(b"json:1", attributes)])
        if dialect == 2:
            assert attributes == {b"number": b"7"}

    def wait_for_count(self, client, index, count):
        deadline = time.monotonic() + 10
        while (
            client.execute_command("FT.SEARCH", index, "*", "LIMIT", 0, 0)[
                b"total_results"
            ]
            != count
        ):
            assert time.monotonic() < deadline, "Indexing did not finish"
            time.sleep(0.01)

    def test_score_hides_field_with_same_name(self, resp3):
        self.populate(resp3)
        resp3.hset("doc:1", "distance", "field-value")
        args = (
            "FT.SEARCH",
            "idx",
            "*=>[KNN 1 @v $blob AS distance]",
            "PARAMS",
            2,
            "blob",
            struct.pack("ff", 0, 0),
            "DIALECT",
            2,
        )
        # Like Redis Search, the score wins over the field in the RESP3 map.
        result = resp3.execute_command(*args)
        attributes = result[b"results"][0][b"extra_attributes"]
        assert attributes[b"distance"] == b"1"
        assert b"field-value" not in attributes.values()
        # RESP2 is unchanged and still has both pairs.
        fields = self.client.execute_command(*args)[2]
        assert fields[:2] == [b"distance", b"1"]
        assert fields.count(b"distance") == 2

    @pytest.mark.parametrize("protocol", [2, 3])
    def test_lua(self, resp3, protocol):
        self.populate(resp3)
        client = resp3 if protocol == 3 else self.client
        search = (
            "return redis.call('FT.SEARCH', 'idx', '@n:[2 2]', "
            "'RETURN', '1', 'title')"
        )
        # Without setresp, the script receives and returns the RESP2 reply.
        assert client.eval(search, 0) == [1, b"doc:2", [b"title", b"hello world"]]

        # With setresp(3) the reply follows the script's protocol, not the
        # caller's; a RESP2 caller receives the map flattened to an array.
        result = client.eval("redis.setresp(3); " + search, 0)
        expected = envelope(1, [row(b"doc:2", {b"title": b"hello world"})])
        if protocol == 3:
            assert result == expected
        else:
            # Lua tables don't keep map order, so compare the pairs as dicts.
            def pairs(items):
                return dict(zip(items[::2], items[1::2]))

            result = pairs(result)
            result[b"results"] = [pairs(item) for item in result[b"results"]]
            for item in result[b"results"]:
                item[b"extra_attributes"] = pairs(item[b"extra_attributes"])
            assert result == expected

    def test_withscores(self, resp3):
        self.populate(resp3)
        args = ("FT.SEARCH", "idx", "hello", "WITHSCORES", "RETURN", 1, "title")
        result = resp3.execute_command(*args)
        assert result[b"total_results"] == 3
        resp2 = self.client.execute_command(*args)
        # Like Redis Search, the score is a double between id and the fields.
        scores = {resp2[i]: float(resp2[i + 1]) for i in range(1, len(resp2), 3)}
        for item in result[b"results"]:
            assert list(item) == [b"id", b"score", b"extra_attributes", b"values"]
            assert isinstance(item[b"score"], float)
            assert item[b"score"] == pytest.approx(scores[item[b"id"]])

        result = resp3.execute_command(
            "FT.SEARCH", "idx", "hello", "WITHSCORES", "SORTBY", "n",
            "WITHSORTKEYS", "NOCONTENT", "LIMIT", 0, 2,
        )
        assert [list(item) for item in result[b"results"]] == [
            [b"id", b"score", b"sortkey", b"values"]
        ] * 2
        assert [item[b"sortkey"] for item in result[b"results"]] == [b"#1", b"#2"]

    def test_vector_range(self, resp3):
        self.populate(resp3)
        result = resp3.execute_command(
            "FT.SEARCH",
            "idx",
            "@v:[VECTOR_RANGE 10 $blob]=>{$YIELD_DISTANCE_AS: d}",
            "PARAMS",
            2,
            "blob",
            struct.pack("ff", 0, 0),
            "SORTBY",
            "d",
            "WITHSORTKEYS",
            "RETURN",
            1,
            "d",
            "LIMIT",
            0,
            2,
            "DIALECT",
            2,
        )
        assert result == envelope(
            3,
            [
                row(b"doc:1", {b"d": b"1"}, sortkey=b"#1"),
                row(b"doc:2", {b"d": b"4"}, sortkey=b"#4"),
            ],
        )

    def test_search_cursor(self, resp3):
        self.populate(resp3)
        args = ("FT.SEARCH", "idx", "@n:[1 3]", "SORTBY", "n", "RETURN", 1, "n")
        rows = [row(f"doc:{i}".encode(), {b"n": str(i).encode()}) for i in (1, 2, 3)]
        result, cursor = resp3.execute_command(*args, "WITHCURSOR", "COUNT", 2)
        assert result == envelope(3, rows[:2])
        assert cursor != 0
        # Like Redis Search, a read reports no total_results.
        assert resp3.execute_command(
            "FT.CURSOR", "READ", "idx", cursor
        ) == [envelope(0, rows[2:]), 0]

        assert resp3.execute_command(
            *args, "LIMIT", 0, 0, "WITHCURSOR"
        ) == [envelope(3, []), 0]

    def test_aggregate_cursor(self, resp3):
        self.populate(resp3)
        rows = [
            {b"extra_attributes": {b"n": str(i).encode()}, b"values": []}
            for i in (1, 2, 3)
        ]
        result, cursor = resp3.execute_command(
            "FT.AGGREGATE", "idx", "@n:[1 3]", "LOAD", 1, "@n", "SORTBY", 2,
            "@n", "ASC", "WITHCURSOR", "COUNT", 2,
        )
        assert result == envelope(3, rows[:2])
        assert cursor != 0
        assert resp3.execute_command(
            "FT.CURSOR", "READ", "idx", cursor
        ) == [envelope(0, rows[2:]), 0]

    def test_hybrid_keeps_its_reply(self, resp3):
        self.populate(resp3)
        args = (
            "FT.HYBRID", "idx", "SEARCH", "hello", "VSIM", "@v", "$blob",
            "LOAD", 1, "@n", "PARAMS", 2, "blob", struct.pack("ff", 0, 0),
        )
        assert resp3.execute_command(*args) == self.client.execute_command(*args)

    def test_errors(self, resp3):
        with pytest.raises(ResponseError):
            resp3.execute_command("FT.SEARCH", "missing", "*")
        assert resp3.ping()

    def test_protocol_renegotiation(self, resp3):
        self.populate(resp3)
        with resp3.client() as client:
            args = ("FT.SEARCH", "idx", "@n:[1 1]", "NOCONTENT")
            assert client.execute_command(*args) == envelope(1, [row(b"doc:1")])
            client.execute_command("HELLO", 2)
            assert client.execute_command(*args) == [1, b"doc:1"]
            client.execute_command("HELLO", 3)
            assert client.execute_command(*args) == envelope(1, [row(b"doc:1")])


class TestResp3Cluster(ValkeySearchClusterTestCase):
    REPLICAS_COUNT = 0

    def test_fanout(self):
        primary = self.client_for_primary(0)
        kwargs = dict(primary.connection_pool.connection_kwargs)
        kwargs["protocol"] = 3
        pool = ConnectionPool(**kwargs)
        try:
            with Valkey(
                connection_pool=pool
            ) as client, self.new_cluster_client() as writer:
                client.execute_command("FT.CREATE", "idx", "SCHEMA", "n", "NUMERIC")
                for i in range(30):
                    writer.hset(f"doc:{i}", mapping={"n": str(i)})
                deadline = time.monotonic() + 15
                while client.execute_command(
                    "FT.SEARCH", "idx", "@n:[0 +inf]", "LIMIT", 0, 0
                ) != envelope(30, []):
                    assert (
                        time.monotonic() < deadline
                    ), "Cluster indexing did not finish"
                    time.sleep(0.05)
                result = client.execute_command(
                    "FT.SEARCH", "idx", "@n:[0 +inf]", "RETURN", 1, "n", "LIMIT", 0, 30
                )
                result[b"results"].sort(
                    key=lambda item: int(item[b"extra_attributes"][b"n"])
                )
                assert result == envelope(
                    30,
                    [
                        row(f"doc:{i}".encode(), {b"n": str(i).encode()})
                        for i in range(30)
                    ],
                )
                aggregated = client.execute_command(
                    "FT.AGGREGATE",
                    "idx",
                    "@n:[0 +inf]",
                    "LOAD",
                    1,
                    "n",
                )
                aggregated[b"results"].sort(
                    key=lambda item: int(item[b"extra_attributes"][b"n"])
                )
                assert aggregated == envelope(
                    30,
                    [
                        {
                            b"extra_attributes": {b"n": str(i).encode()},
                            b"values": [],
                        }
                        for i in range(30)
                    ],
                )
                assert client.ping()
        finally:
            pool.disconnect()
