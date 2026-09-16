"""Integration tests for WITHCURSOR and FT.CURSOR READ/DEL."""

import json
import struct

import pytest
from valkey.client import Valkey
from valkey.exceptions import OutOfMemoryError, ResponseError
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchClusterTestCaseDebugMode,
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper

QUERY = "@price:[-inf inf]"
NUM_DOCS = 5


def create_index(client: Valkey, name="idx"):
    client.execute_command(
        "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", "h:",
        "SCHEMA", "name", "TAG", "price", "NUMERIC",
    )
    waiters.wait_for_true(
        lambda: IndexingTestHelper.is_indexing_complete_on_node(client, name))


def populate(client: Valkey):
    for i in range(1, NUM_DOCS + 1):
        client.execute_command("HSET", f"h:{i}", "name", f"n{i}", "price", i * 10)


def aggregate(client: Valkey, *cursor_args, index="idx"):
    return client.execute_command(
        "FT.AGGREGATE", index, QUERY, "LOAD", "1", "@price",
        "SORTBY", "2", "@price", "ASC", "MAX", "1000", "WITHCURSOR", *cursor_args,
    )


def prices(batch):
    """[count, [price, p], ...] -> [p, ...], checking the count."""
    assert batch[0] == len(batch) - 1
    return [row[1] for row in batch[1:]]


def num_cursors(client: Valkey):
    return int(client.info("SEARCH")["search_num_cursors"])


def search(client: Valkey, *args):
    return client.execute_command(
        "FT.SEARCH", "idx", QUERY, "SORTBY", "price", "ASC", *args)


class TestSearchCursor(ValkeySearchTestCaseBase):
    def _client(self) -> Valkey:
        client: Valkey = self.server.get_new_client()
        create_index(client)
        populate(client)
        return client

    def test_paging_to_exhaustion(self):
        client = self._client()
        total, rows, cursor = search(
            client, "RETURN", "1", "price", "WITHCURSOR", "COUNT", "2")
        assert total == NUM_DOCS
        assert rows == [[b"h:1", [b"price", b"10"]], [b"h:2", [b"price", b"20"]]]
        assert cursor != 0
        assert num_cursors(client) == 1

        # FT.CURSOR READ replies [[n, row...], cursor] for search cursors too.
        batch, next_cursor = client.execute_command(
            "FT.CURSOR", "READ", "idx", cursor, "COUNT", "2")
        assert batch == [2, [b"h:3", [b"price", b"30"]], [b"h:4", [b"price", b"40"]]]
        assert next_cursor == cursor
        batch, next_cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert batch == [1, [b"h:5", [b"price", b"50"]]]
        assert next_cursor == 0
        assert num_cursors(client) == 0

    def test_row_shapes(self):
        client = self._client()
        # Each row matches one row of the non-cursor reply.
        # RETURN fixes the field order, which otherwise varies between queries.
        fields = ["RETURN", "2", "price", "name", "WITHSORTKEYS", "LIMIT", "0", "2"]
        plain = search(client, *fields)
        total, rows, cursor = search(client, *fields, "WITHCURSOR")
        assert total == plain[0]
        assert rows == [plain[1:4], plain[4:7]]
        assert cursor == 0

        plain = search(client, "NOCONTENT", "LIMIT", "0", "3")
        total, rows, cursor = search(client, "NOCONTENT", "LIMIT", "0", "3",
                                     "WITHCURSOR", "COUNT", "1")
        assert rows == [[plain[1]]]
        # READ without COUNT reuses the WITHCURSOR COUNT of 1.
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert batch == [1, [plain[2]]]
        batch, cursor = client.execute_command(
            "FT.CURSOR", "READ", "idx", cursor, "COUNT", "2")
        assert batch == [1, [plain[3]]]
        assert cursor == 0

    def test_limit_window(self):
        client = self._client()
        # The cursor pages through the LIMIT window only.
        total, rows, cursor = search(client, "RETURN", "0", "LIMIT", "1", "3",
                                     "WITHCURSOR", "COUNT", "2")
        assert total == NUM_DOCS
        assert [row[0] for row in rows] == [b"h:2", b"h:3"]
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert [row[0] for row in batch[1:]] == [b"h:4"]
        assert cursor == 0
        assert search(client, "LIMIT", "0", "0", "WITHCURSOR") == [NUM_DOCS, [], 0]

    def test_index_dropped(self):
        client = self._client()
        _, _, cursor = search(client, "WITHCURSOR", "COUNT", "1")
        # Dropping the index discards its cursors right away.
        client.execute_command("FT.DROPINDEX", "idx")
        assert num_cursors(client) == 0
        create_index(client)
        with pytest.raises(ResponseError, match="Cursor not found"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor)


class TestAggregateCursor(ValkeySearchTestCaseBase):
    def _client(self) -> Valkey:
        client: Valkey = self.server.get_new_client()
        create_index(client)
        populate(client)
        return client

    def test_paging_to_exhaustion(self):
        client = self._client()
        batch, cursor = aggregate(client, "COUNT", "2")
        assert prices(batch) == [b"10", b"20"]
        assert cursor != 0
        assert num_cursors(client) == 1
        assert client.execute_command("FT._DEBUG", "SHOW_CURSORS")[0][0] == cursor

        batch, next_cursor = client.execute_command(
            "FT.CURSOR", "READ", "idx", cursor, "COUNT", "2")
        assert prices(batch) == [b"30", b"40"]
        assert next_cursor == cursor

        batch, next_cursor = client.execute_command(
            "FT.CURSOR", "READ", "idx", cursor)
        assert prices(batch) == [b"50"]
        assert next_cursor == 0
        assert num_cursors(client) == 0

        with pytest.raises(ResponseError, match=f"Cursor not found, id: {cursor}"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor)

    def test_all_rows_in_first_reply(self):
        client = self._client()
        batch, cursor = aggregate(client, "COUNT", "5")
        assert prices(batch) == [b"10", b"20", b"30", b"40", b"50"]
        assert cursor == 0
        batch, cursor = aggregate(client)  # default COUNT is 1000
        assert len(prices(batch)) == NUM_DOCS
        assert cursor == 0
        assert num_cursors(client) == 0

    def test_withcursor_placement(self):
        client = self._client()
        for args in [
            ["WITHCURSOR", "COUNT", "2", "LOAD", "1", "@price",
             "SORTBY", "2", "@price", "ASC"],
            ["LOAD", "1", "@price", "WITHCURSOR", "COUNT", "2",
             "SORTBY", "2", "@price", "ASC"],
            ["LOAD", "1", "@price", "SORTBY", "2", "@price", "ASC",
             "WITHCURSOR", "COUNT", "9", "WITHCURSOR", "COUNT", "2"],
        ]:
            batch, cursor = client.execute_command(
                "FT.AGGREGATE", "idx", QUERY, *args)
            assert prices(batch) == [b"10", b"20"], args
            assert client.execute_command("FT.CURSOR", "DEL", "idx", cursor) == b"OK"
        assert num_cursors(client) == 0

    def test_del(self):
        client = self._client()
        _, cursor = aggregate(client, "COUNT", "1")
        assert client.execute_command("FT.CURSOR", "DEL", "idx", cursor) == b"OK"
        assert num_cursors(client) == 0
        with pytest.raises(ResponseError, match="Cursor does not exist"):
            client.execute_command("FT.CURSOR", "DEL", "idx", cursor)

    def test_other_index_and_db(self):
        client = self._client()
        create_index(client, "other")
        _, cursor = aggregate(client, "COUNT", "1")
        # As in Redis, any existing index may be named.
        batch, _ = client.execute_command("FT.CURSOR", "READ", "other", cursor, "COUNT", "1")
        assert prices(batch) == [b"20"]
        with pytest.raises(ResponseError, match="not found"):
            client.execute_command("FT.CURSOR", "READ", "nosuch", cursor)
        # A cursor can only be used from the database it was created in.
        db1: Valkey = self.server.get_new_client()
        db1.select(1)
        create_index(db1)
        with pytest.raises(ResponseError, match="Cursor not found"):
            db1.execute_command("FT.CURSOR", "READ", "idx", cursor)
        with pytest.raises(ResponseError, match="Cursor does not exist"):
            db1.execute_command("FT.CURSOR", "DEL", "idx", cursor)
        batch, _ = client.execute_command("FT.CURSOR", "READ", "idx", cursor, "COUNT", "1")
        assert prices(batch) == [b"30"]
        assert client.execute_command("FT.CURSOR", "DEL", "other", cursor) == b"OK"

    def test_index_removal_discards_cursors(self):
        client = self._client()
        create_index(client, "other")
        _, cursor = aggregate(client, "COUNT", "1")
        _, other_cursor = aggregate(client, "COUNT", "1", index="other")
        assert num_cursors(client) == 2

        # DROPINDEX discards only that index's cursors.
        client.execute_command("FT.DROPINDEX", "idx")
        assert num_cursors(client) == 1
        with pytest.raises(ResponseError, match="Cursor not found"):
            client.execute_command("FT.CURSOR", "READ", "other", cursor)
        client.execute_command("FT.CURSOR", "READ", "other", other_cursor, "COUNT", "1")

        # FLUSHDB discards the rest.
        client.execute_command("FLUSHDB")
        assert num_cursors(client) == 0

    def test_swapdb_moves_cursors(self):
        client = self._client()
        _, cursor = aggregate(client, "COUNT", "1")
        client.execute_command("SWAPDB", "0", "1")
        # The index moved to db 1, and so did its cursor.
        with pytest.raises(ResponseError, match="not found in database 0"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        db1: Valkey = self.server.get_new_client()
        db1.select(1)
        batch, _ = db1.execute_command("FT.CURSOR", "READ", "idx", cursor, "COUNT", "1")
        assert prices(batch) == [b"20"]
        assert db1.execute_command("FT.CURSOR", "DEL", "idx", cursor) == b"OK"

    def test_max_idle_expiration(self):
        client = self._client()
        _, cursor = aggregate(client, "COUNT", "1", "MAXIDLE", "100")
        _, keep = aggregate(client, "COUNT", "1")
        assert num_cursors(client) == 2
        waiters.wait_for_equal(lambda: num_cursors(client), 1, timeout=5)
        assert client.execute_command("FT._DEBUG", "SHOW_CURSORS")[0][0] == keep
        with pytest.raises(ResponseError, match="Cursor not found"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        client.execute_command("FT.CURSOR", "DEL", "idx", keep)

    def test_limits(self):
        client = self._client()
        for args, error in [
            (["COUNT", "0"], "COUNT must be between 1 and 100000"),
            (["COUNT", "100001"], "COUNT must be between 1 and 100000"),
            (["MAXIDLE", "0"], "MAXIDLE must be between 1 and"),
        ]:
            with pytest.raises(ResponseError, match=error):
                aggregate(client, *args)
        client.execute_command("CONFIG", "SET", "search.cursor-max-count", "3")
        client.execute_command("CONFIG", "SET", "search.cursor-max-idle-ms", "1000")
        with pytest.raises(ResponseError, match="COUNT must be between 1 and 3"):
            aggregate(client, "COUNT", "4")
        with pytest.raises(ResponseError, match="MAXIDLE must be between 1 and 1000"):
            aggregate(client, "MAXIDLE", "1001")
        _, cursor = aggregate(client, "COUNT", "3")
        with pytest.raises(ResponseError, match="COUNT must be between 1 and 3"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor, "COUNT", "4")
        # The default MAXIDLE is capped by the configured maximum.
        assert client.execute_command("FT._DEBUG", "SHOW_CURSORS")[0][1] <= 1000
        client.execute_command("FT.CURSOR", "DEL", "idx", cursor)

    def test_multi(self):
        client = self._client()
        pipe = client.pipeline(transaction=True)
        pipe.execute_command("FT.AGGREGATE", "idx", QUERY, "LOAD", "1", "@price",
                             "SORTBY", "2", "@price", "ASC", "WITHCURSOR", "COUNT", "3")
        (batch, cursor), = pipe.execute()
        assert prices(batch) == [b"10", b"20", b"30"]
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert prices(batch) == [b"40", b"50"]
        assert cursor == 0

    def test_oom(self):
        client = self._client()
        _, cursor = aggregate(client, "COUNT", "1")
        client.config_set("maxmemory", client.info("memory")["used_memory"])
        try:
            with pytest.raises(OutOfMemoryError):
                aggregate(client, "COUNT", "1")
            # Cursors can still be read and released.
            batch, _ = client.execute_command("FT.CURSOR", "READ", "idx", cursor, "COUNT", "1")
            assert prices(batch) == [b"20"]
            assert client.execute_command("FT.CURSOR", "DEL", "idx", cursor) == b"OK"
        finally:
            client.config_set("maxmemory", 0)


class TestCursorCluster(ValkeySearchClusterTestCase):
    def test_cursor_is_node_local(self):
        cluster_client = self.new_cluster_client()
        cluster_client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "h:",
            "SCHEMA", "name", "TAG", "price", "NUMERIC",
        )
        populate(cluster_client)
        nodes = [self.client_for_primary(i) for i in range(len(self.replication_groups))]
        for node in nodes:
            waiters.wait_for_true(
                lambda n=node: IndexingTestHelper.is_indexing_complete_on_node(n, "idx"))

        # The query fans out; the cursor lives on the node that ran it.
        batch, cursor = aggregate(nodes[0], "COUNT", "2")
        assert prices(batch) == [b"10", b"20"]
        total, rows, search_cursor = search(
            nodes[0], "RETURN", "1", "price", "WITHCURSOR", "COUNT", "3")
        assert total == NUM_DOCS
        assert [row[1] for row in rows] == [[b"price", b"10"], [b"price", b"20"], [b"price", b"30"]]
        assert num_cursors(nodes[0]) == 2
        with pytest.raises(ResponseError, match="Cursor not found"):
            nodes[1].execute_command("FT.CURSOR", "READ", "idx", cursor)

        batch, cursor = nodes[0].execute_command(
            "FT.CURSOR", "READ", "idx", cursor, "COUNT", "3")
        assert prices(batch) == [b"30", b"40", b"50"]
        assert cursor == 0
        batch, search_cursor = nodes[0].execute_command(
            "FT.CURSOR", "READ", "idx", search_cursor)
        assert batch == [2, [b"h:4", [b"price", b"40"]], [b"h:5", [b"price", b"50"]]]
        assert search_cursor == 0
        assert num_cursors(nodes[0]) == 0


class TestCursorAcl(ValkeySearchTestCaseBase):
    def test_acl_rejects_cursor_access(self):
        client: Valkey = self.server.get_new_client()
        create_index(client)
        populate(client)
        # A user that may not read the index's keys.
        client.execute_command("ACL SETUSER u on >p ~other:* &* +@all")
        _, agg_cursor = aggregate(client, "COUNT", "1")
        _, _, search_cursor = search(client, "WITHCURSOR", "COUNT", "1")

        denied = "The user does not have permission to access the key prefix"
        other: Valkey = self.server.get_new_client()
        other.execute_command("AUTH", "u", "p")
        # The queries themselves are rejected...
        with pytest.raises(ResponseError, match=denied):
            aggregate(other, "COUNT", "1")
        with pytest.raises(ResponseError, match=denied):
            search(other, "WITHCURSOR", "COUNT", "1")
        # ... and so is reading or releasing someone else's cursor.
        for cursor in (agg_cursor, search_cursor):
            with pytest.raises(ResponseError, match=denied):
                other.execute_command("FT.CURSOR", "READ", "idx", cursor)
            with pytest.raises(ResponseError, match=denied):
                other.execute_command("FT.CURSOR", "DEL", "idx", cursor)

        # A rejected command leaves the cursors untouched.
        assert num_cursors(client) == 2
        batch, _ = client.execute_command("FT.CURSOR", "READ", "idx", agg_cursor, "COUNT", "1")
        assert prices(batch) == [b"20"]
        client.execute_command("FT.CURSOR", "DEL", "idx", agg_cursor)
        client.execute_command("FT.CURSOR", "DEL", "idx", search_cursor)


class TestCursorReadDefaultCount(ValkeySearchTestCaseBase):
    def _client(self) -> Valkey:
        client: Valkey = self.server.get_new_client()
        create_index(client)
        populate(client)
        return client

    def test_default_count_comes_from_withcursor(self):
        client = self._client()
        batch, cursor = aggregate(client, "COUNT", "3")
        assert len(prices(batch)) == 3
        # No COUNT on READ: the WITHCURSOR COUNT is used, not the 1000 default.
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert len(prices(batch)) == 2  # only 2 rows left of 5
        assert cursor == 0

        # An explicit COUNT applies to that read only.
        batch, cursor = aggregate(client, "COUNT", "2")
        batch, cursor = client.execute_command(
            "FT.CURSOR", "READ", "idx", cursor, "COUNT", "1")
        assert len(prices(batch)) == 1
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert len(prices(batch)) == 2
        assert cursor == 0  # 2 + 1 + 2 rows read of 5

        # FT.SEARCH cursors behave the same way.
        _, rows, cursor = search(client, "NOCONTENT", "LIMIT", "0", "5",
                                 "WITHCURSOR", "COUNT", "2")
        assert len(rows) == 2
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert batch[0] == 2
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert batch[0] == 1
        assert cursor == 0

    def test_default_count_without_withcursor_count(self):
        client = self._client()
        # WITHCURSOR with no COUNT means 1000, so everything fits in one reply.
        batch, cursor = aggregate(client)
        assert len(prices(batch)) == NUM_DOCS
        assert cursor == 0


class TestCursorInFlightAccounting(ValkeySearchTestCaseDebugMode):
    def test_open_cursor_is_not_an_in_flight_query(self):
        client: Valkey = self.server.get_new_client()
        create_index(client)
        populate(client)
        _, cursor = aggregate(client, "COUNT", "1")
        assert num_cursors(client) == 1
        # The query is over even though the cursor holds its output.
        waiters.wait_for_equal(
            lambda: int(client.info("SEARCH")["search_async_queries_in_flight"]), 0)
        client.execute_command("FT.CURSOR", "DEL", "idx", cursor)
        assert int(client.info("SEARCH")["search_async_queries_in_flight"]) == 0


# The power-set content test below runs every combination of these dimensions
# and checks that paging a cursor returns exactly what the same query returns
# without WITHCURSOR.
CORPUS_SIZE = 20
CORPUS_INDEX = "cidx"


def build_corpus(client: Valkey, key_type: str, nodes=None):
    """Creates the `cidx` index over `c:` keys and fills it."""
    vec = ("vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "3",
           "DISTANCE_METRIC", "L2")
    if key_type == "HASH":
        schema = ("price", "NUMERIC", "tag", "TAG") + vec
        on = ("ON", "HASH")
    else:
        schema = ("$.price", "AS", "price", "NUMERIC",
                  "$.tag", "AS", "tag", "TAG", "$.vec", "AS") + vec
        on = ("ON", "JSON")
    client.execute_command("FT.CREATE", CORPUS_INDEX, *on, "PREFIX", "1", "c:",
                           "SCHEMA", *schema)
    for i in range(CORPUS_SIZE):
        values = [float(i), float(i + 1), float(i + 2)]
        if key_type == "HASH":
            client.execute_command("HSET", f"c:{i:02d}", "price", i, "tag", "a",
                                   "vec", struct.pack("<3f", *values))
        else:
            client.execute_command(
                "JSON.SET", f"c:{i:02d}", "$",
                json.dumps({"price": i, "tag": "a", "vec": values}))
    for node in (nodes or [client]):
        waiters.wait_for_true(
            lambda n=node: IndexingTestHelper.is_indexing_complete_on_node(
                n, CORPUS_INDEX))


def corpus_query(query_type: str):
    """Returns (query, trailing args) for a vector or non-vector query."""
    if query_type == "vector":
        blob = struct.pack("<3f", 1.0, 2.0, 3.0)
        return ("*=>[KNN 10 @vec $BLOB]", ["PARAMS", "2", "BLOB", blob])
    return ("@price:[-inf +inf]", [])


def corpus_command(command: str, query_type: str):
    """Returns the command words, without any WITHCURSOR clause."""
    query, params = corpus_query(query_type)
    if command == "AGGREGATE":
        return (["FT.AGGREGATE", CORPUS_INDEX, query, "LOAD", "2", "@__key",
                 "@price", "SORTBY", "2", "@price", "ASC", "MAX", "1000"]
                + params)
    args = ["FT.SEARCH", CORPUS_INDEX, query, "SORTBY", "price", "ASC",
            "LIMIT", "0", "100"]
    if command == "SEARCH-NOCONTENT":
        args.append("NOCONTENT")
    else:
        args += ["RETURN", "1", "price"]
    return args + params


def freeze(value):
    """Makes a reply row hashable and comparable."""
    if isinstance(value, list):
        return tuple(freeze(item) for item in value)
    return value


def row_key(command: str, row):
    """The key of a row: __key for aggregate rows, the first element
    otherwise."""
    if command == "AGGREGATE":
        return row[row.index(b"__key") + 1]
    return row[0]


def plain_rows(command: str, reply):
    """Rows of a reply with no WITHCURSOR, as a list of comparable items."""
    if command == "AGGREGATE":
        assert reply[0] == len(reply) - 1
        return [freeze(row) for row in reply[1:]]
    if command == "SEARCH-NOCONTENT":
        return [(key,) for key in reply[1:]]
    return [freeze(reply[i:i + 2]) for i in range(1, len(reply), 2)]


def cursor_rows(command: str, reply):
    """(rows, cursor_id) of the first reply of a WITHCURSOR query."""
    if command == "AGGREGATE":
        batch, cursor = reply
        assert batch[0] == len(batch) - 1
        return [freeze(row) for row in batch[1:]], cursor
    _, rows, cursor = reply
    return [freeze(row) for row in rows], cursor


def read_rows(client: Valkey, cursor: int, rows: list):
    """Pages an open cursor to exhaustion, appending to `rows`."""
    while cursor:
        batch, cursor = client.execute_command(
            "FT.CURSOR", "READ", CORPUS_INDEX, cursor)
        assert batch[0] == len(batch) - 1
        rows += [freeze(row) if isinstance(row, list) else (row,)
                 for row in batch[1:]]
    return rows


def force_timeout(clients, enabled: bool):
    for client in clients:
        client.execute_command(
            f"FT._DEBUG CONTROLLED_VARIABLE SET ForceTimeout "
            f"{'yes' if enabled else 'no'}")
        client.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET timeoutpollfrequency 1")


class CursorContentsMixin:
    """Runs one power-set case: the cursor's rows must match the plain reply."""

    def run_case(self, client: Valkey, command: str, query_type: str,
                 in_multi: bool, timeout: bool, nodes=None):
        nodes = nodes or [client]
        args = corpus_command(command, query_type)
        expected = plain_rows(command, client.execute_command(*args))
        if not timeout:
            assert expected, "the plain query returned nothing"

        cursor_args = args + ["WITHCURSOR", "COUNT", "3"]
        if timeout:
            # The partial results setting must not matter for a cursor.
            for node in nodes:
                node.execute_command("CONFIG", "SET",
                                     "search.enable-partial-results", "no")
            force_timeout(nodes, True)
        try:
            if in_multi:
                pipe = client.pipeline(transaction=True)
                pipe.execute_command(*cursor_args)
                reply, = pipe.execute()
            else:
                reply = client.execute_command(*cursor_args)
        finally:
            if timeout:
                force_timeout(nodes, False)
                for node in nodes:
                    node.execute_command("CONFIG", "SET",
                                         "search.enable-partial-results", "yes")

        rows, cursor = cursor_rows(command, reply)
        assert len(rows) <= 3
        read_rows(client, cursor, rows)
        if timeout:
            # A timed-out query returns what it found, never an error. A
            # partial vector search can surface documents the complete query
            # would have ranked out, so only the keys are checked.
            corpus = {f"c:{i:02d}".encode() for i in range(CORPUS_SIZE)}
            assert {row_key(command, row) for row in rows} <= corpus
            assert len(rows) <= len(expected)
        else:
            assert sorted(rows) == sorted(expected)
        waiters.wait_for_equal(lambda: num_cursors(client), 0)


@pytest.mark.parametrize("key_type", ["HASH", "JSON"])
@pytest.mark.parametrize("query_type", ["nonvector", "vector"])
@pytest.mark.parametrize("command", ["AGGREGATE", "SEARCH", "SEARCH-NOCONTENT"])
@pytest.mark.parametrize("in_multi", [False, True])
@pytest.mark.parametrize("timeout", [False, True])
class TestCursorContentsCMD(ValkeySearchTestCaseDebugMode, CursorContentsMixin):
    def test_cursor_contents(self, key_type, query_type, command, in_multi,
                             timeout):
        client: Valkey = self.server.get_new_client()
        build_corpus(client, key_type)
        self.run_case(client, command, query_type, in_multi, timeout)


@pytest.mark.parametrize("key_type", ["HASH", "JSON"])
@pytest.mark.parametrize("query_type", ["nonvector", "vector"])
@pytest.mark.parametrize("command", ["AGGREGATE", "SEARCH", "SEARCH-NOCONTENT"])
class TestCursorContentsCME(ValkeySearchClusterTestCaseDebugMode,
                            CursorContentsMixin):
    def test_cursor_contents(self, key_type, query_type, command):
        for timeout in (False, True):
            cluster_client = self.new_cluster_client()
            nodes = [self.client_for_primary(i)
                     for i in range(len(self.replication_groups))]
            if timeout:
                cluster_client.execute_command("FT.DROPINDEX", CORPUS_INDEX)
                cluster_client.execute_command("FLUSHALL")
            build_corpus(cluster_client, key_type, nodes)
            # The query fans out from the node it is sent to.
            self.run_case(nodes[0], command, query_type, False, timeout, nodes)
