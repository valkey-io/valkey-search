"""Integration tests for WITHCURSOR and FT.CURSOR READ/DEL."""

import pytest
from valkey.client import Valkey
from valkey.exceptions import OutOfMemoryError, ResponseError
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
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
        "SORTBY", "2", "@price", "ASC", "WITHCURSOR", *cursor_args,
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
        batch, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert batch == [2, [plain[2]], [plain[3]]]
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
        client.execute_command("FT.DROPINDEX", "idx")
        create_index(client)
        with pytest.raises(ResponseError, match="The index was dropped while the cursor was idle"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert num_cursors(client) == 0


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

    def test_index_dropped(self):
        client = self._client()
        _, cursor = aggregate(client, "COUNT", "1")
        client.execute_command("FT.DROPINDEX", "idx")
        with pytest.raises(ResponseError, match="not found"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert num_cursors(client) == 1
        create_index(client)
        with pytest.raises(ResponseError, match="The index was dropped while the cursor was idle"):
            client.execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert num_cursors(client) == 0

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

        batch, cursor = nodes[0].execute_command("FT.CURSOR", "READ", "idx", cursor)
        assert prices(batch) == [b"30", b"40", b"50"]
        assert cursor == 0
        batch, search_cursor = nodes[0].execute_command(
            "FT.CURSOR", "READ", "idx", search_cursor)
        assert batch == [2, [b"h:4", [b"price", b"40"]], [b"h:5", [b"price", b"50"]]]
        assert search_cursor == 0
        assert num_cursors(nodes[0]) == 0
