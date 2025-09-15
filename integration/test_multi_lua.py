from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
from util import waiters


class TestMultiLua(ValkeySearchTestCaseBase):
    BOOKS_INDEX = "books_index"
    QUEUED = b"QUEUED"
    OK = b"OK"

    def _search_books(
        self, client: Valkey, index: str, from_price: int, to_price: int
    ):
        params = [
            "FT.SEARCH",
            index,
            f"@price: [{from_price} {to_price}]",
        ]
        return client.execute_command(*params)

    def _create_books_price_index(self, client: Valkey, index: str):
        assert (
            client.execute_command(
                "FT.CREATE", index, "SCHEMA", "price", "NUMERIC"
            )
            == self.OK
        )

    def test_multi_exec_case1(self):
        """
        Test that HSET done outside a MULTI/EXEC on a key that was modified
        in the MULTI/EXEC does not block the client.
        """
        client: Valkey = self.server.get_new_client()
        self._create_books_price_index(client, self.BOOKS_INDEX)
        assert client.execute_command("MULTI") == self.OK
        assert client.hset("cpp_book", "price", "60") == self.QUEUED
        assert client.hset("rust_book", "price", "60") == self.QUEUED
        assert client.execute_command("EXEC") == [1, 1]
        assert client.hset("rust_book", "price", "50") == 0
        assert self._search_books(client, self.BOOKS_INDEX, 50, 50) == [
            1,
            b"rust_book",
            [b"price", b"50"],
        ]

    def test_multi_exec_case2(self):
        """
        Similar to test case 1, but we perform operations before the MULTI/EXEC block.
        """
        client: Valkey = self.server.get_new_client()
        self._create_books_price_index(client, self.BOOKS_INDEX)
        assert client.hset("cpp_book", "price", "60") == 1
        # We should find the "cpp_book" entry.
        assert self._search_books(client, self.BOOKS_INDEX, 60, 100) == [
            1,
            b"cpp_book",
            [b"price", b"60"],
        ]

        # Begin a MULTI block, update the prices, execute the the MULTI then immediately update the price
        # for the cpp_book, followed by a search query.
        assert client.execute_command("MULTI") == self.OK
        assert client.hset("cpp_book", "price", 65) == self.QUEUED
        assert client.hset("rust_book", "price", 50) == self.QUEUED
        client.execute_command("EXEC")
        # This call should not be blocked.
        assert client.hset("cpp_book", "price", 70) == 0

        # We should only find the "rust_book" entry.
        assert self._search_books(client, self.BOOKS_INDEX, 50, 60) == [
            1,
            b"rust_book",
            [b"price", b"50"],
        ]
