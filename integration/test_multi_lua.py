import logging
import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from ft_info_parser import FTInfoParser
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchClusterTestCase,
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import find_local_key
from indexes import *


INDEX = "idx"
OK = b"OK"
QUEUED = b"QUEUED"


def _create_books_price_index(client: Valkey, index: str = INDEX):
    assert client.execute_command("FT.CREATE", index, "SCHEMA", "price", "NUMERIC") == OK


def _search_books(client: Valkey, index: str, l: int, h: int):
    return client.execute_command("FT.SEARCH", index, f"@price:[{l} {h}]")


def _lua_call(cmd: str, *args: str) -> str:
    quoted = ", ".join(f"'{a}'" for a in args)
    return f"return redis.call('{cmd}', {quoted})"


# ---------------------------------------------------------------------------
# CMD (standalone) tests
# ---------------------------------------------------------------------------

class TestMultiLua(ValkeySearchTestCaseBase):

    def test_multi_exec_case1(self):
        """
        Test that HSET done outside a MULTI/EXEC on a key that was modified
        in the MULTI/EXEC does not block the client.
        """
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        assert client.execute_command("MULTI") == OK
        assert client.hset("cpp_book", "price", "60") == QUEUED
        assert client.hset("rust_book", "price", "60") == QUEUED
        assert client.execute_command("EXEC") == [1, 1]
        assert client.hset("rust_book", "price", "50") == 0
        assert _search_books(client, INDEX, 50, 50) == [1, b"rust_book", [b"price", b"50"]]

    def test_multi_exec_case2(self):
        """
        Similar to test case 1, but we perform operations before the MULTI/EXEC block.
        """
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        assert client.hset("cpp_book", "price", "60") == 1
        # We should find the "cpp_book" entry.
        assert _search_books(client, INDEX, 60, 100) == [
            1,
            b"cpp_book",
            [b"price", b"60"],
        ]

        # Begin a MULTI block, update the prices, execute the the MULTI then immediately update the price
        # for the cpp_book, followed by a search query.
        assert client.execute_command("MULTI") == OK
        assert client.hset("cpp_book", "price", 65) == QUEUED
        assert client.hset("rust_book", "price", 50) == QUEUED
        client.execute_command("EXEC")
        # This call should not be blocked.
        assert client.hset("cpp_book", "price", 70) == 0

        # We should only find the "rust_book" entry.
        assert _search_books(client, INDEX, 50, 60) == [
            1,
            b"rust_book",
            [b"price", b"50"],
        ]
    
    def test_multi_exec_ft_list(self):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT._LIST") == QUEUED
        results = client.execute_command("EXEC")
        assert results is not None
        assert INDEX.encode() in results[0]
    
    def test_multi_exec_ft_create(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC") == QUEUED
        results = client.execute_command("EXEC")
        assert results[0] == OK
        assert client.execute_command("FT._LIST")[0] == INDEX.encode()
    
    def test_multi_exec_ft_dropindex(self):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.DROPINDEX", INDEX) == QUEUED
        results = client.execute_command("EXEC")
        assert results[0] == OK
        assert client.execute_command("FT._LIST") == []

    @pytest.mark.parametrize("extra_args", [[], ["LOCAL"]], ids=["info", "info local"])
    def test_multi_exec_ft_info(self, extra_args):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.INFO", INDEX, *extra_args) == QUEUED
        results = client.execute_command("EXEC")
        assert results is not None
        info = FTInfoParser(results[0])
        assert info.index_name == INDEX

    @pytest.mark.parametrize("extra_args", [[], ["LOCALONLY"]], ids=["search", "search localonly"])
    def test_multi_exec_ft_search(self, extra_args):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        client.hset("book:1", "price", "42")
        client.hset("book:2", "price", "44")
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[42 43]", *extra_args) == QUEUED
        results = client.execute_command("EXEC")
        assert results[0] == [1, b"book:1", [b"price", b"42"]]

    def test_multi_exec_ft_aggregate(self):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        client.hset("book:1", "price", "10")
        assert client.execute_command("MULTI") == OK
        assert client.execute_command(
            "FT.AGGREGATE", INDEX, "@price:[5 15]", "LOAD", "1", "price"
        ) == QUEUED
        results = client.execute_command("EXEC")
        assert results[0] == [1, [b'price', b'10']]

    def test_multi_exec_ingestion_consistency(self):
        """Keys ingested inside MULTI/EXEC are visible in a query within the same MULTI/EXEC."""
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        assert client.execute_command("MULTI") == OK
        assert client.hset("book:1", "price", "99") == QUEUED
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[99 99]") == QUEUED
        results = client.execute_command("EXEC")
        assert results[1] == [1, b'book:1', [b'price', b'99']]

    # --- LUA equivalents (CMD) ---

    def test_lua_ft_list(self):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        result = client.execute_command("EVAL", "return redis.call('FT._LIST')", "0")
        assert INDEX.encode() in result
    
    def test_lua_ft_create(self):
        client: Valkey = self.server.get_new_client()
        result = client.execute_command("EVAL", _lua_call("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC"), "0")
        assert result == OK
        assert client.execute_command("FT._LIST")[0] == INDEX.encode()

    def test_lua_ft_dropindex(self):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        result = client.execute_command("EVAL", _lua_call("FT.DROPINDEX", INDEX), "0")
        assert result == OK
        assert client.execute_command("FT._LIST") == []

    @pytest.mark.parametrize("extra_arg", [None, "LOCAL"], ids=["info", "info local"])
    def test_lua_ft_info(self, extra_arg):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        args = (INDEX, extra_arg) if extra_arg else (INDEX,)
        result = client.execute_command("EVAL", _lua_call("FT.INFO", *args), "0")
        info = FTInfoParser(result)
        assert info.index_name == INDEX

    @pytest.mark.parametrize("extra_arg", [None, "LOCALONLY"], ids=["search", "search localonly"])
    def test_lua_ft_search(self, extra_arg):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        client.hset("book:1", "price", "7")
        args = (INDEX, "@price:[7 10]", extra_arg) if extra_arg else (INDEX, "@price:[7 10]")
        result = client.execute_command("EVAL", _lua_call("FT.SEARCH", *args), "0")
        assert result == [1, b'book:1', [b'price', b'7']]

    def test_lua_ft_aggregate(self):
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        client.hset("book:1", "price", "5")
        result = client.execute_command(
            "EVAL", _lua_call("FT.AGGREGATE", INDEX, "@price:[0 10]", "LOAD", "1", "price"), "0"
        )
        assert result == [1, [b'price', b'5']]

    def test_lua_ingestion_consistency(self):
        """Keys ingested inside a Lua script are visible in a query within the same script."""
        client: Valkey = self.server.get_new_client()
        _create_books_price_index(client)
        script = (
            "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) "
            "return redis.call('FT.SEARCH', ARGV[3], ARGV[4])"
        )
        result = client.execute_command(
            "EVAL", script, "1", "book:1", "price", "55", INDEX, "@price:[55 60]"
        )
        assert result == [1, b'book:1', [b'price', b'55']]


# ---------------------------------------------------------------------------
# CME (cluster) tests
# ---------------------------------------------------------------------------

# Error message for cluster fanout in multi/lua context
FANOUT_NOT_SUPPORTED_ERR = "MULTI/EXEC or Lua script are not supported in CME mode"


class TestMultiLuaCluster(ValkeySearchClusterTestCase):

    def _setup_index(self) -> tuple[Valkey, ValkeyCluster]:
        """Create index, insert one document per shard, return (client, cluster)."""
        client: Valkey = self.new_client_for_primary(0)
        cluster: ValkeyCluster = self.new_cluster_client()
        assert client.execute_command("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC") == OK
        for i, primary in enumerate(self.get_all_primary_clients()):
            cluster.execute_command("HSET", find_local_key(primary, f"book:shard{i}:"), "price", "42")
        return client, cluster

    # --- Commands that must always succeed in CME multi/exec ---

    def test_cme_multi_exec_ft_list(self):
        client, _ = self._setup_index()
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT._LIST") == QUEUED
        results = client.execute_command("EXEC")
        assert INDEX.encode() in results[0]

    def test_cme_multi_exec_ft_create(self):
        """FT.CREATE inside MULTI/EXEC in cluster mode skips fanout and returns OK."""
        client: Valkey = self.new_client_for_primary(0)
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC") == QUEUED
        results = client.execute_command("EXEC")
        assert results[0] == OK
        assert client.execute_command("FT._LIST")[0] == INDEX.encode()

    def test_cme_multi_exec_ft_dropindex(self):
        """FT.DROPINDEX inside MULTI/EXEC in cluster mode skips fanout and returns OK."""
        client, _ = self._setup_index()
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.DROPINDEX", INDEX) == QUEUED
        results = client.execute_command("EXEC")
        assert results[0] == OK
        assert client.execute_command("FT._LIST") == []

    @pytest.mark.parametrize("scope", [None, "LOCAL", "PRIMARY", "CLUSTER"], ids=["info", "info local", "info primary", "info cluster"])
    def test_cme_multi_exec_ft_info(self, scope):
        client, _ = self._setup_index()
        assert client.execute_command("MULTI") == OK
        cmd = ["FT.INFO", INDEX] + ([scope] if scope else [])
        assert client.execute_command(*cmd) == QUEUED
        results = client.execute_command("EXEC")
        info = FTInfoParser(results[0])
        assert info.index_name == INDEX

    def test_cme_multi_exec_ft_search_rejects(self):
        """FT.SEARCH without LOCALONLY in MULTI/EXEC must be rejected in cluster mode."""
        client, _ = self._setup_index()
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[42 42]") == QUEUED
        results = client.execute_command("EXEC")
        # EXEC returns error for the queued command
        assert isinstance(results[0], ResponseError)
        assert FANOUT_NOT_SUPPORTED_ERR in str(results[0])

    def test_cme_multi_exec_ft_search_localonly_succeeds(self):
        """FT.SEARCH with LOCALONLY in MULTI/EXEC succeeds in cluster mode."""
        client, cluster = self._setup_index()
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[42 42]", "LOCALONLY") == QUEUED
        results = client.execute_command("EXEC")
        # Returns local shard results only
        assert results[0][0] == 1

    def test_cme_multi_exec_ft_aggregate_rejects(self):
        """FT.AGGREGATE in MULTI/EXEC must be rejected in cluster mode."""
        client, _ = self._setup_index()
        assert client.execute_command("MULTI") == OK
        assert client.execute_command(
            "FT.AGGREGATE", INDEX, "@price:[0 100]", "LOAD", "1", "price"
        ) == QUEUED
        results = client.execute_command("EXEC")
        assert isinstance(results[0], ResponseError)
        assert FANOUT_NOT_SUPPORTED_ERR in str(results[0])

    def test_cme_multi_exec_ingestion_consistency(self):
        """Keys ingested inside MULTI/EXEC are visible in a LOCALONLY query within the same MULTI/EXEC."""
        client: Valkey = self.new_client_for_primary(0)
        assert client.execute_command("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC") == OK
        key = find_local_key(client, "book:")
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("HSET", key, "price", "77") == QUEUED
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[77 77]", "LOCALONLY") == QUEUED
        results = client.execute_command("EXEC")
        assert results[1] == [1, key.encode(), [b'price', b'77']]

    # --- LUA equivalents (CME) ---

    def test_cme_lua_ft_list(self):
        client, _ = self._setup_index()
        result = client.execute_command("EVAL", "return redis.call('FT._LIST')", "0")
        assert INDEX.encode() in result

    @pytest.mark.parametrize("scope", [None, "LOCAL", "PRIMARY", "CLUSTER"], ids=["info", "info local", "info primary", "info cluster"])
    def test_cme_lua_ft_info(self, scope):
        client, _ = self._setup_index()
        args = (INDEX, scope) if scope else (INDEX,)
        result = client.execute_command("EVAL", _lua_call("FT.INFO", *args), "0")
        info = FTInfoParser(result)
        assert info.index_name == INDEX

    def test_cme_lua_ft_search_rejects(self):
        """FT.SEARCH without LOCALONLY in Lua must be rejected in cluster mode."""
        client, _ = self._setup_index()
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("EVAL", _lua_call("FT.SEARCH", INDEX, "@price:[42 42]"), "0")
        assert FANOUT_NOT_SUPPORTED_ERR in str(exc_info.value)

    def test_cme_lua_ft_search_localonly(self):
        """FT.SEARCH with LOCALONLY in Lua succeeds in cluster mode."""
        client, _ = self._setup_index()
        result = client.execute_command(
            "EVAL", _lua_call("FT.SEARCH", INDEX, "@price:[42 42]", "LOCALONLY"), "0"
        )
        assert result[0] == 1

    def test_cme_lua_ft_aggregate_rejects(self):
        """FT.AGGREGATE in Lua must be rejected in cluster mode."""
        client, _ = self._setup_index()
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command(
                "EVAL", _lua_call("FT.AGGREGATE", INDEX, "@price:[0 100]", "LOAD", "1", "price"), "0"
            )
        assert FANOUT_NOT_SUPPORTED_ERR in str(exc_info.value)

    def test_cme_lua_ft_create(self):
        client: Valkey = self.new_client_for_primary(0)
        result = client.execute_command(
            "EVAL", _lua_call("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC"), "0"
        )
        assert result == OK
        assert client.execute_command("FT._LIST")[0] == INDEX.encode()

    def test_cme_lua_ft_dropindex(self):
        client, _ = self._setup_index()
        result = client.execute_command("EVAL", _lua_call("FT.DROPINDEX", INDEX), "0")
        assert result == OK
        assert client.execute_command("FT._LIST") == []

    def test_cme_lua_ingestion_consistency(self):
        """Keys ingested inside Lua are visible in a LOCALONLY query within the same script."""
        client: Valkey = self.new_client_for_primary(0)
        assert client.execute_command("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC") == OK
        key = find_local_key(client, "book:")
        script = (
            "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) "
            "return redis.call('FT.SEARCH', ARGV[3], ARGV[4], 'LOCALONLY')"
        )
        result = client.execute_command("EVAL", script, "1", key, "price", "88", INDEX, "@price:[88 88]")
        assert result == [1, key.encode(), [b'price', b'88']]
