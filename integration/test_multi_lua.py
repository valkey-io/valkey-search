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
FANOUT_NOT_SUPPORTED_ERR = "MULTI/EXEC or Lua script are not supported in CME mode"


def _lua_call(cmd: str, *args: str) -> str:
    quoted = ", ".join(f"'{a}'" for a in args)
    return f"return redis.call('{cmd}', {quoted})"


class TestMultiLuaCMD(ValkeySearchTestCaseBase):
    """Test all search commands in MULTI/EXEC and Lua contexts for standalone (CMD) mode."""

    def test_multi_exec_all_commands(self):
        client: Valkey = self.server.get_new_client()
        # FT.CREATE in MULTI/EXEC
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC", "title", "TEXT") == QUEUED
        assert client.execute_command("EXEC")[0] == OK

        # FT._LIST in MULTI/EXEC
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT._LIST") == QUEUED
        assert INDEX.encode() in client.execute_command("EXEC")[0]

        # FT.INFO (default and LOCAL) in MULTI/EXEC
        for extra in [[], ["LOCAL"]]:
            assert client.execute_command("MULTI") == OK
            assert client.execute_command("FT.INFO", INDEX, *extra) == QUEUED
            info = FTInfoParser(client.execute_command("EXEC")[0])
            assert info.index_name == INDEX

        # Ingest data and test FT.SEARCH with hybrid query (numeric + text)
        client.hset("doc:1", mapping={"price": "42", "title": "hello world"})
        client.hset("doc:2", mapping={"price": "99", "title": "hello"})
        for extra in [[], ["LOCALONLY"]]:
            assert client.execute_command("MULTI") == OK
            assert client.execute_command("FT.SEARCH", INDEX, "@price:[40 50] @title:hello", *extra) == QUEUED
            results = client.execute_command("EXEC")
            assert results[0][0] == 1 and results[0][1] == b"doc:1"

        # FT.AGGREGATE in MULTI/EXEC
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.AGGREGATE", INDEX, "@price:[5 50]", "LOAD", "1", "price") == QUEUED
        assert client.execute_command("EXEC")[0][0] == 1

        # Ingestion consistency: key ingested in MULTI visible in query within same MULTI
        assert client.execute_command("MULTI") == OK
        assert client.hset("doc:3", "price", "77") == QUEUED
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[77 77]") == QUEUED
        results = client.execute_command("EXEC")
        assert results[1] == [1, b'doc:3', [b'price', b'77']]

        # FT.DROPINDEX in MULTI/EXEC
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.DROPINDEX", INDEX) == QUEUED
        assert client.execute_command("EXEC")[0] == OK
        assert client.execute_command("FT._LIST") == []

    def test_lua_all_commands(self):
        client: Valkey = self.server.get_new_client()

        # FT.CREATE in Lua
        assert client.execute_command("EVAL", _lua_call("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC", "title", "TEXT"), "0") == OK

        # FT._LIST in Lua
        assert INDEX.encode() in client.execute_command("EVAL", "return redis.call('FT._LIST')", "0")

        # FT.INFO (default and LOCAL) in Lua
        for scope in [None, "LOCAL"]:
            args = (INDEX, scope) if scope else (INDEX,)
            info = FTInfoParser(client.execute_command("EVAL", _lua_call("FT.INFO", *args), "0"))
            assert info.index_name == INDEX

        # Ingest data and test FT.SEARCH
        client.hset("doc:1", mapping={"price": "7", "title": "hello world"})
        client.hset("doc:2", mapping={"price": "99", "title": "hello"})
        for scope in [None, "LOCALONLY"]:
            args = (INDEX, "@price:[5 10] @title:hello", scope) if scope else (INDEX, "@price:[5 10] @title:hello")
            result = client.execute_command("EVAL", _lua_call("FT.SEARCH", *args), "0")
            assert result[0] == 1 and result[1] == b'doc:1'

        # FT.AGGREGATE in Lua
        result = client.execute_command("EVAL", _lua_call("FT.AGGREGATE", INDEX, "@price:[0 10]", "LOAD", "1", "price"), "0")
        assert result[0] == 1

        # Ingestion consistency: key ingested in Lua visible in query within same script
        script = "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) return redis.call('FT.SEARCH', ARGV[3], ARGV[4])"
        result = client.execute_command("EVAL", script, "1", "doc:3", "price", "55", INDEX, "@price:[55 60]")
        assert result == [1, b'doc:3', [b'price', b'55']]

        # FT.DROPINDEX in Lua
        assert client.execute_command("EVAL", _lua_call("FT.DROPINDEX", INDEX), "0") == OK
        assert client.execute_command("FT._LIST") == []


class TestMultiLuaCME(ValkeySearchClusterTestCase):
    """Test all search commands in MULTI/EXEC and Lua contexts for cluster (CME) mode."""

    def test_multi_exec_all_commands(self):
        client: Valkey = self.new_client_for_primary(0)
        cluster: ValkeyCluster = self.new_cluster_client()

        # FT.CREATE in MULTI/EXEC
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC", "title", "TEXT") == QUEUED
        assert client.execute_command("EXEC")[0] == OK

        # FT._LIST in MULTI/EXEC
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT._LIST") == QUEUED
        assert INDEX.encode() in client.execute_command("EXEC")[0]

        # FT.INFO in MULTI/EXEC
        for scope in [None, "LOCAL", "PRIMARY", "CLUSTER"]:
            assert client.execute_command("MULTI") == OK
            cmd = ["FT.INFO", INDEX] + ([scope] if scope else [])
            assert client.execute_command(*cmd) == QUEUED
            info = FTInfoParser(client.execute_command("EXEC")[0])
            assert info.index_name == INDEX

        # Ingest data on local shard
        key = find_local_key(client, "doc:")
        cluster.execute_command("HSET", key, "price", "42", "title", "hello world")

        # FT.SEARCH without LOCALONLY must be REJECTED
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[42 42]") == QUEUED
        results = client.execute_command("EXEC")
        assert isinstance(results[0], ResponseError) and FANOUT_NOT_SUPPORTED_ERR in str(results[0])

        # FT.SEARCH with LOCALONLY succeeds
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[42 42] @title:hello", "LOCALONLY") == QUEUED
        assert client.execute_command("EXEC")[0][0] == 1

        # FT.AGGREGATE must be REJECTED
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.AGGREGATE", INDEX, "@price:[0 100]", "LOAD", "1", "price") == QUEUED
        results = client.execute_command("EXEC")
        assert isinstance(results[0], ResponseError) and FANOUT_NOT_SUPPORTED_ERR in str(results[0])

        # Ingestion consistency: key ingested in MULTI visible in LOCALONLY query
        key2 = find_local_key(client, "doc2:")
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("HSET", key2, "price", "77") == QUEUED
        assert client.execute_command("FT.SEARCH", INDEX, "@price:[77 77]", "LOCALONLY") == QUEUED
        results = client.execute_command("EXEC")
        assert results[1] == [1, key2.encode(), [b'price', b'77']]

        # FT.DROPINDEX in MULTI/EXEC
        assert client.execute_command("MULTI") == OK
        assert client.execute_command("FT.DROPINDEX", INDEX) == QUEUED
        assert client.execute_command("EXEC")[0] == OK
        assert client.execute_command("FT._LIST") == []

    def test_lua_all_commands(self):
        client: Valkey = self.new_client_for_primary(0)
        cluster: ValkeyCluster = self.new_cluster_client()

        # FT.CREATE in Lua
        assert client.execute_command("EVAL", _lua_call("FT.CREATE", INDEX, "SCHEMA", "price", "NUMERIC", "title", "TEXT"), "0") == OK

        # FT._LIST in Lua
        assert INDEX.encode() in client.execute_command("EVAL", "return redis.call('FT._LIST')", "0")

        # FT.INFO (all scopes)
        for scope in [None, "LOCAL", "PRIMARY", "CLUSTER"]:
            args = (INDEX, scope) if scope else (INDEX,)
            info = FTInfoParser(client.execute_command("EVAL", _lua_call("FT.INFO", *args), "0"))
            assert info.index_name == INDEX

        # Ingest data on local shard
        key = find_local_key(client, "doc:")
        cluster.execute_command("HSET", key, "price", "42", "title", "hello world")

        # FT.SEARCH without LOCALONLY must be REJECTED
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("EVAL", _lua_call("FT.SEARCH", INDEX, "@price:[42 42]"), "0")
        assert FANOUT_NOT_SUPPORTED_ERR in str(exc_info.value)

        # FT.SEARCH with LOCALONLY succeeds
        result = client.execute_command("EVAL", _lua_call("FT.SEARCH", INDEX, "@price:[42 42] @title:hello", "LOCALONLY"), "0")
        assert result[0] == 1

        # FT.AGGREGATE must be REJECTED
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("EVAL", _lua_call("FT.AGGREGATE", INDEX, "@price:[0 100]", "LOAD", "1", "price"), "0")
        assert FANOUT_NOT_SUPPORTED_ERR in str(exc_info.value)

        # Ingestion consistency: key ingested in Lua visible in LOCALONLY query
        key2 = find_local_key(client, "doc2:")
        script = "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) return redis.call('FT.SEARCH', ARGV[3], ARGV[4], 'LOCALONLY')"
        result = client.execute_command("EVAL", script, "1", key2, "price", "88", INDEX, "@price:[88 88]")
        assert result == [1, key2.encode(), [b'price', b'88']]

        # FT.DROPINDEX in Lua (skips fanout, succeeds)
        assert client.execute_command("EVAL", _lua_call("FT.DROPINDEX", INDEX), "0") == OK
        assert client.execute_command("FT._LIST") == []
