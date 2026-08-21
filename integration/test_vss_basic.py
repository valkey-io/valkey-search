import time
import logging
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from ft_info_parser import FTInfoParser
from indexes import float_to_bytes
import pytest


def create_vector_index(
    client: Valkey, index_name: str, index_type: str, extra_args: list[str]
):
    assert client.execute_command(
        "FT.CREATE",
        index_name,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "",
        "SCHEMA",
        "v",
        "VECTOR",
        index_type,
        str(6 + len(extra_args)),
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "L2",
        *extra_args,
    ) == b"OK"


def wait_for_backfill_complete(client: Valkey, index_name: str):
    wait_for_true(
        lambda: FTInfoParser(client.execute_command("FT.INFO", index_name)).is_backfill_complete()
    )


def knn_search(client: Valkey, index_name: str, vector: bytes):
    return client.execute_command(
        "FT.SEARCH",
        index_name,
        "*=>[KNN 1 @v $BLOB]",
        "PARAMS",
        "2",
        "BLOB",
        vector,
        "NOCONTENT",
    )


class TestVSSBasic(ValkeySearchTestCaseBase):

    def test_module_loaded(self):
        client: Valkey = self.server.get_new_client()
        self.verify_modules_loaded(client)

    @pytest.mark.parametrize(
        "index_type,extra_args",
        [
            ("HNSW", ["M", "2", "EF_CONSTRUCTION", "1"]),
            ("FLAT", []),
        ],
    )
    def test_zero_length_hash_key_vector_is_indexed(
        self, index_type: str, extra_args: list[str]
    ):
        client: Valkey = self.server.get_new_client()
        query_vector = float_to_bytes([1.0, 2.0, 3.0])

        client.hset("", mapping={"v": query_vector})
        create_vector_index(client, "idx", index_type, extra_args)
        wait_for_backfill_complete(client, "idx")

        result = knn_search(client, "idx", query_vector)
        assert result[0] == 1
        assert result[1] == b""


class TestVSSClusterBasic(ValkeySearchClusterTestCase):

    def test_cluster_starting(self):
        client: Valkey = self.new_client_for_primary(0)
        self.verify_modules_loaded(client)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        assert cluster_client.set("hello", "world") == True
