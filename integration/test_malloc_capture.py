from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging, subprocess
from typing import Any, Union
from valkeytestframework.util import waiters

class TestMallocCapture(ValkeySearchTestCaseDebugMode):

    def test_malloc_capture(self):
        """
        Test malloc capture logic
        """
        client: Valkey = self.server.get_new_client()
        assert (
            client.execute_command(
                "CONFIG SET search.info-developer-visible yes"
            )
            == b"OK"
        )
        index = Index(
            "index-name-large-enough-to-require-allocation", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n"), Tag("t")]
        )
        index.create(client)
        index.load_data(client, 10)

        client.execute_command("FT._DEBUG malloc_capture enable")

        for i in range(10):
            nominal_hnsw_result = index.query(client, "@n:[-inf inf]")

        result = client.execute_command("FT._DEBUG malloc_capture get")
        for row in result:
            print("Count: ", row[0])
            for i in range(1, len(row)):
                print(" ",row[i].decode("utf-8"))

        i = client.info("SEARCH")
        assert 10 == i["search_pooled_memory_total_pools"]
        assert 0 == i["search_pooled_memory_active"]

        print("Avg Allocation: ", i["search_pooled_memory_total_bytes"] / 10, " Avg: MaxInuse:", i["search_pooled_memory_total_max_inuse"] / 10)
        print("Total pool Allocations: ", i["search_pooled_memory_total_mallocs"])
