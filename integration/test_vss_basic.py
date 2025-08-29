import time
import logging
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker


class TestVSSBasic(ValkeySearchTestCaseBase):

    def test_module_loaded(self):
        client: Valkey = self.server.get_new_client()
        # Validate that the valkey-search module is loaded.
        module_list_data = client.execute_command("MODULE LIST")
        module_list_count = len(module_list_data)
        # We expect JSON & Search to be loaded
        assert module_list_count == 2
        assert b"search" in module_list_data
        assert b"json" in module_list_data

class TestVSSClusterBasic(ValkeySearchClusterTestCase):

    def test_cluster_starting(self):
        client: Valkey = self.new_client_for_primary(0)
        module_list_data = client.execute_command("MODULE LIST")
        module_list_count = len(module_list_data)
        # We expect JSON & Search to be loaded
        assert module_list_count == 2
        assert b"search" in module_list_data
        assert b"json" in module_list_data
        cluster_client: ValkeyCluster = self.new_cluster_client()
        assert cluster_client.set("hello", "world") == True
