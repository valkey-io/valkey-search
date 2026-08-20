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
        self.verify_modules_loaded(client)


class TestVSSClusterBasic(ValkeySearchClusterTestCase):

    def test_cluster_starting(self):
        client: Valkey = self.new_client_for_primary(0)
        self.verify_modules_loaded(client)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        assert cluster_client.set("hello", "world") == True
