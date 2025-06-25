import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestVSSBasic(ValkeySearchTestCaseBase):

    def test_module_loaded(self):
        client: Valkey = self.server.get_new_client()
        # Validate that the valkey-search module is loaded.
        module_list_data = client.execute_command("MODULE LIST")
        module_list_count = len(module_list_data)
        assert module_list_count == 1
        module_loaded = False
        for module in module_list_data:
            if module[b"name"] == b"search":
                module_loaded = True
                break
        assert module_loaded
