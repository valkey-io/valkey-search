from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
from util import waiters


class TestMultiLua(ValkeySearchTestCaseBase):
    def test_multi_exec(self):
        """
        Test Multi/Exec does not break the logic of the module.
        """
        client: Valkey = self.server.get_new_client()
        assert (
            client.execute_command(
                "FT.CREATE", "idx1", "SCHEMA", "price", "NUMERIC"
            )
            == b"OK"
        )

        assert client.execute_command("MULTI") == b"OK"
        assert (
            client.execute_command("HINCRBYFLOAT hash:1 field:7 1.0")
            == b"QUEUED"
        )
        assert client.execute_command("EXEC") == [b"1"]
        assert client.execute_command("HINCRBYFLOAT hash:1 field:7 1.0") == b"2"
