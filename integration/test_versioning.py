from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode, ValkeySearchClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging, time
from typing import Any
from util import waiters
import pytest

class TestVersioningCMD(ValkeySearchTestCaseDebugMode):
    def test_versioningCMD(self):
        """
            Test RDB Versioning logic
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("CONFIG SET search.info-developer-visible yes")
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
       
        hnsw_index.create(client)
        hnsw_index.load_data(client, 1000)

        client.execute_command("ft._debug controlled_variable set override_min_version", 10 << 16)

        client.execute_command("save")
        with pytest.raises(ResponseError) as e:
            client.execute_command("DEBUG RELOAD")
        print("Error Message:", str(e))
        assert "Error trying to load the RDB dump" in str(e)

class TestVersioningCME(ValkeySearchClusterTestCaseDebugMode):
    def test_versioningCME(self):
        """
            Test versioning logic of metadata on the wire.
        """
        for c in self.get_all_primary_clients():
            c.execute_command("CONFIG SET search.info-developer-visible yes")

        client: Valkey = self.get_primary(0).get_new_client()
        client.execute_command("ft._debug controlled_variable set override_min_version", 10 << 16)

        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
        
        with pytest.raises(ResponseError) as e:
            hnsw_index.create(client)
        print("Error Message:", str(e))
        assert "Unable to contact all cluster members" in str(e)

        assert client.execute_command("ft._list") == [hnsw_index.name.encode()]
        assert self.get_primary(1).get_new_client().execute_command("ft._list") == []
        assert self.get_primary(2).get_new_client().execute_command("ft._list") == []
