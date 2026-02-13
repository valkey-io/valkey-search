from valkey_search_test_case import *
import pytest
import time
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper
from ft_info_parser import FTInfoParser

def is_backfill_complete(node, index_name):
    raw = node.execute_command("FT.INFO", index_name, "CLUSTER")
    parser = FTInfoParser([])
    info = parser._parse_key_value_list(raw)
    if not info:
        return False
    backfill_in_progress = int(info["backfill_in_progress"])
    state = info["state"]
    return backfill_in_progress == 0 and state == "ready"
class TestSkipInitialScanCMD(ValkeySearchTestCaseBase):
    """Test suite for FT.CREATE SKIPINITIALSCAN option"""

    def test_skipinitialscan_option(self):
        """Test that skipinitialscan prevents initial data scanning"""
        client = self.get_primary_connection()
        
        # Preload hash data
        client.hset("doc:1", mapping={"title": "Document 1", "tag": "red"})
        client.hset("doc:2", mapping={"title": "Document 2", "tag": "blue"})
        
        # Create index without skipinitialscan
        client.execute_command(
            "FT.CREATE", "idx_normal", "ON", "HASH", 
            "SCHEMA", "tag", "TAG"
        )
        
        # Create index with skipinitialscan
        client.execute_command(
            "FT.CREATE", "idx_skip", "ON", "HASH", "SKIPINITIALSCAN",
            "SCHEMA", "title", "TEXT", "tag", "TAG"
        )

        # Add data after index creation
        client.hset("doc:3", mapping={"title": "Document 1", "tag": "red"})
        client.hset("doc:4", mapping={"title": "Document 2", "tag": "blue"})



        waiters.wait_for_true(lambda: IndexingTestHelper.is_backfill_complete_on_node(client, "idx_normal"))        
        
        for index, count in [["idx_normal", 4], ["idx_skip", 2]]:
            results = client.execute_command("FT.SEARCH", index, "@tag:{red | blue}")
            assert results[0] == count

        #
        # Save/restore test
        # 
        client.execute_command("SAVE")
        self.server.restart(remove_rdb=False)

        for index, count in [["idx_normal", 4], ["idx_skip", 2]]:
            results = client.execute_command("FT.SEARCH", index, "@tag:{red | blue}")
            assert results[0] == count

                   

class TestSkipInitialScanCME(ValkeySearchClusterTestCase):
    """Test suite for FT.CREATE SKIPINITIALSCAN option"""

    def test_skipinitialscan_option(self):
        """Test that skipinitialscan prevents initial data scanning"""
        client0 = self.get_primary(0).get_new_client()
        client = self.new_cluster_client()

        # Preload hash data
        for i in range(100):
            client.hset(f"doc:{i}", mapping={"title": "Document 1", "tag": "red"})
        
        # Create index without skipinitialscan
        client0.execute_command(
            "FT.CREATE", "idx_normal", "ON", "HASH", 
            "SCHEMA", "tag", "TAG"
        )
        
        # Create index with skipinitialscan
        client0.execute_command(
            "FT.CREATE", "idx_skip", "ON", "HASH", "SKIPINITIALSCAN",
            "SCHEMA", "title", "TEXT", "tag", "TAG"
        )
        waiters.wait_for_true(lambda: is_backfill_complete(client, "idx_normal"))        

        for index, count in [["idx_normal", 100], ["idx_skip", 0]]:
            for node in self.get_nodes():
                results = node.client.execute_command("FT.SEARCH", index, "@tag:{red | blue}")
                assert results[0] == count

