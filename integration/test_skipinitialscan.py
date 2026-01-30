from valkey_search_test_case import *
import pytest
import time
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper

class TestSkipInitialScan(ValkeySearchTestCaseBase):
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
        waiters.wait_for_true(lambda: IndexingTestHelper.is_backfill_complete_on_node(client, "idx_normal"))        
        
        for index, count in [["idx_normal", 2], ["idx_skip", 0]]:
            results = client.execute_command("FT.SEARCH", index, "@tag:{red | blue}")
            assert results[0] == count
