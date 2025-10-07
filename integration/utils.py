"""
Utility functions and helper classes for Valkey Search integration tests.
"""

from typing import Dict, Any, Optional
from valkey.client import Valkey
from valkey import ResponseError
from ft_info_parser import FTInfoParser

class IndexingTestHelper:
    """
    Helper class containing common functions for testing indexing operations.
    """
    
    @staticmethod
    def is_indexing_complete_on_node(node: Valkey, index_name: str) -> bool:
        """
        Check if indexing is complete on a specific node.
        
        This is the most comprehensive check that verifies both backfill completion
        and that the index is in ready state.
        
        """
        parser = IndexingTestHelper.get_ft_info(node, index_name)
        return parser.is_backfill_complete() and parser.is_ready() if parser else False
    
    @staticmethod
    def is_indexing_complete_cluster(node: Valkey, index_name: str) -> bool:
        """Check if indexing is complete on a cluster node using CLUSTER mode."""
        try:
            raw = node.execute_command("FT.INFO", index_name, "CLUSTER")
            info = _parse_info_kv_list(raw)
            if not info:
                return False
            backfill_in_progress = int(info.get("backfill_in_progress", 1))
            state = info.get("state", "")
            return backfill_in_progress == 0 and state == "ready"
        except ResponseError:
            return False
    
    @staticmethod
    def get_ft_info(client: Valkey, index_name: str) -> Optional[FTInfoParser]:
        """Execute FT.INFO command and return FTInfoParser instance."""
        try:
            info_response = client.execute_command("FT.INFO", index_name)
            return FTInfoParser(info_response)
        except ResponseError:
            return None
    
    @staticmethod
    def is_backfill_complete_simple(client: Valkey, index_name: str) -> bool:
        """Simple check for backfill completion using FTInfoParser."""
        parser = IndexingTestHelper.get_ft_info(client, index_name)
        return parser.is_backfill_complete() if parser else False
    
    @staticmethod
    def wait_for_indexing_complete_on_all_nodes(nodes: list, index_name: str, timeout: int = 10) -> bool:
        """Wait for indexing to complete on all provided nodes."""
        import time
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_complete = True
            for node in nodes:
                if not IndexingTestHelper.is_indexing_complete_on_node(node, index_name):
                    all_complete = False
                    break
            
            if all_complete:
                return True
            
            time.sleep(0.1)  # Small delay between checks
        
        return False
