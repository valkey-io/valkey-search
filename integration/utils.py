"""
Utility functions and helper classes for Valkey Search integration tests.
"""

import functools
import threading
import time
from typing import Dict, Any, Optional
from valkey.client import Valkey
from valkey import ResponseError
from ft_info_parser import FTInfoParser
from valkeytestframework.util import waiters


def wait_for_background_tasks(seconds=10):
    """Decorator that adds a sleep after the test body to let runByMain tasks
    complete before the fixture teardown shuts down the server."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            time.sleep(seconds)
            return result
        return wrapper
    return decorator

def run_in_thread(func):
    """Run func in thread, return (thread, result, error) for later inspection."""
    result, error = [None], [None]
    def wrapper():
        try:
            result[0] = func()
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=wrapper)
    t.start()
    return t, result, error

def find_local_key(client: Valkey, prefix: str = "key:") -> str:
    """
    Find a key whose slot is owned by the given node client.

    CLUSTER SLOTS sample output
    [
        [0, 5460,
            ["127.0.0.1", 30001, "09dbe9720cda62f7865eabc5fd8857c5d2678366", ["hostname", "host-1.valkey.example.com"]],
            ["127.0.0.1", 30004, "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf", ["hostname", "host-2.valkey.example.com"]]],
        [5461, 10922, ...],
        [10923, 16383, ...],
    ]
    """
    node_port = client.connection_pool.connection_kwargs['port']
    for slot_range in client.execute_command("CLUSTER", "SLOTS"):
        if slot_range[2][1] == node_port:
            for i in range(10000):
                key = f"{prefix}{i}"
                if slot_range[0] <= client.execute_command("CLUSTER", "KEYSLOT", key) <= slot_range[1]:
                    return key
    raise RuntimeError(f"No key found for node on port {node_port}")

def wait_for_pausepoint(client, pausepoint_name, timeout=10):
    """Wait for a pausepoint to be hit by at least one thread."""
    waiters.wait_for_true(
        lambda: client.execute_command("FT._DEBUG", "PAUSEPOINT", "TEST", pausepoint_name) > 0,
        timeout=timeout
    )
    return True

class IndexingTestHelper:
    """Helper class containing common functions for testing indexing operations."""
    @staticmethod
    def get_ft_info(client: Valkey, index_name: str, cluster=False) -> FTInfoParser:
        """Execute FT.INFO command and return FTInfoParser instance."""
        if cluster:
            info_response = client.execute_command("FT.INFO", index_name, "CLUSTER")
        else:
            info_response = client.execute_command("FT.INFO", index_name)
        return FTInfoParser(info_response)
    
    @staticmethod
    def get_ft_list(client: Valkey) -> set:
        """Execute FT._LIST command and return normalized set of index names as strings."""
        result = client.execute_command("FT._LIST")
        normalized = set()
        for item in result:
            if isinstance(item, bytes):
                normalized.add(item.decode('utf-8'))
            else:
                normalized.add(str(item))
        return normalized
    
    @staticmethod
    def is_indexing_complete_on_node(client: Valkey, index_name: str) -> bool:
        """
        Check if indexing is complete on a specific node.
        
        This is the most comprehensive check that verifies both backfill completion
        and that the index is in ready state.
        
        """
        parser = IndexingTestHelper.get_ft_info(client, index_name)
        return parser.is_backfill_complete() and parser.is_ready()

    @staticmethod
    def is_backfill_complete_on_node(client: Valkey, index_name: str) -> bool:
        """Check if backfill is complete on a single node."""
        parser = IndexingTestHelper.get_ft_info(client, index_name)
        return parser.is_backfill_complete()

    @staticmethod
    def wait_for_backfill_complete_on_node(client: Valkey, index_name: str) -> bool:
        """Check if backfill is complete on a single node."""
        waiters.wait_for_true(lambda: IndexingTestHelper.is_backfill_complete_on_node(client, index_name))
    
    @staticmethod
    def is_indexing_complete_cluster(client: Valkey, index_name: str) -> bool:
        """Check if indexing is complete on a cluster node using CLUSTER mode.""" 
        parser = IndexingTestHelper.get_ft_info(client, index_name, cluster=True)
        return parser.is_backfill_complete() and parser.is_ready()
    
    @staticmethod
    def wait_for_indexing_complete_on_all_nodes(clients: list, index_name: str):
        """Wait for indexing to complete on all provided nodes."""
        
        def check_all_nodes_complete():
            return all(
                IndexingTestHelper.is_indexing_complete_on_node(client, index_name) 
                for client in clients
            )
        
        waiters.wait_for_true(check_all_nodes_complete)
