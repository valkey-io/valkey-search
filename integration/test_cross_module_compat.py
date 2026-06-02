"""
Test cross-module compatibility, specifically memory allocator interposition issues.

This test validates the fix for issue #365 where loading valkey-search before
valkey-json would cause crashes due to ELF symbol interposition of memory
allocation functions (__wrap_malloc, __wrap_free, etc.).

The crash occurred because:
1. valkey-search exports __wrap_* symbols globally (for crash dump backtraces)
2. When valkey-json is loaded second, its malloc/free calls resolve to
   valkey-search's __wrap_* functions via ELF interposition
3. valkey-search's memory tracking doesn't know about valkey-json's allocations
4. Crash in je_malloc_usable_size or "free(): invalid size" abort

The fix uses a version script to hide memory wrapper symbols while keeping
other symbols exported for useful backtraces.
"""

from valkey_search_test_case import *
from valkeytestframework.conftest import resource_port_tracker
import valkey
import pytest
import os
from indexes import *


class ValkeySearchFirstTestCase(ValkeySearchTestCaseCommon):
    """
    Test case where valkey-search is loaded BEFORE valkey-json.
    This is the problematic load order that caused issue #365.
    """

    @pytest.fixture(autouse=True)
    def setup_test(self, request):
        test_name = self.normalize_dir_name(request.node.name)
        self.test_name = test_name

        primary = self.start_new_server(is_primary=True)
        self.rg = ReplicationGroup(primary=primary, replicas=[])
        self.server = self.rg.primary.server
        self.client = self.rg.primary.client
        self.nodes: List[Node] = [self.rg.primary]

        yield

        ReplicationGroup.cleanup(self.rg)

    def get_config_file_lines(self, testdir, port) -> List[str]:
        """
        Load valkey-search FIRST, then valkey-json.
        This is the opposite order from the default test cases and
        triggers the ELF symbol interposition issue fixed in #365.
        """
        return [
            "enable-debug-command yes",
            f"dir {testdir}",
            # Load valkey-search FIRST
            f"loadmodule {os.getenv('MODULE_PATH')}",
            # Load valkey-json SECOND - this was crashing before the fix
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
        ]

    def start_new_server(self, is_primary=True) -> Node:
        server, client, logfile = self.start_server(
            self.get_bind_port(),
            self.test_name,
            False,
            is_primary,
        )
        return Node(client=client, server=server, logfile=logfile)


class TestCrossModuleMemoryCompat(ValkeySearchFirstTestCase):
    """
    Test suite for cross-module memory allocator compatibility.
    
    These tests verify that loading valkey-search before valkey-json
    does not cause memory allocation crashes (issue #365).
    """

    def test_json_module_loads_after_search(self):
        """
        Test that valkey-json loads successfully when valkey-search is loaded first.
        
        Before the fix, the server would crash immediately when valkey-json
        tried to allocate memory, because free()/malloc() would resolve to
        valkey-search's __wrap_* functions.
        """
        # If we got here, the server started successfully with both modules
        # Verify both modules are loaded
        modules = self.client.execute_command("MODULE", "LIST")
        module_names = [m[1].decode() if isinstance(m[1], bytes) else m[1] for m in modules]
        
        assert "search" in module_names, "valkey-search module should be loaded"
        assert "ReJSON" in module_names or "json" in module_names, \
            "valkey-json module should be loaded"

    def test_json_operations_with_search_loaded_first(self):
        """
        Test basic JSON operations work when valkey-search is loaded first.
        
        This exercises valkey-json's memory allocator to ensure it doesn't
        crash due to symbol interposition issues.
        """
        # Create a JSON document - this triggers memory allocation in valkey-json
        self.client.execute_command("JSON.SET", "test:doc1", "$", '{"name": "test", "value": 123}')
        
        # Read it back
        result = self.client.execute_command("JSON.GET", "test:doc1")
        assert b'"name"' in result or b'name' in result
        
        # Update it - more memory operations
        self.client.execute_command("JSON.SET", "test:doc1", "$.value", "456")
        
        # Delete it - this triggers free() in valkey-json
        self.client.execute_command("DEL", "test:doc1")

    def test_search_and_json_together(self):
        """
        Test that search and JSON operations work together when search is loaded first.
        
        This is a more comprehensive test that exercises both modules' memory
        allocators interleaved.
        """
        # Create some JSON documents
        for i in range(10):
            self.client.execute_command(
                "JSON.SET", f"test:doc{i}", "$",
                f'{{"id": {i}, "vector": [1.0, 2.0, 3.0]}}'
            )
        
        # Create a search index on JSON documents
        self.client.execute_command(
            "FT.CREATE", "test_idx",
            "ON", "JSON",
            "PREFIX", "1", "test:",
            "SCHEMA",
            "$.id", "AS", "id", "NUMERIC",
            "$.vector", "AS", "vector", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"
        )
        
        # Wait for backfill
        import time
        time.sleep(1)
        
        # Search - this exercises valkey-search's memory allocator
        result = self.client.execute_command(
            "FT.SEARCH", "test_idx", "@id:[0 10]",
            "LIMIT", "0", "10"
        )
        
        # Verify we got results
        assert result[0] >= 1, "Should find at least one document"
        
        # Clean up - triggers free() in both modules
        self.client.execute_command("FT.DROPINDEX", "test_idx")
        for i in range(10):
            self.client.execute_command("DEL", f"test:doc{i}")

    def test_repeated_json_alloc_free_cycles(self):
        """
        Stress test memory allocation/deallocation cycles in valkey-json
        when valkey-search is loaded first.
        
        This test specifically targets the crash scenario where repeated
        alloc/free cycles would eventually hit the wrong tracking data.
        """
        # Do many cycles of create/delete to stress the allocator
        for cycle in range(5):
            # Create many documents
            for i in range(100):
                self.client.execute_command(
                    "JSON.SET", f"stress:doc{i}", "$",
                    f'{{"cycle": {cycle}, "index": {i}, "data": "test_data"}}'
                )
            
            # Delete them all
            for i in range(100):
                self.client.execute_command("DEL", f"stress:doc{i}")
        
        # If we got here without crashing, the fix works
        assert True, "Completed stress test without memory allocator crash"
