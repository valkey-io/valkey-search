import pytest
import struct
import time
from valkeytestframework.conftest import resource_port_tracker
from valkey_search_test_case import ValkeySearchTestCaseDebugMode

class TestRegistryOwnershipCollision(ValkeySearchTestCaseDebugMode):
    def test_registry_ownership_collision(self):
        """
        Test that an incompatible schema does not erase the registry entry
        of a compatible schema, which would cause a use-after-free crash on ASAN.
        """
        r = self.client

        # Create a vector of 128 dimensions (512 bytes)
        vec128 = struct.pack('128f', *[0.1]*128)
        r.hset('doc1', mapping={'vec': vec128})

        # Create Schema A which perfectly matches the vector
        r.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2')
        
        # Wait for background indexer
        time.sleep(1)

        # Create Schema B which expects 256 dimensions
        try:
            r.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '256', 'DISTANCE_METRIC', 'L2')
        except Exception:
            pass

        # Wait for background indexer to scan the key and reject it
        time.sleep(1)

        # Drop Schema A. If the bug exists, this drops the only reference to the vector record, freeing the memory.
        r.execute_command('FT.DROPINDEX', 'idx1')
        
        # Wait for background untrack
        time.sleep(1)

        # If the memory was freed, ASAN will crash right here when reading the dangling StringRef
        val = r.hget('doc1', 'vec')
        assert val == vec128
