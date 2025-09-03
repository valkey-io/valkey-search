import pytest
import time
import struct
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

@pytest.mark.cluster
class TestAsyncClientThrottling(ValkeySearchClusterTestCase):
    """
    Test async client throttling during slot migration.
    
    Throttling has 3 phases:
    - Phase 1: Queue <= threshold → no blocking
    - Phase 2: Queue > threshold, sync=0 → block all async
    - Phase 3: Queue > threshold, sync>0 → ratio-based blocking
    
    Tests reduced to 1 writer thread to slow down processing and ensure sustained queue buildup.
    """
    
    CLUSTER_SIZE = 2
    # Disable JSON module to avoid atomic slot migration incompatibility
    LOAD_JSON_MODULE = False
    
    def append_startup_args(self, args):
        """Reduce threads to slow down backfill for testing."""
        args = super().append_startup_args(args)
        args.update({
            "search.writer-threads": "1",
            "search.reader-threads": "1"
        })
        return args
    
    def test_threshold_async_client_throttle(self):
        """
        Test threshold-based blocking with async mutations.
        Verifies blocking when async mutations exceed threshold.
        """
        source_client = self.client_for_primary(0)
        target_client = self.client_for_primary(1)
        
        # Create index on source (may propagate via coordinator)
        source_client.execute_command(
            "FT.CREATE", "idx", "SCHEMA", 
            "description", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"
        )
        
        # Wait for coordinator propagation
        time.sleep(1)
        
        # Enable throttling on BOTH nodes
        for client in [source_client, target_client]:
            client.execute_command(
                "CONFIG", "SET", "search.async-client-throttling-enabled", "yes"
            )
            client.execute_command(
                "CONFIG", "SET", "search.async-clients-block-threshold", "1"
            )
        
        # Add 100 documents with vector data to ensure throttling
        num_keys = 100
        keys = [f"{{migrate}}:p{i}" for i in range(num_keys)]
        
        for i, key in enumerate(keys):
            # Create binary vector data (3 float32 values)
            vector_data = struct.pack('fff', float(i+1), float(i+2), float(i+3))
            source_client.execute_command("HSET", key, "description", vector_data)
        
        # Get slot for migration
        slot = source_client.execute_command("CLUSTER", "KEYSLOT", keys[0])
        
        # Get node IDs
        source_id = source_client.execute_command("CLUSTER", "MYID").decode('utf-8')
        target_id = target_client.execute_command("CLUSTER", "MYID").decode('utf-8')
        
        # Perform atomic slot migration
        source_client.execute_command(
            "CLUSTER", "MIGRATESLOTS",
            "SLOTSRANGE", str(slot), str(slot),
            "NODE", target_id
        )
        
        # Wait for backfill to complete
        time.sleep(3)
        
        # Check post-migration metrics
        search_info = target_client.execute_command("INFO", "search")
        ft_info = target_client.execute_command("FT.INFO", "idx")
        
        # Parse FT.INFO
        ft_dict = {}
        for i in range(0, len(ft_info), 2):
            key = ft_info[i].decode('utf-8') if isinstance(ft_info[i], bytes) else ft_info[i]
            value = ft_info[i+1]
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            ft_dict[key] = value
        
        num_docs = int(ft_dict.get("num_docs", 0))
        last_blocked_duration = int(search_info.get("search_last_throttled_duration_us", 0))
        
        # Validate migration completed successfully
        assert num_docs == num_keys, \
            f"Expected num_docs == {num_keys}, got {num_docs}"
        
        # Validate that blocking occurred with measurable duration
        assert last_blocked_duration > 0, \
            f"Expected blocking duration > 0, got {last_blocked_duration}us"
        
        # Validate queue counters
        async_queue = int(search_info.get("search_async_mutation_queue", -1))
        sync_queue = int(search_info.get("search_sync_mutation_queue", -1))
        assert async_queue >= 0, f"Invalid async_mutation_queue: {async_queue}"
        assert sync_queue == 0, f"Expected sync_queue=0 (Phase 2), got {sync_queue}"
        
        # Note: throttled_clients_count might be 0 after unblocking completes
        # The duration metric proves blocking occurred even if count is currently 0
        
        # Verify all throttling metrics exist
        required_metrics = [
            "search_async_mutation_queue",
            "search_sync_mutation_queue", 
            "search_throttled_clients_count",
            "search_last_throttled_duration_us",
            "search_current_throttled_duration_us"
        ]
        for metric in required_metrics:
            assert metric in search_info, f"Missing required metric: {metric}"
    
    def test_ratio_based_throttle_with_sync_traffic(self):
        """
        Test Phase 3 ratio-based blocking with concurrent async + sync traffic.
        """
        import threading
        
        source_client = self.client_for_primary(0)
        target_client = self.client_for_primary(1)
        cluster_client = self.new_cluster_client()
        
        source_client.execute_command(
            "FT.CREATE", "idx_phase3", "SCHEMA",
            "description", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"
        )
        time.sleep(1)
        
        for client in [source_client, target_client]:
            client.execute_command("CONFIG", "SET", "search.async-client-throttling-enabled", "yes")
            client.execute_command("CONFIG", "SET", "search.async-clients-block-threshold", "3")
        
        # Sync keys on target
        sync_keys = [f"{{sync}}:s{i}" for i in range(50)]
        for i, key in enumerate(sync_keys):
            vector_data = struct.pack('fff', float(200+i), float(201+i), float(202+i))
            target_client.execute_command("HSET", key, "description", vector_data)
        
        # Migration keys on source
        migration_keys = [f"{{phase3}}:p{i}" for i in range(150)]
        for i, key in enumerate(migration_keys):
            vector_data = struct.pack('fff', float(i+1), float(i+2), float(i+3))
            source_client.execute_command("HSET", key, "description", vector_data)
        migration_slot = source_client.execute_command("CLUSTER", "KEYSLOT", migration_keys[0])
        
        # Background sync threads
        stop_background = threading.Event()
        bg_count = [0]
        
        def sync_traffic(tid):
            my_keys = sync_keys[tid::5]
            while not stop_background.is_set():
                for key in my_keys:
                    try:
                        vector_data = struct.pack('fff', float(300+tid), float(301+tid), float(302+tid))
                        cluster_client.execute_command("HSET", key, "description", vector_data)
                        bg_count[0] += 1
                    except:
                        pass
                time.sleep(0.002)
        
        bg_threads = [threading.Thread(target=lambda t=i: sync_traffic(t), daemon=True) for i in range(5)]
        for t in bg_threads:
            t.start()
        time.sleep(0.3)
        
        target_id = target_client.execute_command("CLUSTER", "MYID").decode('utf-8')
        source_client.execute_command("CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", str(migration_slot), str(migration_slot), "NODE", target_id)
        time.sleep(1.5)
        
        stop_background.set()
        for t in bg_threads:
            t.join(timeout=1)
        time.sleep(1)
        
        search_info = target_client.execute_command("INFO", "search")
        duration = int(search_info.get("search_last_throttled_duration_us", 0))
        count = target_client.execute_command("CLUSTER", "COUNTKEYSINSLOT", migration_slot)
        
        assert count == len(migration_keys)
        assert duration > 0
        assert bg_count[0] > 0
