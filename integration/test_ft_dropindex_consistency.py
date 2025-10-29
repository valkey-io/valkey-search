from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkey.exceptions import ResponseError
from valkeytestframework.util import waiters
from concurrent.futures import ThreadPoolExecutor
import pytest

RETRY_MIN_THRESHOLD=50

def do_dropindex(node0, index_name):
    dropindex_result = None
    try:
        dropindex_result = node0.execute_command("FT.DROPINDEX", index_name)
    except Exception as e:
        dropindex_result = e
    return dropindex_result

# type0: reset handle cluster message pausepoint on node1 first
# type1: reset consistency check pausepoint on node0 first
def run_pausepoint_reset(type, node0, node1):
    reset_pausepoint_result = None
    reset_pause_handle_message_result = None
    exception = None
    try:
        def wait_for_pausepoint():
            res = str(node0.execute_command("FT._DEBUG PAUSEPOINT TEST fanout_remote_pausepoint"))
            return int(res) > 0

        def counter_is_increasing():
            count = int(node1.info("SEARCH")["search_pause_handle_cluster_message_round_count"])
            return count > 0

        # wait for reaching consistency check pausepoint
        waiters.wait_for_true(wait_for_pausepoint, timeout=5)
        # wait for reaching handle cluster message pausepoint
        waiters.wait_for_true(counter_is_increasing, timeout=5)

        if type == 0:
            metadata_reconciliation_completed_count_before = node1.info("SEARCH")["search_coordinator_metadata_reconciliation_completed_count"]
            # reset handle cluster message pausepoint first
            reset_pause_handle_message_result = node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage no")
            # wait for metadata to reconcile
            waiters.wait_for_true(
                lambda: int(node1.info("SEARCH")["search_coordinator_metadata_reconciliation_completed_count"]) 
                    > metadata_reconciliation_completed_count_before, 
                timeout=5
            )
            # reset consistency check pausepoint second
            reset_pausepoint_result = node0.execute_command("FT._DEBUG PAUSEPOINT RESET fanout_remote_pausepoint")
        elif type == 1:
            # reset consistency check pausepoint first
            reset_pausepoint_result = node0.execute_command("FT._DEBUG PAUSEPOINT RESET fanout_remote_pausepoint")
            # reset handle cluster message pausepoint second
            reset_pause_handle_message_result = node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage no")
        else:
            # reset pausepoint in invalid type case to prevent infinite loop
            reset_pausepoint_result = node0.execute_command("FT._DEBUG PAUSEPOINT RESET fanout_remote_pausepoint")
            reset_pause_handle_message_result = node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage no")
            exception = ValueError(f"Invalid type {type} for pausepoint order control. Must be 0 or 1")
    except Exception as e:
        exception = e
    return reset_pausepoint_result, reset_pause_handle_message_result, exception

class TestFTDropindexConsistency(ValkeySearchClusterTestCaseDebugMode):

    def test_dropindex_success(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command(
            "FT.DROPINDEX", index_name
        ) == b"OK"

    def test_duplicate_dropindex(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command(
            "FT.DROPINDEX", index_name
        ) == b"OK"

        with pytest.raises(ResponseError) as e:
            node0.execute_command("FT.DROPINDEX", index_name)
        err_msg = "Index with name '" + index_name + "' not found"
        assert err_msg in str(e)

    # synchronize the handle metadata on node1 and consistency check on node0
    # release pausepoint on node1 first to finish metadata reconciliation first
    # expect very small number of retries in consistency check
    def test_dropindex_synchronize_handle_message_first(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]

        assert node0.execute_command("FT._DEBUG PAUSEPOINT SET fanout_remote_pausepoint") == b"OK"
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage yes") == b"OK"

        # use ThreadPoolExecutor to get return values from function
        with ThreadPoolExecutor() as executor:
            future1 = executor.submit(do_dropindex, node0, index_name)
            future2 = executor.submit(run_pausepoint_reset, 0, node0, node1)
            
            dropindex_result = future1.result(timeout=10)
            reset_pausepoint_result, reset_pause_handle_message_result, exception = future2.result(timeout=10)

        assert exception is None, f"Unexpected exception: {exception}"
        assert dropindex_result == b"OK"
        # assert no retry when handle message on node1 released first
        # taking the threshold to account for retries caused by grpc launch latency
        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count_after - retry_count_before <= RETRY_MIN_THRESHOLD
        pause_handle_cluster_message_round_count = int(node1.info("SEARCH")["search_pause_handle_cluster_message_round_count"])
        assert pause_handle_cluster_message_round_count > 0
    
    # synchronize the handle metadata on node1 and consistency check on node0
    # release pausepoint on node0 first to start consistency check first
    # expect large number of retries in consistency check
    def test_dropindex_synchronize_consistency_check_first(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]

        assert node0.execute_command("FT._DEBUG PAUSEPOINT SET fanout_remote_pausepoint") == b"OK"
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET PauseHandleClusterMessage yes") == b"OK"

        with ThreadPoolExecutor() as executor:
            future1 = executor.submit(do_dropindex, node0, index_name)
            future2 = executor.submit(run_pausepoint_reset, 1, node0, node1)
            
            dropindex_result = future1.result(timeout=10)
            reset_pausepoint_result, reset_pause_handle_message_result, exception = future2.result(timeout=10)

        assert exception is None, f"Unexpected exception: {exception}"
        assert dropindex_result == b"OK"
        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count_after - retry_count_before > RETRY_MIN_THRESHOLD
        pause_handle_cluster_message_round_count = int(node1.info("SEARCH")["search_pause_handle_cluster_message_round_count"])
        assert pause_handle_cluster_message_round_count > 0