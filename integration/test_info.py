import struct
import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchClusterTestCase, ValkeySearchTestCaseBase
from indexes import *
from valkeytestframework.conftest import resource_port_tracker
import pytest
import os
import platform


class TestVSSBasic(ValkeySearchTestCaseBase):

    def test_info_fields_present(self):
        client: Valkey = self.server.get_new_client()
        # Validate that the info fields are present.
        info_data = client.info("SEARCH")

        integer_fields = [
            "search_query_queue_size",
            "search_writer_queue_size",
            "search_worker_pool_suspend_cnt",
            "search_writer_resumed_cnt",
            "search_reader_resumed_cnt",
            "search_writer_suspension_expired_cnt",
            "search_rdb_load_success_cnt",
            "search_rdb_load_failure_cnt",
            "search_rdb_save_success_cnt",
            "search_rdb_save_failure_cnt",
            "search_successful_requests_count",
            "search_failure_requests_count",
            "search_hybrid_requests_count",
            "search_inline_filtering_requests_count",
            "search_hnsw_add_exceptions_count",
            "search_hnsw_remove_exceptions_count",
            "search_hnsw_modify_exceptions_count",
            "search_hnsw_search_exceptions_count",
            "search_hnsw_create_exceptions_count",
            "search_string_interning_store_size",
            "search_string_interning_memory_bytes",
            "search_string_interning_memory_human", # less than 1KiB
            "search_vector_externing_entry_count",
            "search_vector_externing_hash_extern_errors",
            "search_vector_externing_generated_value_cnt",
            "search_vector_externing_num_lru_entries",
            "search_vector_externing_lru_promote_cnt",
            "search_vector_externing_deferred_entry_cnt",
            "search_number_of_attributes",
            "search_number_of_indexes",
            "search_total_indexed_documents",
            "search_total_active_write_threads",
            "search_total_indexing_time",
            "search_used_memory_bytes",
            "search_index_reclaimable_memory"
        ]

        string_fields = [
            "search_background_indexing_status"
        ]

        bytes_fields = [
            "search_used_memory_human"
        ]
        
        double_fields = [
            "search_used_read_cpu",
            "search_used_write_cpu"
        ]

        for field in integer_fields:
            assert field in info_data
            print (info_data)
            integer = int(info_data.get(field))
        
        for field in double_fields:
            assert field in info_data
            print (info_data)
            double = float(info_data.get(field))
                          
        for field in string_fields:
            assert field in info_data
            string = info_data[field]
            assert isinstance(string, str)

        for field in bytes_fields:
            assert field in info_data
            bytes_value = info_data[field]
            assert (isinstance(bytes_value, str) and bytes_value.endswith("iB")) or  \
                   (((os.environ.get('SAN_BUILD'), 'no') != 'no') and bytes_value == 0)

class TestClusterInfo(ValkeySearchClusterTestCase):
    
    @pytest.mark.skipif(platform.system() == "Darwin", reason="Skip on macOS")
    def test_coordinator_cpu_metric(self):
        client = self.new_cluster_client()
        info_data = client.info("SEARCH")
        assert "search_coordinator_threads_cpu_time_sec" in info_data
        # Sanity check - no activity yet
        assert float(info_data["search_coordinator_threads_cpu_time_sec"]) == 0.0

        # Create index with data
        index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1)])
        index.create(client)
        index.load_data(client, 10000)

        # Run search queries until metric increases
        i = 0
        max_batches = 1000
        for batch in range(max_batches):
            for _ in range(1000):
                query_vector = struct.pack('<3f', i, i+1, i+2)
                client.execute_command(
                    "FT.SEARCH", "hnsw", "*=>[KNN 5 @v $query_vector]", "PARAMS", "2", "query_vector", query_vector
                )
                i += 1
            
            info_data = client.info("SEARCH")
            if float(info_data["search_coordinator_threads_cpu_time_sec"]) > 0.0:
                break
        else:
            pytest.fail(f"CPU metric remained 0 after {max_batches * 1000} queries")