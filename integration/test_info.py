import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import pytest
import os


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
            "search_number_of_active_indexes",
            "search_number_of_active_indexes_running_queries",
            "search_number_of_active_indexes_indexing",
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
