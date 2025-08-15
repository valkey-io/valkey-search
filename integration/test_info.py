import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


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
            "search_vector_externing_entry_count",
            "search_vector_externing_hash_extern_errors",
            "search_vector_externing_generated_value_cnt",
            "search_vector_externing_num_lru_entries",
            "search_vector_externing_lru_promote_cnt",
            "search_vector_externing_deferred_entry_cnt",
            "search_number_of_attributes",
            "search_number_of_indexes",
            "search_total_indexed_documents",
            "search_used_memory_bytes",
            "search_index_reclaimable_memory",
            "search_used_memory_indexes",
            "search_smallest_memory_index",
            "search_largest_memory_index",
            "search_used_memory_vector_index",
            "search_global_idle_user",
            "search_global_idle_internal",
            "search_global_total_user",
            "search_global_total_internal",
            "search_gc_bytes_collected",
            "search_gc_total_cycles",
            "search_gc_total_ms_run",
            "search_gc_total_docs_not_collected",
            "search_gc_marked_deleted_vectors"
        ]

        string_fields = [
            "search_background_indexing_status"
        ]

        bytes_fields = [
            "search_used_memory_human",
            "search_used_memory_indexes_human",
            "search_smallest_memory_index_human",
            "search_largest_memory_index_human"
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
            if isinstance(bytes_value, bytes):
                bytes_value = bytes_value.decode('utf-8')
            elif isinstance(bytes_value, int):
                bytes_value = str(bytes_value)
            assert isinstance(bytes_value, str)
            assert bytes_value.endswith("iB") or bytes_value.isdigit()
