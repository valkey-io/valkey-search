"""
Endurance test for save/restore operations at maximum memory.
This test loads data until OOM, saves RDB, restores, and verifies
data integrity to detect memory issues during restore operations.
"""

import os
import time
import logging
from valkey import ResponseError, OutOfMemoryError
from valkey.client import Valkey

from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper
from ft_info_parser import FTInfoParser


class TestEnduranceSaveRestore(ValkeySearchTestCaseBase):
    """
    Endurance test for save/restore operations at maximum memory.
    
    This test:
    1. Sets maxmemory with noeviction policy
    2. Loads data until OOM
    3. Saves RDB
    4. Restarts and restores from RDB
    5. Verifies data integrity and functionality
    """
    
    # Configuration
    MAXMEMORY_THRESHOLD = 1024 * 1024 * 1024  # 1GB
    BATCH_SIZE = 100
    DOC_CONTENT_SIZE = 1000
    
    def append_startup_args(self, args):
        """Add maxmemory configuration to server startup"""
        args["maxmemory"] = str(self.MAXMEMORY_THRESHOLD)
        args["maxmemory-policy"] = "noeviction"
        return args
    
    def _print_memory_info(self, client: Valkey, label: str) -> dict:
        """Print memory info and return the info dict."""
        info = client.info("memory")
        print(f"\n{'='*60}")
        print(f"INFO MEMORY - {label}")
        print('='*60)
        
        # Print memory metrics with both raw and human-readable values
        used_memory = info.get('used_memory', 0)
        used_memory_human = info.get('used_memory_human', 'N/A')
        print(f"  used_memory: {used_memory:,} bytes ({used_memory_human})")
        
        used_memory_rss = info.get('used_memory_rss', 0)
        used_memory_rss_human = info.get('used_memory_rss_human', 'N/A')
        print(f"  used_memory_rss: {used_memory_rss:,} bytes ({used_memory_rss_human})")
        
        used_memory_peak = info.get('used_memory_peak', 0)
        used_memory_peak_human = info.get('used_memory_peak_human', 'N/A')
        print(f"  used_memory_peak: {used_memory_peak:,} bytes ({used_memory_peak_human})")
        
        maxmemory = info.get('maxmemory', 0)
        maxmemory_human = info.get('maxmemory_human', 'N/A')
        print(f"  maxmemory: {maxmemory:,} bytes ({maxmemory_human})")
        
        # Fragmentation metrics
        mem_frag_ratio = info.get('mem_fragmentation_ratio', 'N/A')
        print(f"  mem_fragmentation_ratio: {mem_frag_ratio}")
        
        allocator_frag_ratio = info.get('allocator_frag_ratio', 'N/A')
        print(f"  allocator_frag_ratio: {allocator_frag_ratio}")
        
        allocator_frag_bytes = info.get('allocator_frag_bytes', 0)
        if allocator_frag_bytes and allocator_frag_bytes > 0:
            allocator_frag_mb = allocator_frag_bytes / (1024 * 1024)
            print(f"  allocator_frag_bytes: {allocator_frag_bytes:,} bytes ({allocator_frag_mb:.2f} MB)")
        else:
            print(f"  allocator_frag_bytes: N/A")
        
        print('='*60 + '\n')
        
        return info
    
    def _load_data_until_oom(self, client: Valkey, index_name: str, data_generator) -> int:
        """Load data using pipeline until OOM is hit. Returns number of documents loaded.
        
        Args:
            client: Valkey client
            index_name: Name of the index
            data_generator: Function that takes doc_id and returns dict of field->value mappings
        """
        doc_id = 0
        logging.info(f"Starting data load (maxmemory: {self.MAXMEMORY_THRESHOLD:,}, batch: {self.BATCH_SIZE})")
        print(f"\nLoading data (maxmemory: {self.MAXMEMORY_THRESHOLD:,}, batch: {self.BATCH_SIZE})")
        
        start_time = time.time()
        
        while True:
            try:
                pipe = client.pipeline(transaction=False)
                for i in range(self.BATCH_SIZE):
                    current_id = doc_id + i
                    fields = data_generator(current_id)
                    
                    # Use mapping parameter for hset
                    pipe.hset(f"doc:{current_id}", mapping=fields)
                
                pipe.execute()
                doc_id += self.BATCH_SIZE
                
                if doc_id % 1000 == 0:
                    info = client.info("memory")
                    used = info.get('used_memory', 0)
                    max_mem = info.get('maxmemory', 0)
                    elapsed = time.time() - start_time
                    rate = doc_id / elapsed if elapsed > 0 else 0
                    print(f"  {doc_id} docs, {used:,} / {max_mem:,} bytes ({rate:.0f} docs/sec)")
                    logging.info(f"Loaded {doc_id} documents ({rate:.0f} docs/sec)")
                    
            except (OutOfMemoryError, ResponseError) as e:
                error_str = str(e).upper()
                if "OOM" in error_str or isinstance(e, OutOfMemoryError):
                    elapsed = time.time() - start_time
                    print(f"\n*** OOM after {doc_id} documents: {e}")
                    logging.info(f"OOM reached after {doc_id} documents in {elapsed:.2f}s")
                    break
                raise
        
        return doc_id
    
    def _verify_index_searchable(self, client: Valkey, index_name: str, query: str) -> int:
        """Verify index is searchable and return number of results"""
        print(f"Verifying index '{index_name}' is searchable with query: {query}")
        result = client.execute_command(
            "FT.SEARCH",
            index_name,
            query,
            "LIMIT", "0", "10"
        )
        count = result[0]
        print(f"Search returned {count} results")
        return count
        
    def _get_index_doc_count(self, client: Valkey, index_name: str) -> int:
        """Get the number of documents in the index"""
        info_result = client.execute_command("FT.INFO", index_name)
        parser = FTInfoParser(info_result)
        
        # Assert that num_docs is present in the FT.INFO response
        assert "num_docs" in parser.parsed_data, \
            f"num_docs not found in FT.INFO response for index '{index_name}'"
        
        return parser.num_docs
    
    def _run_endurance_test(self, index_name: str, schema_args: list, 
                            data_generator, search_query: str, 
                            write_test_data: dict, write_verify_query: str):
        """Helper method to run save/restore endurance test for any index type"""
        client = self.client
        
        # Create index
        print("\n" + "="*60)
        print(f"CREATING INDEX: {index_name}")
        print(f"Schema: {schema_args}")
        print("="*60)
        
        create_args = ["FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA"]
        create_args.extend(schema_args)
        client.execute_command(*create_args)
        
        # Step 1: Initial memory state
        mem_initial = self._print_memory_info(client, "INITIAL STATE")
        
        # Step 2: Load data until OOM
        doc_count = self._get_index_doc_count(client, index_name)
        print(f"\nStarting with {doc_count} existing documents")
        
        new_docs = self._load_data_until_oom(client, index_name, data_generator)
        print(f"Loaded {new_docs} documents")
        
        mem_after_load = self._print_memory_info(client, "AFTER OOM")
        
        # Step 3: Verify index state before SAVE
        print("\nChecking index state before SAVE...")
        doc_count_before_save = self._get_index_doc_count(client, index_name)
        print(f"  Documents in index before SAVE: {doc_count_before_save}")
        
        # Get total keys before save to verify restore completeness
        dbsize_before_save = client.dbsize()
        print(f"  Total keys in database before SAVE: {dbsize_before_save}")
        
        # Get detailed FT.INFO
        ft_info = client.execute_command("FT.INFO", index_name)
        parser = FTInfoParser(ft_info)
        print(f"FT.INFO before SAVE:")
        print(f"  num_docs: {parser.num_docs}")
        print(f"  hash_indexing_failures: {parser.hash_indexing_failures}")
        
        # Step 4: Save RDB
        print("\nSAVE...")
        client.execute_command("SAVE")
        print("  SAVE completed successfully")
        
        mem_after_save = self._print_memory_info(client, "AFTER SAVE")
        
        # Step 5: Restart and restore
        print("\nRestarting server...")
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        
        # Reconnect and wait for server to be ready
        client = self.server.get_new_client()
        self.client = client
        
        # Wait for server to be responsive after restart
        # Large datasets can take significant time to restore, using generous timeout
        print("Waiting for server to be ready...")
        max_wait = 28800  # 8 hours;
        waited = 0
        while waited < max_wait:
            try:
                client.ping()
                print(f"  Server is responsive after {waited}s")
                break
            except Exception as e:
                time.sleep(1)
                waited += 1
        else:
            raise TimeoutError(f"Server did not become responsive after {max_wait}s - this may indicate a problem with the restore")
        
        mem_after_restore = self._print_memory_info(client, "AFTER RESTORE")
        
        # Step 6: Check index state after restore
        print("\nChecking index state after restore...")
        
        # Get detailed FT.INFO after restore
        ft_info_after = client.execute_command("FT.INFO", index_name)
        parser_after = FTInfoParser(ft_info_after)
        print(f"FT.INFO after RESTORE:")
        print(f"  num_docs: {parser_after.num_docs}")
        print(f"  hash_indexing_failures: {parser_after.hash_indexing_failures}")
        
        # Check total keys in database and verify it matches before save
        dbsize_after_restore = client.dbsize()
        print(f"  Total keys in database after restore: {dbsize_after_restore}")
        assert dbsize_after_restore == dbsize_before_save, \
            f"DBSIZE mismatch: expected {dbsize_before_save} keys, got {dbsize_after_restore} after restore"
        
        # Step 7: Verify data integrity
        print("\nVerifying data integrity...")
        restored_docs = self._get_index_doc_count(client, index_name)
        print(f"  Index restored with {restored_docs} documents")
        
        # Assert that document count matches exactly before and after save/restore
        assert restored_docs == doc_count_before_save, \
            f"Document count mismatch: expected {doc_count_before_save} docs, got {restored_docs} after restore"
        
        # Verify searchable
        search_results = self._verify_index_searchable(client, index_name, search_query)
        print(f"  Index is searchable ({search_results} results)")
        assert search_results > 0, "Index is not searchable after restore"
        
        # Final summary
        print("\n" + "="*60)
        print("ENDURANCE TEST SUMMARY")
        print("="*60)
        print(f"  Documents: {restored_docs:,}")
        print(f"  Memory initial: {mem_initial['used_memory']:,} bytes")
        print(f"  Memory after load: {mem_after_load['used_memory']:,} bytes")
        print(f"  Memory after save: {mem_after_save['used_memory']:,} bytes")
        print(f"  Memory after restore: {mem_after_restore['used_memory']:,} bytes")
        print(f"  Restore overhead: {mem_after_restore['used_memory'] - mem_after_save['used_memory']:+,} bytes")
        print("="*60)
        print("✓ TEST COMPLETED SUCCESSFULLY")
        print("="*60)
    
    def test_endurance_text_index(self):
        """Endurance test for TEXT index"""
        # Generate content template
        base_text = "Document with searchable content. "
        content_template = (base_text * (self.DOC_CONTENT_SIZE // len(base_text) + 1))[:self.DOC_CONTENT_SIZE]
        
        def text_data_generator(doc_id):
            return {"content": f"{doc_id} {content_template}"}
        
        self._run_endurance_test(
            index_name="endurance_text_idx",
            schema_args=["content", "TEXT"],
            data_generator=text_data_generator,
            search_query="searchable",
            write_test_data={"content": "endurance_test_unique_content"},
            write_verify_query="@content:endurance_test_unique_content"
        )
    
    def test_endurance_tag_index(self):
        """Endurance test for TAG index"""
        def tag_data_generator(doc_id):
            return {
                "category": "electronics,gadgets,wearables",
                "product_type": "smartwatch|fitness",
                "brand": f"brand_{doc_id % 100}",
                "features": "waterproof;heartrate;gps"
            }
        
        self._run_endurance_test(
            index_name="endurance_tag_idx",
            schema_args=[
                "category", "TAG", "SEPARATOR", ",",
                "product_type", "TAG", "SEPARATOR", "|",
                "brand", "TAG",
                "features", "TAG", "SEPARATOR", ";"
            ],
            data_generator=tag_data_generator,
            search_query="@category:{electronics}",
            write_test_data={
                "category": "test_category",
                "product_type": "test_product",
                "brand": "endurance_unique_brand",
                "features": "test_feature"
            },
            write_verify_query="@brand:{endurance_unique_brand}"
        )
    
    def test_endurance_numeric_index(self):
        """Endurance test for NUMERIC index"""
        def numeric_data_generator(doc_id):
            return {
                "price": str(100 + (doc_id % 900)),
                "quantity": str(10 + (doc_id % 90)),
                "rating": str(1 + (doc_id % 99)),
                "timestamp": str(1640000000 + doc_id)
            }
        
        self._run_endurance_test(
            index_name="endurance_numeric_idx",
            schema_args=[
                "price", "NUMERIC",
                "quantity", "NUMERIC",
                "rating", "NUMERIC",
                "timestamp", "NUMERIC"
            ],
            data_generator=numeric_data_generator,
            search_query="@price:[100 500]",
            write_test_data={
                "price": "99999",
                "quantity": "50",
                "rating": "45",
                "timestamp": "1640000000"
            },
            write_verify_query="@price:[99998 100000]"
        )
