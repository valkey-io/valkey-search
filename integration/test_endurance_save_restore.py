"""
Endurance test for repeated save/restore cycles at maximum memory.
This test loads data until OOM, saves RDB, restores, and repeats
to detect ephemeral memory spikes during restore operations.

IMPORTANT: This test includes swap guardrails to ensure the system
never swaps to disk during testing, as swapping would invalidate
performance and memory measurements.
"""

import os
import time
import logging
from valkey import ResponseError, OutOfMemoryError
from valkey.client import Valkey

from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper


class TestEnduranceSaveRestore(ValkeySearchTestCaseBase):
    """
    Endurance test for save/restore operations at maximum memory.
    
    This test:
    1. Sets maxmemory with noeviction policy
    2. Loads data until OOM
    3. Saves RDB
    4. Restarts and restores from RDB
    5. Verifies data integrity and functionality
    6. Repeats for multiple cycles to detect memory issues
    
    SWAP GUARDRAILS:
    - Monitors system swap usage throughout the test
    - Fails immediately if any swap activity is detected
    - Checks swap before and after critical operations
    """
    
    # Configuration
    MAXMEMORY_THRESHOLD = 1024 * 1024 * 1024  # 1GB
    BATCH_SIZE = 100
    DOC_CONTENT_SIZE = 1000
    NUM_CYCLES = 1  # Number of save/restore cycles to perform per index type
    
    # Swap monitoring state
    _initial_swap_used = None
    _swap_total = None
    _swap_check_enabled = True
    
    def append_startup_args(self, args):
        """Add maxmemory configuration to server startup"""
        args["maxmemory"] = str(self.MAXMEMORY_THRESHOLD)
        args["maxmemory-policy"] = "noeviction"
        return args
    
    def _get_swap_info(self) -> dict:
        """
        Get current swap usage from /proc/meminfo.
        
        Returns dict with:
        - swap_total: Total swap space in bytes
        - swap_free: Free swap space in bytes  
        - swap_used: Used swap space in bytes (calculated as total - free)
        - swap_cached: Cached swap in bytes
        """
        swap_info = {
            'swap_total': 0,
            'swap_free': 0,
            'swap_used': 0,
            'swap_cached': 0
        }
        
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if line.startswith('SwapTotal:'):
                        # Format: "SwapTotal:       12345 kB"
                        swap_info['swap_total'] = int(line.split()[1]) * 1024  # Convert KB to bytes
                    elif line.startswith('SwapFree:'):
                        swap_info['swap_free'] = int(line.split()[1]) * 1024
                    elif line.startswith('SwapCached:'):
                        swap_info['swap_cached'] = int(line.split()[1]) * 1024
            
            # Calculate used swap
            swap_info['swap_used'] = swap_info['swap_total'] - swap_info['swap_free']
            
        except FileNotFoundError:
            logging.warning("/proc/meminfo not found - swap monitoring disabled (non-Linux system?)")
            self._swap_check_enabled = False
        except Exception as e:
            logging.warning(f"Error reading swap info: {e} - swap monitoring disabled")
            self._swap_check_enabled = False
        
        return swap_info
    
    def _check_swap_usage(self, context: str = ""):
        """
        Check if system is using swap and fail test if swap usage has increased.
        
        Args:
            context: Description of when this check is happening (for error messages)
        
        Raises:
            AssertionError: If swap usage has increased since test start
        """
        if not self._swap_check_enabled:
            return
        
        swap_info = self._get_swap_info()
        swap_used = swap_info['swap_used']
        swap_total = swap_info['swap_total']
        
        # Initialize baseline on first check
        if self._initial_swap_used is None:
            self._initial_swap_used = swap_used
            self._swap_total = swap_total
            
            if swap_total == 0:
                logging.info("✓ No swap configured on system - test will proceed safely without swap risk")
                print("\n✓ No swap configured on system - ideal for this test")
            elif swap_used > 0:
                logging.warning(
                    f"SWAP BASELINE: System already using {swap_used:,} bytes of swap "
                    f"({swap_used / swap_total * 100:.2f}% of {swap_total:,} bytes total)"
                )
                print(
                    f"\n⚠️  WARNING: System already using {swap_used:,} bytes of swap "
                    f"({swap_used / swap_total * 100:.2f}%)"
                )
            else:
                logging.info(f"SWAP BASELINE: No swap in use (total available: {swap_total:,} bytes)")
                print(f"\n✓ Swap available but not in use: {swap_total:,} bytes total")
            return
        
        # Check if swap usage has increased
        swap_increase = swap_used - self._initial_swap_used
        
        if swap_increase > 0:
            error_msg = (
                f"\n{'='*60}\n"
                f"❌ SWAP DETECTED - TEST FAILED\n"
                f"{'='*60}\n"
                f"Context: {context}\n"
                f"Initial swap used: {self._initial_swap_used:,} bytes\n"
                f"Current swap used: {swap_used:,} bytes\n"
                f"Swap increase: {swap_increase:,} bytes\n"
                f"Swap cached: {swap_info['swap_cached']:,} bytes\n"
                f"Total swap: {swap_total:,} bytes\n"
                f"{'='*60}\n"
                f"\nThe test must not cause any swapping to disk as this invalidates\n"
                f"memory measurements and indicates the system is under excessive\n"
                f"memory pressure. Consider reducing MAXMEMORY_THRESHOLD or running\n"
                f"on a system with more RAM.\n"
                f"{'='*60}"
            )
            logging.error(error_msg)
            print(error_msg)
            assert False, f"Swap usage increased by {swap_increase:,} bytes during: {context}"
        
        # Log successful check for debugging
        if context and swap_total > 0:
            logging.debug(
                f"Swap check PASSED at {context}: "
                f"{swap_used:,}/{swap_total:,} bytes used (no increase)"
            )
    
    def _print_memory_info(self, client: Valkey, label: str) -> dict:
        """Print memory info and return the info dict. Also checks swap usage."""
        info = client.info("memory")
        print(f"\n{'='*60}")
        print(f"INFO MEMORY - {label}")
        print('='*60)
        
        keys = [
            'used_memory',
            'used_memory_human', 
            'used_memory_rss',
            'used_memory_peak',
            'maxmemory',
            'mem_fragmentation_ratio'
        ]
        
        for key in keys:
            print(f"  {key}: {info.get(key, 'N/A')}")
        
        # Always show system swap info to make monitoring visible
        if self._swap_check_enabled and self._swap_total is not None:
            swap_info = self._get_swap_info()
            print(f"  system_swap_used: {swap_info['swap_used']:,} bytes")
            print(f"  system_swap_total: {swap_info['swap_total']:,} bytes")
            if swap_info['swap_total'] == 0:
                print(f"  swap_status: ✓ No swap (ideal)")
            elif swap_info['swap_used'] == 0:
                print(f"  swap_status: ✓ No swap in use")
            else:
                pct = (swap_info['swap_used'] / swap_info['swap_total'] * 100)
                print(f"  swap_status: ⚠️  {pct:.1f}% in use")
        
        print('='*60 + '\n')
        
        # Check for swap usage increase
        self._check_swap_usage(label)
        
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
        
        # Check swap before starting load
        self._check_swap_usage("before data load")
        
        start_time = time.time()
        last_swap_check = time.time()
        
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
                    
                    # Check swap every 10 seconds during load
                    if time.time() - last_swap_check >= 10:
                        self._check_swap_usage(f"during data load ({doc_id} docs)")
                        last_swap_check = time.time()
                    
            except (OutOfMemoryError, ResponseError) as e:
                error_str = str(e).upper()
                if "OOM" in error_str or isinstance(e, OutOfMemoryError):
                    elapsed = time.time() - start_time
                    print(f"\n*** OOM after {doc_id} documents: {e}")
                    logging.info(f"OOM reached after {doc_id} documents in {elapsed:.2f}s")
                    
                    # Final swap check after OOM
                    self._check_swap_usage(f"after OOM ({doc_id} docs)")
                    break
                raise
        
        return doc_id
    
    def _verify_index_searchable(self, client: Valkey, index_name: str, query: str) -> int:
        """Verify index is searchable and return number of results"""
        logging.info(f"Verifying index '{index_name}' is searchable with query: {query}")
        result = client.execute_command(
            "FT.SEARCH",
            index_name,
            query,
            "LIMIT", "0", "10"
        )
        count = result[0]
        logging.info(f"Search returned {count} results")
        return count
    
    # currently not being used but added in for increasing functionality for later iterations.
    def _verify_index_writable(self, client: Valkey, index_name: str, test_data: dict, verify_query: str, max_wait_time: int = 600) -> bool:
        """Verify index can accept new writes and index them. Retries if not writable yet.
        
        Args:
            client: Valkey client
            index_name: Name of the index
            test_data: Dictionary of field->value to write
            verify_query: Query to verify the data was indexed
            max_wait_time: Maximum time to wait for index to become writable (seconds)
        """
        start_time = time.time()
        attempt = 0
        
        while time.time() - start_time < max_wait_time:
            attempt += 1
            test_key = f"endurance_test_key_{int(time.time())}_{attempt}"
            logging.info(f"Testing write capability (attempt {attempt}), key: {test_key}")
            
            # Write new data using mapping parameter
            client.hset(test_key, mapping=test_data)
            logging.debug(f"Wrote test data: {test_data}")
            
            # Give it time to index
            time.sleep(2.0)
            
            # Verify it's searchable
            result = client.execute_command(
                "FT.SEARCH",
                index_name,
                verify_query,
                "NOCONTENT"
            )
            
            # Clean up
            client.delete(test_key)
            
            found = result[0] > 0
            
            if found:
                elapsed = time.time() - start_time
                logging.info(f"Write verification PASSED after {elapsed:.1f}s (attempt {attempt})")
                return True
            else:
                elapsed = time.time() - start_time
                logging.warning(f"Write verification FAILED (attempt {attempt}, {elapsed:.1f}s elapsed). Index may still be re-indexing...")
                
                # Check if index is still indexing
                try:
                    ft_info = client.execute_command("FT.INFO", index_name)
                    for i, item in enumerate(ft_info):
                        if item == b"indexing":
                            is_indexing = int(ft_info[i + 1]) == 1
                            if is_indexing:
                                logging.info("Index is still re-indexing, will retry...")
                            else:
                                logging.warning("Index shows indexing=0 but writes not searchable yet")
                            break
                except Exception as e:
                    logging.warning(f"Could not check indexing status: {e}")
                
                # Wait 30 seconds before retry
                if time.time() - start_time < max_wait_time:
                    logging.info("Waiting 30 seconds before retry...")
                    time.sleep(30)
        
        # Timeout reached
        elapsed = time.time() - start_time
        logging.error(f"Write verification FAILED after {elapsed:.1f}s and {attempt} attempts")
        return False
    
    def _get_index_doc_count(self, client: Valkey, index_name: str) -> int:
        """Get the number of documents in the index"""
        info_result = client.execute_command("FT.INFO", index_name)
        
        for i, item in enumerate(info_result):
            if item == b"num_docs" or item == "num_docs":
                return int(info_result[i + 1])
        
        return 0
    
    def _run_endurance_cycles(self, index_name: str, schema_args: list, 
                              data_generator, search_query: str, 
                              write_test_data: dict, write_verify_query: str):
        """Helper method to run endurance cycles for any index type"""
        client = self.client
        
        logging.info(f"="*60)
        logging.info(f"Starting endurance test for index: {index_name}")
        logging.info(f"Schema: {schema_args}")
        logging.info(f"Number of cycles: {self.NUM_CYCLES}")
        logging.info(f"="*60)
        
        # Create index
        print("\n" + "="*60)
        print(f"CREATING INDEX: {index_name}")
        print("="*60)
        logging.info(f"Creating index '{index_name}'")
        
        create_args = ["FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA"]
        create_args.extend(schema_args)
        client.execute_command(*create_args)
        logging.info(f"Index '{index_name}' created successfully")
        
        # Track memory across cycles
        memory_history = []
        
        for cycle in range(self.NUM_CYCLES):
            logging.info(f"\n{'='*60}")
            logging.info(f"STARTING CYCLE {cycle + 1}/{self.NUM_CYCLES}")
            logging.info(f"{'='*60}")
            print("\n" + "="*60)
            print(f"CYCLE {cycle + 1}/{self.NUM_CYCLES}")
            print("="*60)
            
            # Step 1: Initial memory state
            mem_initial = self._print_memory_info(client, f"CYCLE {cycle + 1} - INITIAL")
            
            # Step 2: Load data until OOM
            doc_count = self._get_index_doc_count(client, index_name)
            print(f"\nStarting with {doc_count} existing documents")
            
            new_docs = self._load_data_until_oom(client, index_name, data_generator)
            print(f"Loaded {new_docs} new documents in this cycle")
            
            mem_after_load = self._print_memory_info(client, f"CYCLE {cycle + 1} - AFTER OOM")
            
            # Step 3: Verify index state before SAVE
            print("\nChecking index state before SAVE...")
            doc_count_before_save = self._get_index_doc_count(client, index_name)
            print(f"  Documents in index before SAVE: {doc_count_before_save}")
            logging.info(f"Index contains {doc_count_before_save} documents before SAVE")
            
            # Get detailed FT.INFO
            try:
                ft_info = client.execute_command("FT.INFO", index_name)
                logging.info(f"FT.INFO before SAVE: {ft_info}")
                # Log key metrics
                for i, item in enumerate(ft_info):
                    if item in [b"indexing", b"num_docs", b"hash_indexing_failures", b"total_indexing_time"]:
                        logging.info(f"  {item}: {ft_info[i+1]}")
            except Exception as e:
                logging.warning(f"Could not get FT.INFO: {e}")
            
            # Step 4: Save RDB
            print("\nSAVE...")
            logging.info("Executing SAVE command...")
            self._check_swap_usage(f"CYCLE {cycle + 1} - before SAVE")
            print("  ✓ Swap check passed")
            
            try:
                client.execute_command("SAVE")
                print("  SAVE completed successfully")
                logging.info("SAVE completed successfully")
            except Exception as e:
                print(f"  SAVE failed: {e}")
                logging.error(f"SAVE failed: {e}")
                raise
            
            mem_after_save = self._print_memory_info(client, f"CYCLE {cycle + 1} - AFTER SAVE")
            
            # Step 5: Restart and restore
            print("\nRestarting server...")
            os.environ["SKIPLOGCLEAN"] = "1"
            self.server.restart(remove_rdb=False)
            
            # Reconnect and wait for server to be ready
            client = self.server.get_new_client()
            self.client = client
            
            # Wait for server to be responsive after restart
            print("Waiting for server to be ready...")
            logging.info("Waiting for server to be responsive...")
            max_wait = 60
            waited = 0
            while waited < max_wait:
                try:
                    client.ping()
                    logging.info(f"Server is responsive after {waited}s")
                    break
                except Exception as e:
                    logging.debug(f"Server not ready yet: {e}")
                    time.sleep(1)
                    waited += 1
            else:
                raise TimeoutError("Server did not become responsive after restart")
            
            # Wait for re-indexing to complete after restore
            print("Waiting for re-indexing to complete after restore...")
            logging.info("Waiting for re-indexing to complete...")
            
            # Check swap periodically during re-indexing
            start_reindex = time.time()
            check_interval = 0
            while not IndexingTestHelper.is_backfill_complete_on_node(client, index_name):
                time.sleep(2)
                check_interval += 2
                # Check swap every 10 seconds during re-indexing
                if check_interval >= 10:
                    self._check_swap_usage(f"CYCLE {cycle + 1} - during re-indexing after restore")
                    print("  ✓ Swap check passed during re-indexing")
                    check_interval = 0
            
            logging.info("Re-indexing completed")
            
            mem_after_restore = self._print_memory_info(client, f"CYCLE {cycle + 1} - AFTER RESTORE")
            
            # Step 5.5: Check index state after restore
            print("\nChecking index state after restore...")
            logging.info("Checking index state after restore...")
            
            # Get detailed FT.INFO after restore
            try:
                ft_info_after = client.execute_command("FT.INFO", index_name)
                logging.info(f"FT.INFO after RESTORE: {ft_info_after}")
                # Log key metrics
                for i, item in enumerate(ft_info_after):
                    if item in [b"indexing", b"num_docs", b"hash_indexing_failures", b"total_indexing_time"]:
                        logging.info(f"  {item}: {ft_info_after[i+1]}")
                        print(f"  {item}: {ft_info_after[i+1]}")
            except Exception as e:
                logging.warning(f"Could not get FT.INFO after restore: {e}")
            
            # Check total keys in database
            try:
                dbsize = client.dbsize()
                print(f"  Total keys in database: {dbsize}")
                logging.info(f"Total keys in database after restore: {dbsize}")
            except Exception as e:
                logging.warning(f"Could not get DBSIZE: {e}")
            
            # Step 6: Verify data integrity Verify index exists and has documents (may be re-indexing)
            print("\nVerifying data integrity...")
            restored_docs = self._get_index_doc_count(client, index_name)
            print(f"  Index restored with {restored_docs} documents (initially)")
            logging.info(f"Index restored with {restored_docs} documents (expected: {doc_count_before_save})")
            
            if restored_docs != doc_count_before_save:
                pct_indexed = (restored_docs / doc_count_before_save * 100) if doc_count_before_save > 0 else 0
                logging.warning(f"Re-indexing in progress: {restored_docs:,}/{doc_count_before_save:,} documents ({pct_indexed:.1f}%)")
                print(f"Re-indexing in progress: {pct_indexed:.1f}% complete")
            else:
                logging.info("All documents indexed immediately after restore")
                print(f"All {restored_docs:,} documents indexed")
            
            assert restored_docs > 0, "Index restored but contains no documents"
            
            # Verify searchable
            search_results = self._verify_index_searchable(client, index_name, search_query)
            print(f"  Index is searchable ({search_results} results)")
            assert search_results > 0, "Index is not searchable after restore"
            
            
            # Step 7: Track memory metrics
            memory_history.append({
                'cycle': cycle + 1,
                'initial': mem_initial['used_memory'],
                'after_load': mem_after_load['used_memory'],
                'after_save': mem_after_save['used_memory'],
                'after_restore': mem_after_restore['used_memory'],
                'docs': restored_docs
            })
            
            print(f"\n CYCLE {cycle + 1} COMPLETED SUCCESSFULLY")
            
            # Clean up between cycles (except last cycle) to free memory
            if cycle < self.NUM_CYCLES - 1:
                print(f"\nCleaning up for next cycle...")
                logging.info("Flushing database to prepare for next cycle")
                try:
                    client.flushdb()
                    logging.info("Database flushed successfully")
                    
                    # IMPORTANT: flushdb() deletes the index too!
                    # We must recreate it for the next cycle to work
                    create_args = ["FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA"]
                    create_args.extend(schema_args)
                    client.execute_command(*create_args)
                    logging.info(f"Index '{index_name}' recreated for next cycle")
                    IndexingTestHelper.wait_for_backfill_complete_on_node(client, index_name)
                except Exception as e:
                    logging.error(f"Cleanup failed: {e}")
                    raise
        
        # Final summary
        print("\n" + "="*60)
        print("ENDURANCE TEST SUMMARY")
        print("="*60)
        
        for mem in memory_history:
            print(f"\nCycle {mem['cycle']}:")
            print(f"  Documents: {mem['docs']}")
            print(f"  Memory initial: {mem['initial']:,} bytes")
            print(f"  Memory after load: {mem['after_load']:,} bytes")
            print(f"  Memory after save: {mem['after_save']:,} bytes")
            print(f"  Memory after restore: {mem['after_restore']:,} bytes")
            print(f"  Restore overhead: {mem['after_restore'] - mem['after_save']:+,} bytes")
        
        # Check for memory growth across cycles
        if len(memory_history) > 1:
            first_restore = memory_history[0]['after_restore']
            last_restore = memory_history[-1]['after_restore']
            growth = last_restore - first_restore
            growth_percent = (growth / first_restore) * 100 if first_restore > 0 else 0
            
            print(f"\nMemory growth across {self.NUM_CYCLES} cycles:")
            print(f"  First cycle restore: {first_restore:,} bytes")
            print(f"  Last cycle restore: {last_restore:,} bytes")
            print(f"  Growth: {growth:+,} bytes ({growth_percent:+.1f}%)")
            
            # Flag significant growth (>10%)
            if abs(growth_percent) > 10:
                print(f"\n  WARNING: Significant memory change detected ({growth_percent:+.1f}%)")
        
        print("\n" + "="*60)
        print(f"✓ ALL {self.NUM_CYCLES} CYCLES COMPLETED SUCCESSFULLY")
        print("="*60)
        
        # Final assertion
        assert len(memory_history) == self.NUM_CYCLES, \
            f"Expected {self.NUM_CYCLES} cycles, completed {len(memory_history)}"
    
    def test_endurance_text_index(self):
        """Endurance test for TEXT index"""
        # Generate content template
        base_text = "Document with searchable content. "
        content_template = (base_text * (self.DOC_CONTENT_SIZE // len(base_text) + 1))[:self.DOC_CONTENT_SIZE]
        
        def text_data_generator(doc_id):
            return {"content": f"{doc_id} {content_template}"}
        
        self._run_endurance_cycles(
            index_name="endurance_text_idx",
            schema_args=["content", "TEXT"],
            data_generator=text_data_generator,
            search_query="searchable",
            write_test_data={"content": "endurance_test_unique_content"},
            write_verify_query="@content:endurance_test_unique_content"
        )
        # Note: Server kept running for next test to use restart()
    
    def test_endurance_tag_index(self):
        """Endurance test for TAG index"""
        # Test framework already provides a fresh server for each test
        
        def tag_data_generator(doc_id):
            return {
                "category": "electronics,gadgets,wearables",
                "product_type": "smartwatch|fitness",
                "brand": f"brand_{doc_id % 100}",
                "features": "waterproof;heartrate;gps"
            }
        
        self._run_endurance_cycles(
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
        # Note: Server kept running for next test to use restart()
    
    def test_endurance_numeric_index(self):
        """Endurance test for NUMERIC index"""
        # Test framework already provides a fresh server for each test
        
        def numeric_data_generator(doc_id):
            return {
                "price": str(100 + (doc_id % 900)),
                "quantity": str(10 + (doc_id % 90)),
                "rating": str(1 + (doc_id % 99)),
                "timestamp": str(1640000000 + doc_id)
            }
        
        self._run_endurance_cycles(
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
        # Note: Pytest will handle server cleanup after all tests complete
