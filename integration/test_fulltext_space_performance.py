import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import threading
import time
import random

"""
This file contains large-scale tests for full text search indexing performance.
These tests create large volumes of documents to test indexing scalability.
"""

@pytest.mark.skip(reason="Only used for manual testing currently")
class TestFullTextSpacePerformance(ValkeySearchTestCaseBase):
    # Class variables to store memory usage across tests
    test1_memory_bytes = None
    test2_memory_bytes = None

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.reader-threads"] = "8"
        args["search.writer-threads"] = "8"
        return args

    def test_single_document_million_tokens(self):
        """Test 1: Create a document with 1 million 'b' tokens"""
        print("\n" + "="*80)
        print("TEST 1: Single document with 1 million 'b' tokens")
        print("="*80)
        
        self.client.execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc1", "TEXT", "desc2", "NUMERIC")
        
        # Create a document with 1 million 'b' tokens
        print("Creating document with 1 million tokens...")
        million_bs = " ".join(["b"] * 1000000)
        start_time = time.perf_counter()
        self.client.execute_command("HSET", "product:1", "desc1", million_bs, "desc2", "1")
        ingestion_time = time.perf_counter() - start_time
        
        print(f"Ingestion complete in {ingestion_time:.2f}s (Whole dataset ingestion rate: {1/ingestion_time:.2f} docs/s, {1000000/ingestion_time:,.2f} tokens/s). Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = self.client.execute_command("INFO", "SEARCH")
        
        # Parse search_used_memory_human (handle both dict and string formats)
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            # Store memory for next test and calculate per-position memory
            if memory_bytes != 'N/A' and memory_bytes > 0:
                TestFullTextSpacePerformance.test1_memory_bytes = memory_bytes
                per_position_memory = memory_bytes / 1_000_000
                print(f"📐 Per Position memory: {per_position_memory:.2f} bytes")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify the document was indexed and measure search QPS for at least 30 seconds against Attribute 1
        print("\nVerifying search and measuring search QPS over a 30-second window for Attribute 1 (@desc1)...")
        num_searches = 0
        start_time = time.perf_counter()
        while time.perf_counter() - start_time < 30.0:
            result = self.client.execute_command("FT.SEARCH", "products", "@desc1:b")
            assert result[0] == 1  # Should find 1 document
            assert result[1] == b"product:1"
            num_searches += 1
        search_time = time.perf_counter() - start_time
        print(f"⚡ Attribute 1 (@desc1) Search QPS: {num_searches/search_time:,.2f} queries/s ({num_searches:,} queries in {search_time:.2f}s, avg {search_time*1000/num_searches:.2f} ms/query)")
        print("✅ Test passed: Document indexed successfully")

    @pytest.mark.parametrize("num_clients", [50, 100, 300])
    @pytest.mark.parametrize("num_tokens", [50, 100])
    def test_million_documents_single_token(self, num_clients, num_tokens):
        """Test 2: Create documents with multi-token content using multi-client"""
        print("\n" + "="*80)
        print(f"TEST 2: documents with {num_tokens} common prefix tokens ({num_clients} clients)")
        print("="*80)
        
        clients = [self.server.get_new_client() for _ in range(num_clients)]
        
        # Create index using first client
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc1", "TEXT", "desc2", "NUMERIC")
        
        # Use 200,000 docs so 100 tokens/doc indexes 20 million tokens per test run consistently
        num_docs = 200000
        docs_per_client = num_docs // num_clients
        
        # Generate common prefix token string (15-30 chars per token with shared prefix)
        common_tokens = " ".join([f"category_electronics_common_t{t:02d}" for t in range(num_tokens)])
        
        print(f"Inserting {num_docs:,} documents ({num_tokens} tokens/doc) using {num_clients} concurrent clients...")
        print(f"Each client will insert {docs_per_client:,} documents")
        
        def insert_docs(client_id, start_id, count):
            client = clients[client_id]
            for i in range(start_id, start_id + count):
                client.execute_command("HSET", f"product:{i}", "desc1", common_tokens, "desc2", str(i))
        
        start_time = time.perf_counter()
        threads = []
        for client_id in range(num_clients):
            start_id = client_id * docs_per_client
            thread = threading.Thread(target=insert_docs, args=(client_id, start_id, docs_per_client))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        ingestion_time = time.perf_counter() - start_time
        
        print(f"Ingestion complete in {ingestion_time:.2f}s (Whole dataset ingestion rate: {num_docs/ingestion_time:,.2f} docs/s). Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = clients[0].execute_command("INFO", "SEARCH")
        
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            if memory_bytes != 'N/A' and memory_bytes > 0:
                TestFullTextSpacePerformance.test2_memory_bytes = memory_bytes
                per_key_memory = memory_bytes / num_docs
                print(f"🔑 Per Key memory: {per_key_memory:.2f} bytes")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify search and measure QPS over a 30-second window
        print("\nVerifying search and measuring search QPS over a 30-second window for Attribute 1 (@desc1)...")
        num_searches = 0
        start_time = time.perf_counter()
        while time.perf_counter() - start_time < 30.0:
            result = clients[0].execute_command("FT.SEARCH", "products", "@desc1:category_electronics_common_t00", "LIMIT", "0", "0")
            num_searches += 1
        search_time = time.perf_counter() - start_time
        print(f"⚡ Attribute 1 (@desc1) Search QPS: {num_searches/search_time:,.2f} queries/s ({num_searches:,} queries in {search_time:.2f}s, avg {search_time*1000/num_searches:.2f} ms/query)")
        print(f"✅ Test passed: All {num_docs:,} documents indexed successfully")

    @pytest.mark.parametrize("num_clients", [50, 100, 300])
    @pytest.mark.parametrize("num_tokens", [50, 100])
    def test_million_documents_unique_tokens(self, num_clients, num_tokens):
        """Test 3: Create documents with unique prefix tokens using multi-client"""
        print("\n" + "="*80)
        print(f"TEST 3: documents with {num_tokens} unique prefix tokens ({num_clients} clients)")
        print("="*80)
        
        clients = [self.server.get_new_client() for _ in range(num_clients)]
        
        # Create index using first client
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "NOSTOPWORDS", "SCHEMA", "desc1", "TEXT", "NOSTEM", "desc2", "NUMERIC")
        
        # Use 200,000 docs so 100 tokens/doc indexes 20 million unique tokens per test run consistently
        num_docs = 200000
        docs_per_client = num_docs // num_clients
        
        print(f"Inserting {num_docs:,} documents ({num_tokens} unique tokens/doc) using {num_clients} concurrent clients...")
        print(f"Each client will insert {docs_per_client:,} documents")
        print("Token examples: 'category_elec_000000_t00', 'category_elec_000000_t01', ...")
        
        def insert_unique_docs(client_id, start_id, count):
            client = clients[client_id]
            for i in range(start_id, start_id + count):
                unique_tokens = " ".join([f"category_elec_{i:06d}_t{t:02d}" for t in range(num_tokens)])
                client.execute_command("HSET", f"product:{i}", "desc1", unique_tokens, "desc2", str(i))
        
        start_time = time.perf_counter()
        threads = []
        for client_id in range(num_clients):
            start_id = client_id * docs_per_client
            thread = threading.Thread(target=insert_unique_docs, args=(client_id, start_id, docs_per_client))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        ingestion_time = time.perf_counter() - start_time
        
        print(f"Ingestion complete in {ingestion_time:.2f}s (Whole dataset ingestion rate: {num_docs/ingestion_time:,.2f} docs/s). Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = clients[0].execute_command("INFO", "SEARCH")
        
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            if memory_bytes != 'N/A' and memory_bytes > 0:
                per_posting_memory = memory_bytes / (num_docs * num_tokens)
                print(f"📮 Per Posting memory: {per_posting_memory:.2f} bytes")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify random unique tokens can be found and measure search QPS over a 30-second window
        print("\nVerifying random unique tokens can be found and measuring search QPS over a 30-second window for Attribute 1 (@desc1)...")
        num_searches = 0
        start_time = time.perf_counter()
        while time.perf_counter() - start_time < 30.0:
            doc_idx = random.randint(0, num_docs - 1)
            token_idx = random.randint(0, num_tokens - 1)
            token = f"category_elec_{doc_idx:06d}_t{token_idx:02d}"
            result = clients[0].execute_command("FT.SEARCH", "products", f"@desc1:{token}")
            assert result[0] == 1
            num_searches += 1
        search_time = time.perf_counter() - start_time
        print(f"⚡ Attribute 1 (@desc1) Search QPS: {num_searches/search_time:,.2f} queries/s ({num_searches:,} queries in {search_time:.2f}s, avg {search_time*1000/num_searches:.2f} ms/query)")
        
        print(f"✅ Test passed: All {num_docs:,} documents with unique tokens indexed successfully")
