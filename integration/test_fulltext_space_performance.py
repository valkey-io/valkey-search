import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import threading

"""
This file contains large-scale tests for full text search indexing performance.
These tests create large volumes of documents to test indexing scalability.
"""

class TestFullTextSpacePerformance(ValkeySearchTestCaseBase):
    # Class variables to store memory usage across tests
    test1_memory_bytes = None
    test2_memory_bytes = None

    def test_single_document_million_tokens(self):
        """Test 1: Create a document with 1 million 'b' tokens"""
        print("\n" + "="*80)
        print("TEST 1: Single document with 1 million 'b' tokens")
        print("="*80)
        
        self.client.execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")
        
        # Create a document with 1 million 'b' tokens
        print("Creating document with 1 million tokens...")
        million_bs = " ".join(["b"] * 1000000)
        self.client.execute_command("HSET", "product:1", "desc", million_bs)
        
        print("Ingestion complete. Fetching memory usage...")
        
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
        
        # Verify the document was indexed
        result = self.client.execute_command("FT.SEARCH", "products", "b")
        assert result[0] == 1  # Should find 1 document
        assert result[1] == b"product:1"
        print("✅ Test passed: Document indexed successfully")

    def test_million_documents_single_token(self):
        """Test 2: Create 1 million documents, each with a single 'b' token using multi-client"""
        print("\n" + "="*80)
        print("TEST 2: 1 million documents with single 'b' token (multi-client)")
        print("="*80)
        
        # Create multiple clients for concurrent insertion
        num_clients = 10
        clients = [self.server.get_new_client() for _ in range(num_clients)]
        
        # Create index using first client
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")
        
        # Insert 1 million documents, each with just "b"
        num_docs = 1000000
        docs_per_client = num_docs // num_clients
        
        print(f"Inserting {num_docs:,} documents using {num_clients} concurrent clients...")
        print(f"Each client will insert {docs_per_client:,} documents")
        
        def insert_docs(client_id, start_id, count):
            client = clients[client_id]
            for i in range(start_id, start_id + count):
                client.execute_command("HSET", f"product:{i}", "desc", "b")
        
        threads = []
        for client_id in range(num_clients):
            start_id = client_id * docs_per_client
            thread = threading.Thread(target=insert_docs, args=(client_id, start_id, docs_per_client))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        print("Ingestion complete. Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = clients[0].execute_command("INFO", "SEARCH")
        
        # Parse search_used_memory_human (handle both dict and string formats)
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            # Store memory and calculate per-key memory (difference from test 1)
            if memory_bytes != 'N/A' and memory_bytes > 0:
                TestFullTextSpacePerformance.test2_memory_bytes = memory_bytes
                if TestFullTextSpacePerformance.test1_memory_bytes is not None:
                    per_key_memory = (memory_bytes - TestFullTextSpacePerformance.test1_memory_bytes) / 1_000_000
                    print(f"🔑 Per Key memory: {per_key_memory:.2f} bytes (excluding position memory)")
                else:
                    per_key_memory = memory_bytes / 1_000_000
                    print(f"🔑 Per Key memory: {per_key_memory:.2f} bytes (test 1 not run, showing total)")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify all documents were indexed
        result = clients[0].execute_command("FT.SEARCH", "products", "b", "LIMIT", "0", "0")
        assert result[0] == num_docs  # Should find all 1 million documents
        print(f"✅ Test passed: All {num_docs:,} documents indexed successfully")

    def test_million_documents_unique_tokens(self):
        """Test 3: Create 1 million documents, each with a unique token using multi-client"""
        print("\n" + "="*80)
        print("TEST 3: 1 million documents with unique tokens (multi-client)")
        print("="*80)
        
        # Create multiple clients for concurrent insertion
        num_clients = 10
        clients = [self.server.get_new_client() for _ in range(num_clients)]
        
        # Create index using first client
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")
        
        def generate_unique_token(n):
            """Generate unique token string for index n (a, b, c, ..., z, aa, ab, ...)"""
            result = ""
            n += 1  # Start from 1 to match a=1, b=2, etc.
            while n > 0:
                n -= 1
                result = chr(ord('a') + (n % 26)) + result
                n //= 26
            return result
        
        # Insert 1 million documents, each with a unique token
        num_docs = 1000000
        docs_per_client = num_docs // num_clients
        
        print(f"Inserting {num_docs:,} documents with unique tokens using {num_clients} concurrent clients...")
        print(f"Each client will insert {docs_per_client:,} documents")
        print("Token examples: 'a', 'b', 'c', ..., 'z', 'aa', 'ab', ...")
        
        def insert_unique_docs(client_id, start_id, count):
            client = clients[client_id]
            for i in range(start_id, start_id + count):
                unique_token = generate_unique_token(i)
                client.execute_command("HSET", f"product:{i}", "desc", unique_token)
        
        threads = []
        for client_id in range(num_clients):
            start_id = client_id * docs_per_client
            thread = threading.Thread(target=insert_unique_docs, args=(client_id, start_id, docs_per_client))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        print("Ingestion complete. Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = clients[0].execute_command("INFO", "SEARCH")
        
        # Parse search_used_memory_human (handle both dict and string formats)
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            # Calculate per-posting memory (difference from test 2)
            if memory_bytes != 'N/A' and memory_bytes > 0:
                if TestFullTextSpacePerformance.test2_memory_bytes is not None:
                    per_posting_memory = (memory_bytes - TestFullTextSpacePerformance.test2_memory_bytes) / 1_000_000
                    print(f"📮 Per Posting memory: {per_posting_memory:.2f} bytes (excluding key memory)")
                else:
                    per_posting_memory = memory_bytes / 1_000_000
                    print(f"📮 Per Posting memory: {per_posting_memory:.2f} bytes (test 2 not run, showing total)")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify some random unique tokens can be found
        print("\nVerifying random unique tokens can be found...")
        test_tokens = [generate_unique_token(1), generate_unique_token(2), generate_unique_token(3)]
        for token in test_tokens:
            result = clients[0].execute_command("FT.SEARCH", "products", token)
            assert result[0] == 1  # Each unique token should find exactly 1 document
        
        print(f"✅ Test passed: All {num_docs:,} documents with unique tokens indexed successfully")
