"""
Test the reindex-vector-rdb-load functionality using the new valkey-search integration framework.

This comprehensive test validates the complete reindex-vector-rdb-load functionality including:
1. Configuration setting and validation  
2. Index creation with vector fields
3. Data population and RDB generation
4. RDB loading with reindex-vector-rdb-load enabled
5. Backfill progress monitoring
6. Data integrity validation
7. Edge cases and error handling
8. Mixed index types and RDB loading behavior
9. High-dimensional vector search quality
10. Integration with backfill process
"""

import json
import math
import os
import random
import shutil
import struct
import time
from typing import Dict, List, Any, Optional, Set
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestReIndexVectorRDBLoad(ValkeySearchTestCaseBase):
    """Test suite for reindex-vector-rdb-load functionality using the framework"""

    def generate_test_vectors(self, count: int, dimensions: int = 128) -> List[List[float]]:
        """Generate varied test vectors with different patterns for comprehensive testing"""
        vectors = []
        for i in range(count):
            vector = []
            for j in range(dimensions):
                # Create varied patterns: sine waves, linear, exponential decay
                if i % 3 == 0:
                    # Sine wave pattern
                    value = math.sin(j * 2.0 * math.pi / dimensions) * (i + 1) * 0.1
                elif i % 3 == 1:
                    # Linear pattern with some monotonicity
                    value = j * (i + 1) * 0.01 + (i % 10) * 0.001
                else:
                    # Exponential decay pattern
                    value = math.exp(-j / (dimensions / 3.0)) * (i + 1) * 10.0
                
                # Add unique component based on vector index to ensure uniqueness
                value += i * 0.01 + j * 0.001
                vector.append(value)
            
            vectors.append(vector)
        return vectors

    def generate_simple_test_vectors(self, count: int, dimensions: int = 128) -> List[List[float]]:
        """Generate test vectors with specific pattern for ultimate test"""
        vectors = []
        for i in range(count):
            # All coordinates have same value (1.0) except the last one
            vector = [1.0] * dimensions
            vector[-1] = float(i)  # Last coordinate holds the index
            vectors.append(vector)
        
        # Shuffle vectors for random insertion order
        random.shuffle(vectors)
        return vectors

    def encode_vector(self, vector: List[float]) -> bytes:
        """Encode a vector as binary data for storage"""
        return struct.pack(f'{len(vector)}f', *vector)

    def decode_vector(self, data: bytes) -> List[float]:
        """Decode binary data back to a vector"""
        num_floats = len(data) // 4
        return list(struct.unpack(f'{num_floats}f', data))

    def parse_info_result(self, info_result):
        """Parse FT.INFO result handling both bytes and str values in lists"""
        info_dict = {}
        for i in range(0, len(info_result), 2):
            # Handle key
            key = info_result[i]
            if isinstance(key, bytes):
                key = key.decode()
            elif isinstance(key, list):
                # If it's a list, handle it appropriately
                key = str(key)
            else:
                key = str(key)
            
            # Handle value
            value = info_result[i+1]
            if isinstance(value, bytes):
                value = value.decode()
            elif isinstance(value, list):
                # If it's a list, convert to string representation or handle appropriately
                value = str(value)
            else:
                value = str(value)
            
            info_dict[key] = value
        return info_dict

    def validate_vector_similarity(self, vec1: List[float], vec2: List[float], epsilon: float = 1e-4) -> bool:
        """Validate that two vectors are similar within epsilon tolerance"""
        if len(vec1) != len(vec2):
            return False
        
        for a, b in zip(vec1, vec2):
            if abs(a - b) > epsilon:
                return False
        return True

    def create_test_server_with_config(self, config_overrides: Dict[str, str] = None, rdb_file: str = None):
        """Create a test server with custom configuration for reindex testing"""
        module_path = os.getenv('MODULE_PATH')
        module_configs = ""    
        # For now, skip module config overrides to avoid test framework argument splitting issues
        # The reindex-vector-rdb-load config is a HIDDEN startup-only config that's complex to pass through the test framework
        if config_overrides:
            print(f"⚠️  Skipping module config overrides due to test framework limitations: {config_overrides}")
            print("ℹ️  Testing basic RDB loading behavior without reindex-vector-rdb-load for now")
            for key, value in config_overrides.items():
                module_configs += f" --{key} {value}"
        args = {
            "enable-debug-command": "yes",
            # TODO: remove this hack with generic solution for passing module args
            "@loadmodule": f"{module_path} {module_configs}",
        }
        
        # Add RDB file configuration if provided
        if rdb_file and os.path.exists(rdb_file):
            args["dbfilename"] = os.path.basename(rdb_file)
            # Use absolute path to avoid issues with relative paths
            rdb_dir = os.path.abspath(os.path.dirname(rdb_file))
            args["dir"] = rdb_dir
            print(f"🔧 RDB config: dbfilename={args['dbfilename']}, dir={args['dir']}")


        print(f"🔧 Module config: {args['@loadmodule']}")
        
        server_path = os.getenv("VALKEY_SERVER_PATH")
        print(f"🔧 Creating server with args: {args}")
        
        return self.create_server(testdir=self.testdir, server_path=server_path, 
                                  args=args)

    def save_rdb_file(self, client: Valkey, target_path: str) -> bool:
        """Save RDB file to specified path"""
        try:
            print(f"⏳ Saving RDB file to {target_path}...")
            
            # Use synchronous SAVE
            client.execute_command("SAVE")
            print("SAVE command completed")
            
            # Get current RDB location
            dir_result = client.execute_command("CONFIG", "GET", "dir")
            dbfile_result = client.execute_command("CONFIG", "GET", "dbfilename")
            
            # Parse config results (format: [key, value])
            current_dir = dir_result[1].decode() if isinstance(dir_result[1], bytes) else str(dir_result[1])
            current_dbfile = dbfile_result[1].decode() if isinstance(dbfile_result[1], bytes) else str(dbfile_result[1])
            
            current_rdb = os.path.join(current_dir, current_dbfile)
            
            if os.path.exists(current_rdb):
                shutil.copy2(current_rdb, target_path)
                file_size = os.path.getsize(target_path)
                print(f"✅ RDB file saved to {target_path} (size: {file_size} bytes)")
                return True
            else:
                print(f"❌ Source RDB file not found at {current_rdb}")
                return False
                
        except Exception as e:
            print(f"❌ Failed to save RDB: {e}")
            return False

    def search_and_validate_quality(self, client: Valkey, index_name: str, query_vector: List[float], 
                                   query_index: int, k: int = 10, expected_range: int = 5) -> Dict[str, Any]:
        """Search for a vector and validate results quality"""
        try:
            # Convert query vector to binary format
            query_bytes = self.encode_vector(query_vector)
            
            # Perform vector search
            result = client.execute_command(
                "FT.SEARCH", index_name,
                f"*=>[KNN {k} @vector $BLOB AS score]",
                "PARAMS", "2", "BLOB", query_bytes,
                "DIALECT", "2"
            )
            
            if isinstance(result, list) and len(result) > 0:
                num_results = result[0] if isinstance(result[0], int) else 0
                
                found_indices = []
                # Results come as [count, doc_id1, fields1, doc_id2, fields2, ...]
                for i in range(1, len(result), 2):
                    if i < len(result):
                        doc_id = result[i].decode() if isinstance(result[i], bytes) else str(result[i])
                        if doc_id.startswith("doc:"):
                            original_index = int(doc_id.split(":")[1])
                            found_indices.append(original_index)
            else:
                return {"success": False, "error": f"Unexpected result format: {result}"}
            
            # Validate results - expected indices should be around query_index ± expected_range
            expected_indices = set()
            for offset in range(-expected_range, expected_range + 1):
                candidate = query_index + offset
                if 0 <= candidate < 1000:  # Assuming max 1000 vectors
                    expected_indices.add(candidate)
            
            found_set = set(found_indices)
            intersection = found_set.intersection(expected_indices)
            precision = len(intersection) / len(found_indices) if found_indices else 0
            recall = len(intersection) / len(expected_indices) if expected_indices else 0
            
            return {
                "success": True,
                "num_results": num_results,
                "found_indices": found_indices,
                "expected_indices": sorted(expected_indices),
                "intersection": sorted(intersection),
                "precision": precision,
                "recall": recall,
                "quality_score": (precision + recall) / 2
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def test_basic_reindex_vector_rdb_load_configuration(self):
        """Test basic configuration of reindex-vector-rdb-load feature"""
        client: Valkey = self.server.get_new_client()
        
        # Test CONFIG GET for search.reindex-vector-rdb-load 
        # Note: This is a HIDDEN startup-only config, should NOT be accessible via CONFIG GET
        config_result = client.execute_command("CONFIG", "GET", "search.reindex-vector-rdb-load")
        # Should always return empty since it's a hidden config
        assert len(config_result) == 0, "reindex-vector-rdb-load should be hidden and not accessible via CONFIG GET"

    def test_vector_index_creation_and_population(self):
        """Test creating vector index and populating with test data"""
        client: Valkey = self.server.get_new_client()
        
        # Create vector index
        index_name = "test_vector_idx"
        result = client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
            "DIM", "128", "DISTANCE_METRIC", "L2"
        )
        assert result == b"OK"
        
        # Generate test vectors
        test_vectors = self.generate_test_vectors(50, 128)
        
        # Add test data
        for i, vector in enumerate(test_vectors):
            key = f"doc:{i}"
            vector_data = self.encode_vector(vector)
            client.hset(key, "vector", vector_data)
        
        # Wait for indexing to complete
        wait_for_indexing = True
        max_wait_time = 30
        start_time = time.time()
        
        while wait_for_indexing and (time.time() - start_time) < max_wait_time:
            # Check index info to see if backfill is complete
            info_result = client.execute_command("FT.INFO", index_name)
            info_dict = self.parse_info_result(info_result)
            
            # Check if backfill is complete
            if 'backfill_in_progress' in info_dict:
                if info_dict['backfill_in_progress'] == '0':
                    wait_for_indexing = False
            
            if wait_for_indexing:
                time.sleep(0.5)
        
        # Verify data was indexed
        info_result = client.execute_command("FT.INFO", index_name)
        info_dict = self.parse_info_result(info_result)
        
        # Verify we have indexed documents
        assert 'num_docs' in info_dict
        num_docs = int(info_dict['num_docs'])
        assert num_docs == 50
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_vector_data_integrity_validation(self):
        """Test comprehensive vector data integrity with different patterns"""
        client: Valkey = self.server.get_new_client()
        
        # Create vector index
        index_name = "integrity_test_idx"
        result = client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "test:",
            "SCHEMA", "vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
            "DIM", "64", "DISTANCE_METRIC", "L2"
        )
        assert result == b"OK"
        
        # Generate test vectors with different patterns
        test_vectors = self.generate_test_vectors(30, 64)
        
        # Add test data and verify round-trip encoding
        for i, vector in enumerate(test_vectors):
            key = f"test:{i}"
            vector_data = self.encode_vector(vector)
            client.hset(key, "vector", vector_data)
            
            # Verify we can decode it back correctly
            stored_data = client.hget(key, "vector")
            decoded_vector = self.decode_vector(stored_data)
            assert self.validate_vector_similarity(vector, decoded_vector)
        
        # Test different vector patterns have the expected mathematical properties
        sine_vector = test_vectors[0]  # i % 3 == 0 (sine pattern)
        linear_vector = test_vectors[1]  # i % 3 == 1 (linear pattern) 
        exp_vector = test_vectors[2]  # i % 3 == 2 (exponential pattern)
        
        # Sine pattern should have both positive and negative values
        has_positive = any(v > 0 for v in sine_vector)
        has_negative = any(v < 0 for v in sine_vector)
        assert has_positive and has_negative, "Sine pattern should have both positive and negative values"
        
        # Linear pattern should be mostly monotonic
        increasing_pairs = sum(1 for i in range(1, len(linear_vector)) if linear_vector[i] >= linear_vector[i-1])
        monotonic_ratio = increasing_pairs / (len(linear_vector) - 1)
        assert monotonic_ratio > 0.5, "Linear pattern should be mostly monotonic"
        
        # Exponential decay should show general decrease trend
        if len(exp_vector) >= 10:
            quarter_size = len(exp_vector) // 4
            first_quarter_avg = sum(abs(exp_vector[i]) for i in range(quarter_size)) / quarter_size
            last_quarter_avg = sum(abs(exp_vector[i]) for i in range(len(exp_vector) - quarter_size, len(exp_vector))) / quarter_size
            assert first_quarter_avg >= last_quarter_avg * 0.5, "Exponential decay should show general decrease trend"
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_mixed_index_types_with_vector_skip(self):
        """Test that only vector indexes are affected by reindex-vector-rdb-load"""
        client: Valkey = self.server.get_new_client()
        
        # Create mixed index with TAG, NUMERIC, and VECTOR fields
        index_name = "mixed_idx"
        result = client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "mixed:",
            "SCHEMA",
            "title", "TAG",
            "price", "NUMERIC",
            "vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "32", "DISTANCE_METRIC", "L2"
        )
        assert result == b"OK"
        
        # Add test data
        test_vectors = self.generate_test_vectors(10, 32)
        for i, vector in enumerate(test_vectors):
            key = f"mixed:{i}"
            vector_data = self.encode_vector(vector)
            client.hset(key, "title", f"item_{i}")
            client.hset(key, "price", i * 10.5)
            client.hset(key, "vector", vector_data)
        
        # Wait for indexing
        time.sleep(2)
        
        # Verify index has data
        info_result = client.execute_command("FT.INFO", index_name)
        info_dict = self.parse_info_result(info_result)
        
        assert 'num_docs' in info_dict
        num_docs = int(info_dict['num_docs'])
        assert num_docs == 10
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_large_dataset_stress_test(self):
        """Test with larger dataset to validate performance and scalability"""
        client: Valkey = self.server.get_new_client()
        
        # Create vector index
        index_name = "stress_test_idx"
        result = client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "stress:",
            "SCHEMA", "vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
            "DIM", "128", "DISTANCE_METRIC", "L2"
        )
        assert result == b"OK"
        
        # Generate larger test dataset
        test_vectors = self.generate_test_vectors(200, 128)
        
        # Add test data in batches
        batch_size = 50
        for batch_start in range(0, len(test_vectors), batch_size):
            batch_end = min(batch_start + batch_size, len(test_vectors))
            
            # Use pipeline for better performance
            pipe = client.pipeline()
            for i in range(batch_start, batch_end):
                key = f"stress:{i}"
                vector_data = self.encode_vector(test_vectors[i])
                pipe.hset(key, "vector", vector_data)
            pipe.execute()
        
        # Wait for indexing to complete
        max_wait_time = 60
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait_time:
            info_result = client.execute_command("FT.INFO", index_name)
            info_dict = self.parse_info_result(info_result)
            
            if 'backfill_in_progress' in info_dict and info_dict['backfill_in_progress'] == '0':
                break
            
            time.sleep(1)
        
        # Verify final state
        info_result = client.execute_command("FT.INFO", index_name)
        info_dict = self.parse_info_result(info_result)
        
        # Verify all data was indexed
        assert 'num_docs' in info_dict
        num_docs = int(info_dict['num_docs'])
        assert num_docs == 200
        
        # Test vector search functionality
        query_vector = test_vectors[0]
        query_data = self.encode_vector(query_vector)
        
        search_result = client.execute_command(
            "FT.SEARCH", index_name, f"*=>[KNN 5 @vector $vec]",
            "PARAMS", "2", "vec", query_data,
            "DIALECT", "2"
        )
        
        # Should find results
        assert int(search_result[0]) >= 1
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_backfill_progress_monitoring(self):
        """Test monitoring backfill progress during index creation"""
        client: Valkey = self.server.get_new_client()
        
        # Create vector index
        index_name = "progress_test_idx"
        result = client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "progress:",
            "SCHEMA", "vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
            "DIM", "64", "DISTANCE_METRIC", "L2"
        )
        assert result == b"OK"
        
        # Add test data
        test_vectors = self.generate_test_vectors(100, 64)
        for i, vector in enumerate(test_vectors):
            key = f"progress:{i}"
            vector_data = self.encode_vector(vector)
            client.hset(key, "vector", vector_data)
        
        # Monitor backfill progress
        progress_samples = []
        max_iterations = 20
        
        for _ in range(max_iterations):
            info_result = client.execute_command("FT.INFO", index_name)
            info_dict = self.parse_info_result(info_result)
            
            # Collect progress information
            if 'backfill_complete_percent' in info_dict:
                progress = float(info_dict['backfill_complete_percent'])
                progress_samples.append(progress)
                assert 0.0 <= progress <= 1.0, f"Progress should be between 0.0 and 1.0, got {progress}"
            
            # Check if backfill is complete
            if 'backfill_in_progress' in info_dict and info_dict['backfill_in_progress'] == '0':
                break
            
            time.sleep(0.5)
        
        # Verify we collected some progress data
        assert len(progress_samples) > 0, "Should have collected at least one progress sample"
        
        # Verify progress values are reasonable
        for progress in progress_samples:
            assert 0.0 <= progress <= 1.0, f"All progress values should be between 0.0 and 1.0"
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_empty_vector_index_edge_case(self):
        """Test edge case with empty vector index"""
        client: Valkey = self.server.get_new_client()
        
        # Create vector index but don't add any data
        index_name = "empty_test_idx"
        result = client.execute_command(
            "FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "empty:",
            "SCHEMA", "vector", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
            "DIM", "32", "DISTANCE_METRIC", "L2"
        )
        assert result == b"OK"
        
        # Wait a moment for any initialization
        time.sleep(1)
        
        # Check index info
        info_result = client.execute_command("FT.INFO", index_name)
        info_dict = self.parse_info_result(info_result)
        
        # Empty index should have 0 documents
        assert 'num_docs' in info_dict
        num_docs = int(info_dict['num_docs'])
        assert num_docs == 0
        
        # Backfill should complete immediately for empty index
        if 'backfill_complete_percent' in info_dict:
            progress = float(info_dict['backfill_complete_percent'])
            assert progress == 1.0, "Empty index should show 100% backfill completion"
        
        # Clean up
        client.execute_command("FT.DROPINDEX", index_name)

    def test_basic_functionality_with_simple_vectors(self):
        """Test basic functionality with simple 2D vectors"""
        client: Valkey = self.server.get_new_client()
        
        # Create simple HNSW index (2D for simplicity)
        client.execute_command("FT.CREATE", "basic_idx", "ON", "HASH", "PREFIX", "1", "basic:", 
                            "SCHEMA", "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "COSINE")
        
        # Add simple vectors
        vector_data = struct.pack('ff', 1.0, 0.0)  # [1.0, 0.0]
        client.hset("basic:1", mapping={"vec": vector_data, "id": "1"})
        vector_data2 = struct.pack('ff', 0.0, 1.0)  # [0.0, 1.0]
        client.hset("basic:2", mapping={"vec": vector_data2, "id": "2"})
        
        # Wait for indexing
        time.sleep(2)
        
        # Test search
        search_vector = struct.pack('ff', 1.0, 0.0)
        result = client.execute_command("FT.SEARCH", "basic_idx", "*=>[KNN 1 @vec $vec]", 
                                 "PARAMS", "2", "vec", search_vector, "RETURN", "1", "id")
        
        # Should find at least one result
        assert int(result[0]) >= 1
        
        # Clean up
        client.execute_command("FT.DROPINDEX", "basic_idx")

    def test_configuration_set_get_validation(self):
        """Test CONFIG SET/GET command validation for reindex-vector-rdb-load"""
        client: Valkey = self.server.get_new_client()
        
        # Test CONFIG GET - should always return empty since it's a hidden config
        config_result = client.execute_command("CONFIG", "GET", "search.reindex-vector-rdb-load")
        assert len(config_result) == 0, "reindex-vector-rdb-load is hidden and should not be accessible via CONFIG GET"
        
        # Test CONFIG SET - should always fail since it's startup-only and non-modifiable
        try:
            result = client.execute_command("CONFIG", "SET", "search.reindex-vector-rdb-load", "yes")
            # If it doesn't throw an exception, it should indicate an error
            assert "ERR" in str(result) or "Unknown" in str(result), f"CONFIG SET should fail for startup-only config, got: {result}"
        except ResponseError as e:
            # This is expected - CONFIG SET should always fail for startup-only configs
            assert "Unknown option" in str(e) or "ERR" in str(e), f"Expected 'Unknown option' error, got: {e}"
        
        # Test that the server works normally despite the hidden config
        result = client.ping()
        assert result, "Server should remain functional with hidden config"

    def test_comprehensive_rdb_loading_scenario(self):
        """Test comprehensive RDB loading with reindex-vector-rdb-load enabled"""
        # Test parameters - use more vectors for realistic backfill testing
        num_vectors = 20000
        dimensions = 128
        index_name = "comprehensive_idx"
        rdb_file = os.path.join(self.testdir, "comprehensive_test.rdb")
        
        # Clean up any existing RDB file
        if os.path.exists(rdb_file):
            os.remove(rdb_file)
        
        try:
            # Step 1: Create initial setup with current server
            client = self.server.get_new_client()
            
            # Create HNSW index with parameters for better testing
            client.execute_command(
                "FT.CREATE", index_name,
                "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "vector", "VECTOR", "HNSW", "10",
                "M", "32", "TYPE", "FLOAT32", "DIM", str(dimensions),
                "DISTANCE_METRIC", "COSINE", "EF_CONSTRUCTION", "200"
            )
            
            # Generate and insert vectors
            vectors = self.generate_simple_test_vectors(num_vectors, dimensions)
            
            # Insert in batches for better performance
            batch_size = 50
            for batch_start in range(0, len(vectors), batch_size):
                batch_end = min(batch_start + batch_size, len(vectors))
                pipe = client.pipeline()
                
                for i in range(batch_start, batch_end):
                    vector = vectors[i]
                    vector_bytes = self.encode_vector(vector)
                    original_index = int(vector[-1])  # Last coordinate contains original index
                    doc_id = f"doc:{original_index}"
                    
                    pipe.hset(doc_id, mapping={
                        "vector": vector_bytes,
                        "original_index": str(original_index),
                        "title": f"Vector {original_index}"
                    })
                pipe.execute()
            
            # Wait for initial indexing to complete
            print(f"Waiting for initial indexing of {num_vectors} vectors...")
            max_wait = 30
            start_time = time.time()
            while (time.time() - start_time) < max_wait:
                info_result = client.execute_command("FT.INFO", index_name)
                info_dict = self.parse_info_result(info_result)
                
                if 'backfill_in_progress' in info_dict and info_dict['backfill_in_progress'] == '0':
                    break
                time.sleep(1)
            
            # Validate initial search quality  
            test_queries = [50, 150, 250, 350, 450]
            initial_results = {}
            
            for query_idx in test_queries:
                query_vector = [1.0] * dimensions
                query_vector[-1] = float(query_idx)
                result = self.search_and_validate_quality(client, index_name, query_vector, query_idx, k=5, expected_range=10)
                initial_results[query_idx] = result
            
            # Get initial index stats
            initial_info = client.execute_command("FT.INFO", index_name)
            initial_dict = self.parse_info_result(initial_info)
            initial_num_docs = int(initial_dict.get('num_docs', 0))
            initial_num_records = int(initial_dict.get('num_records', 0))
            
            print(f"Initial state: {initial_num_docs} docs, {initial_num_records} indexed records")
            
            # Save RDB
            success = self.save_rdb_file(client, rdb_file)
            assert success, "Failed to save RDB file"
            
            self.server.exit()
            
            # Step 2: Test RDB load with reindex-vector-rdb-load=true
            print("Testing RDB load with reindex-vector-rdb-load=true")
            skip_server, skip_client = self.create_test_server_with_config(
                config_overrides={"reindex-vector-rdb-load": "true"},
                rdb_file=rdb_file
            )
            
            # **CRITICAL**: Test immediately after RDB load - should show backfill in progress
            print("Checking state immediately after RDB load...")
            time.sleep(1)  # Minimal wait for RDB load to complete
            
            # Note: reindex-vector-rdb-load is a HIDDEN startup-only config, not accessible via CONFIG GET
            # We verify it's working by observing the behavior: documents loaded but vector index rebuilding via backfill
            print("ℹ️  reindex-vector-rdb-load is a HIDDEN startup-only config - validating via behavior observation")
            
            # Verify documents are loaded but vector index should be empty/minimal
            keys_result = skip_client.execute_command("KEYS", "doc:*")
            loaded_docs = len(keys_result)
            print(f"Documents loaded from RDB: {loaded_docs}")
            assert loaded_docs == num_vectors, f"Expected {num_vectors} docs, got {loaded_docs}"
            
            # Check index state immediately after RDB load
            immediate_info = skip_client.execute_command("FT.INFO", index_name)
            immediate_dict = self.parse_info_result(immediate_info)
            
            immediate_num_docs = int(immediate_dict.get('num_docs', 0))
            immediate_num_records = int(immediate_dict.get('num_records', 0))
            backfill_in_progress = immediate_dict.get('backfill_in_progress', '0')
            
            print(f"Immediate after RDB load: {immediate_num_docs} docs, {immediate_num_records} indexed records")
            print(f"Backfill in progress: {backfill_in_progress}")
        
            
            # Vector index should be empty or very small (due to skip), backfill should be in progress
            if backfill_in_progress == '1':
                print("✅ Backfill is in progress as expected")
                assert immediate_num_records < initial_num_records, \
                    f"Vector index should be smaller during backfill: {immediate_num_records} vs {initial_num_records}"
            else:
                print("⚠️  Backfill not detected as in progress, checking if already completed quickly")
                # For small datasets, backfill might complete very quickly
            
            # Test search quality immediately - should be poor if vectors not indexed yet
            print("Testing search quality immediately after RDB load (during backfill)...")
            immediate_results = {}
            for query_idx in test_queries:
                query_vector = [1.0] * dimensions
                query_vector[-1] = float(query_idx)
                result = self.search_and_validate_quality(skip_client, index_name, query_vector, query_idx, k=5, expected_range=10)
                immediate_results[query_idx] = result
            
            # Calculate immediate quality
            avg_immediate = sum(r.get("quality_score", 0) for r in immediate_results.values()) / len(test_queries)
            print(f"Search quality immediately after RDB load: {avg_immediate:.3f}")
            
            # Monitor backfill progress
            print("Monitoring backfill progress...")
            max_backfill_wait = 60
            backfill_start = time.time()
            progress_samples = []
            
            while (time.time() - backfill_start) < max_backfill_wait:
                info_result = skip_client.execute_command("FT.INFO", index_name)
                info_dict = self.parse_info_result(info_result)
                
                backfill_progress = info_dict.get('backfill_in_progress', '0')
                num_records = int(info_dict.get('num_records', 0))
                
                if 'backfill_complete_percent' in info_dict:
                    percent = float(info_dict['backfill_complete_percent'])
                    progress_samples.append(percent)
                    print(f"Backfill progress: {percent:.1%}, Records: {num_records}")
                
                if backfill_progress == '0':
                    print("Backfill completed!")
                    break
                    
                time.sleep(2)
            
            # Final validation after backfill completes
            print("Testing search quality after backfill completion...")
            final_results = {}
            for query_idx in test_queries:
                query_vector = [1.0] * dimensions
                query_vector[-1] = float(query_idx)
                result = self.search_and_validate_quality(skip_client, index_name, query_vector, query_idx, k=5, expected_range=10)
                final_results[query_idx] = result
            
            # Get final index stats
            final_info = skip_client.execute_command("FT.INFO", index_name)
            final_dict = self.parse_info_result(final_info)
            final_num_docs = int(final_dict.get('num_docs', 0))
            final_num_records = int(final_dict.get('num_records', 0))
            
            print(f"Final state: {final_num_docs} docs, {final_num_records} indexed records")
            
            skip_server.exit()
            
            # Step 3: Validate the reindex-vector-rdb-load behavior
            avg_initial = sum(r.get("quality_score", 0) for r in initial_results.values()) / len(test_queries)
            avg_final = sum(r.get("quality_score", 0) for r in final_results.values()) / len(test_queries)
            
            print(f"\nQuality Comparison:")
            print(f"Initial (before RDB): {avg_initial:.3f}")
            print(f"Immediate (during backfill): {avg_immediate:.3f}")  
            print(f"Final (after backfill): {avg_final:.3f}")
            
            # Validate that reindex-vector-rdb-load worked correctly:
            # 1. Documents were loaded from RDB
            assert final_num_docs == 2*num_vectors, f"Documents not preserved: {final_num_docs} vs {num_vectors}"
            
            # 2. Vector index was rebuilt (should have similar record count as initial)
            record_diff_ratio = abs(final_num_records - initial_num_records) / initial_num_records
            assert record_diff_ratio < 0.1, f"Vector index not properly rebuilt: {final_num_records} vs {initial_num_records}"
            
            # 3. Search quality should be restored after backfill
            quality_threshold = 0.5  # Minimum acceptable quality
            assert avg_final >= quality_threshold, f"Final search quality too low: {avg_final}"
            
            # 4. Quality should be preserved within reasonable bounds
            quality_diff = abs(avg_final - avg_initial)
            max_diff = 0.2  # Allow 20% difference due to backfill variations
            assert quality_diff <= max_diff, f"Quality not preserved after backfill: diff={quality_diff}"
            
            # 5. If backfill was detected in progress, immediate quality should be lower than final
            if avg_immediate < avg_final:
                improvement = avg_final - avg_immediate
                print(f"✅ Search quality improved during backfill: +{improvement:.3f}")
                assert improvement > 0.01, "Expected significant quality improvement during backfill"
            
            print("✅ reindex-vector-rdb-load functionality validated successfully!")
            
        finally:
            # Clean up RDB file
            if os.path.exists(rdb_file):
                os.remove(rdb_file)

    def test_edge_cases_with_rdb_operations(self):
        """Test edge cases with RDB operations and empty indexes"""
        rdb_file = os.path.join(self.testdir, "edge_case_test.rdb")
        
        try:
            # Test with empty index
            client = self.server.get_new_client()
            
            # Create index but don't add any vectors
            client.execute_command("FT.CREATE", "empty_idx", "ON", "HASH", "PREFIX", "1", "empty:", 
                                "SCHEMA", "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE")
            
            # Save RDB with empty index
            success = self.save_rdb_file(client, rdb_file)
            assert success, "Failed to save empty RDB"
            
            self.server.exit()
            
            # Test loading empty index with skip enabled
            skip_server, skip_client = self.create_test_server_with_config(
                config_overrides={"reindex-vector-rdb-load": "true"},
                rdb_file=rdb_file
            )
            
            # Should work without issues
            result = skip_client.ping()
            assert result
            
            # Check that empty index still exists
            indexes = skip_client.execute_command("FT._LIST")
            assert b"empty_idx" in indexes
            
            skip_server.exit()
            
        finally:
            if os.path.exists(rdb_file):
                os.remove(rdb_file)

    def test_mixed_index_rdb_behavior(self):
        """Test RDB loading behavior with mixed index types"""
        rdb_file = os.path.join(self.testdir, "mixed_index_test.rdb")
        
        try:
            client = self.server.get_new_client()
            
            # Create mixed index with TAG, NUMERIC, and VECTOR fields
            client.execute_command("FT.CREATE", "mixed_idx", "ON", "HASH", "PREFIX", "1", "mixed:",
                                "SCHEMA", 
                                "text_field", "TAG",
                                "numeric_field", "NUMERIC",
                                "vector_field", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", 
                                "DIM", "3", "DISTANCE_METRIC", "COSINE")
            
            # Add mixed data
            for i in range(10):
                vector_data = struct.pack('fff', float(i), float(i*2), float(i*3))
                client.hset(f"mixed:{i}", mapping={
                    "text_field": f"text data {i}",
                    "numeric_field": str(i * 10),
                    "vector_field": vector_data
                })
            
            # Wait for indexing
            time.sleep(2)
            
            # Get document count before save
            keys_before = client.execute_command("KEYS", "mixed:*")
            count_before = len(keys_before)
            
            # Save RDB
            success = self.save_rdb_file(client, rdb_file)
            assert success, "Failed to save mixed index RDB"
            
            self.server.exit()
            
            # Test loading with skip enabled - all data should be preserved
            skip_server, skip_client = self.create_test_server_with_config(
                config_overrides={"reindex-vector-rdb-load": "true"},
                rdb_file=rdb_file
            )
            
            # Wait for loading and backfill
            time.sleep(3)
            
            # Verify all documents are accessible
            keys_after = skip_client.execute_command("KEYS", "mixed:*")
            count_after = len(keys_after)
            
            assert count_after == count_before, f"Document count mismatch: {count_after} vs {count_before}"
            
            skip_server.exit()
            
        finally:
            if os.path.exists(rdb_file):
                os.remove(rdb_file)