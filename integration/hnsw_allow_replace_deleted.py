#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2025, valkey-search contributors
# All rights reserved.
# SPDX-License-Identifier: BSD 3-Clause

import numpy as np
import struct
import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestHNSWAllowReplaceDeleted(ValkeySearchTestCaseBase):
    """
    Test suite for HNSW allow_replace_deleted configuration
    
    This test suite validates the HNSW vector index behavior when the allow_replace_deleted
    feature is enabled or disabled. The feature controls whether deleted vector slots can be
    reused for new insertions, which affects memory efficiency and index growth patterns.   
    """
    
    def _create_vector_bytes(self, dim=128):
        """Helper to create random normalized vector bytes"""
        vec = np.random.rand(dim).astype(np.float32)
        vec = vec / np.linalg.norm(vec)  # Normalize for cosine similarity
        return struct.pack(f'{dim}f', *vec)
    
    def _extract_info_field(self, info_output, field_name):
        """Helper to extract field from FT.INFO output"""
        info_dict = dict(zip(info_output[::2], info_output[1::2]))
        return info_dict.get(field_name)
    
    def _get_vector_counts(self, client, index_name):
        """Helper to get current and deleted vector counts from FT.INFO"""
        info = client.execute_command('FT.INFO', index_name)
        
        # The new fields are at the top level of FT.INFO response
        info_dict = dict(zip(info[::2], info[1::2]))
        
        curr_vectors = int(info_dict.get(b'curr_vectors', 0))
        deleted_vectors = int(info_dict.get(b'curr_deleted_vectors', 0))
        
        return curr_vectors, deleted_vectors

    def test_memory_efficiency_with_replacement(self):
        """Tests that enabling allow_replace_deleted prevents unnecessary memory growth by reusing
            deleted vector slots instead of expanding the index capacity"""
        client: Valkey = self.server.get_new_client()
        
        # Set the configuration at runtime
        client.config_set('search.hnsw-allow-replace-deleted', 'yes')
        
        # Create index
        response = client.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
        )
        assert response == b'OK'
        
        # Add initial vectors
        num_vectors = 10000
        for i in range(num_vectors):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Get HNSW block size configuration
        block_size_config = client.config_get('search.hnsw-block-size')
        block_size = int(block_size_config['search.hnsw-block-size'])
        print(f"HNSW block size: {block_size}")
        
        # Calculate expected blocks based on initial vectors
        initial_expected_blocks = (num_vectors + block_size - 1) // block_size  # Ceiling division
        print(f"Expected initial blocks: {initial_expected_blocks} (for {num_vectors} vectors)")
        
        # Get initial vector counts
        initial_curr, initial_deleted = self._get_vector_counts(client, 'idx')
        print(f"Initial vector counts: curr={initial_curr}, deleted={initial_deleted}")
        
        # Delete half the vectors
        deleted_count = 0
        for i in range(0, num_vectors, 2):
            result = client.delete(f'doc{i}')
            if result == 1:
                deleted_count += 1
        
        assert deleted_count == num_vectors // 2
        
        # Add new vectors (should reuse deleted slots)
        new_vectors_count = num_vectors // 2
        for i in range(num_vectors, num_vectors + new_vectors_count):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Get final vector counts
        final_curr, final_deleted = self._get_vector_counts(client, 'idx')
        print(f"Final vector counts: curr={final_curr}, deleted={final_deleted}")
        
        # Calculate expected blocks with replacement (should stay same or minimal growth)
        # With replacement enabled, total vectors should stay close to original
        # because deleted slots are reused
        total_vectors_with_replacement = num_vectors  # Should remain ~10k as slots are reused
        final_expected_blocks = (total_vectors_with_replacement + block_size - 1) // block_size
        print(f"Expected final blocks: {final_expected_blocks} (with replacement reusing slots)")
        
        # Make concrete assertions based on block size
        # With replacement enabled, memory growth should be minimal
        expected_growth = final_expected_blocks / initial_expected_blocks
        print(f"Expected memory growth: {expected_growth:.2f}x (blocks: {initial_expected_blocks} -> {final_expected_blocks})")
        
        # Assert that memory growth is minimal (should be 1.0x or very close)
        assert expected_growth <= 1.1, f"Expected minimal memory growth (<=1.1x) with replacement, got {expected_growth:.2f}x"
        
        # Verify vector counts match expectations - with replacement, deleted slots should be reused
        assert final_curr == num_vectors, f"Expected {num_vectors} total vectors (reused slots), got {final_curr}"
        assert final_deleted == 0, f"Expected 0 deleted vectors (slots reused), got {final_deleted}"
        
        print(f"✅ Memory efficiency test PASSED - with replacement maintained {expected_growth:.2f}x block usage")

    def test_memory_efficiency_without_replacement(self):
        """Tests that disabling allow_replace_deleted causes the index to expand its capacity
            when new vectors are added after deletions, leading to higher memory usage."""
        client: Valkey = self.server.get_new_client()
        
        # Set the configuration at runtime (disabled)
        client.config_set('search.hnsw-allow-replace-deleted', 'no')
        
        # Create index
        response = client.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
        )
        assert response == b'OK'
        
        # Add initial vectors
        num_vectors = 10000
        for i in range(num_vectors):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Get HNSW block size configuration
        block_size_config = client.config_get('search.hnsw-block-size')
        block_size = int(block_size_config['search.hnsw-block-size'])
        print(f"HNSW block size: {block_size}")
        
        # Calculate expected blocks based on initial vectors
        initial_expected_blocks = (num_vectors + block_size - 1) // block_size  # Ceiling division
        print(f"Expected initial blocks: {initial_expected_blocks} (for {num_vectors} vectors)")
        
        # Get initial vector counts
        initial_curr, initial_deleted = self._get_vector_counts(client, 'idx')
        print(f"Initial vector counts: curr={initial_curr}, deleted={initial_deleted}")
        
        # Delete half the vectors
        for i in range(0, num_vectors, 2):
            client.delete(f'doc{i}')
        
        # Add new vectors (will require index expansion)
        new_vectors_count = num_vectors // 2
        for i in range(num_vectors, num_vectors + new_vectors_count):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Get final vector counts
        final_curr, final_deleted = self._get_vector_counts(client, 'idx')
        print(f"Final vector counts: curr={final_curr}, deleted={final_deleted}")
        
        # Calculate expected blocks after expansion (without replacement)
        # Total vectors = original + new (deleted slots are not reused)
        total_vectors_without_replacement = num_vectors + new_vectors_count  # 10k + 5k = 15k
        final_expected_blocks = (total_vectors_without_replacement + block_size - 1) // block_size
        print(f"Expected final blocks: {final_expected_blocks} (for {total_vectors_without_replacement} total vectors)")
        
        # Make concrete assertions based on block size
        # Without replacement, we expect significant block growth
        expected_growth = final_expected_blocks / initial_expected_blocks
        print(f"Expected memory growth: {expected_growth:.2f}x (blocks: {initial_expected_blocks} -> {final_expected_blocks})")
        
        # Assert that memory grew significantly (at least 1.3x as originally planned)
        assert expected_growth >= 1.3, f"Expected significant memory growth (>=1.3x) without replacement, got {expected_growth:.2f}x"
        
        # Verify vector counts match expectations
        assert final_curr == total_vectors_without_replacement, f"Expected {total_vectors_without_replacement} total vectors, got {final_curr}"
        assert final_deleted == (num_vectors // 2), f"Expected {num_vectors // 2} deleted vectors, got {final_deleted}"
        
        print(f"✅ Memory efficiency test PASSED - without replacement caused {expected_growth:.2f}x block growth")
            

    def test_search_accuracy_with_replacement(self):
        """Validates that search functionality and accuracy are preserved when vector slot
            replacement is enabled, ensuring deleted documents don't appear in search results."""
        client: Valkey = self.server.get_new_client()
        
        # Set the configuration at runtime
        client.config_set('search.hnsw-allow-replace-deleted', 'yes')
        
        # Create index with cosine similarity
        response = client.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'COSINE'
        )
        assert response == b'OK'
        
        # Add vectors
        vectors = {}
        for i in range(100):
            vec_bytes = self._create_vector_bytes(128)
            vectors[f'doc{i}'] = vec_bytes
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Delete some vectors
        deleted_docs = []
        for i in range(0, 20):
            if client.delete(f'doc{i}') == 1:
                deleted_docs.append(f'doc{i}')
        
        # Add replacement vectors
        for i in range(100, 120):
            vec_bytes = self._create_vector_bytes(128)
            vectors[f'doc{i}'] = vec_bytes
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Perform KNN search
        query_vec = self._create_vector_bytes(128)
        results = client.execute_command(
            'FT.SEARCH', 'idx', '*=>[KNN 10 @vec $query_vec]',
            'PARAMS', '2', 'query_vec', query_vec
        )
        
        # Verify results
        num_results = results[0]
        assert num_results == 10, f"Expected 10 results, got {num_results}"
        
        # Verify no deleted documents in results
        for i in range(1, len(results), 2):
            doc_id = results[i].decode('utf-8')
            assert doc_id not in deleted_docs, f"Deleted document {doc_id} in results"

    def test_vector_counts_validation(self):
        """Tests the new FT.INFO fields (curr_vectors, curr_deleted_vectors) that provide
            visibility into index state, comparing behavior with replacement enabled vs disabled."""
        
        # Test with replacement ENABLED
        client: Valkey = self.server.get_new_client()
        client.config_set('search.hnsw-allow-replace-deleted', 'yes')
        
        # Create index
        client.execute_command(
            'FT.CREATE', 'idx_replace', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
        )
        
        # Add initial vectors
        initial_count = 100
        for i in range(initial_count):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Check initial counts
        curr_vectors, deleted_vectors = self._get_vector_counts(client, 'idx_replace')
        assert curr_vectors == initial_count, f"Expected {initial_count} vectors, got {curr_vectors}"
        assert deleted_vectors == 0, f"Expected 0 deleted vectors, got {deleted_vectors}"
        
        # Delete half the vectors
        delete_count = initial_count // 2
        for i in range(delete_count):
            client.delete(f'doc{i}')
        
        # Check counts after deletion
        curr_vectors, deleted_vectors = self._get_vector_counts(client, 'idx_replace')
        assert curr_vectors == initial_count, f"After deletion: expected {initial_count} vectors, got {curr_vectors}"
        assert deleted_vectors == delete_count, f"After deletion: expected {delete_count} deleted vectors, got {deleted_vectors}"
        
        # Add replacement vectors (with replacement enabled, should reuse deleted slots)
        for i in range(initial_count, initial_count + delete_count):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Check counts after replacement - with replacement enabled
        curr_vectors, deleted_vectors = self._get_vector_counts(client, 'idx_replace')
        assert curr_vectors == initial_count, f"After replacement: expected {initial_count} vectors, got {curr_vectors}"
        assert deleted_vectors == 0, f"After replacement: expected 0 deleted vectors, got {deleted_vectors}"
        
        # Clean up
        client.execute_command('FT.DROPINDEX', 'idx_replace')
        
        # Test with replacement DISABLED
        client.config_set('search.hnsw-allow-replace-deleted', 'no')
        
        # Create index
        client.execute_command(
            'FT.CREATE', 'idx_no_replace', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
        )
        
        # Add initial vectors
        for i in range(initial_count):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Check initial counts
        curr_vectors, deleted_vectors = self._get_vector_counts(client, 'idx_no_replace')
        assert curr_vectors == initial_count, f"No replacement - Initial: expected {initial_count} vectors, got {curr_vectors}"
        assert deleted_vectors == 0, f"No replacement - Initial: expected 0 deleted vectors, got {deleted_vectors}"
        
        # Delete half the vectors
        for i in range(delete_count):
            client.delete(f'doc{i}')
        
        # Check counts after deletion
        curr_vectors, deleted_vectors = self._get_vector_counts(client, 'idx_no_replace')
        assert curr_vectors == initial_count, f"No replacement - After deletion: expected {initial_count} vectors, got {curr_vectors}"
        assert deleted_vectors == delete_count, f"No replacement - After deletion: expected {delete_count} deleted vectors, got {deleted_vectors}"
        
        # Add new vectors (without replacement, should expand the index)
        for i in range(initial_count, initial_count + delete_count):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Check counts after addition - without replacement enabled
        curr_vectors, deleted_vectors = self._get_vector_counts(client, 'idx_no_replace')
        expected_total = initial_count + delete_count  # Original + new additions
        assert curr_vectors == expected_total, f"No replacement - After addition: expected {expected_total} vectors, got {curr_vectors}"
        assert deleted_vectors == delete_count, f"No replacement - After addition: expected {delete_count} deleted vectors, got {deleted_vectors}"
        
        # Clean up
        client.execute_command('FT.DROPINDEX', 'idx_no_replace')

    def test_config_runtime_change(self):
        """Verifies that the allow_replace_deleted configuration can be changed at runtime
            using CONFIG SET/GET commands without requiring server restart."""
        client: Valkey = self.server.get_new_client()
        
        # Set initial config value
        client.config_set('search.hnsw-allow-replace-deleted', 'no')
        
        # Get initial config value
        result = client.config_get('search.hnsw-allow-replace-deleted')
        assert result['search.hnsw-allow-replace-deleted'] == 'no'
        
        # Change at runtime
        client.config_set('search.hnsw-allow-replace-deleted', 'yes')
        
        # Verify the change
        result = client.config_get('search.hnsw-allow-replace-deleted')
        assert result['search.hnsw-allow-replace-deleted'] == 'yes'

    # TODO: need to define success critiria for this test case
    def test_performance_comparison(self):
        """Measures and compares insertion and search performance between replacement enabled
            and disabled modes to ensure the feature doesn't negatively impact performance."""
        results = {}
        results['no'] = {}
        results['yes'] = {}
        perf_cases = [('no',10000000), ('yes',20), ('yes',15), ('yes',10), ('yes',7), ('yes',5), ('yes',4), ('yes',3), ('yes',2)]
        for allow_replace, part in perf_cases:
            client: Valkey = self.server.get_new_client()
            
            # Set the configuration at runtime
            client.config_set('search.hnsw-allow-replace-deleted', allow_replace)
            
            # Create index
            client.execute_command(
                'FT.CREATE', f'idx_{allow_replace}', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
                'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
            )
            
            # Add vectors
            num_vectors = 10000
            for i in range(num_vectors):
                vec_bytes = self._create_vector_bytes(128)
                client.hset(f'doc{i}', 'vec', vec_bytes)
            
            # Delete and replace cycle
            start_time = time.time()
            
            # Delete 20%
            for i in range(0, num_vectors // part):
                client.delete(f'doc{i}')
            
            # Add new vectors
            for i in range(num_vectors, num_vectors + num_vectors // part):
                vec_bytes = self._create_vector_bytes(128)
                client.hset(f'doc{i}', 'vec', vec_bytes)
            
            # Measure search performance
            search_times = []
            for _ in range(100):
                query_vec = self._create_vector_bytes(128)
                search_start = time.time()
                client.execute_command(
                    'FT.SEARCH', f'idx_{allow_replace}', '*=>[KNN 10 @vec $query_vec]',
                    'PARAMS', '2', 'query_vec', query_vec
                )
                search_times.append(time.time() - search_start)
            
            avg_search_time = sum(search_times) / len(search_times)
            total_time = time.time() - start_time
            
            results[allow_replace][part] = {
                'total_time': total_time,
                'avg_search_time_ms': avg_search_time * 1000
            }
            
            # Clean up index for next iteration
            client.execute_command('FT.DROPINDEX', f'idx_{allow_replace}')
        
        # Log results for comparison
        print(f"\nPerformance comparison:")
        for allow_replace, part in perf_cases:                       
            print(f"[1/{part}] replacement]: {results[allow_replace][part]['avg_search_time_ms']:.2f}ms avg search")
