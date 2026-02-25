from valkey_search_test_case import *
import valkey, time
import pytest
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
from valkeytestframework.util import waiters
from valkey.cluster import ValkeyCluster, ClusterNode

def search_command(index: str) -> list[str]:
    return [
        "FT.SEARCH",
        index,
        "*=>[KNN 10 @v $BLOB]",
        "PARAMS",
        "2",
        "BLOB",
        float_to_bytes([10.0, 10.0, 10.0]),
    ]

def index_on_node(client, name:str) -> bool:
    indexes = client.execute_command("FT._LIST")
    return name.encode() in indexes

def sum_of_remote_searches(nodes: list[Node]) -> int:
    return sum([n.client.info("search")["search_coordinator_server_search_index_partition_success_count"] for n in nodes])

def do_json_backfill_test(test, client, primary, replica):
    assert(primary.info("replication")["role"] == "master")
    assert(replica.info("replication")["role"] == "slave")
    index = Index("test", [Vector("v", 3, type="FLAT")], type=KeyDataType.JSON)
    index.load_data(client, 100)
    replica.readonly()

    index.create(primary)
    waiters.wait_for_true(lambda: index_on_node(primary, index.name))
    waiters.wait_for_true(lambda: index_on_node(replica, index.name))
    waiters.wait_for_true(lambda: index.backfill_complete(primary))
    waiters.wait_for_true(lambda: index.backfill_complete(replica))
    p_result = primary.execute_command(*search_command(index.name))
    for n in test.nodes:
        n.client.execute_command("ft._debug CONTROLLED_VARIABLE set ForceReplicasOnly yes")
    r_result = replica.execute_command(*search_command(index.name))
    print("After second Search")
    print("PResult:", p_result)
    print("RResult:", r_result)
    assert len(p_result) == 21
    assert len(r_result) == 21

class TestJsonBackfill(ValkeySearchClusterTestCaseDebugMode):
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_json_backfill_CME(self):
        """
        Validate that JSON backfill works correctly on a replica
        """
        rg = self.get_replication_group(0)
        primary = rg.get_primary_connection()
        replica = rg.get_replica_connection(0)
        do_json_backfill_test(self, self.new_cluster_client(), primary, replica)

class TestSearchFTDropindexCMD(ValkeySearchTestCaseDebugMode):
    """
    Test suite for FT.DROPINDEX search command. We expect that
    clients will not be able to drop index on the replica.
    """
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_json_backfill_CMD(self):
        do_json_backfill_test(self, self.client, self.get_primary_connection(), self.get_replica_connection(0))

class TestCreateNonVectorIndexes(ValkeySearchClusterTestCase):
    """
    Test create and search for JSON non-vector indexes
    """
    def test_non_vector_indexes(self):
        numeric_indx_name = "numeric"
        tag_idx_name = "tag"
        client = self.new_cluster_client()
        index_numeric = Index(numeric_indx_name, [Numeric("n")], type=KeyDataType.JSON)
        index_tag = Index(tag_idx_name, [Tag("t")], type=KeyDataType.JSON)
        indexes = [index_numeric, index_tag]
        for index in indexes:
            index.load_data(client, 10)
            index.create(client)
            for primary_client in self.get_all_primary_clients():
                waiters.wait_for_true(lambda: index_on_node(primary_client, index.name))
                waiters.wait_for_true(lambda: index.backfill_complete(primary_client))
        
        # Run numeric query
        numeric_result = client.execute_command(
            "FT.SEARCH",
            numeric_indx_name,
            f"@n:[-inf +inf]"
        )
        assert numeric_result[0] == 10
        # Run tag query
        tag_result = client.execute_command(
            "FT.SEARCH",
            tag_idx_name,
            f"@t:{{Tag:*}}"
        )
        assert tag_result[0] == 10

class TestJsonVectorFieldAlias(ValkeySearchTestCaseBase):
    """
    Test different ways to reference a JSON vector field with an alias.
    This addresses the issue where vector fields with aliases in JSON indexes
    may not work with all expected search syntax formats.
    """
    def test_json_vector_field_alias_search_formats(self):
        """
        Creates: FT.CREATE idx ON JSON SCHEMA $.embedding AS vec VECTOR HNSW 3 ...
        
        All of these search formats SHOULD work without error:
        1. Using alias: @vec
        2. Using full JSON path: @$.embedding
        3. Using field name without prefix: @embedding (if supported)
        """
        client = self.client
        
        # Create index with a JSON vector field using an alias (using helper)
        index = Index("json_vec_alias", 
                     [Vector("$.embedding", dim=3, alias="vec")], 
                     type=KeyDataType.JSON)
        index.create(client)
        
        # Insert test data
        index.load_data(client, 10)
        
        # Wait for backfill to complete
        waiters.wait_for_true(lambda: index.backfill_complete(client))
        
        # Create index directly with FT.CREATE command
        # This creates: FT.CREATE json_vec_alias ON JSON SCHEMA $.embedding AS vec VECTOR HNSW 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC L2
        # client.execute_command(
        #     "FT.CREATE", "json_vec_alias",
        #     "ON", "JSON",
        #     "SCHEMA",
        #     "$.embedding", "AS", "vec", "VECTOR", "HNSW", "6",
        #     "TYPE", "FLOAT32",
        #     "DIM", "3",
        #     "DISTANCE_METRIC", "L2"
        # )
        
        # # Insert test JSON documents with vector data
        # for i in range(10):
        #     client.execute_command(
        #         "JSON.SET", f"json_vec_alias:{i:08d}", "$",
        #         f'{{"embedding": [{float(i)}, {float(i+1)}, {float(i+2)}]}}'
        #     )
        
        # # Wait for backfill to complete
        # # Give it a moment for the index to start processing
        # time.sleep(1)
        
        # def backfill_complete():
        #     try:
        #         info = client.execute_command("FT.INFO", "json_vec_alias")
        #         # FT.INFO returns a list of key-value pairs
        #         # Convert to dict for easier access
        #         info_dict = {}
        #         for i in range(0, len(info), 2):
        #             key = info[i].decode('utf-8') if isinstance(info[i], bytes) else str(info[i])
        #             value = info[i+1]
        #             info_dict[key] = value
                
        #         # Check if backfill_in_progress exists and is 0
        #         backfill_status = info_dict.get('backfill_in_progress', 1)
        #         return backfill_status == 0
        #     except Exception as e:
        #         print(f"Error checking backfill status: {e}")
        #         return False
        
        # waiters.wait_for_true(backfill_complete, timeout=30)
        
        # Test vector to search for
        test_vector = float_to_bytes([1.0, 2.0, 3.0])
        
        # Track results from each format
        results = {}
        errors = {}
        
        # Test Case 1: Search using alias - @vec
        # This SHOULD work but currently may fail according to the issue
        print("\n=== Test Case 1: Search using alias @vec ===")
        try:
            result1 = client.execute_command(
                "FT.SEARCH", "json_vec_alias", "*=>[KNN 1 @vec $BLOB]",
                "PARAMS", "2", "BLOB", test_vector
            )
            results['@vec'] = result1
            print(f"✓ Search with alias @vec: {result1[0]} results")
        except Exception as e:
            errors['@vec'] = str(e)
            print(f"✗ Search with @vec failed: {e}")
        
        # Test Case 2: Search using full JSON path - @$.embedding
        # This is documented to work in the issue
        print("\n=== Test Case 2: Search using full JSON path @$.embedding ===")
        try:
            result2 = client.execute_command(
                "FT.SEARCH", "json_vec_alias", "*=>[KNN 1 @$.embedding $BLOB]",
                "PARAMS", "2", "BLOB", test_vector
            )
            results['@$.embedding'] = result2
            print(f"✓ Search with full path @$.embedding: {result2[0]} results")
        except Exception as e:
            errors['@$.embedding'] = str(e)
            print(f"✗ Search with @$.embedding failed: {e}")
        
        # Test Case 3: Search using field name without $ - @embedding
        # NOTE: This is expected to FAIL for JSON indexes - commenting out as it's not a bug
        # print("\n=== Test Case 3: Search using name @embedding (without $) ===")
        # try:
        #     result3 = client.execute_command(
        #         "FT.SEARCH", "json_vec_alias", "*=>[KNN 1 @embedding $BLOB]",
        #         "PARAMS", "2", "BLOB", test_vector
        #     )
        #     results['@embedding'] = result3
        #     print(f"✓ Search with name @embedding: {result3[0]} results")
        # except Exception as e:
        #     errors['@embedding'] = str(e)
        #     print(f"✗ Search with @embedding failed: {e}")
        
        # Summary
        print("\n=== SUMMARY ===")
        print(f"Working formats: {list(results.keys())}")
        print(f"Failed formats: {list(errors.keys())}")
        
        # According to the issue, BOTH formats should work:
        # - @vec (the alias)
        # - @$.embedding (the full JSON path)
        
        # Document the actual behavior
        print("\n=== ISSUE ANALYSIS ===")
        if '@vec' in results and '@$.embedding' not in results:
            print("⚠️  CURRENT BEHAVIOR: Only alias (@vec) works")
            print(f"   ✓ @vec works: {results['@vec'][0]} results")
            print(f"   ✗ @$.embedding fails: {errors.get('@$.embedding', 'Unknown')}")
            print("\n   EXPECTED: Both @vec AND @$.embedding should work!")
        elif '@$.embedding' in results and '@vec' not in results:
            print("⚠️  CURRENT BEHAVIOR: Only full path (@$.embedding) works")
            print(f"   ✓ @$.embedding works: {results['@$.embedding'][0]} results")
            print(f"   ✗ @vec fails: {errors.get('@vec', 'Unknown')}")
            print("\n   EXPECTED: Both @vec AND @$.embedding should work!")
        elif '@vec' in results and '@$.embedding' in results:
            print("✓ Both @vec and @$.embedding work correctly!")
            assert results['@vec'][0] == results['@$.embedding'][0], \
                "Alias and full path should return same number of results"
        else:
            print("✗ Neither format works - unexpected behavior!")
        
        # Assert that BOTH formats should work according to the issue
        # This test will FAIL until the bug is fixed
        assert '@vec' in results, \
            f"Alias @vec should work but failed with: {errors.get('@vec', 'Unknown error')}"
        
        assert '@$.embedding' in results, \
            f"Full JSON path @$.embedding should work but failed with: {errors.get('@$.embedding', 'Unknown error')}"
        
        # Both should return the same results
        assert results['@vec'][0] == results['@$.embedding'][0], \
            "Alias and full path should return same number of results"
        

    def test_search_with_identifier_and_alias(self):
        """Test FT.SEARCH works with both identifier and alias when they differ.
        
        Bug: When a JSON field has identifier='$.embedding' and alias='vec',
        searching with @$.embedding (identifier) should work, not just @vec (alias).
        """
        import numpy as np
        client: Valkey = self.server.get_new_client()
        
        def make_vector(vals):
            return np.array(vals, dtype=np.float32).tobytes()
        
        # Create index with identifier='$.embedding' and alias='vec'
        client.execute_command(
            "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "doc:",
            "SCHEMA",
            "$.embedding", "AS", "vec", "VECTOR", "HNSW", "6", 
            "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"
        )
        
        # Insert test data
        client.execute_command(
            "JSON.SET", "doc:1", "$",
            '{"embedding": [1.0, 2.0, 3.0]}'
        )
        
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        
        query_vec = make_vector([1.0, 2.0, 3.0])
        
        # Test 1: Search using alias should work
        result_alias = client.execute_command(
            "FT.SEARCH", "idx", "*=>[KNN 1 @vec $vec]",
            "PARAMS", "2", "vec", query_vec,
            "DIALECT", "2", "NOCONTENT"
        )
        assert result_alias[0] == 1, f"Search with alias @vec failed: {result_alias}"
        
        # Test 2: Search using identifier should also work (this tests the fix)
        result_identifier = client.execute_command(
            "FT.SEARCH", "idx", "*=>[KNN 1 @$.embedding $vec]",
            "PARAMS", "2", "vec", query_vec,
            "DIALECT", "2", "NOCONTENT"
        )
        assert result_identifier[0] == 1, f"Search with identifier @$.embedding failed: {result_identifier}"
        
        # Both should return the same document
        assert result_alias[1] == result_identifier[1], \
            f"Alias and identifier should return same doc: {result_alias[1]} vs {result_identifier[1]}"
