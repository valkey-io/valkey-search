from valkey_search_test_case import ValkeySearchClusterTestCase, ValkeySearchTestCaseBase
import valkey
import pytest
from valkeytestframework.conftest import resource_port_tracker


INDEX_NAME = "test_replica_index"
CREATE_INDEX_SCHEMA = [
    "FT.CREATE", INDEX_NAME, "ON", "HASH", "SCHEMA", 
    "embedding", "VECTOR", "HNSW", "12", "m", "10", "TYPE", "FLOAT32", 
    "DIM", "100", "DISTANCE_METRIC", "COSINE", "EF_CONSTRUCTION", "5", "EF_RUNTIME", "10"
]

def ft_create_fails_on_replica(primary_client, replica_client):
    # Test that FT.CREATE fails on replica
    with pytest.raises(valkey.exceptions.ResponseError) as e:
        replica_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert "You can't write against a read only replica" in str(e.value)

    # Verify that FT.CREATE works on master
    result = primary_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert result == b"OK"
    assert primary_client.execute_command("FT._LIST") == [INDEX_NAME.encode('utf-8')]
    
    # Test again that FT.CREATE fails on replica
    with pytest.raises(valkey.exceptions.ResponseError) as e:
        replica_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert "You can't write against a read only replica" in str(e.value)


class TestSearchFTCreateCME(ValkeySearchClusterTestCase):
    """
    Test suite for FT.CREATE search command. We expect that
    clients will not be able to create index on the replica.
    """
    def test_ft_create_fails_on_replica_cme(self):
        """Test that FT.CREATE fails when executed on a replica."""
        # Use existing cluster servers instead of creating new ones
        primary_client, replica_client = self.setup_replica_from_existing_cluster_servers(primary_idx=0, replica_idx=1)

        try:
            ft_create_fails_on_replica(primary_client, replica_client)
        finally:
            # Cleanup: restore replica back to standalone
            self.cleanup_replica_cluster(replica_idx=1)


class TestSearchFTCreateCMD(ValkeySearchTestCaseBase):
    """
    Test suite for FT.CREATE search command. We expect that
    clients will not be able to create index on the replica.
    """
    def test_ft_create_fails_on_replica_cmd(self):
        """Test that FT.CREATE fails when executed on a replica."""
        replica_server, replica_client = self.create_new_replica()

        try:
            ft_create_fails_on_replica(self.client, replica_client)
        finally:
            # restore replica back to standalone
            self.cleanup_replica(replica_client)
