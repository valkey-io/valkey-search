from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
import valkey
import pytest
from valkeytestframework.conftest import resource_port_tracker


INDEX_NAME = "test_replica_index"
CREATE_INDEX_SCHEMA = [
    "FT.CREATE",
    INDEX_NAME,
    "ON",
    "HASH",
    "SCHEMA",
    "embedding",
    "VECTOR",
    "HNSW",
    "12",
    "m",
    "10",
    "TYPE",
    "FLOAT32",
    "DIM",
    "100",
    "DISTANCE_METRIC",
    "COSINE",
    "EF_CONSTRUCTION",
    "5",
    "EF_RUNTIME",
    "10",
]


def ft_dropindex_fails_on_replica(primary_client, replica_client):
    result = primary_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert result == b"OK"
    assert primary_client.execute_command("FT._LIST") == [
        INDEX_NAME.encode("utf-8")
    ]

    # Test that FT.DROPINDEX fails on replica
    with pytest.raises(valkey.exceptions.ResponseError) as e:
        replica_client.execute_command("FT.DROPINDEX", INDEX_NAME)
    assert "You can't write against a read only replica" in str(e.value)

    # Verify that FT.DROPINDEX works on master
    result = primary_client.execute_command("FT.DROPINDEX", INDEX_NAME)
    assert result == b"OK"
    assert primary_client.execute_command("FT._LIST") == []


class TestSearchFTDropindexCME(ValkeySearchClusterTestCase):
    """
    Test suite for FT.DROPINDEX search command. We expect that
    clients will not be able to drop index on the replica.
    """

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_ft_dropindex_fails_on_replica_cme(self):
        """Test that FT.DROPINDEX fails when executed on a replica."""
        rg = self.get_replication_group(0)
        ft_dropindex_fails_on_replica(
            rg.get_primary_connection(),
            rg.get_replica_connection(0),
        )


class TestSearchFTDropindexCMD(ValkeySearchTestCaseBase):
    """
    Test suite for FT.DROPINDEX search command. We expect that
    clients will not be able to drop index on the replica.
    """

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_ft_dropindex_fails_on_replica_cmd(self):
        """Test that FT.DROPINDEX fails when executed on a replica."""
        primary_client = self.get_primary_connection()
        replica_client = self.get_replica_connection(0)
        ft_dropindex_fails_on_replica(primary_client, replica_client)
