from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
import valkey
import pytest
from valkeytestframework.conftest import resource_port_tracker
import os

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


def ft_create_fails_on_replica(primary_client, replica_client):
    # Test that FT.CREATE fails on replica
    with pytest.raises(valkey.exceptions.ResponseError) as e:
        replica_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert "You can't write against a read only replica" in str(e.value)

    # Verify that FT.CREATE works on master
    result = primary_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert result == b"OK"
    assert primary_client.execute_command("FT._LIST") == [
        INDEX_NAME.encode("utf-8")
    ]

    # Test again that FT.CREATE fails on replica
    with pytest.raises(valkey.exceptions.ResponseError) as e:
        replica_client.execute_command(*CREATE_INDEX_SCHEMA)
    assert "You can't write against a read only replica" in str(e.value)


class TestSearchFTCreateCME(ValkeySearchClusterTestCase):
    """
    Test suite for FT.CREATE search command. We expect that
    clients will not be able to create index on the replica.
    """

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 2}], indirect=True
    )
    def test_ft_create_fails_on_replica_cme(self):
        """Test that FT.CREATE fails when executed on a replica."""
        rg = self.get_replication_group(0)
        ft_create_fails_on_replica(
            rg.get_primary_connection(), rg.get_replica_connection(0)
        )


class TestSearchFTCreateCMD(ValkeySearchTestCaseBase):
    """
    Test suite for FT.CREATE search command. We expect that
    clients will not be able to create index on the replica.
    """

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 2}], indirect=True
    )
    def test_ft_create_fails_on_replica_cmd(self):
        """Test that FT.CREATE fails when executed on a replica."""
        ft_create_fails_on_replica(
            self.get_primary_connection(),
            self.get_replica_connection(0),
        )
