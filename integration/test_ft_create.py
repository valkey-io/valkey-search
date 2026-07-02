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
        "setup_test", [{"replica_count": 1}], indirect=True
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
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_ft_create_fails_on_replica_cmd(self):
        """Test that FT.CREATE fails when executed on a replica."""
        ft_create_fails_on_replica(
            self.get_primary_connection(),
            self.get_replica_connection(0),
        )

    def test_same_field_as_text_and_tag(self):
        """Regression test for issue #1195.

        The same source field may be indexed multiple times under distinct
        aliases (e.g. once as TEXT and once as TAG). This used to be rejected
        with "Duplicate field in schema". Validate that the index is created
        and that both aliases are independently queryable.
        """
        client = self.server.get_new_client()

        # The exact command from the issue report.
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "blog:post:",
            "SCHEMA",
            "sku", "AS", "sku_text", "TEXT",
            "sku", "AS", "sku_tag", "TAG", "SORTABLE",
        ) == b"OK"

        assert client.execute_command(
            "HSET", "blog:post:1", "sku", "widget"
        ) == 1

        # The TAG alias matches the whole value.
        result = client.execute_command(
            "FT.SEARCH", "idx", "@sku_tag:{widget}", "NOCONTENT"
        )
        assert result[0] == 1
        assert result[1] == b"blog:post:1"

        # The TEXT alias matches the tokenized value.
        result = client.execute_command(
            "FT.SEARCH", "idx", "@sku_text:widget", "NOCONTENT"
        )
        assert result[0] == 1
        assert result[1] == b"blog:post:1"

    def test_duplicate_alias_still_rejected(self):
        """Two attributes sharing the same alias must still be rejected."""
        client = self.server.get_new_client()
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command(
                "FT.CREATE", "iddup", "ON", "HASH",
                "SCHEMA",
                "sku", "AS", "dup", "TEXT",
                "price", "AS", "dup", "TAG",
            )
        assert "Duplicate field in schema - dup" in str(e.value)
