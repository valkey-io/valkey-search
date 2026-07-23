"""Integration tests for FT.SEARCH INKEYS option."""

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFTSearchInkeys(ValkeySearchTestCaseBase):

    def _setup_hash_index(self, client: Valkey):
        client.execute_command(
            "FT.CREATE", "idx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "score", "NUMERIC",
            "category", "TAG",
        )
        for i in range(5):
            client.execute_command(
                "HSET", f"doc:{i}",
                "score", str(i * 10),
                "category", f"cat{i % 2}",
            )

    def test_inkeys_with_nocontent(self):
        """NOCONTENT: reply is count + keys only, no field arrays."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "3", "doc:0", "doc:1", "doc:2",
            "NOCONTENT",
            "DIALECT", "2",
        )

        assert result[0] == 3
        assert len(result) == 4
        keys = {result[i].decode() for i in range(1, len(result))}
        assert keys == {"doc:0", "doc:1", "doc:2"}

    def test_inkeys_with_sortby_and_limit(self):
        """INKEYS + SORTBY + LIMIT: ordering survives truncation."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "4", "doc:0", "doc:1", "doc:3", "doc:4",
            "SORTBY", "score", "DESC",
            "LIMIT", "0", "2",
            "DIALECT", "2",
        )

        assert result[0] == 4
        keys = [result[i].decode() for i in range(1, len(result), 2)]
        assert keys == ["doc:4", "doc:3"]

    def test_inkeys_invalid_count_errors(self):
        """Malformed INKEYS counts reject at parse time."""
        client: Valkey = self.server.get_new_client()
        self._setup_hash_index(client)

        # Zero count — empty result.
        result = client.execute_command(
            "FT.SEARCH", "idx", "@score:[-inf +inf]",
            "INKEYS", "0",
            "DIALECT", "2",
        )
        assert result[0] == 0

        # Negative count.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "-1", "doc:0",
                "DIALECT", "2",
            )

        # Non-integer count.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "abc",
                "DIALECT", "2",
            )

        # Count exceeds provided keys.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "idx", "@score:[-inf +inf]",
                "INKEYS", "5", "doc:0", "doc:1", "doc:2",
            )
