"""Integration tests for FT.ALIASADD, FT.ALIASDEL, and FT.ALIASUPDATE."""

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

# A simple tag index used across tests.
INDEX_NAME = "alias_test_idx"
INDEX_NAME_2 = "alias_test_idx2"
ALIAS_NAME = "my_alias"

CREATE_TAG_INDEX = [
    "FT.CREATE", INDEX_NAME,
    "ON", "HASH",
    "PREFIX", "1", "doc:",
    "SCHEMA", "category", "TAG",
]

CREATE_TAG_INDEX_2 = [
    "FT.CREATE", INDEX_NAME_2,
    "ON", "HASH",
    "PREFIX", "1", "doc2:",
    "SCHEMA", "category", "TAG",
]


class TestFTAliasAdd(ValkeySearchTestCaseBase):
    """Tests for FT.ALIASADD."""

    def test_aliasadd_happy_path(self):
        """Adding a valid alias returns OK and the alias resolves to the index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        # Alias should resolve to the underlying index via FT.INFO
        info = client.execute_command("FT.INFO", ALIAS_NAME)
        assert info is not None

    def test_aliasadd_duplicate_alias(self):
        """Adding the same alias twice returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME)
        assert "Alias already exists" in str(exc_info.value)

    def test_aliasadd_nonexistent_index(self):
        """Adding an alias for a non-existent index returns an error."""
        client = self.client
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASADD", ALIAS_NAME, "no_such_index")
        assert "no_such_index" in str(exc_info.value)

    def test_aliasadd_alias_to_alias(self):
        """Adding an alias that points to another alias returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASADD", "alias2", ALIAS_NAME)
        assert "Unknown index name or name is an alias" in str(exc_info.value)

    def test_aliasadd_multiple_aliases_same_index(self):
        """Multiple aliases can point to the same index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_a", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_b", INDEX_NAME) == b"OK"
        # Both aliases should resolve
        assert client.execute_command("FT.INFO", "alias_a") is not None
        assert client.execute_command("FT.INFO", "alias_b") is not None


class TestFTAliasDel(ValkeySearchTestCaseBase):
    """Tests for FT.ALIASDEL."""

    def test_aliasdel_happy_path(self):
        """Deleting an existing alias returns OK; underlying index still accessible."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"
        # Alias should no longer resolve
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)
        # Underlying index still accessible
        assert client.execute_command("FT.INFO", INDEX_NAME) is not None

    def test_aliasdel_nonexistent_alias(self):
        """Deleting a non-existent alias returns an error."""
        client = self.client
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASDEL", "no_such_alias")
        assert "Alias does not exist" in str(exc_info.value)

    def test_aliasdel_then_readd(self):
        """After deleting an alias it can be re-added."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.INFO", ALIAS_NAME) is not None


class TestFTAliasUpdate(ValkeySearchTestCaseBase):
    """Tests for FT.ALIASUPDATE."""

    def test_aliasupdate_create_new(self):
        """ALIASUPDATE on a new alias behaves like ALIASADD."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.INFO", ALIAS_NAME) is not None

    def test_aliasupdate_reassign(self):
        """ALIASUPDATE reassigns an existing alias to a different index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2) == b"OK"
        # Alias must now resolve to INDEX_NAME_2: search for the key after b"index_name"
        info = client.execute_command("FT.INFO", ALIAS_NAME)
        info_list = list(info)
        idx = next(
            (i for i, v in enumerate(info_list) if v == b"index_name"), None
        )
        assert idx is not None, "FT.INFO response missing 'index_name' field"
        assert info_list[idx + 1] == INDEX_NAME_2.encode()
        # INDEX_NAME must still be accessible directly (atomicity: old index intact).
        assert client.execute_command("FT.INFO", INDEX_NAME) is not None

    def test_aliasupdate_nonexistent_index(self):
        """ALIASUPDATE with a non-existent index returns an error."""
        client = self.client
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASUPDATE", ALIAS_NAME, "no_such_index")
        assert "no_such_index" in str(exc_info.value)

    def test_aliasupdate_alias_to_alias(self):
        """ALIASUPDATE that would create alias-to-alias returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASUPDATE", "alias2", ALIAS_NAME)
        assert "Unknown index name or name is an alias" in str(exc_info.value)

    def test_aliasupdate_upsert_existing(self):
        """ALIASUPDATE on an existing alias silently reassigns it (upsert)."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        # Second ALIASUPDATE to same alias must succeed, not error.
        assert client.execute_command(
            "FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2
        ) == b"OK"
        info = client.execute_command("FT.INFO", ALIAS_NAME)
        info_list = list(info)
        idx = next(
            (i for i, v in enumerate(info_list) if v == b"index_name"), None
        )
        assert idx is not None
        assert info_list[idx + 1] == INDEX_NAME_2.encode()


class TestFTAliasDropIndex(ValkeySearchTestCaseBase):
    """Tests that FT.DROPINDEX works when called with an alias name."""

    def test_dropindex_via_real_name_removes_alias(self):
        """FT.DROPINDEX by real index name drops the index and cleans up the alias."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        assert client.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        # Index is gone.
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", INDEX_NAME)
        # Alias is also gone (purged by RemoveIndexSchemaInternal).
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)

    def test_dropindex_via_alias_name(self):
        """FT.DROPINDEX called with an alias name resolves to the real index and drops it."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        # Drop using the alias name directly.
        assert client.execute_command("FT.DROPINDEX", ALIAS_NAME) == b"OK"

        # Both the index and the alias are gone.
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", INDEX_NAME)
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)

    def test_dropindex_leaves_other_aliases_intact(self):
        """Dropping one index only removes aliases pointing to that index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_idx1", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_idx2", INDEX_NAME_2) == b"OK"

        assert client.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        # alias_idx1 is gone.
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", "alias_idx1")
        # alias_idx2 still resolves.
        assert client.execute_command("FT.INFO", "alias_idx2") is not None


class TestFTAliasSearch(ValkeySearchTestCaseBase):
    """Tests that FT.SEARCH and FT.AGGREGATE work transparently via alias."""

    def test_search_via_alias(self):
        """FT.SEARCH accepts an alias name and returns results."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        client.execute_command("HSET", "doc:1", "category", "books")
        client.execute_command("HSET", "doc:2", "category", "electronics")
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        result = client.execute_command("FT.SEARCH", ALIAS_NAME, "@category:{books}")
        assert result[0] == 1
        assert result[1] == b"doc:1"

    def test_aggregate_via_alias(self):
        """FT.AGGREGATE accepts an alias name and returns results."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        client.execute_command("HSET", "doc:1", "category", "books")
        client.execute_command("HSET", "doc:2", "category", "electronics")
        client.execute_command("HSET", "doc:3", "category", "books")
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        result = client.execute_command(
            "FT.AGGREGATE", ALIAS_NAME, "@category:{books}",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "count",
        )
        # result[0] is the number of groups
        assert result[0] >= 1


class TestFTAliasCrossDb(ValkeySearchTestCaseBase):
    """Tests that aliases are isolated per database."""

    def test_alias_not_visible_in_other_db(self):
        """An alias created in db 0 is not visible in db 1."""
        client = self.client
        # Create index and alias in db 0
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        # Switch to db 1 and verify alias is not there
        client.execute_command("SELECT", "1")
        try:
            with pytest.raises(ResponseError):
                client.execute_command("FT.INFO", ALIAS_NAME)
        finally:
            # Switch back to db 0 to avoid polluting other tests
            client.execute_command("SELECT", "0")

    def test_alias_independent_per_db(self):
        """Same alias name can exist in different databases pointing to different indexes."""
        client = self.client
        # db 0: create index and alias
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        # db 1: create a different index with the same alias name
        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
            assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME_2) == b"OK"
            info = client.execute_command("FT.INFO", ALIAS_NAME)
            info_list = list(info)
            idx = next(
                (i for i, v in enumerate(info_list) if v == b"index_name"), None
            )
            assert idx is not None, "FT.INFO response missing 'index_name' field"
            assert info_list[idx + 1] == INDEX_NAME_2.encode()
        finally:
            client.execute_command("SELECT", "0")


class TestFTAliasFlushDB(ValkeySearchTestCaseBase):
    """Tests that FLUSHDB removes aliases whose target index was dropped."""

    def test_flushdb_removes_alias_for_dropped_index(self):
        """After FLUSHDB, an alias pointing to a dropped index is no longer resolvable."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        client.execute_command("FLUSHDB")

        # Index is gone.
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", INDEX_NAME)
        # Alias is also gone (its target was dropped).
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)

    def test_flushdb_does_not_affect_other_db_aliases(self):
        """FLUSHDB on db 0 does not remove aliases in db 1."""
        client = self.client
        # Create index + alias in db 0.
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        # Create index + alias in db 1.
        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
            assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME_2) == b"OK"
        finally:
            client.execute_command("SELECT", "0")

        # Flush db 0 only.
        client.execute_command("FLUSHDB")

        # Alias in db 1 must still resolve.
        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command("FT.INFO", ALIAS_NAME) is not None
        finally:
            client.execute_command("SELECT", "0")
