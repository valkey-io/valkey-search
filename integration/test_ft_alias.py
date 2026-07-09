"""Integration tests for FT.ALIASADD, FT.ALIASDEL, and FT.ALIASUPDATE."""

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from ft_info_parser import FTInfoParser
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


class TestFTAliasBasic(ValkeySearchTestCaseBase):
    """Minimal smoke tests for ALIASADD/DEL/UPDATE — detailed correctness is
    covered by compatibility tests (generate.py / TestAliasCompatibility)."""

    def test_aliasadd_and_resolve(self):
        """ALIASADD returns OK and FT.INFO resolves through the alias."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        info = list(client.execute_command("FT.INFO", ALIAS_NAME))
        idx = next((i for i, v in enumerate(info) if v == b"index_name"), None)
        assert idx is not None
        assert info[idx + 1] == INDEX_NAME.encode()

    def test_aliasdel_removes_alias(self):
        """ALIASDEL makes the alias unresolvable; index still accessible."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)
        assert client.execute_command("FT.INFO", INDEX_NAME) is not None

    def test_aliasupdate_reassigns(self):
        """ALIASUPDATE moves an alias to a different index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2) == b"OK"
        info = list(client.execute_command("FT.INFO", ALIAS_NAME))
        idx = next((i for i, v in enumerate(info) if v == b"index_name"), None)
        assert info[idx + 1] == INDEX_NAME_2.encode()

    def test_multiple_aliases_same_index(self):
        """Multiple aliases can point to the same index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_a", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_b", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.INFO", "alias_a") is not None
        assert client.execute_command("FT.INFO", "alias_b") is not None

    def test_aliasdel_then_readd(self):
        """After deleting an alias it can be re-added."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.INFO", ALIAS_NAME) is not None

    def test_aliasupdate_upsert(self):
        """ALIASUPDATE on a non-existent alias creates it (upsert semantics)."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.INFO", ALIAS_NAME) is not None

    def test_aliasupdate_alias_to_alias(self):
        """ALIASUPDATE that would create alias-to-alias returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASUPDATE", "alias2", ALIAS_NAME)
        assert "Unknown index name or name is an alias" in str(exc_info.value)

    def test_search_via_alias(self):
        """FT.SEARCH resolves alias and returns correct results."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        client.execute_command("HSET", "doc:1", "category", "books")
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        result = client.execute_command("FT.SEARCH", ALIAS_NAME, "@category:{books}")
        assert result[0] == 1
        assert result[1] == b"doc:1"


class TestFTAliasDropIndex(ValkeySearchTestCaseBase):
    """Tests for FT.DROPINDEX interactions with aliases (edge cases not in compatibility)."""

    def test_dropindex_via_alias_name(self):
        """FT.DROPINDEX called with an alias name resolves to the real index and drops it."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        assert client.execute_command("FT.DROPINDEX", ALIAS_NAME) == b"OK"

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

        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", "alias_idx1")
        assert client.execute_command("FT.INFO", "alias_idx2") is not None


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


def _get_aliases_from_info(client, name):
    """Extract the aliases list from FT.INFO response for the given name."""
    info = client.execute_command("FT.INFO", name)
    return FTInfoParser(info).aliases


class TestFTInfoAliases(ValkeySearchTestCaseBase):
    """Tests that FT.INFO reports aliases in the response."""

    def test_info_no_aliases(self):
        """FT.INFO on an index with no aliases returns an empty aliases list."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        aliases = _get_aliases_from_info(client, INDEX_NAME)
        assert aliases == []

    def test_info_multiple_aliases_sorted(self):
        """FT.INFO reports multiple aliases in sorted order."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "z_alias", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", "a_alias", INDEX_NAME) == b"OK"
        aliases = _get_aliases_from_info(client, INDEX_NAME)
        assert aliases == ["a_alias", "z_alias"]

    def test_info_alias_updated_after_aliasupdate(self):
        """After ALIASUPDATE moves an alias, FT.INFO reflects the change."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASUPDATE", ALIAS_NAME, INDEX_NAME_2) == b"OK"
        assert _get_aliases_from_info(client, INDEX_NAME) == []
        assert _get_aliases_from_info(client, INDEX_NAME_2) == [ALIAS_NAME]

    def test_info_via_alias_shows_aliases(self):
        """FT.INFO queried via alias still reports the aliases of the real index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        aliases = _get_aliases_from_info(client, ALIAS_NAME)
        assert aliases == [ALIAS_NAME]

    def test_info_alias_removed_after_aliasdel(self):
        """After ALIASDEL, FT.INFO no longer reports the deleted alias."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"
        aliases = _get_aliases_from_info(client, INDEX_NAME)
        assert aliases == []

class TestFTAliasWrongArity(ValkeySearchTestCaseBase):
    """Tests that alias commands reject wrong argument counts."""

    def test_aliasadd_too_few_args(self):
        """FT.ALIASADD with only one argument returns a wrong-arity error."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASADD", "only_alias")

    def test_aliasadd_too_many_args(self):
        """FT.ALIASADD with extra arguments returns a wrong-arity error."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASADD", "a", "b", "extra")

    def test_aliasdel_no_args(self):
        """FT.ALIASDEL with no arguments returns a wrong-arity error."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASDEL")

    def test_aliasdel_too_many_args(self):
        """FT.ALIASDEL with extra arguments returns a wrong-arity error."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASDEL", "a", "extra")

    def test_aliasupdate_too_few_args(self):
        """FT.ALIASUPDATE with only one argument returns a wrong-arity error."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASUPDATE", "only_alias")

    def test_aliasupdate_too_many_args(self):
        """FT.ALIASUPDATE with extra arguments returns a wrong-arity error."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASUPDATE", "a", "b", "extra")

class TestFTAliasSwapDB(ValkeySearchTestCaseBase):
    """Tests that SWAPDB moves aliases in lockstep with their indexes."""

    def test_swapdb_moves_alias_with_index(self):
        """After SWAPDB 0 1, the alias created in db 0 is accessible in db 1."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        client.execute_command("SWAPDB", "0", "1")

        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)

        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command("FT.INFO", ALIAS_NAME) is not None
        finally:
            client.execute_command("SELECT", "0")

    def test_swapdb_exchanges_aliases_between_dbs(self):
        """SWAPDB 0 1 swaps aliases from both databases."""
        client = self.client
        alias_a = "alias_in_db0"
        alias_b = "alias_in_db1"

        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", alias_a, INDEX_NAME) == b"OK"

        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
            assert client.execute_command("FT.ALIASADD", alias_b, INDEX_NAME_2) == b"OK"
        finally:
            client.execute_command("SELECT", "0")

        client.execute_command("SWAPDB", "0", "1")

        assert client.execute_command("FT.INFO", alias_b) is not None
        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", alias_a)

        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command("FT.INFO", alias_a) is not None
            with pytest.raises(ResponseError):
                client.execute_command("FT.INFO", alias_b)
        finally:
            client.execute_command("SELECT", "0")

    def test_search_via_alias_after_swapdb(self):
        """FT.SEARCH via alias works correctly after SWAPDB moves it."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        client.execute_command("HSET", "doc:1", "category", "books")
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        client.execute_command("SWAPDB", "0", "1")

        client.execute_command("SELECT", "1")
        try:
            result = client.execute_command(
                "FT.SEARCH", ALIAS_NAME, "@category:{books}"
            )
            assert result[0] == 1
        finally:
            client.execute_command("SELECT", "0")

    def test_swapdb_with_itself_is_noop(self):
        """SWAPDB 0 0 is a no-op; alias remains accessible."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        client.execute_command("SWAPDB", "0", "0")

        assert client.execute_command("FT.INFO", ALIAS_NAME) is not None

    def test_swapdb_one_db_has_no_aliases(self):
        """SWAPDB where only one DB has aliases moves them to the other DB."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        finally:
            client.execute_command("SELECT", "0")

        client.execute_command("SWAPDB", "0", "1")

        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)

        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command("FT.INFO", ALIAS_NAME) is not None
        finally:
            client.execute_command("SELECT", "0")

import os
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from util import waiters
from utils import IndexingTestHelper

def _restart_and_wait(test_case, index_name):
    """Save, restart the server, and wait until the index is ready."""
    test_case.client.execute_command("save")
    old_skiplogclean = os.environ.get("SKIPLOGCLEAN")
    try:
        os.environ["SKIPLOGCLEAN"] = "1"
        test_case.server.restart(remove_rdb=False)
        test_case.client.ping()
        # Wait until the index has finished loading/backfilling before querying.
        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(
                test_case.client, index_name),
            timeout=30,
        )
    finally:
        if old_skiplogclean is None:
            os.environ.pop("SKIPLOGCLEAN", None)
        else:
            os.environ["SKIPLOGCLEAN"] = old_skiplogclean


class TestFTAliasRDBPersistence(ValkeySearchTestCaseDebugMode):
    """Tests that aliases survive a SAVE + server restart cycle."""

    def test_alias_survives_save_restore(self):
        """An alias created before SAVE is still resolvable after restart."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        _restart_and_wait(self, INDEX_NAME)

        assert client.execute_command("FT.INFO", ALIAS_NAME) is not None
        assert client.execute_command("FT.INFO", INDEX_NAME) is not None

    def test_multiple_aliases_survive_save_restore(self):
        """Multiple aliases pointing to the same index all survive restart."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_x", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", "alias_y", INDEX_NAME) == b"OK"

        _restart_and_wait(self, INDEX_NAME)

        assert client.execute_command("FT.INFO", "alias_x") is not None
        assert client.execute_command("FT.INFO", "alias_y") is not None

    def test_deleted_alias_not_restored(self):
        """An alias deleted before SAVE does not reappear after restart."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"

        _restart_and_wait(self, INDEX_NAME)

        with pytest.raises(ResponseError):
            client.execute_command("FT.INFO", ALIAS_NAME)
        # Underlying index still present
        assert client.execute_command("FT.INFO", INDEX_NAME) is not None

    def test_alias_search_works_after_restore(self):
        """FT.SEARCH via alias returns correct results after save/restore."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        client.execute_command("HSET", "doc:1", "category", "books")
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        _restart_and_wait(self, INDEX_NAME)

        result = client.execute_command("FT.SEARCH", ALIAS_NAME, "@category:{books}")
        assert result[0] == 1

class TestFTAliasNameCollision(ValkeySearchTestCaseBase):
    """Tests behaviour when an alias name matches an existing index name."""

    def test_aliasadd_name_same_as_existing_index_allowed(self):
        """
        ALIASADD allows an alias whose name matches an existing real index.
        The real index takes precedence during name resolution.
        """
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        assert client.execute_command("FT.ALIASADD", INDEX_NAME, INDEX_NAME_2) == b"OK"
        # Real index still resolves directly via its own name.
        info = list(client.execute_command("FT.INFO", INDEX_NAME))
        idx = next((i for i, v in enumerate(info) if v == b"index_name"), None)
        assert idx is not None
        assert info[idx + 1] == INDEX_NAME.encode()

    def test_aliasupdate_name_same_as_existing_index_allowed(self):
        """
        ALIASUPDATE allows an alias name that matches a real index.
        """
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
        assert client.execute_command("FT.ALIASUPDATE", INDEX_NAME, INDEX_NAME_2) == b"OK"
        # Real index still resolves directly.
        info = list(client.execute_command("FT.INFO", INDEX_NAME))
        idx = next((i for i, v in enumerate(info) if v == b"index_name"), None)
        assert idx is not None
        assert info[idx + 1] == INDEX_NAME.encode()

class TestFTAliasDelEdgeCases(ValkeySearchTestCaseBase):
    """Edge cases for FT.ALIASDEL."""

    def test_aliasdel_on_index_name_returns_error(self):
        """FT.ALIASDEL called with a real index name (not an alias) returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASDEL", INDEX_NAME)
        assert "Alias does not exist" in str(exc_info.value)

    def test_aliasdel_after_drop_index_returns_error(self):
        """FT.ALIASDEL on an alias whose index was already dropped returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        # Drop the index — this should also purge the alias.
        assert client.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"
        # Now trying to delete the (already-purged) alias must fail.
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASDEL", ALIAS_NAME)
        assert "Alias does not exist" in str(exc_info.value)

class TestFTListDoesNotExposeAliases(ValkeySearchTestCaseBase):
    """FT._LIST must return only real index names, never alias names."""

    def test_alias_not_in_ft_list(self):
        """FT._LIST does not include alias names."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        listed = client.execute_command("FT._LIST")
        assert INDEX_NAME.encode() in listed
        assert ALIAS_NAME.encode() not in listed

    def test_ft_list_unchanged_after_aliasdel(self):
        """FT._LIST is unchanged after an alias is deleted."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", ALIAS_NAME) == b"OK"

        listed = client.execute_command("FT._LIST")
        assert INDEX_NAME.encode() in listed
        assert ALIAS_NAME.encode() not in listed

class TestFTAliasDropRecreateIndex(ValkeySearchTestCaseBase):
    """Tests alias behaviour across drop-and-recreate of the target index."""

    def test_alias_readd_after_drop_and_recreate_index(self):
        """
        After dropping an index (which purges its alias) and recreating the
        index with the same name, the alias can be re-added successfully.
        """
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"

        assert client.execute_command("FT.DROPINDEX", INDEX_NAME) == b"OK"

        # Recreate the index with the same name.
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"

        # Re-adding the alias to the new index must succeed.
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.INFO", ALIAS_NAME) is not None

class TestFTAliasNameBoundaries(ValkeySearchTestCaseBase):
    """Boundary tests for alias name values."""

    def test_aliasadd_empty_alias_name_rejected(self):
        """FT.ALIASADD with an empty alias name returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASADD", "", INDEX_NAME)
        assert "Alias name cannot be empty" in str(exc_info.value)

    def test_aliasupdate_empty_alias_name_rejected(self):
        """FT.ALIASUPDATE with an empty alias name returns an error."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASUPDATE", "", INDEX_NAME)
        assert "Alias name cannot be empty" in str(exc_info.value)

    def test_aliasdel_empty_alias_name_rejected(self):
        """FT.ALIASDEL with an empty alias name returns an error."""
        client = self.client
        with pytest.raises(ResponseError) as exc_info:
            client.execute_command("FT.ALIASDEL", "")
        assert "Alias name cannot be empty" in str(exc_info.value)

    def test_aliasadd_empty_index_name(self):
        """FT.ALIASADD with an empty index name is rejected."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASADD", ALIAS_NAME, "")

    def test_aliasadd_very_long_alias_name(self):
        """FT.ALIASADD with a 512-character alias name succeeds."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        long_alias = "a" * 512
        assert client.execute_command("FT.ALIASADD", long_alias, INDEX_NAME) == b"OK"
        assert client.execute_command("FT.INFO", long_alias) is not None
        assert client.execute_command("FT.ALIASDEL", long_alias) == b"OK"


class TestFTAliasList(ValkeySearchTestCaseBase):
    """Tests for FT.ALIASLIST edge cases not covered by compatibility tests."""

    def test_aliaslist_basic(self):
        """FT.ALIASLIST returns the alias and its target index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        result = client.execute_command("FT.ALIASLIST")
        assert result == [ALIAS_NAME.encode(), INDEX_NAME.encode()]

    def test_aliaslist_empty_no_indexes(self):
        """FT.ALIASLIST returns an empty list when no indexes exist."""
        client = self.client
        result = client.execute_command("FT.ALIASLIST")
        assert result == []

    def test_aliaslist_wrong_arity(self):
        """FT.ALIASLIST with extra arguments returns a wrong-arity error."""
        client = self.client
        with pytest.raises(ResponseError):
            client.execute_command("FT.ALIASLIST", "extra_arg")

    def test_aliaslist_per_db_isolation(self):
        """FT.ALIASLIST only shows aliases for the current database."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "db0_alias", INDEX_NAME) == b"OK"

        client.execute_command("SELECT", "1")
        try:
            assert client.execute_command(*CREATE_TAG_INDEX_2) == b"OK"
            assert client.execute_command("FT.ALIASADD", "db1_alias", INDEX_NAME_2) == b"OK"
            result = client.execute_command("FT.ALIASLIST")
            assert result == [b"db1_alias", INDEX_NAME_2.encode()]
        finally:
            client.execute_command("SELECT", "0")

        result = client.execute_command("FT.ALIASLIST")
        assert result == [b"db0_alias", INDEX_NAME.encode()]

    def test_aliaslist_empty_after_flushdb(self):
        """FT.ALIASLIST is empty after FLUSHDB removes the index."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", ALIAS_NAME, INDEX_NAME) == b"OK"
        client.execute_command("FLUSHDB")
        result = client.execute_command("FT.ALIASLIST")
        assert result == []

    def test_aliaslist_after_swapdb(self):
        """FT.ALIASLIST reflects alias movement after SWAPDB."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "swap_alias", INDEX_NAME) == b"OK"

        client.execute_command("SWAPDB", "0", "1")

        result = client.execute_command("FT.ALIASLIST")
        assert result == []

        client.execute_command("SELECT", "1")
        try:
            result = client.execute_command("FT.ALIASLIST")
            assert result == [b"swap_alias", INDEX_NAME.encode()]
        finally:
            client.execute_command("SELECT", "0")


class TestFTAliasListRDBPersistence(ValkeySearchTestCaseDebugMode):
    """Tests that FT.ALIASLIST survives a SAVE + server restart cycle."""

    def test_aliaslist_survives_save_restore(self):
        """Aliases shown by FT.ALIASLIST persist after save/restore."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "persist_a", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASADD", "persist_b", INDEX_NAME) == b"OK"

        _restart_and_wait(self, INDEX_NAME)

        result = client.execute_command("FT.ALIASLIST")
        assert result == [
            b"persist_a", INDEX_NAME.encode(),
            b"persist_b", INDEX_NAME.encode(),
        ]

    def test_aliaslist_deleted_alias_not_restored(self):
        """A deleted alias does not reappear in FT.ALIASLIST after restart."""
        client = self.client
        assert client.execute_command(*CREATE_TAG_INDEX) == b"OK"
        assert client.execute_command("FT.ALIASADD", "temp_alias", INDEX_NAME) == b"OK"
        assert client.execute_command("FT.ALIASDEL", "temp_alias") == b"OK"

        _restart_and_wait(self, INDEX_NAME)

        result = client.execute_command("FT.ALIASLIST")
        assert result == []
