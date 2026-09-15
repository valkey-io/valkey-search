import pytest
from .data_sets import load_data
from .generate import BaseCompatibilityTest

'''
Capture alias compatibility answers from Redisearch
'''


@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestAliasCompatibility(BaseCompatibilityTest):
    # Alias management & error cases

    ANSWER_FILE_NAME = "alias-answers.pickle.gz"

    @pytest.fixture(autouse=True)
    def _setup_alias_data(self, key_type):
        """Load alias dataset for the current key_type (runs after setup_method)."""
        load_data(self.client, "alias", key_type)
        self.data_set_name = "alias"
        self.key_type = key_type

    def test_aliasadd_search(self, key_type):
        """FT.SEARCH via alias returns same results as via index name."""
        self.execute_command(["FT.INFO", "alias_search"])
        self.execute_command(["FT.SEARCH", "alias_search", "@price:[0 +inf]"])

    def test_aliasadd_aggregate(self, key_type):
        """FT.AGGREGATE via alias returns same results as via index name."""
        self.execute_command(["FT.INFO", "alias_agg"])
        self.execute_command(["FT.AGGREGATE", "alias_agg", "@category:{electronics}",
            "LOAD", "1", "@category",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "count",
        ])

    def test_aliasadd_collides_with_existing_index(self, key_type):
        """FT.ALIASADD where alias name matches a different existing index."""
        if key_type == "hash":
            self.client.execute_command(
                "FT.CREATE", "second_idx", "ON", "HASH", "PREFIX", "1", "bdoc:",
                "SCHEMA", "val", "NUMERIC")
        else:
            self.client.execute_command(
                "FT.CREATE", "second_idx", "ON", "JSON", "PREFIX", "1", "bdoc:",
                "SCHEMA", "$.val", "AS", "val", "NUMERIC")
        self.execute_command(["FT.ALIASADD", "second_idx", f"{key_type}_idx1"])

    def test_aliasupdate_collides_with_existing_index(self, key_type):
        """FT.ALIASUPDATE where alias name matches a different existing index."""
        if key_type == "hash":
            self.client.execute_command(
                "FT.CREATE", "second_idx", "ON", "HASH", "PREFIX", "1", "bdoc:",
                "SCHEMA", "val", "NUMERIC")
        else:
            self.client.execute_command(
                "FT.CREATE", "second_idx", "ON", "JSON", "PREFIX", "1", "bdoc:",
                "SCHEMA", "$.val", "AS", "val", "NUMERIC")
        self.execute_command(["FT.ALIASUPDATE", "second_idx", f"{key_type}_idx1"])

    def test_aliasadd_duplicate(self, key_type):
        """FT.ALIASADD with an already-existing alias returns an error."""
        self.execute_command(["FT.ALIASADD", "alias_search", f"{key_type}_idx1"])

    def test_aliasadd_nonexistent_index(self, key_type):
        """FT.ALIASADD for a non-existent index returns an error."""
        self.execute_command(["FT.ALIASADD", "new_alias", "no_such_index"])

    def test_aliasadd_alias_to_alias(self, key_type):
        """FT.ALIASADD pointing to an existing alias returns an error."""
        self.execute_command(["FT.ALIASADD", "chain_alias", "alias_search"])

    def test_aliasdel_nonexistent(self, key_type):
        """FT.ALIASDEL on a non-existent alias returns an error."""
        self.execute_command(["FT.ALIASDEL", "no_such_alias"])

    def test_aliasupdate_nonexistent_index(self, key_type):
        """FT.ALIASUPDATE for a non-existent index returns an error."""
        self.execute_command(["FT.ALIASUPDATE", "new_alias", "no_such_index"])

    # Alias name restriction tests (divergence: we reject empty aliases)

    def test_aliasadd_null_byte_in_name(self, key_type):
        """FT.ALIASADD with embedded null byte in alias name."""
        self.execute_command(["FT.ALIASADD", "alias\x00bad", f"{key_type}_idx1"])

    def test_aliasupdate_null_byte_in_name(self, key_type):
        """FT.ALIASUPDATE with embedded null byte in alias name."""
        self.execute_command(["FT.ALIASUPDATE", "alias\x00bad", f"{key_type}_idx1"])

    def test_null_byte_alias_functional(self, key_type):
        """Create a null-byte alias and verify it works for INFO, SEARCH, and DEL."""
        alias_name = "null\x00alias"
        self.execute_command(["FT.ALIASADD", alias_name, f"{key_type}_idx1"])
        self.execute_command(["FT.INFO", alias_name])
        self.execute_command(["FT.SEARCH", alias_name, "@price:[0 +inf]"])
        self.execute_command(["FT.ALIASDEL", alias_name])

    def test_aliasadd_collides_with_index_name(self, key_type):
        """FT.ALIASADD where alias name equals an existing index name."""
        self.execute_command(["FT.ALIASADD", f"{key_type}_idx2", f"{key_type}_idx1"])

    def test_aliasupdate_collides_with_index_name(self, key_type):
        """FT.ALIASUPDATE where alias name equals an existing index name."""
        self.execute_command(["FT.ALIASUPDATE", f"{key_type}_idx2", f"{key_type}_idx1"])

    # --- FT.INFO and FT.DROPINDEX alias resolution tests ---

    def test_ft_info_on_alias(self, key_type):
        """FT.INFO on an alias — does it resolve to the underlying index?"""
        # Verify alias exists first
        self.execute_command(["FT.INFO", "alias_search"])
        # Compare with direct index access
        self.execute_command(["FT.INFO", f"{key_type}_idx1"])

    def test_ft_info_alias_same_name_as_other_index(self, key_type):
        """FT.INFO where alias name equals a *different* existing index name.

        Creates alias 'idx2' -> idx1, then FT.INFO 'idx2'. Does RediSearch
        return info for the alias target (idx1) or the index named idx2?
        """
        # Verify idx2 exists as an index before aliasing
        self.execute_command(["FT.INFO", f"{key_type}_idx2"])
        # Create an alias whose name matches the other existing index
        self.execute_command(
            ["FT.ALIASADD", f"{key_type}_idx2", f"{key_type}_idx1"])
        # Now query FT.INFO with the name that is both an alias and an index
        self.execute_command(["FT.INFO", f"{key_type}_idx2"])

    def test_ft_dropindex_on_alias(self, key_type):
        """FT.DROPINDEX on an alias — does it drop the alias or the index?"""
        # Verify alias and index exist before drop
        self.execute_command(["FT.INFO", "alias_search"])
        self.execute_command(["FT.INFO", f"{key_type}_idx1"])
        # Drop via alias
        self.execute_command(["FT.DROPINDEX", "alias_search"])
        # Check if the underlying index still exists after dropping via alias
        self.execute_command(["FT.INFO", f"{key_type}_idx1"])
        # Check if the alias still resolves
        self.execute_command(["FT.INFO", "alias_search"])

    def test_ft_dropindex_alias_same_name_as_other_index(self, key_type):
        """FT.DROPINDEX where alias name equals a different existing index.

        Creates alias 'idx2' -> idx1, then FT.DROPINDEX 'idx2'. Does
        RediSearch drop the alias target (idx1), the index named idx2, or
        the alias mapping itself?
        """
        # Verify both indexes exist before
        self.execute_command(["FT.INFO", f"{key_type}_idx1"])
        self.execute_command(["FT.INFO", f"{key_type}_idx2"])
        # Create alias with same name as idx2, pointing to idx1
        self.execute_command(
            ["FT.ALIASADD", f"{key_type}_idx2", f"{key_type}_idx1"])
        # Verify alias resolves (should show idx2 the index, since index wins)
        self.execute_command(["FT.INFO", f"{key_type}_idx2"])
        # Drop using the ambiguous name
        self.execute_command(["FT.DROPINDEX", f"{key_type}_idx2"])
        # Check what survived: is idx1 still alive?
        self.execute_command(["FT.INFO", f"{key_type}_idx1"])
        # Is idx2 accessible? (now should resolve via alias to idx1)
        self.execute_command(["FT.INFO", f"{key_type}_idx2"])
        # Does FT._LIST show what's left?
        self.execute_command(["FT._LIST"])


