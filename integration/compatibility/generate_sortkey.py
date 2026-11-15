# Never use time.sleep() to wait for indexing: writes are searchable
# immediately (see README).
import pytest

from .data_sets import (SORTKEY_ALIAS_DATA_SET, SORTKEY_COLLISION_DATA_SET,
                        SORTKEY_NIL_DATA_SET, SORTKEY_PREFIX_DATA_SET)
from .generate import BaseCompatibilityTest

'''
Capture RediSearch answers for the WITHSORTKEYS sort-key prefix rule
(issue #1353 item 4): '#' for NUMERIC fields, '$' otherwise, on both the
filter and KNN query paths.
'''

HASH = pytest.mark.parametrize("key_type", ["hash"])
HASH_AND_JSON = pytest.mark.parametrize("key_type", ["hash", "json"])


class TestSortKeyPrefixCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "sortkey-answers.pickle.gz"

    # All field types valkey-search accepts in FT.CREATE (GEO is rejected).
    SORTABLE_FIELDS = ("z", "t", "n", "f", "vec")

    @HASH
    def test_withsortkeys_prefix_by_field_type(self, key_type):
        self.setup_data(SORTKEY_PREFIX_DATA_SET, key_type)
        for field in self.SORTABLE_FIELDS:
            self.check("FT.SEARCH", f"{key_type}_idx1", "@m:{all}",
                       "SORTBY", field, "ASC", "WITHSORTKEYS",
                       "RETURN", "1", field, "DIALECT", "2")

    @HASH
    def test_knn_withsortkeys_prefix(self, key_type):
        self.setup_data(SORTKEY_PREFIX_DATA_SET, key_type)
        for field in ("z", "n"):
            self.check("FT.SEARCH", f"{key_type}_idx1",
                       "@m:{all}=>[KNN 3 @vec $B]",
                       "PARAMS", "2", "B", b"AAAAAAAA",
                       "SORTBY", field, "ASC", "WITHSORTKEYS",
                       "RETURN", "1", field, "DIALECT", "2")

    @HASH
    def test_knn_distance_alias_prefix_only(self, key_type):
        # special case when sorting by knn distance instead of regular schema field
        # Notice: result sort key must be 0 to avoid number formatting noise.
        self.setup_data(SORTKEY_PREFIX_DATA_SET, key_type)
        self.check("FT.SEARCH", f"{key_type}_idx1",
                   "@m:{all}=>[KNN 1 @vec $B AS dist]",
                   "PARAMS", "2", "B", b"AAAAAAAA", # identical vector in SORTKEY_PREFIX_DATA_SET
                   "SORTBY", "dist", "ASC", "WITHSORTKEYS",
                   "RETURN", "1", "dist", "DIALECT", "2")

    @HASH
    def test_withsortkeys_absent_is_nil(self, key_type):
        # Absent sort key (issue #1353 item 5): missing SORTBY field, and no SORTBY.
        self.setup_data(SORTKEY_NIL_DATA_SET, key_type)
        self.check("FT.SEARCH", f"{key_type}_idx1", "@m:{all}",
                   "SORTBY", "p", "ASC", "WITHSORTKEYS",
                   "RETURN", "1", "m", "DIALECT", "2")
        self.check("FT.SEARCH", f"{key_type}_idx1", "@m:{solo}",
                   "WITHSORTKEYS", "RETURN", "1", "m", "DIALECT", "2")

    @HASH
    def test_knn_withsortkeys_absent_is_nil(self, key_type):
        # KNN variants of the absent-sort-key cases.
        self.setup_data(SORTKEY_NIL_DATA_SET, key_type)
        self.check("FT.SEARCH", f"{key_type}_idx1",
                   "@m:{all}=>[KNN 3 @vec $B]",
                   "PARAMS", "2", "B", b"AAAAAAAA",
                   "SORTBY", "p", "ASC", "WITHSORTKEYS",
                   "RETURN", "1", "p", "DIALECT", "2")
        self.check("FT.SEARCH", f"{key_type}_idx1",
                   "@m:{solo}=>[KNN 1 @vec $B]",
                   "PARAMS", "2", "B", b"AAAAAAAA",
                   "WITHSORTKEYS", "RETURN", "1", "m", "DIALECT", "2")

    @HASH_AND_JSON
    def test_sortby_alias(self, key_type):
        # issue #1440: the sort attribute's alias differs from its field.
        self.setup_data(SORTKEY_ALIAS_DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        knn = ["*=>[KNN 3 @vec $B]", "PARAMS", "2", "B", b"AAAAAAAA"]
        for query in (["@s:[0 100]"], ["*"], knn):
            self.check("FT.SEARCH", idx, *query, "SORTBY", "s", "DESC",
                       "NOCONTENT", "DIALECT", "2")
            self.check("FT.SEARCH", idx, *query, "SORTBY", "s", "DESC",
                       "WITHSORTKEYS", "RETURN", "1", "s", "DIALECT", "2")
            self.check("FT.SEARCH", idx, *query, "SORTBY", "s", "DESC",
                       "WITHSORTKEYS", "RETURN", "1", "n", "DIALECT", "2")

    @HASH
    def test_sortby_alias_collides_with_field(self, key_type):
        # issue #1440: alias b (field a) is also the field behind attribute c.
        self.setup_data(SORTKEY_COLLISION_DATA_SET, key_type)
        self.check("FT.SEARCH", f"{key_type}_idx1", "@b:[0 10]",
                   "SORTBY", "b", "ASC", "WITHSORTKEYS",
                   "RETURN", "1", "c", "DIALECT", "2")
        self.check("FT.SEARCH", f"{key_type}_idx1", "*",
                   "SORTBY", "b", "DESC", "WITHSORTKEYS",
                   "RETURN", "2", "b", "c", "DIALECT", "2")
