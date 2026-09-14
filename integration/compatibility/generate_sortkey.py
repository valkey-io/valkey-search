import time

import pytest

from .data_sets import SORTKEY_PREFIX_DATA_SET
from .generate import BaseCompatibilityTest

'''
Capture RediSearch answers for the WITHSORTKEYS sort-key prefix rule
(issue #1353 item 4): '#' for NUMERIC fields, '$' otherwise, on both the
filter and KNN query paths.
'''


@pytest.mark.parametrize("key_type", ["hash"])
class TestSortKeyPrefixCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "sortkey-answers.pickle.gz"

    # All field types valkey-search accepts in FT.CREATE (GEO is rejected).
    SORTABLE_FIELDS = ("z", "t", "n", "f", "vec")

    def test_withsortkeys_prefix_by_field_type(self, key_type):
        self.setup_data(SORTKEY_PREFIX_DATA_SET, key_type)
        for field in self.SORTABLE_FIELDS:
            self.check("FT.SEARCH", f"{key_type}_idx1", "@m:{all}",
                       "SORTBY", field, "ASC", "WITHSORTKEYS",
                       "RETURN", "1", field, "DIALECT", "2")

    def test_knn_withsortkeys_prefix(self, key_type):
        self.setup_data(SORTKEY_PREFIX_DATA_SET, key_type)
        time.sleep(0.5)
        for field in ("z", "n"):
            self.check("FT.SEARCH", f"{key_type}_idx1",
                       "@m:{all}=>[KNN 3 @vec $B]",
                       "PARAMS", "2", "B", b"AAAAAAAA",
                       "SORTBY", field, "ASC", "WITHSORTKEYS",
                       "RETURN", "1", field, "DIALECT", "2")

    def test_knn_distance_alias_prefix_only(self, key_type):
        # special case when sorting by knn distance instead of regular schema field
        # Notice: result sort key must be 0 to avoid number formatting noise.
        self.setup_data(SORTKEY_PREFIX_DATA_SET, key_type)
        time.sleep(0.5)
        self.check("FT.SEARCH", f"{key_type}_idx1",
                   "@m:{all}=>[KNN 1 @vec $B AS dist]",
                   "PARAMS", "2", "B", b"AAAAAAAA", # identical vector in SORTKEY_PREFIX_DATA_SET
                   "SORTBY", "dist", "ASC", "WITHSORTKEYS",
                   "RETURN", "1", "dist", "DIALECT", "2")
