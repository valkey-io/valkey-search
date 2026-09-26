import pytest, struct
from .generate import BaseCompatibilityTest
from .data_sets import HYBRID_VECTOR_DIM, VECTOR_DIM

# Text relevance and hybrid KNN need the `hybrid text` corpus (TEXT fields plus
# a vector ramp; see data_sets.py). Its keys are <key_type>:00 .. :23; `alpha`
# is in the title of every document whose index is not a multiple of 4, and the
# L2 distance from HYBRID_NEAR grows with the index.
HYBRID_NEAR = [1.0, 0.0, 0.0, 0.0]
HYBRID_FAR = [9.0, 15.0, 0.0, 0.0]


@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestSearchCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "search-answers.pickle.gz"

    def check_knn(self, index, knn, in_keys, dialect, query_vector=None):
        """Execute a KNN vector query with INKEYS restriction."""
        if query_vector is None:
            query_vector = [0.0] * VECTOR_DIM
        blob = struct.pack(f"<{VECTOR_DIM}f", *query_vector)
        self.check("ft.search", index, f"*=>[KNN {knn} @v1 $BLOB]",
                   "INKEYS", str(len(in_keys)), *in_keys,
                   "PARAMS", "2", "BLOB", blob, "DIALECT", str(dialect))

    def test_inkeys_basic(self, key_type, dialect):
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "3", keys[0], keys[1], keys[2], "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "1", keys[5], "DIALECT", str(dialect))

    def test_inkeys_nonexistent(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "2", "nonexistent:99", "nonexistent:100", "DIALECT", str(dialect))

    def test_inkeys_mixed(self, key_type, dialect):
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "3", keys[0], "nonexistent:99", keys[1], "DIALECT", str(dialect))

    def test_inkeys_with_limit(self, key_type, dialect):
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "5", keys[0], keys[1], keys[2], keys[3], keys[4], "SORTBY", "n1", "ASC", "LIMIT", "0", "3", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "5", keys[0], keys[1], keys[2], keys[3], keys[4], "SORTBY", "n1", "ASC", "LIMIT", "2", "2", "DIALECT", str(dialect))

    def test_inkeys_with_sortby(self, key_type, dialect):
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        for sort_key in ["n1", "n2"]:
            for direction in ["ASC", "DESC"]:
                self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "5", keys[0], keys[1], keys[2], keys[3], keys[4], "SORTBY", sort_key, direction, "DIALECT", str(dialect))

    def test_inkeys_with_return(self, key_type, dialect):
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "3", keys[0], keys[1], keys[2], "RETURN", "2", "n1", "t1", "DIALECT", str(dialect))

    def test_inkeys_with_filter(self, key_type, dialect):
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        self.check("ft.search", f"{key_type}_idx1", "@n1:[0 5]", "INKEYS", "4", keys[0], keys[1], keys[2], keys[3], "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@t3:{all_the_same_value}", "INKEYS", "3", keys[0], keys[1], keys[2], "DIALECT", str(dialect))

    def test_inkeys_error_zero_count(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "0", "DIALECT", str(dialect))

    def test_inkeys_knn_underreturn(self, key_type, dialect):
        # Vectors track n1 over range(-5, 10); querying near [0,0,0] makes the
        # last keys the farthest, so far_keys fall outside a small global top-K.
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        near_keys = keys[:3]
        far_keys = keys[-3:]

        self.check_knn(f"{key_type}_idx1", 3, far_keys, dialect)
        self.check_knn(f"{key_type}_idx1", len(keys), far_keys, dialect)
        self.check_knn(f"{key_type}_idx1", 3, near_keys, dialect)

    def test_inkeys_nocontent(self, key_type, dialect):
        # NOCONTENT skips content loading for an ordinary search; INKEYS must
        # still apply the predicate. keys[:5] hold n1 -5..-1, so @n1:[-3 5]
        # keeps only some of them.
        keys = [entry[0] for entry in self.setup_data("sortable numbers", key_type)]
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-3 5]", "INKEYS", "6", *keys[:5], "nonexistent:99", "NOCONTENT", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-3 5]", "INKEYS", "6", *keys[:5], "nonexistent:99", "NOCONTENT", "SORTBY", "n1", "DESC", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-3 5]", "INKEYS", "6", *keys[:5], "nonexistent:99", "NOCONTENT", "LIMIT", "0", "2", "SORTBY", "n1", "ASC", "DIALECT", str(dialect))


    def inkeys(self, key_type, *ids):
        keys = [f"{key_type}:{i:02d}" for i in ids]
        return ["INKEYS", str(len(keys)), *keys]

    def check_standalone(self, *cmd):
        """Text scores are computed from shard-local corpus statistics in a
        cluster, so they match the reference only on a standalone server
        (unsupported_tests.md 5.9)."""
        self.check(*cmd)
        self.answers[-1]["cluster_excluded"] = True

    def test_inkeys_withscores(self, key_type, dialect):
        self.setup_data("hybrid text", key_type)
        # 04 and 08 lack `alpha`; the rest have it with differing frequency
        # and document length, so their BM25 scores differ.
        inkeys = self.inkeys(key_type, 1, 2, 3, 4, 8, 13, 22)
        for query in ["@title:alpha", "@title:beta", "@title:alpha @body:stone"]:
            self.check_standalone("ft.search", f"{key_type}_idx1", query, *inkeys, "WITHSCORES", "SCORER", "BM25STD", "DIALECT", str(dialect))
        self.check_standalone("ft.search", f"{key_type}_idx1", "@title:alpha", *inkeys, "WITHSCORES", "NOCONTENT", "SCORER", "BM25STD", "DIALECT", str(dialect))
        self.check_standalone("ft.search", f"{key_type}_idx1", "@title:alpha", *inkeys, "WITHSCORES", "SCORER", "BM25STD", "LIMIT", "0", "2", "DIALECT", str(dialect))

    def test_inkeys_text_knn(self, key_type, dialect):
        self.setup_data("hybrid text", key_type)
        # Near and far `alpha` documents plus 04/12, which lack `alpha`, and
        # 00, which is nearest to HYBRID_NEAR overall but also lacks it.
        inkeys = self.inkeys(key_type, 0, 1, 2, 4, 12, 13, 14, 21)
        for vector in [HYBRID_NEAR, HYBRID_FAR]:
            blob = struct.pack(f"<{HYBRID_VECTOR_DIM}f", *vector)
            for knn in ["3", "10"]:
                self.check("ft.search", f"{key_type}_idx1", f"@title:alpha=>[KNN {knn} @vec $q]", *inkeys, "PARAMS", "2", "q", blob, "DIALECT", str(dialect))
            self.check("ft.search", f"{key_type}_idx1", "@title:alpha=>[KNN 3 @vec $q AS dist]", *inkeys, "SORTBY", "dist", "RETURN", "1", "dist", "PARAMS", "2", "q", blob, "DIALECT", str(dialect))
            self.check("ft.search", f"{key_type}_idx1", "@title:alpha=>[KNN 3 @vec $q]", *inkeys, "NOCONTENT", "PARAMS", "2", "q", blob, "DIALECT", str(dialect))
