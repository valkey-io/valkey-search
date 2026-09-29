import pytest, struct
from .generate import BaseCompatibilityTest
from .data_sets import VECTOR_DIM


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

    def test_infields_basic_field_scoping(self, key_type, dialect):
        """Basic field scoping: restrict to title, then to body."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "title", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "body", "DIALECT", str(dialect))

    def test_infields_with_limit(self, key_type, dialect):
        """INFIELDS combined with LIMIT — return all matches to avoid order ambiguity."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "title",
                   "LIMIT", "0", "10",
                   "DIALECT", str(dialect))

    def test_infields_zero_count_noop(self, key_type, dialect):
        """INFIELDS 0 is a no-op — searches all fields, matching reference."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "0",
                   "DIALECT", str(dialect))

    def test_infields_prefix_query(self, key_type, dialect):
        """INFIELDS with prefix query — verify output matches reference."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "app*",
                   "INFIELDS", "1", "title",
                   "DIALECT", str(dialect))

    def test_infields_multiple_fields(self, key_type, dialect):
        """INFIELDS with multiple fields — union of title and body."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "2", "title", "body",
                   "DIALECT", str(dialect))

    def test_infields_with_sortby(self, key_type, dialect):
        """INFIELDS combined with SORTBY."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "title",
                   "SORTBY", "title", "ASC",
                   "DIALECT", str(dialect))

    def test_infields_non_text_predicate_only(self, key_type, dialect):
        """INFIELDS with a pure non-text predicate (numeric/tag only, no text terms)."""
        self.setup_data("pure text small", key_type)
        # Pure numeric predicate with INFIELDS — verify behavior matches Redis Stack
        self.check("ft.search", f"{key_type}_idx1", "@price:[0 10]",
                   "INFIELDS", "1", "title",
                   "DIALECT", str(dialect))
        # Pure tag predicate with INFIELDS
        self.check("ft.search", f"{key_type}_idx1", "@color:{red}",
                   "INFIELDS", "1", "title",
                   "DIALECT", str(dialect))

    def test_infields_duplicate_fields(self, key_type, dialect):
        """Duplicate field names — verify dedup matches Redis Stack."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "3", "title", "title", "body",
                   "DIALECT", str(dialect))

    def test_infields_mixed_text_and_filter(self, key_type, dialect):
        """Mixed text term + non-text filter with INFIELDS scoping the text part."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple @color:{red}",
                   "INFIELDS", "1", "title",
                   "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "apple @price:[0 5]",
                   "INFIELDS", "1", "body",
                   "DIALECT", str(dialect))

    def test_infields_with_verbatim(self, key_type, dialect):
        """INFIELDS + VERBATIM — no stemming applied, matches Redis Stack."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "title", "VERBATIM",
                   "DIALECT", str(dialect))

    def test_infields_with_slop_inorder(self, key_type, dialect):
        """INFIELDS + SLOP + INORDER for multi-term queries."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple banana",
                   "INFIELDS", "1", "title",
                   "SLOP", "0", "INORDER",
                   "DIALECT", str(dialect))

    def test_infields_with_sortby_and_return(self, key_type, dialect):
        """INFIELDS + SORTBY + RETURN projection."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "title",
                   "SORTBY", "title", "ASC",
                   "RETURN", "1", "title",
                   "DIALECT", str(dialect))

    def test_infields_with_nocontent_and_sortby(self, key_type, dialect):
        """NOCONTENT + SORTBY — verify ordering matches Redis Stack.
        No LIMIT so all results are returned, avoids flakiness from ties
        at the page boundary."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "2", "title", "body",
                   "SORTBY", "price", "ASC",
                   "NOCONTENT",
                   "DIALECT", str(dialect))
