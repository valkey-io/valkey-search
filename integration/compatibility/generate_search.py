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
