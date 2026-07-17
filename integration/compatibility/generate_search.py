import pytest
from .generate import BaseCompatibilityTest


@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestSearchCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "search-answers.pickle.gz"

    def test_inkeys_basic(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "3", f"{key_type}:00", f"{key_type}:01", f"{key_type}:02", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "1", f"{key_type}:05", "DIALECT", str(dialect))

    def test_inkeys_nonexistent(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "2", "nonexistent:99", "nonexistent:100", "DIALECT", str(dialect))

    def test_inkeys_mixed(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "3", f"{key_type}:00", "nonexistent:99", f"{key_type}:01", "DIALECT", str(dialect))

    def test_inkeys_with_limit(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "5", f"{key_type}:00", f"{key_type}:01", f"{key_type}:02", f"{key_type}:03", f"{key_type}:04", "SORTBY", "n1", "ASC", "LIMIT", "0", "3", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "5", f"{key_type}:00", f"{key_type}:01", f"{key_type}:02", f"{key_type}:03", f"{key_type}:04", "SORTBY", "n1", "ASC", "LIMIT", "2", "2", "DIALECT", str(dialect))

    def test_inkeys_with_sortby(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        for sort_key in ["n1", "n2"]:
            for direction in ["ASC", "DESC"]:
                self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "5", f"{key_type}:00", f"{key_type}:01", f"{key_type}:02", f"{key_type}:03", f"{key_type}:04", "SORTBY", sort_key, direction, "DIALECT", str(dialect))

    def test_inkeys_with_return(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "3", f"{key_type}:00", f"{key_type}:01", f"{key_type}:02", "RETURN", "2", "n1", "t1", "DIALECT", str(dialect))

    def test_inkeys_with_filter(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[0 5]", "INKEYS", "4", f"{key_type}:00", f"{key_type}:01", f"{key_type}:02", f"{key_type}:03", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "@t3:{all_the_same_value}", "INKEYS", "3", f"{key_type}:00", f"{key_type}:01", f"{key_type}:02", "DIALECT", str(dialect))

    def test_inkeys_error_zero_count(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check("ft.search", f"{key_type}_idx1", "@n1:[-inf inf]", "INKEYS", "0", "DIALECT", str(dialect))
