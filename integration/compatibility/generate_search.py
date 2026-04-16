import pytest
from .data_sets import load_data
from .generate import BaseCompatibilityTest


@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestInfieldsCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "search-answers.pickle.gz"

    def test_infields_basic_field_scoping(self, key_type, dialect):
        """Basic field scoping: restrict to title, then to body."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "title", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "body", "DIALECT", str(dialect))

    def test_infields_invalid_field_ignored(self, key_type, dialect):
        """Non-existent and non-TEXT fields are silently ignored."""
        self.setup_data("pure text small", key_type)
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "nonexistent_field", "DIALECT", str(dialect))
        self.check("ft.search", f"{key_type}_idx1", "apple",
                   "INFIELDS", "1", "color", "DIALECT", str(dialect))

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
