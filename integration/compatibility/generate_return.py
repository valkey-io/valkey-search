# Never use time.sleep() to wait for indexing: writes are searchable
# immediately (see README).
import pytest

from .data_sets import RETURN_CLAUSE_DATA_SET
from .generate import BaseCompatibilityTest

'''
Capture RediSearch answers for repeated RETURN clauses (issue #1353 item 7):
the last RETURN clause wins, including a later clause overriding an earlier
`RETURN 0`, while the NOCONTENT keyword stays sticky. Also for repeated
output names within one clause (issue #1521).
'''


@pytest.mark.parametrize("key_type", ["hash"])
class TestReturnClauseCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "return-answers.pickle.gz"

    def test_repeated_return_clauses(self, key_type):
        self.setup_data(RETURN_CLAUSE_DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        for tail in (
            # A later RETURN overrides an earlier RETURN 0 (the item-7 case).
            ("RETURN", "0", "RETURN", "1", "title"),
            # Last-one-wins in the other direction: a final RETURN 0 drops fields.
            ("RETURN", "1", "title", "RETURN", "0"),
            # A later clause replaces (not extends) an earlier field list.
            ("RETURN", "1", "title", "RETURN", "1", "p"),
            # NOCONTENT is sticky: no later RETURN cancels it.
            ("NOCONTENT", "RETURN", "1", "title"),
            ("RETURN", "0", "NOCONTENT", "RETURN", "1", "title"),
            # A lone RETURN 0 behaves like NOCONTENT.
            ("RETURN", "0"),
            # Within one clause each output name is returned once, first wins
            # (issue #1521). Two names for one field stay two entries.
            ("RETURN", "3", "p", "title", "p"),
            ("RETURN", "4", "p", "AS", "title", "title"),
            ("RETURN", "6", "title", "AS", "a", "title", "AS", "b"),
            ("RETURN", "6", "title", "AS", "a", "p", "AS", "a"),
        ):
            self.check("FT.SEARCH", idx, "@m:{all}", *tail, "DIALECT", "2")
