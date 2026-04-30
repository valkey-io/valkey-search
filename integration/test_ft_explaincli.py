#!/usr/bin/env python3

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def _decode(result):
    """Decode EXPLAINCLI byte-string result into a list of str."""
    return [line.decode() if isinstance(line, bytes) else line
            for line in result]

class TestFTExplainCli(ValkeySearchTestCaseBase):
    """Test cases for FT.EXPLAINCLI command."""

    def test_text_index_queries(self):
        """Validate predicate trees for a TEXT-only index."""
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "txtidx",
            "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT", "content", "TEXT",
        )
        # (label, query_string, extra_args, expected_tree_lines)
        cases = [
            ("single term",
             "hello", [],
             ['TEXT-TERM("hello", field=*)']),
            ("multi-term AND",
             "hello world", [],
             ['AND{',
              '  TEXT-TERM("hello", field=*)',
              '  TEXT-TERM("world", field=*)',
              '}']),
            ("field-specific term",
             "@title:hello", [],
             ['TEXT-TERM("hello", field=title)']),
            ("exact phrase",
             '@title:"hello world"', [],
             ['AND(slop=0, inorder=true){',
              '  TEXT-TERM("hello", field=title)',
              '  TEXT-TERM("world", field=title)',
              '}']),
            ("prefix",
             "@title:hel*", [],
             ['TEXT-PREFIX("hel", field=title)']),
            ("fuzzy distance=1",
             "@title:%hello%", [],
             ['TEXT-FUZZY("hello", distance=1, field=title)']),
            ("fuzzy distance=2",
             "@title:%%hello%%", [],
             ['TEXT-FUZZY("hello", distance=2, field=title)']),
            ("fuzzy distance=3",
             "@title:%%%hello%%%", [],
             ['TEXT-FUZZY("hello", distance=3, field=title)']),
            ("VERBATIM single term",
             "hello", ["VERBATIM"],
             ['TEXT-TERM("hello", field=*)']),
            ("INORDER + SLOP",
             "hello world", ["INORDER", "SLOP", "2"],
             ['AND(slop=2, inorder=true){',
              '  TEXT-TERM("hello", field=*)',
              '  TEXT-TERM("world", field=*)',
              '}']),
            ("VERBATIM + INORDER + SLOP",
             "hello world", ["VERBATIM", "INORDER", "SLOP", "3"],
             ['AND(slop=3, inorder=true){',
              '  TEXT-TERM("hello", field=*)',
              '  TEXT-TERM("world", field=*)',
              '}']),
            ("multi-field terms",
             "@title:hello @content:world", [],
             ['AND{',
              '  TEXT-TERM("hello", field=title)',
              '  TEXT-TERM("world", field=content)',
              '}']),
        ]
        for label, query, extra_args, expected in cases:
            result = _decode(
                client.execute_command("FT.EXPLAINCLI", "txtidx", query, *extra_args))
            assert result == expected, f"[{label}] query={query!r}\n  got: {result}"

    def test_mixed_index_queries(self):
        """Validate predicate trees with NUMERIC, TAG, and TEXT fields."""
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "mixidx",
            "ON", "HASH", "PREFIX", "1", "item:",
            "SCHEMA",
            "name", "TEXT", "description", "TEXT",
            "price", "NUMERIC", "rating", "NUMERIC",
            "category", "TAG", "status", "TAG",
        )
        # (label, query_string, expected_tree_lines)
        cases = [
            ("single numeric",
             "@price:[10 100]",
             ['NUMERIC(price)']),
            ("single tag",
             "@category:{books}",
             ['TAG(category)']),
            ("tag multi-value",
             "@category:{books|electronics}",
             ['TAG(category)']),
            ("numeric AND tag",
             "@price:[10 100] @category:{books}",
             ['AND{',
              '  NUMERIC(price)',
              '  TAG(category)',
              '}']),
            ("numeric OR tag",
             "@price:[10 100] | @category:{books}",
             ['OR{',
              '  NUMERIC(price)',
              '  TAG(category)',
              '}']),
            ("negated numeric",
             "-@price:[10 100]",
             ['NOT{',
              '  NUMERIC(price)',
              '}']),
            ("negated tag",
             "-@category:{books}",
             ['NOT{',
              '  TAG(category)',
              '}']),
            ("text + numeric + tag AND",
             "@name:laptop @price:[500 2000] @category:{electronics}",
             ['AND{',
              '  TEXT-TERM("laptop", field=name)',
              '  NUMERIC(price)',
              '  TAG(category)',
              '}']),
            ("grouped OR",
             "(@price:[0 50] @category:{books}) | "
             "(@price:[100 500] @category:{electronics})",
             ['OR{',
              '  AND{',
              '    NUMERIC(price)',
              '    TAG(category)',
              '  }',
              '  AND{',
              '    NUMERIC(price)',
              '    TAG(category)',
              '  }',
              '}']),
            ("negated group",
             "-(@price:[0 10] @category:{clearance})",
             ['NOT{',
              '  AND{',
              '    NUMERIC(price)',
              '    TAG(category)',
              '  }',
              '}']),
            ("two numerics",
             "@price:[10 100] @rating:[4 5]",
             ['AND{',
              '  NUMERIC(price)',
              '  NUMERIC(rating)',
              '}']),
            ("numeric with inf",
             "@price:[-inf +inf]",
             ['NUMERIC(price)']),
            ("two tags",
             "@category:{books} @status:{active}",
             ['AND{',
              '  TAG(category)',
              '  TAG(status)',
              '}']),
            ("OR precedence: A B | C",
             "@price:[10 50] @category:{books} | @status:{active}",
             ['OR{',
              '  AND{',
              '    NUMERIC(price)',
              '    TAG(category)',
              '  }',
              '  TAG(status)',
              '}']),
            ("double negation",
             "-(-@price:[10 100])",
             ['NOT{',
              '  NOT{',
              '    NUMERIC(price)',
              '  }',
              '}']),
            ("negated OR group",
             "-(@price:[0 50] | @category:{clearance})",
             ['NOT{',
              '  OR{',
              '    NUMERIC(price)',
              '    TAG(category)',
              '  }',
              '}']),
            ("mixed text OR chains",
             "@name:laptop @price:[100 500] | @category:{books} @status:{active}",
             ['OR{',
              '  AND{',
              '    TEXT-TERM("laptop", field=name)',
              '    NUMERIC(price)',
              '  }',
              '  AND{',
              '    TAG(category)',
              '    TAG(status)',
              '  }',
              '}']),
        ]

        for label, query, expected in cases:
            result = _decode(
                client.execute_command("FT.EXPLAINCLI", "mixidx", query))
            assert result == expected, f"[{label}] query={query!r}\n  got: {result}"

    def test_errors(self):
        """Error cases: wrong arg count, bad arg, nonexistent index, empty query."""
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "erridx",
            "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT",
        )
        with pytest.raises(ResponseError, match="Wrong number of arguments"):
            client.execute_command("FT.EXPLAINCLI", "erridx")
        with pytest.raises(ResponseError):
            client.execute_command("FT.EXPLAINCLI", "erridx", "query", "BADARG")
        with pytest.raises(ResponseError):
            client.execute_command("FT.EXPLAINCLI", "nonexistent", "hello")
        result = client.execute_command("FT.EXPLAINCLI", "erridx", "")
        assert isinstance(result, list)
