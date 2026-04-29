#!/usr/bin/env python3

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFTExplainCli(ValkeySearchTestCaseBase):
    """Test cases for FT.EXPLAINCLI command"""

    def _create_text_index(self, client, index_name):
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT", "content", "TEXT"
        )

    def _create_mixed_index(self, client, index_name):
        """Create an index with TEXT, NUMERIC, and TAG fields."""
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "SCHEMA",
            "name", "TEXT",
            "description", "TEXT",
            "price", "NUMERIC",
            "rating", "NUMERIC",
            "category", "TAG",
            "status", "TAG",
        )

    @staticmethod
    def _parse_tree(result):
        """Decode the EXPLAINCLI result into a list of strings."""
        return [line.decode() if isinstance(line, bytes) else line
                for line in result]

    def test_ft_explaincli_basic(self):
        """Validate predicate tree output for various query types."""
        client = self.server.get_new_client()
        self._create_text_index(client, "testidx")

        # --- Single text term (default field) ---
        result = self._parse_tree(
            client.execute_command("FT.EXPLAINCLI", "testidx", "hello"))
        assert result == ['TEXT-TERM("hello", field=*)']

        # --- Multi-term query produces AND of TEXT-TERMs ---
        result = self._parse_tree(
            client.execute_command("FT.EXPLAINCLI", "testidx", "hello world"))
        assert result == [
            'AND{',
            '  TEXT-TERM("hello", field=*)',
            '  TEXT-TERM("world", field=*)',
            '}',
        ]

        # --- Field-specific term ---
        result = self._parse_tree(
            client.execute_command("FT.EXPLAINCLI", "testidx", "@title:hello"))
        assert result == ['TEXT-TERM("hello", field=title)']

        # --- Exact phrase produces AND with slop/inorder ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "testidx", '@title:"hello world"'))
        assert result == [
            'AND(slop=0, inorder=true){',
            '  TEXT-TERM("hello", field=title)',
            '  TEXT-TERM("world", field=title)',
            '}',
        ]

        # --- Prefix query ---
        result = self._parse_tree(
            client.execute_command("FT.EXPLAINCLI", "testidx", "@title:hel*"))
        assert result == ['TEXT-PREFIX("hel", field=title)']

        # --- Fuzzy query (distance 1) ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "testidx", "@title:%hello%"))
        assert result == ['TEXT-FUZZY("hello", distance=1, field=title)']

        # --- Fuzzy query (distance 2) ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "testidx", "@title:%%hello%%"))
        assert result == ['TEXT-FUZZY("hello", distance=2, field=title)']

        # --- Fuzzy query (distance 3) ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "testidx", "@title:%%%hello%%%"))
        assert result == ['TEXT-FUZZY("hello", distance=3, field=title)']

        # --- VERBATIM does not change tree shape for a single term ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "testidx", "hello", "VERBATIM"))
        assert result == ['TEXT-TERM("hello", field=*)']

        # --- INORDER + SLOP on multi-term query ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "testidx", "hello world",
                "INORDER", "SLOP", "2"))
        assert result == [
            'AND(slop=2, inorder=true){',
            '  TEXT-TERM("hello", field=*)',
            '  TEXT-TERM("world", field=*)',
            '}',
        ]

        # --- All options combined ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "testidx", "hello world",
                "VERBATIM", "INORDER", "SLOP", "3"))
        assert result == [
            'AND(slop=3, inorder=true){',
            '  TEXT-TERM("hello", field=*)',
            '  TEXT-TERM("world", field=*)',
            '}',
        ]

    def test_ft_explaincli_mixed_field_types(self):
        """Validate predicate trees with NUMERIC, TAG, and TEXT fields."""
        client = self.server.get_new_client()
        self._create_mixed_index(client, "mixedidx")

        # --- Single numeric range ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx", "@price:[10 100]"))
        assert result == ['NUMERIC(price)']

        # --- Single tag filter ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx", "@category:{books}"))
        assert result == ['TAG(category)']

        # --- Tag with multiple values (OR within tag) ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "@category:{books|electronics}"))
        assert result == ['TAG(category)']

        # --- Numeric AND tag (implicit AND) ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "@price:[10 100] @category:{books}"))
        assert result == [
            'AND{',
            '  NUMERIC(price)',
            '  TAG(category)',
            '}',
        ]

        # --- OR between numeric and tag ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "@price:[10 100] | @category:{books}"))
        assert result == [
            'OR{',
            '  NUMERIC(price)',
            '  TAG(category)',
            '}',
        ]

        # --- Negated numeric ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx", "-@price:[10 100]"))
        assert result == [
            'NOT{',
            '  NUMERIC(price)',
            '}',
        ]

        # --- Negated tag ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx", "-@category:{books}"))
        assert result == [
            'NOT{',
            '  TAG(category)',
            '}',
        ]

        # --- Mixed: text + numeric + tag with AND ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "@name:laptop @price:[500 2000] @category:{electronics}"))
        assert result == [
            'AND{',
            '  TEXT-TERM("laptop", field=name)',
            '  NUMERIC(price)',
            '  TAG(category)',
            '}',
        ]

        # --- Complex: OR with grouped AND clauses ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "(@price:[0 50] @category:{books}) | "
                "(@price:[100 500] @category:{electronics})"))
        assert result == [
            'OR{',
            '  AND{',
            '    NUMERIC(price)',
            '    TAG(category)',
            '  }',
            '  AND{',
            '    NUMERIC(price)',
            '    TAG(category)',
            '  }',
            '}',
        ]

        # --- Negated group ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "-(@price:[0 10] @category:{clearance})"))
        assert result == [
            'NOT{',
            '  AND{',
            '    NUMERIC(price)',
            '    TAG(category)',
            '  }',
            '}',
        ]

        # --- Two numeric ranges with AND ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "@price:[10 100] @rating:[4 5]"))
        assert result == [
            'AND{',
            '  NUMERIC(price)',
            '  NUMERIC(rating)',
            '}',
        ]

        # --- Numeric with inf ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx", "@price:[-inf +inf]"))
        assert result == ['NUMERIC(price)']

        # --- Two tags with AND ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "@category:{books} @status:{active}"))
        assert result == [
            'AND{',
            '  TAG(category)',
            '  TAG(status)',
            '}',
        ]

        # --- OR precedence: A B | C  =>  OR{ AND{A,B}, C } ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "@price:[10 50] @category:{books} | @status:{active}"))
        assert result == [
            'OR{',
            '  AND{',
            '    NUMERIC(price)',
            '    TAG(category)',
            '  }',
            '  TAG(status)',
            '}',
        ]

        # --- Double negation ---
        result = self._parse_tree(
            client.execute_command(
                "FT.EXPLAINCLI", "mixedidx",
                "-(-@price:[10 100])"))
        assert result == [
            'NOT{',
            '  NOT{',
            '    NUMERIC(price)',
            '  }',
            '}',
        ]

    def test_ft_explaincli_errors(self):
        """Test error cases: wrong args, bad args, nonexistent index, empty query"""
        client = self.server.get_new_client()
        self._create_text_index(client, "testidx")
        # Too few arguments
        with pytest.raises(ResponseError, match="Wrong number of arguments"):
            client.execute_command("FT.EXPLAINCLI", "testidx")
        # Unknown optional argument
        with pytest.raises(ResponseError):
            client.execute_command("FT.EXPLAINCLI", "testidx", "query", "BADARG")
        # Non-existent index
        with pytest.raises(ResponseError):
            client.execute_command("FT.EXPLAINCLI", "nonexistent", "hello")
        # Empty query
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "")
        assert isinstance(result, list)
