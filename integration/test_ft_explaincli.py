#!/usr/bin/env python3

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFTExplainCli(ValkeySearchTestCaseBase):
    """Test cases for FT.EXPLAINCLI command"""

    def _create_index(self, client, index_name):
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT", "content", "TEXT"
        )

    def test_ft_explaincli_basic(self):
        """Test basic queries, field-specific queries, and optional args"""
        client = self.server.get_new_client()
        self._create_index(client, "testidx")
        # Simple query
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "hello")
        assert isinstance(result, list) and len(result) > 0
        assert any(b"TEXT-TERM" in line for line in result)
        # Multi-term query
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "hello world")
        assert isinstance(result, list) and len(result) > 0
        # Field-specific query
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "@title:hello")
        assert isinstance(result, list) and len(result) > 0
        assert any(b"TEXT-TERM" in line for line in result)
        # VERBATIM
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "hello", "VERBATIM")
        assert isinstance(result, list) and len(result) > 0
        # INORDER
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "hello world", "INORDER")
        assert isinstance(result, list) and len(result) > 0
        # SLOP
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "hello world", "SLOP", "2")
        assert isinstance(result, list) and len(result) > 0
        # All options combined
        result = client.execute_command(
            "FT.EXPLAINCLI", "testidx", "hello world",
            "VERBATIM", "INORDER", "SLOP", "3"
        )
        assert isinstance(result, list) and len(result) > 0

    def test_ft_explaincli_errors(self):
        """Test error cases: wrong args, bad args, nonexistent index, empty query"""
        client = self.server.get_new_client()
        self._create_index(client, "testidx")
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
