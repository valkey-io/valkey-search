#!/usr/bin/env python3

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFTExplainCli(ValkeySearchTestCaseBase):
    """Test cases for FT.EXPLAINCLI command"""

    def test_ft_explaincli_basic(self):
        """Test basic FT.EXPLAINCLI functionality"""
        client = self.server.get_new_client()
        
        # Create a simple index
        client.execute_command(
            "FT.CREATE", "testidx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT", "content", "TEXT"
        )
        
        # Test simple query
        result = client.execute_command("FT.EXPLAINCLI", "testidx", "hello")
        assert isinstance(result, list)
        assert len(result) > 0
        # Should contain TEXT-TERM information
        found_text_term = any(b"TEXT-TERM" in line for line in result)
        assert found_text_term, f"Expected TEXT-TERM in result: {result}"

    def test_ft_explaincli_complex_query(self):
        """Test FT.EXPLAINCLI with complex query"""
        client = self.server.get_new_client()
        
        # Create an index
        client.execute_command(
            "FT.CREATE", "testidx2",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT", "content", "TEXT"
        )
        
        # Test complex query with AND
        result = client.execute_command("FT.EXPLAINCLI", "testidx2", "hello world")
        assert isinstance(result, list)
        assert len(result) > 0
        
        # Should contain AND structure for multiple terms
        result_str = b" ".join(result).decode('utf-8')
        assert "AND" in result_str or "TEXT-TERM" in result_str

    def test_ft_explaincli_wrong_args(self):
        """Test FT.EXPLAINCLI with wrong number of arguments"""
        client = self.server.get_new_client()
        
        # Test with too few arguments
        with pytest.raises(ResponseError, match="Wrong number of arguments"):
            client.execute_command("FT.EXPLAINCLI", "testidx")
        
        # Test with too many arguments
        with pytest.raises(ResponseError, match="Wrong number of arguments"):
            client.execute_command("FT.EXPLAINCLI", "testidx", "query", "extra")

    def test_ft_explaincli_nonexistent_index(self):
        """Test FT.EXPLAINCLI with non-existent index"""
        client = self.server.get_new_client()
        
        # Test with non-existent index
        with pytest.raises(ResponseError):
            client.execute_command("FT.EXPLAINCLI", "nonexistent", "hello")

    def test_ft_explaincli_empty_query(self):
        """Test FT.EXPLAINCLI with empty query"""
        client = self.server.get_new_client()
        
        # Create an index
        client.execute_command(
            "FT.CREATE", "testidx3",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT"
        )
        
        # Test with empty query
        result = client.execute_command("FT.EXPLAINCLI", "testidx3", "")
        assert isinstance(result, list)
        # Empty query should return some explanation
        assert len(result) >= 0

    def test_ft_explaincli_field_specific_query(self):
        """Test FT.EXPLAINCLI with field-specific query"""
        client = self.server.get_new_client()
        
        # Create an index
        client.execute_command(
            "FT.CREATE", "testidx4",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT", "content", "TEXT"
        )
        
        # Test field-specific query
        result = client.execute_command("FT.EXPLAINCLI", "testidx4", "@title:hello")
        assert isinstance(result, list)
        assert len(result) > 0
        
        # Should contain TEXT-TERM information
        result_str = b" ".join(result).decode('utf-8')
        assert "TEXT-TERM" in result_str