import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestMultiLanguageGuard(ValkeySearchTestCaseBase):
    """Test language parsing behavior in FT.CREATE."""

    def test_english_language_accepted(self):
        client: Valkey = self.server.get_new_client()
        result = client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "ENGLISH",
            "SCHEMA", "t", "TEXT"
        )
        assert result == b"OK"

    def test_no_language_defaults_to_english(self):
        client: Valkey = self.server.get_new_client()
        result = client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "SCHEMA", "t", "TEXT"
        )
        assert result == b"OK"

    def test_unknown_language_rejected(self):
        client: Valkey = self.server.get_new_client()
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.CREATE", "idx", "ON", "HASH",
                "LANGUAGE", "KLINGON",
                "SCHEMA", "t", "TEXT"
            )
