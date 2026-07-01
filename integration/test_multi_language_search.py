"""
Multi-language full-text search integration tests.

Verifies end-to-end stemming, stop words, and normalization for non-English
Snowball languages via the FT.CREATE LANGUAGE -> HSET -> FT.SEARCH pipeline.
"""

import os
from typing import List

import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper


class MultiLanguageTestCase(ValkeySearchTestCaseBase):
    """Base class that enables debug-mode + multi-language-support flags."""

    def get_config_file_lines(self, testdir, port) -> List[str]:
        lines = super().get_config_file_lines(testdir, port)
        load_module = f"loadmodule {os.getenv('MODULE_PATH')}"
        return [
            x.replace(
                load_module,
                load_module + " --debug-mode yes --multi-language-support yes"
            )
            for x in lines
        ]


class TestMultiLanguageSearch(MultiLanguageTestCase):
    """End-to-end multi-language search through the full command stack."""

    def test_stemming(self):
        """Testing the full pipeline of stemming for non-English."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )
        client.execute_command("HSET", "doc:1", "content", "Les enfants continuent")
        client.execute_command("HSET", "doc:2", "content", "La continuation est belle")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        result = client.execute_command("FT.SEARCH", "idx", "@content:continuent")
        assert result[0] >= 1, (
            f"Expected stem expansion for 'continuent', got {result[0]} results"
        )

    def test_stop_words(self):
        """Per-language default stop words are filtered at index and query time."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )
        client.execute_command("HSET", "doc:1", "content", "je suis dans la maison")
        client.execute_command("HSET", "doc:2", "content", "voiture rapide")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Non-stop word is findable
        assert client.execute_command("FT.SEARCH", "idx", "@content:maison")[0] == 1
        # "dans" is a stop word -- searching "dans maison" only matches on
        # "maison", not "dans", proving stop word filtering works
        result = client.execute_command("FT.SEARCH", "idx", "@content:dans maison")
        assert result[0] == 1
        assert result[1] == b"doc:1"

    def test_nfc_normalization(self):
        """Precomposed and decomposed accented chars match after NFC."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT", "NOSTEM"
        )
        # Use raw bytes to bypass Python's source-level NFC normalization.
        # doc:1 = decomposed "cafe" + combining acute (CC 81) + " bon"
        # doc:2 = precomposed "caf" + e-acute (C3 A9) + " mal"
        client.execute_command("HSET", "doc:1", "content",
                              b"cafe\xcc\x81 bon")
        client.execute_command("HSET", "doc:2", "content",
                              b"caf\xc3\xa9 mal")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Search with precomposed -- should find both after NFC unification
        result = client.execute_command("FT.SEARCH", "idx",
                                       b"@content:caf\xc3\xa9")
        assert result[0] == 2, (
            f"NFC normalization should unify precomposed and decomposed, got {result[0]}"
        )

    def test_arabic_nfkc(self):
        """Arabic presentation forms collapse to canonical via NFKC."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "ARABIC",
            "SCHEMA", "content", "TEXT", "NOSTEM"
        )
        # Presentation forms for "ktab": U+FEDB U+FE98 U+FE8E U+FE8F
        # NFKC normalizes to canonical: U+0643 U+062A U+0627 U+0628
        client.execute_command("HSET", "doc:1", "content",
                              b"\xef\xbb\x9b\xef\xba\x98\xef\xba\x8e\xef\xba\x8f")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Search with canonical forms
        result = client.execute_command("FT.SEARCH", "idx",
                                       b"@content:\xd9\x83\xd8\xaa\xd8\xa7\xd8\xa8")
        assert result[0] == 1, "NFKC should normalize presentation forms to canonical"

    def test_non_ascii_punctuation(self):
        """Arabic comma U+060C splits tokens as a word boundary."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "ARABIC",
            "SCHEMA", "content", "TEXT", "NOSTEM"
        )
        # "hello" + Arabic comma (D8 8C) + "world"
        client.execute_command("HSET", "doc:1", "content",
                              b"hello\xd8\x8cworld")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Each word independently searchable proves the comma split them
        assert client.execute_command(
            "FT.SEARCH", "idx", "@content:hello"
        )[0] == 1
        assert client.execute_command(
            "FT.SEARCH", "idx", "@content:world"
        )[0] == 1


class TestMultiLanguageAllAccepted(MultiLanguageTestCase):
    """All 12 Snowball languages can be specified in FT.CREATE."""

    @pytest.mark.parametrize("language", [
        "ENGLISH", "FRENCH", "GERMAN", "SPANISH", "ITALIAN",
        "PORTUGUESE", "RUSSIAN", "SWEDISH", "TURKISH", "DUTCH",
        "INDONESIAN", "ARABIC"
    ])
    def test_language_accepted(self, language):
        client: Valkey = self.server.get_new_client()
        result = client.execute_command(
            "FT.CREATE", f"idx_{language.lower()}", "ON", "HASH",
            "LANGUAGE", language,
            "SCHEMA", "content", "TEXT"
        )
        assert result == b"OK"
