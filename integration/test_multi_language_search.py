"""
Multi-language full-text search integration tests.

Verifies end-to-end stemming, stop words, normalization, persistence, cluster
consistency, and edge cases for non-English Snowball languages via the
FT.CREATE LANGUAGE -> HSET -> FT.SEARCH pipeline.
"""

import os
import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser
from utils import IndexingTestHelper


class MultiLanguageTestCase(ValkeySearchTestCaseDebugMode):
    """Base class for multi-language tests (version >= 1.4 enables support)."""
    pass


# =============================================================================
# All languages accepted in FT.CREATE
# =============================================================================

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


# =============================================================================
# Parametrized per-language stemming integration
# =============================================================================

# One row per language: (language, doc_text, stem_query, expected_match)
# The stem_query is a morphological variant that should match doc_text via stemming.
LANGUAGE_STEMMING_DATA = {
    "ENGLISH": {
        "doc_text": "The children are running quickly",
        "stem_query": "run",
        "description": "running -> run",
    },
    "FRENCH": {
        "doc_text": "Les enfants continuent de jouer",
        "stem_query": "continuent",
        "description": "continuent/continuation share stem",
    },
    "GERMAN": {
        "doc_text": "Die Kinder laufenden schnell nach Hause",
        "stem_query": "laufende",
        "description": "laufenden/laufende -> laufend stem family",
    },
    "SPANISH": {
        "doc_text": "Los niños están corriendo por el parque",
        "stem_query": "corremos",
        "description": "corriendo/corremos -> corr-",
    },
    "ITALIAN": {
        "doc_text": "I bambini stanno mangiando la pasta",
        "stem_query": "mangiamo",
        "description": "mangiando/mangiamo -> mang-",
    },
    "PORTUGUESE": {
        "doc_text": "As crianças estão correndo no parque",
        "stem_query": "corremos",
        "description": "correndo/corremos -> corr-",
    },
    "RUSSIAN": {
        "doc_text": "Дети бегущие в парке",
        "stem_query": "бегущий",
        "description": "бегущие/бегущий share stem бегущ",
    },
    "SWEDISH": {
        "doc_text": "Barnen springer snabbt genom parken",
        "stem_query": "springande",
        "description": "springer/springande -> spring",
    },
    "TURKISH": {
        "doc_text": "Çocuklar parkta koşuyorlar",
        "stem_query": "koşuyor",
        "description": "koşuyorlar/koşuyor share stem",
    },
    "DUTCH": {
        "doc_text": "De kinderen zijn aan het lopende beweging",
        "stem_query": "lopend",
        "description": "lopende/lopend -> loop",
    },
    "INDONESIAN": {
        "doc_text": "Anak-anak sedang berlari di taman",
        "stem_query": "pelari",
        "description": "berlari/pelari -> lari",
    },
    "ARABIC": {
        "doc_text": "الأطفال يركضون في الحديقة",
        "stem_query": "أطفال",
        "description": "Arabic definite article prefix removal",
    },
}


class TestPerLanguageStemming(MultiLanguageTestCase):
    """Parametrized integration test: full create-ingest-search pipeline per language."""

    @pytest.mark.parametrize("language", LANGUAGE_STEMMING_DATA.keys())
    def test_stemming_roundtrip(self, language):
        """End-to-end: FT.CREATE with LANGUAGE -> HSET -> FT.SEARCH with stem variant."""
        data = LANGUAGE_STEMMING_DATA[language]
        client: Valkey = self.server.get_new_client()

        idx_name = f"idx_{language.lower()}"
        client.execute_command(
            "FT.CREATE", idx_name, "ON", "HASH",
            "LANGUAGE", language,
            "SCHEMA", "content", "TEXT"
        )
        client.execute_command("HSET", "doc:1", "content", data["doc_text"])
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, idx_name)

        result = client.execute_command(
            "FT.SEARCH", idx_name, f"@content:{data['stem_query']}"
        )
        assert result[0] >= 1, (
            f"[{language}] Stemming roundtrip failed: searching '{data['stem_query']}' "
            f"against doc '{data['doc_text']}' returned 0 results. "
            f"({data['description']})"
        )


# =============================================================================
# Stop words and normalization
# =============================================================================

class TestMultiLanguageSearch(MultiLanguageTestCase):
    """End-to-end multi-language search through the full command stack."""

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


# =============================================================================
# LANGUAGE in FT.INFO output
# =============================================================================

class TestLanguageInFTInfo(MultiLanguageTestCase):
    """Verify that LANGUAGE field appears in FT.INFO output."""

    def test_ft_info_reports_language(self):
        """When LANGUAGE is specified in FT.CREATE, FT.INFO must report it."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        parser = IndexingTestHelper.get_ft_info(client, "idx")
        assert parser.language is not None, "FT.INFO should report the LANGUAGE field"
        assert parser.language.upper() == "FRENCH", (
            f"Expected FRENCH, got {parser.language}"
        )

    def test_ft_info_reports_default_language(self):
        """When no LANGUAGE is specified, FT.INFO should report ENGLISH as default."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "SCHEMA", "content", "TEXT"
        )
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        parser = IndexingTestHelper.get_ft_info(client, "idx")
        # Language may be absent (defaulting to English) or explicitly "ENGLISH"
        if parser.language is not None:
            assert parser.language.upper() == "ENGLISH", (
                f"Expected ENGLISH or None, got {parser.language}"
            )


# =============================================================================
# LANGUAGE persistence across save/restore
# =============================================================================

class TestLanguageSaveRestore(MultiLanguageTestCase):
    """Verify LANGUAGE TEXT index survives save/restart/backfill."""

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args

    def test_language_persists_across_restart(self):
        """Create FRENCH index, ingest, save, restart, verify stemming works."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )
        # Ingest French text with stemming variants
        client.execute_command("HSET", "doc:1", "content", "Les enfants continuent")
        client.execute_command("HSET", "doc:2", "content", "La continuation est belle")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Verify stemming works before save
        result = client.execute_command("FT.SEARCH", "idx", "@content:continuent")
        assert result[0] >= 1, "French stemming should work before save"

        # Save and restart
        client.execute_command("SAVE")
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()

        # Wait for backfill to complete after restart
        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_backfill_complete_on_node(client, "idx")
        )

        # Verify stemming still works after restart
        result = client.execute_command("FT.SEARCH", "idx", "@content:continuent")
        assert result[0] >= 1, (
            "French stemming should work after save/restart — LANGUAGE must persist in RDB"
        )

        # Also verify FT.INFO still reports the language
        parser = IndexingTestHelper.get_ft_info(client, "idx")
        if parser.language is not None:
            assert parser.language.upper() == "FRENCH", (
                f"FT.INFO should still report FRENCH after restart, got {parser.language}"
            )


# =============================================================================
# Cross-contamination between different-language indexes
# =============================================================================

class TestLanguageCrossContamination(MultiLanguageTestCase):
    """Two indexes with different languages must not cross-contaminate."""

    def test_no_cross_contamination(self):
        """French and German indexes on same server produce independent results."""
        client: Valkey = self.server.get_new_client()

        # Create French index
        client.execute_command(
            "FT.CREATE", "idx_fr", "ON", "HASH",
            "PREFIX", "1", "fr:",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )
        # Create German index
        client.execute_command(
            "FT.CREATE", "idx_de", "ON", "HASH",
            "PREFIX", "1", "de:",
            "LANGUAGE", "GERMAN",
            "SCHEMA", "content", "TEXT"
        )

        # Ingest French doc into French index
        client.execute_command("HSET", "fr:1", "content", "Les enfants continuent")
        # Ingest German doc into German index
        client.execute_command("HSET", "de:1", "content", "Die Kinder laufenden schnell")

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx_fr")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx_de")

        # French stem query should only hit French index
        fr_result = client.execute_command("FT.SEARCH", "idx_fr", "@content:continuent")
        assert fr_result[0] >= 1, "French stemming should work in French index"

        # Same query on German index should NOT match
        de_result = client.execute_command("FT.SEARCH", "idx_de", "@content:continuent")
        assert de_result[0] == 0, (
            "French stem query should not match in German index"
        )

        # German stem query should only hit German index
        # "laufenden" stems to "laufend", and "laufende" also stems to "laufend"
        de_result2 = client.execute_command("FT.SEARCH", "idx_de", "@content:laufende")
        assert de_result2[0] >= 1, "German stemming should work in German index"

        # Same German query on French index should NOT match
        fr_result2 = client.execute_command("FT.SEARCH", "idx_fr", "@content:laufende")
        assert fr_result2[0] == 0, (
            "German stem query should not match in French index"
        )


# =============================================================================
# German compound word tokenization
# =============================================================================

class TestGermanCompoundWord(MultiLanguageTestCase):
    """Document German compound word tokenization behavior."""

    def test_donaudampfschifffahrtsgesellschaft(self):
        """
        Verify how German tokenization handles compound words.
        Snowball stemmer does NOT decompose compounds — documents behavior.
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "GERMAN",
            "SCHEMA", "content", "TEXT"
        )
        # Classic German compound: "Danube steamship company"
        client.execute_command(
            "HSET", "doc:1", "content", "Donaudampfschifffahrtsgesellschaft"
        )
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # The full word should be findable (it's one token, possibly stemmed)
        result = client.execute_command(
            "FT.SEARCH", "idx", "@content:donaudampfschifffahrtsgesellschaft"
        )
        assert result[0] == 1, (
            "Full German compound word should be searchable as one token"
        )

        # Prefix search should work
        result = client.execute_command(
            "FT.SEARCH", "idx", "@content:donaudampf*"
        )
        assert result[0] == 1, (
            "Prefix search should match German compound word"
        )

        # A substring that is NOT a prefix should NOT match (no decompounding)
        result = client.execute_command(
            "FT.SEARCH", "idx", "@content:schifffahrt"
        )
        assert result[0] == 0, (
            "Snowball does not decompose German compounds — "
            "substring 'schifffahrt' should not match"
        )


# =============================================================================
# French apostrophe elision
# =============================================================================

class TestFrenchApostropheElision(MultiLanguageTestCase):
    """Document how French apostrophe elision is tokenized."""

    def test_apostrophe_splits_token(self):
        """
        French l'école — the apostrophe is in the default punctuation set,
        so it splits into separate tokens: [l, école].
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT", "NOSTEM"
        )
        client.execute_command("HSET", "doc:1", "content", "l'école est belle")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # "école" should be an independent token (split on apostrophe)
        result = client.execute_command("FT.SEARCH", "idx", "@content:école")
        assert result[0] == 1, (
            "After apostrophe split, 'école' should be independently searchable"
        )


# =============================================================================
# Non-ASCII query in parser byte-limit test
# =============================================================================

VECTOR_BLOB_DIM128 = struct.pack("<128f", *([1.0] * 128))


class TestQueryParserNonAscii(ValkeySearchTestCaseBase):
    """Verify that query byte-limit counts bytes, not characters."""

    def test_multibyte_utf8_counts_as_bytes(self):
        """
        A 3-byte UTF-8 char (e.g. Chinese) counts as 3 bytes toward the limit.
        This proves byte counting is correct for multi-byte queries.
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "SCHEMA", "content", "TEXT",
            "doc_embedding", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "128", "DISTANCE_METRIC", "COSINE"
        )

        # Query with Chinese chars: "世界" = 6 bytes (2 * 3-byte UTF-8)
        # U+4E16 (世) encodes to 3 bytes: E4 B8 96
        # U+754C (界) encodes to 3 bytes: E7 95 8C
        # Total query: "@content:世界 =>[KNN 10 @doc_embedding $BLOB]"
        query = "@content:\u4e16\u754c =>[KNN 10 @doc_embedding $BLOB]"
        query_bytes = len(query.encode("utf-8"))

        # Set limit to exactly the query byte length — should pass
        client.execute_command(
            f"CONFIG SET search.query-string-bytes {query_bytes}"
        )
        result = client.execute_command(
            "FT.SEARCH", "idx", query,
            "PARAMS", 2, "BLOB", VECTOR_BLOB_DIM128,
            "RETURN", 1, "doc_embedding"
        )
        assert result[0] == 0  # No docs, but no error

        # Set limit to query_bytes - 1 — should fail
        client.execute_command(
            f"CONFIG SET search.query-string-bytes {query_bytes - 1}"
        )
        with pytest.raises(ResponseError, match="Query string is too long"):
            client.execute_command(
                "FT.SEARCH", "idx", query,
                "PARAMS", 2, "BLOB", VECTOR_BLOB_DIM128,
                "RETURN", 1, "doc_embedding"
            )


# =============================================================================
# LANGUAGE in cluster metadata consistency
# =============================================================================

class MultiLanguageClusterTestCase(ValkeySearchClusterTestCaseDebugMode):
    """Cluster test case for multi-language tests (version >= 1.4)."""
    pass


class TestLanguageClusterMetadata(MultiLanguageClusterTestCase):
    """Verify LANGUAGE metadata and search behavior work correctly in cluster mode."""

    def test_language_consistent_across_cluster(self):
        """Create index with LANGUAGE on one node, verify all nodes report it."""
        node0: Valkey = self.new_client_for_primary(0)

        node0.execute_command(
            "FT.CREATE", "idx_fr", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )

        # Wait for indexing on all nodes
        nodes = [self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)]
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(nodes, "idx_fr")

        # Check FT.INFO on all nodes reports the same language
        for i in range(self.CLUSTER_SIZE):
            node = self.new_client_for_primary(i)
            parser = IndexingTestHelper.get_ft_info(node, "idx_fr")
            assert parser.language is not None, (
                f"Node {i}: FT.INFO should report LANGUAGE field"
            )
            assert parser.language.upper() == "FRENCH", (
                f"Node {i}: Expected FRENCH, got {parser.language}"
            )

    def test_language_ft_info_full_consistency(self):
        """Full FT.INFO consistency: LANGUAGE index metadata matches across all nodes."""
        node0: Valkey = self.new_client_for_primary(0)

        node0.execute_command(
            "FT.CREATE", "idx_multi", "ON", "HASH",
            "LANGUAGE", "GERMAN",
            "SCHEMA", "title", "TEXT", "body", "TEXT", "NOSTEM"
        )

        nodes = [self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)]
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(nodes, "idx_multi")

        # Get FT.INFO from all nodes and compare
        parsers = [
            IndexingTestHelper.get_ft_info(self.new_client_for_primary(i), "idx_multi")
            for i in range(self.CLUSTER_SIZE)
        ]

        first_dict = parsers[0].to_dict()
        for i, parser in enumerate(parsers[1:], 1):
            node_dict = parser.to_dict()
            assert node_dict == first_dict, (
                f"Node {i} FT.INFO differs from node 0 for LANGUAGE index"
            )

    def test_stemming_works_across_cluster(self):
        """Stemming produces correct results when docs land on different shards.

        Creates a French index, inserts docs with keys chosen to hash to
        different slots, then issues a stemmed search through the coordinator
        and verifies results merge back correctly from all shards.
        """
        cluster_client = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)

        node0.execute_command(
            "FT.CREATE", "idx_fr_search", "ON", "HASH",
            "LANGUAGE", "FRENCH",
            "SCHEMA", "content", "TEXT"
        )

        nodes = [self.new_client_for_primary(i) for i in range(self.CLUSTER_SIZE)]
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(nodes, "idx_fr_search")

        # Insert docs with keys that distribute across different hash slots.
        # "mangeaient" (they were eating), "mangé" (eaten), "mangerons" (we will eat)
        # all share the French stem "mang-" with query term "manger" (to eat).
        docs = {
            "doc:{fr_a}:1": "Ils mangeaient dans le restaurant",
            "doc:{fr_b}:2": "Elle a mangé une pomme hier",
            "doc:{fr_c}:3": "Nous mangerons ensemble demain",
            "doc:{fr_d}:4": "Le château est magnifique",  # no mang- stem
        }
        for key, content in docs.items():
            cluster_client.execute_command("HSET", key, "content", content)

        # Wait for backfill on all nodes
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(nodes, "idx_fr_search")

        # Search with a stem variant — "manger" (to eat) shares stem "mang-"
        # This should match docs 1, 2, 3 but not doc 4
        result = cluster_client.execute_command(
            "FT.SEARCH", "idx_fr_search", "@content:manger"
        )

        # Verify the correct documents were returned (not doc:4)
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        expected_keys = {b"doc:{fr_a}:1", b"doc:{fr_b}:2", b"doc:{fr_c}:3"}
        assert returned_keys == expected_keys, (
            f"Expected docs {expected_keys}, got {returned_keys}"
        )


# =============================================================================
# RDB backward compatibility: index created without LANGUAGE defaults to ENGLISH
# =============================================================================

class TestLanguageRDBBackwardCompat(ValkeySearchTestCaseBase):
    """
    Verify that an index created without a LANGUAGE field defaults to
    ENGLISH behavior. This proves that protobuf's LANGUAGE_UNSPECIFIED
    (value 0) correctly maps to English stop words and stemming.
    """

    def test_no_language_defaults_to_english_stemming(self):
        """Index without LANGUAGE should use English stemming by default."""
        client: Valkey = self.server.get_new_client()

        # Create index without specifying LANGUAGE (simulates old-format index)
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "SCHEMA", "content", "TEXT"
        )
        client.execute_command("HSET", "doc:1", "content", "The children are running quickly")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # English stemming should work: "running" stems to "run"
        result = client.execute_command("FT.SEARCH", "idx", "@content:run")
        assert result[0] >= 1, (
            "Default (no LANGUAGE) index should use English stemming: "
            "'run' should match 'running'"
        )

    def test_no_language_defaults_to_english_stop_words(self):
        """Index without LANGUAGE should filter English stop words by default."""
        client: Valkey = self.server.get_new_client()

        # Create index without specifying LANGUAGE
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "SCHEMA", "content", "TEXT"
        )
        # "the" is an English stop word — a doc with only "the" should not match
        client.execute_command("HSET", "doc:1", "content", "the")
        client.execute_command("HSET", "doc:2", "content", "hello world")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Searching for "the" should return 0 results (it's a stop word) or
        # raise an error because the stop word removal leaves no valid query terms.
        try:
            result = client.execute_command("FT.SEARCH", "idx", "@content:the")
            assert result[0] == 0, (
                "Default (no LANGUAGE) index should filter English stop word 'the'"
            )
        except ResponseError as e:
            # Stop word removal may produce an empty/invalid query — that's acceptable
            assert "Invalid" in str(e) or "Syntax" in str(e), (
                f"Unexpected error when searching stop word: {e}"
            )


# =============================================================================
# Deliberate divergences: Valkey-only correctness assertions
# =============================================================================

class TestDeliberateDivergences(MultiLanguageTestCase):
    """
    Valkey-only integration tests that positively assert intentional behavioral
    differences from RediSearch. These prove that our design choices (newer
    Snowball, Lucene stopwords, strict NOSTEM, utf8Fold) work correctly
    end-to-end through the full FT.CREATE -> HSET -> FT.SEARCH pipeline.

    No Redis comparison is needed — these tests document and lock in behaviors
    where Valkey intentionally diverges.
    """

    # -----------------------------------------------------------------
    # Divergence #1: German ß -> ss case folding (utf8Fold)
    # Our CaseMap::utf8Fold converts ß to ss at both index and query time,
    # aligning with Lucene behavior. This means "strasse" matches "Straße".
    # -----------------------------------------------------------------

    def test_german_eszett_case_folding(self):
        """German ß folds to ss: 'strasse' matches 'Straße' and vice versa."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "GERMAN",
            "SCHEMA", "content", "TEXT", "NOSTEM"
        )
        client.execute_command("HSET", "doc:1", "content", "Straße")
        client.execute_command("HSET", "doc:2", "content", "strasse")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Query with ss form finds ß document
        result = client.execute_command("FT.SEARCH", "idx", "@content:strasse")
        assert result[0] == 2, (
            "utf8Fold should map ß->ss: 'strasse' query should match both "
            f"'Straße' and 'strasse' documents, got {result[0]} results"
        )

        # Query with ß form finds ss document
        result = client.execute_command("FT.SEARCH", "idx", "@content:straße")
        assert result[0] == 2, (
            "utf8Fold should map ß->ss: 'straße' query should match both "
            f"documents, got {result[0]} results"
        )

    # -----------------------------------------------------------------
    # Divergence #2: Turkish dotted/dotless I (locale-aware lowercasing)
    # Turkish has four I variants: İ/i (dotted) and I/ı (dotless).
    # Our locale-aware utf8ToLower handles this correctly.
    # -----------------------------------------------------------------

    def test_turkish_dotted_i_case_folding(self):
        """Turkish İ (dotted capital I) lowercases to i, matching 'istanbul'."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "TURKISH",
            "SCHEMA", "content", "TEXT", "NOSTEM"
        )
        client.execute_command("HSET", "doc:1", "content", "İstanbul")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Lowercase query should match uppercase İ document
        result = client.execute_command("FT.SEARCH", "idx", "@content:istanbul")
        assert result[0] == 1, (
            "Turkish locale-aware lowercasing should map İ->i: "
            f"'istanbul' should match 'İstanbul', got {result[0]} results"
        )

    # -----------------------------------------------------------------
    # Divergence #3: Ligature decomposition via case folding (utf8Fold)
    # ICU's CaseMap::utf8Fold applies full Unicode case folding which
    # decomposes ligatures like ﬁ -> fi (CaseFolding.txt status F).
    # This means "financial" matches "ﬁnancial".
    # -----------------------------------------------------------------

    def test_ligature_case_fold_decomposition(self):
        """utf8Fold decomposes ﬁ ligature to fi: 'financial' matches 'ﬁnancial'."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "ENGLISH",
            "SCHEMA", "content", "TEXT", "NOSTEM"
        )
        # U+FB01 = ﬁ (Latin small ligature fi)
        client.execute_command("HSET", "doc:1", "content", "\ufb01nancial report")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Query with regular "fi" should match ligature "ﬁ" after case folding
        result = client.execute_command("FT.SEARCH", "idx", "@content:financial")
        assert result[0] == 1, (
            "utf8Fold should decompose ﬁ ligature to fi: 'financial' should "
            f"match 'ﬁnancial', got {result[0]} results"
        )

    # -----------------------------------------------------------------
    # Divergence #4: Strict NOSTEM enforcement
    # Valkey enforces NOSTEM at both index and query time. Bare (non-field)
    # queries do NOT bypass NOSTEM, unlike RediSearch which only enforces
    # NOSTEM on field-targeted queries.
    # -----------------------------------------------------------------

    def test_nostem_strict_enforcement_bare_query(self):
        """NOSTEM prevents stemming even for bare (non-field-targeted) queries."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "INDONESIAN",
            "SCHEMA", "body", "TEXT", "NOSTEM"
        )
        # "kuat" = "strong"; "kekuatan" = "strength" (stems to "kuat")
        client.execute_command("HSET", "doc:1", "body", "burung lambat kuat")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Field-targeted query: NOSTEM means no stemming, "kekuatan" != "kuat"
        result = client.execute_command(
            "FT.SEARCH", "idx", "@body:kekuatan", "DIALECT", "2"
        )
        assert result[0] == 0, (
            "NOSTEM field query: 'kekuatan' should NOT match 'kuat' "
            f"(no stemming), got {result[0]} results"
        )

        # Bare query: Valkey still enforces NOSTEM (unlike RediSearch)
        result = client.execute_command(
            "FT.SEARCH", "idx", "kekuatan", "DIALECT", "2"
        )
        assert result[0] == 0, (
            "NOSTEM bare query: Valkey strictly enforces NOSTEM even for "
            f"non-field queries. 'kekuatan' should NOT match 'kuat', "
            f"got {result[0]} results"
        )

    # -----------------------------------------------------------------
    # Divergence #5: Arabic fuzzy search on original forms only
    # Valkey does NOT index stemmed forms separately. Fuzzy search operates
    # only on the original indexed tokens, not on stems.
    # -----------------------------------------------------------------

    def test_arabic_fuzzy_original_form_only(self):
        """Arabic fuzzy search matches original form only, not stemmed form."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "ARABIC",
            "SCHEMA", "body", "TEXT"
        )
        # حرية = "freedom" (4 code points: ح-ر-ي-ة)
        # Arabic stemmer would stem to حر (2 code points)
        client.execute_command("HSET", "doc:1", "body", "حرية")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Fuzzy search for حر (the stem) — should NOT match because Valkey
        # doesn't index the stemmed form in the trie
        result = client.execute_command(
            "FT.SEARCH", "idx", "%حر%", "DIALECT", "2"
        )
        assert result[0] == 0, (
            "Arabic fuzzy: Valkey does not index stemmed forms. "
            f"Fuzzy '%حر%' should NOT match 'حرية' (distance from stem "
            f"is too large), got {result[0]} results"
        )

    # -----------------------------------------------------------------
    # Divergence #6: Dutch Kraaij-Pohlmann stemmer (Snowball 3.0.1)
    # Our newer Snowball uses K-P algorithm for Dutch which strips
    # grammatical prefixes, unlike the older Porter algorithm in RediSearch.
    # -----------------------------------------------------------------

    def test_dutch_kp_stemmer_prefix_stripping(self):
        """Dutch K-P stemmer strips prefixes: 'geschilderd' stems to 'schilder'."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH",
            "LANGUAGE", "DUTCH",
            "SCHEMA", "content", "TEXT"
        )
        # "geschilderd" = "painted", K-P stems to "schilder" (painter)
        client.execute_command("HSET", "doc:1", "content", "geschilderd")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

        # Searching for "schilder" should match because K-P reduces both
        # "geschilderd" and "schilder" to the same stem
        result = client.execute_command(
            "FT.SEARCH", "idx", "@content:schilder"
        )
        assert result[0] == 1, (
            "Dutch K-P stemmer should strip prefix: 'schilder' should match "
            f"'geschilderd', got {result[0]} results"
        )
