"""Compare expression evaluator behavior between APPLY and FILTER.

The compatibility tests revealed two classes of failures in FILTER evaluation:
  1. Negation on missing attributes: @status!='inactive' excludes docs with no
     status field (should include them).
  2. String functions on JSON numeric fields: strlen(@price), startswith(@price,..),
     contains(@title,..) return fewer results for JSON key type.

This test loads the same data WITHOUT a filter (so all 10 docs are indexed) and
runs FT.AGGREGATE with APPLY to evaluate the same expressions.  The results show
whether the underlying expression evaluator itself has these issues, or whether
the problems are specific to the FILTER code path.
"""
import pytest
from valkey_search_test_case import ValkeySearchTestCaseBase
from utils import IndexingTestHelper


DOCS = [
    {"status": "active",   "price": "100",  "category": "electronics", "title": "quick fox jumps high",    "rating": "5"},
    {"status": "inactive", "price": "25",   "category": "books",       "title": "slow turtle walks far",   "rating": "3"},
    {"status": "active",   "price": "200",  "category": "clothing",    "title": "red hat sells well",      "rating": "4"},
    {"status": "pending",  "price": "50",   "category": "food",        "title": "fresh apple grows fast",  "rating": "2"},
    {"status": "active",   "price": "75",   "category": "electronics", "title": "bright screen shines on", "rating": "5"},
    {"status": "inactive", "price": "300",  "category": "books",       "title": "old book reads fine",     "rating": "1"},
    {"status": "active",   "price": "150",                              "title": "new phone rings loud",    "rating": "4"},   # missing category
    {"status": "pending",  "price": "10",   "category": "clothing",    "title": "blue shirt fits right"},                     # missing rating
    {                       "price": "500",  "category": "electronics", "title": "fast chip runs cool",     "rating": "5"},   # missing status
    {"status": "active",   "price": "1000", "category": "food",        "title": "big cake bakes slow",     "rating": "3"},
]

# Same data but with numeric types for JSON storage
JSON_DOCS = [
    {"status": "active",   "price": 100,  "category": "electronics", "title": "quick fox jumps high",    "rating": 5},
    {"status": "inactive", "price": 25,   "category": "books",       "title": "slow turtle walks far",   "rating": 3},
    {"status": "active",   "price": 200,  "category": "clothing",    "title": "red hat sells well",      "rating": 4},
    {"status": "pending",  "price": 50,   "category": "food",        "title": "fresh apple grows fast",  "rating": 2},
    {"status": "active",   "price": 75,   "category": "electronics", "title": "bright screen shines on", "rating": 5},
    {"status": "inactive", "price": 300,  "category": "books",       "title": "old book reads fine",     "rating": 1},
    {"status": "active",   "price": 150,                              "title": "new phone rings loud",    "rating": 4},
    {"status": "pending",  "price": 10,   "category": "clothing",    "title": "blue shirt fits right"},
    {                       "price": 500,  "category": "electronics", "title": "fast chip runs cool",     "rating": 5},
    {"status": "active",   "price": 1000, "category": "food",        "title": "big cake bakes slow",     "rating": 3},
]

import json


def _load_hash(client, docs):
    client.execute_command("FLUSHALL", "SYNC")
    client.execute_command(
        "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
        "SCHEMA", "status", "TAG", "price", "NUMERIC",
        "category", "TAG", "title", "TEXT", "NOSTEM", "rating", "NUMERIC",
    )
    for i, doc in enumerate(docs):
        client.execute_command("HSET", f"doc:{i:02d}", *[x for kv in doc.items() for x in kv])
    IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")


def _load_json(client, docs):
    client.execute_command("FLUSHALL", "SYNC")
    client.execute_command(
        "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "doc:",
        "SCHEMA",
        "$.status", "AS", "status", "TAG",
        "$.price", "AS", "price", "NUMERIC",
        "$.category", "AS", "category", "TAG",
        "$.title", "AS", "title", "TEXT", "NOSTEM",
        "$.rating", "AS", "rating", "NUMERIC",
    )
    for i, doc in enumerate(docs):
        client.execute_command("JSON.SET", f"doc:{i:02d}", "$", json.dumps(doc))
    IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")


def _run_apply(client, expr):
    """Run FT.AGGREGATE with APPLY and return a dict mapping doc key to the
    computed apply_result value."""
    result = client.execute_command(
        "FT.AGGREGATE", "idx", "*",
        "LOAD", "1", "@__key",
        "APPLY", expr, "AS", "apply_result",
        "DIALECT", "2",
    )
    out = {}
    for row in result[1:]:
        pairs = {row[j]: row[j + 1] for j in range(0, len(row), 2)}
        key = pairs.get(b"__key", pairs.get("__key"))
        val = pairs.get(b"apply_result", pairs.get("apply_result"))
        if key is not None:
            out[key] = val
    return out


class TestApplyVsFilter(ValkeySearchTestCaseBase):
    """Evaluate the same expressions used by failing FILTER tests via APPLY,
    to determine whether the expression evaluator itself has the issue."""

    # ---- Issue 1: negation with missing attributes ----

    @pytest.mark.parametrize("key_type", ["hash", "json"])
    def test_neq_missing_attr(self, key_type):
        """APPLY @status!='inactive' — doc:08 has no status field.
        Redis treats missing != 'inactive' as true.  Does APPLY agree?"""
        client = self.server.get_new_client()
        if key_type == "hash":
            _load_hash(client, DOCS)
        else:
            _load_json(client, JSON_DOCS)

        results = _run_apply(client, "@status!='inactive'")
        print(f"\n[{key_type}] APPLY @status!='inactive' results:")
        for k in sorted(results.keys()):
            print(f"  {k} => {results[k]}")

        # doc:08 has no status — should evaluate to 1 (true) if missing != value
        assert b"doc:08" in results, "doc:08 missing from APPLY results entirely"
        val = results[b"doc:08"]
        print(f"\n  doc:08 (no status field) => {val}  (expected: 1 = true)")

    # ---- Issue 2: string functions on numeric fields (JSON) ----

    @pytest.mark.parametrize("key_type", ["hash", "json"])
    def test_strlen_on_numeric(self, key_type):
        """APPLY strlen(@price) — price is NUMERIC.
        Hash stores as string so strlen works.  JSON stores as number."""
        client = self.server.get_new_client()
        if key_type == "hash":
            _load_hash(client, DOCS)
        else:
            _load_json(client, JSON_DOCS)

        results = _run_apply(client, "strlen(@price)")
        print(f"\n[{key_type}] APPLY strlen(@price) results:")
        for k in sorted(results.keys()):
            print(f"  {k} => {results[k]}")

        # Verify all 10 docs are present
        assert len(results) == 10, f"Expected 10 docs, got {len(results)}: {sorted(results.keys())}"

    @pytest.mark.parametrize("key_type", ["hash", "json"])
    def test_startswith_on_numeric(self, key_type):
        """APPLY startswith(@price,'1') — price is NUMERIC."""
        client = self.server.get_new_client()
        if key_type == "hash":
            _load_hash(client, DOCS)
        else:
            _load_json(client, JSON_DOCS)

        results = _run_apply(client, "startswith(@price,'1')")
        print(f"\n[{key_type}] APPLY startswith(@price,'1') results:")
        for k in sorted(results.keys()):
            print(f"  {k} => {results[k]}")

        assert len(results) == 10, f"Expected 10 docs, got {len(results)}: {sorted(results.keys())}"

    @pytest.mark.parametrize("key_type", ["hash", "json"])
    def test_contains_on_text(self, key_type):
        """APPLY contains(@title,'slow') — title is TEXT."""
        client = self.server.get_new_client()
        if key_type == "hash":
            _load_hash(client, DOCS)
        else:
            _load_json(client, JSON_DOCS)

        results = _run_apply(client, "contains(@title,'slow')")
        print(f"\n[{key_type}] APPLY contains(@title,'slow') results:")
        for k in sorted(results.keys()):
            print(f"  {k} => {results[k]}")

        assert len(results) == 10, f"Expected 10 docs, got {len(results)}: {sorted(results.keys())}"

        # doc:01 title="slow turtle walks far", doc:09 title="big cake bakes slow"
        # These should return 1 (true)
        for doc_key in [b"doc:01", b"doc:09"]:
            print(f"  {doc_key} => {results[doc_key]}  (expected: 1 = contains 'slow')")
