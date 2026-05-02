"""
Smoke test for FT._DEBUG INDEXSTATS.

Creates one index containing one attribute of each instrumented type
(TAG, NUMERIC, TEXT, VECTOR FLAT, VECTOR HNSW), runs INDEXSTATS on the
empty index, then again after writing a few records. We don't assert
exact values — we only verify that each expected section/key is present
in the reply.
"""

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Tag, Numeric, Text, Vector
from utils import IndexingTestHelper


def _flat_kv_to_dict(reply):
    """Recursively turn a flat [k1, v1, k2, v2, ...] RESP array into a dict.

    Nested arrays of the same shape are recursed into. Anything else (ints,
    strings) is returned as-is.
    """
    if isinstance(reply, list):
        # Heuristic: if every other element is bytes/str, treat as kv map.
        if (
            len(reply) % 2 == 0
            and len(reply) > 0
            and all(isinstance(reply[i], (bytes, str)) for i in range(0, len(reply), 2))
        ):
            out = {}
            for i in range(0, len(reply), 2):
                key = reply[i]
                if isinstance(key, bytes):
                    key = key.decode()
                out[key] = _flat_kv_to_dict(reply[i + 1])
            return out
        return [_flat_kv_to_dict(x) for x in reply]
    return reply


def _decode(s):
    return s.decode() if isinstance(s, bytes) else s


class TestIndexStats(ValkeySearchTestCaseDebugMode):
    def _check_reply(self, reply, has_records: bool):
        d = _flat_kv_to_dict(reply)
        assert isinstance(d, dict), f"top-level reply not a kv map: {reply!r}"

        # Top-level keys.
        assert "indexName" in d
        assert _decode(d["indexName"]) == "idx"
        assert "indexLevel" in d
        assert "attributes" in d

        # Index-level (text-cross-field) section.
        level = d["indexLevel"]
        assert isinstance(level, dict)
        for k in ("numUniqueWords", "keysPerWordHistogram", "wordWithMostKeys"):
            assert k in level, f"indexLevel missing {k}: {level!r}"
        assert isinstance(level["keysPerWordHistogram"], dict)
        # 9 buckets — the "0" bucket is dropped (every word has >=1 key).
        assert len(level["keysPerWordHistogram"]) == 9
        assert "0" not in level["keysPerWordHistogram"]

        # Attribute sections, one per defined field.
        attrs = d["attributes"]
        assert isinstance(attrs, dict)
        assert set(attrs.keys()) == {"t", "n", "txt", "vf", "vh"}, (
            f"unexpected attribute aliases: {list(attrs.keys())}"
        )

        # Per-attribute required keys.
        type_required = {
            "TAG": [
                "numUniqueTags",
                "keysPerTagHistogram",
                "tagsPerKeyAvg",
                "tagsPerKeyMax",
                "keyWithMostTags",
            ],
            "NUMERIC": ["numNumbers", "numSegmentTreeNodes"],
            "TEXT": ["numKeys"],
            "VECTOR_FLAT": ["numVectors", "numChunks", "chunkUtilizationPercent"],
            "VECTOR_HNSW": [
                "numVectors",
                "numChunks",
                "chunkUtilizationPercent",
                "numLayers",
                "numDeletedVectors",
                "outDegreeHistogram",
            ],
        }
        for alias, body in attrs.items():
            assert isinstance(body, dict), f"{alias} body not dict: {body!r}"
            assert "attributeIdentifier" in body
            assert "attributeType" in body
            atype = _decode(body["attributeType"])
            assert atype in type_required, f"{alias} unknown type: {atype}"
            for k in type_required[atype]:
                assert k in body, f"{alias} ({atype}) missing {k}: {body!r}"

            # Histograms are nested kv dicts.
            if atype == "TAG":
                assert isinstance(body["keysPerTagHistogram"], dict)
                assert len(body["keysPerTagHistogram"]) == 10
            if atype == "VECTOR_HNSW":
                assert isinstance(body["outDegreeHistogram"], dict)
                # 2*M+1 buckets; M=4 here so expect 9.
                assert len(body["outDegreeHistogram"]) == 9

        # Sanity: when records are present we should see >0 somewhere.
        if has_records:
            n_body = attrs["n"]
            assert int(n_body["numNumbers"]) > 0

    def test_indexstats(self):
        client: Valkey = self.server.get_new_client()

        idx = Index(
            "idx",
            [
                Tag("t"),
                Numeric("n"),
                Text("txt"),
                Vector("vf", 3, type="FLAT"),
                Vector("vh", 3, type="HNSW", m=4, efc=8),
            ],
        )
        idx.create(client)
        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(
                client, idx.name
            )
        )

        # Empty-index run.
        reply = client.execute_command("FT._DEBUG", "INDEXSTATS", idx.name)
        self._check_reply(reply, has_records=False)

        # Filtered form on empty index — only one attribute back.
        reply = client.execute_command(
            "FT._DEBUG", "INDEXSTATS", idx.name, "n"
        )
        d = _flat_kv_to_dict(reply)
        assert set(d["attributes"].keys()) == {"n"}

        # Add a few records, then re-run.
        idx.load_data(client, 3)
        reply = client.execute_command("FT._DEBUG", "INDEXSTATS", idx.name)
        self._check_reply(reply, has_records=True)

        # Unknown-attribute error.
        try:
            client.execute_command(
                "FT._DEBUG", "INDEXSTATS", idx.name, "no_such_field"
            )
            assert False, "expected error for unknown attribute"
        except Exception as e:
            assert "no_such_field" in str(e)
