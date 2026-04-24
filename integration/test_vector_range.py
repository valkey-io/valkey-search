"""
Integration tests for Vector Range Search feature.

Tests the @field:[VECTOR_RANGE radius $blob] syntax in FT.SEARCH,
covering standalone queries, filter composition, hybrid KNN pre-filtering,
query attributes, FT.SEARCH options, error handling, and dialect compatibility.
"""

import struct
import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def float_to_bytes(floats):
    """Pack a list of floats into a little-endian binary blob."""
    return struct.pack(f"<{len(floats)}f", *floats)


def l2_distance(a, b):
    """Compute L2 (squared Euclidean) distance between two float lists."""
    return sum((x - y) ** 2 for x, y in zip(a, b))


def parse_result_keys(result):
    """Extract key names from a NOCONTENT FT.SEARCH result."""
    return set(result[i].decode("utf-8") for i in range(1, len(result)))


def parse_result_with_fields(result):
    """Parse FT.SEARCH result into {key: {field: value}} dict."""
    parsed = {}
    for i in range(1, len(result), 2):
        key = result[i].decode("utf-8")
        fields = result[i + 1]
        field_dict = {
            fields[j].decode("utf-8"): fields[j + 1]
            for j in range(0, len(fields), 2)
        }
        parsed[key] = field_dict
    return parsed


# ---------------------------------------------------------------------------
# Vectors used across tests (3-dimensional, L2 distance metric)
# ---------------------------------------------------------------------------
# Placed at known positions so distances are easy to reason about.
#   v0 = (0, 0, 0)  -- origin
#   v1 = (1, 0, 0)
#   v2 = (2, 0, 0)
#   v3 = (3, 0, 0)
#   v4 = (10, 0, 0) -- far away
VECTORS = {
    "doc:0": [0.0, 0.0, 0.0],
    "doc:1": [1.0, 0.0, 0.0],
    "doc:2": [2.0, 0.0, 0.0],
    "doc:3": [3.0, 0.0, 0.0],
    "doc:4": [10.0, 0.0, 0.0],
}

QUERY_VEC = [0.0, 0.0, 0.0]  # query from origin


class TestVectorRange(ValkeySearchTestCaseBase):
    """Integration tests for Vector Range queries."""

    # =================================================================
    # Helper methods
    # =================================================================

    def _create_hnsw_index(self, client, index_name="idx", prefix="doc:",
                           extra_fields=None, dim=3, distance="L2"):
        """Create an HNSW vector index with optional extra fields."""
        cmd = [
            "FT.CREATE", index_name, "ON", "HASH",
            "PREFIX", "1", prefix,
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(dim),
            "DISTANCE_METRIC", distance,
        ]
        if extra_fields:
            cmd.extend(extra_fields)
        assert client.execute_command(*cmd) == b"OK"

    def _create_flat_index(self, client, index_name="idx", prefix="doc:",
                           extra_fields=None, dim=3, distance="L2"):
        """Create a FLAT vector index with optional extra fields."""
        cmd = [
            "FT.CREATE", index_name, "ON", "HASH",
            "PREFIX", "1", prefix,
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", str(dim),
            "DISTANCE_METRIC", distance,
        ]
        if extra_fields:
            cmd.extend(extra_fields)
        assert client.execute_command(*cmd) == b"OK"

    def _load_vector_data(self, client, vectors=None, extra_data=None):
        """Load vector data into hash keys. extra_data maps key -> {field: val}."""
        if vectors is None:
            vectors = VECTORS
        for key, vec in vectors.items():
            mapping = {"vec": float_to_bytes(vec)}
            if extra_data and key in extra_data:
                mapping.update(extra_data[key])
            client.hset(key, mapping=mapping)

    def _search(self, client, index, query, *args):
        """Execute FT.SEARCH and return raw result."""
        cmd = ["FT.SEARCH", index, query] + list(args)
        return client.execute_command(*cmd)


    # =================================================================
    # 1. Standalone Vector Range — HNSW index
    # =================================================================

    def test_standalone_vector_range_hnsw(self):
        """
        Standalone Vector Range query on HNSW index returns exactly the
        vectors within the specified radius.
        Req: 1.1, 2.1
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        # L2 distances from origin: doc:0=0, doc:1=1, doc:2=4, doc:3=9, doc:4=100
        # radius=5 should include doc:0 (0), doc:1 (1), doc:2 (4)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 2. Standalone Vector Range — FLAT index
    # =================================================================

    def test_standalone_vector_range_flat(self):
        """
        Standalone Vector Range query on FLAT index returns exactly the
        vectors within the specified radius.
        Req: 1.1, 2.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=1 should include doc:0 (0) and doc:1 (1)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 1 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1"}

    # =================================================================
    # 3. Radius = 0 returns only exact matches
    # =================================================================

    def test_vector_range_radius_zero(self):
        """
        Radius 0 returns only vectors at distance exactly 0.
        Req: 2.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 0 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 1
        keys = parse_result_keys(result)
        assert keys == {"doc:0"}

    # =================================================================
    # 4. Large radius returns all vectors
    # =================================================================

    def test_vector_range_large_radius(self):
        """
        A sufficiently large radius returns all indexed vectors.
        Req: 2.1
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 1000 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 5

    # =================================================================
    # 5. Vector Range AND tag filter
    # =================================================================

    def test_vector_range_and_tag(self):
        """
        Vector Range combined with tag filter via AND returns intersection.
        Req: 2.3
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client, extra_fields=["category", "TAG"])
        extra = {
            "doc:0": {"category": "A"},
            "doc:1": {"category": "B"},
            "doc:2": {"category": "A"},
            "doc:3": {"category": "B"},
            "doc:4": {"category": "A"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=5 matches doc:0,1,2; category A matches doc:0,2,4
        # intersection = doc:0, doc:2
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] @category:{A}",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:2"}

    # =================================================================
    # 6. Vector Range AND numeric filter
    # =================================================================

    def test_vector_range_and_numeric(self):
        """
        Vector Range combined with numeric filter via AND.
        Req: 2.3
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client, extra_fields=["score", "NUMERIC"])
        extra = {
            "doc:0": {"score": "10"},
            "doc:1": {"score": "20"},
            "doc:2": {"score": "30"},
            "doc:3": {"score": "40"},
            "doc:4": {"score": "50"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=5 matches doc:0,1,2; score>=25 matches doc:2,3,4
        # intersection = doc:2
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] @score:[25 +inf]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 1
        keys = parse_result_keys(result)
        assert keys == {"doc:2"}

    # =================================================================
    # 7. Vector Range OR tag filter
    # =================================================================

    def test_vector_range_or_tag(self):
        """
        Vector Range combined with tag filter via OR returns union.
        Req: 2.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["category", "TAG"])
        extra = {
            "doc:0": {"category": "A"},
            "doc:1": {"category": "B"},
            "doc:2": {"category": "A"},
            "doc:3": {"category": "B"},
            "doc:4": {"category": "A"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=0 matches doc:0 only; category B matches doc:1, doc:3
        # union = doc:0, doc:1, doc:3
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 0 $blob] | @category:{B}",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:3"}


    # =================================================================
    # 8. Negated Vector Range
    # =================================================================

    def test_vector_range_negate(self):
        """
        Negated Vector Range returns keys whose distance exceeds the radius
        plus keys lacking the vector field.
        Req: 2.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["category", "TAG"])
        extra = {
            "doc:0": {"category": "A"},
            "doc:1": {"category": "A"},
            "doc:2": {"category": "A"},
            "doc:3": {"category": "A"},
            "doc:4": {"category": "A"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=1 matches doc:0 (0), doc:1 (1)
        # negation should return doc:2, doc:3, doc:4
        result = self._search(
            client, "idx",
            "-@vec:[VECTOR_RANGE 1 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:2", "doc:3", "doc:4"}

    # =================================================================
    # 9. Hybrid: Vector Range pre-filter + KNN
    # =================================================================

    def test_vector_range_prefilter_knn(self):
        """
        Vector Range as pre-filter in a KNN query: KNN results are a subset
        of the range-filtered set.
        Req: 3.1, 3.2
        """
        client = self.server.get_new_client()
        # Need two vector fields: one for range filter, one for KNN
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
            "vec2", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
        ]
        assert client.execute_command(*cmd) == b"OK"

        # Load data: vec for range filter, vec2 for KNN
        for key, v in VECTORS.items():
            client.hset(key, mapping={
                "vec": float_to_bytes(v),
                "vec2": float_to_bytes(v),
            })

        query_blob = float_to_bytes(QUERY_VEC)
        knn_blob = float_to_bytes(QUERY_VEC)
        # Range radius=5 on vec: doc:0,1,2
        # KNN 2 on vec2 from those candidates: doc:0, doc:1 (closest)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $range_blob]=>[KNN 2 @vec2 $knn_blob]",
            "PARAMS", "4", "range_blob", query_blob, "knn_blob", knn_blob,
            "NOCONTENT",
        )
        assert result[0] == 2
        keys = parse_result_keys(result)
        # All KNN results must be within the range-filtered set
        assert keys.issubset({"doc:0", "doc:1", "doc:2"})

    # =================================================================
    # 10. Hybrid: Vector Range pre-filter + KNN with fewer candidates than K
    # =================================================================

    def test_vector_range_prefilter_knn_fewer_candidates(self):
        """
        When range filter yields fewer candidates than K, return only those.
        Req: 3.2
        """
        client = self.server.get_new_client()
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
            "vec2", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
        ]
        assert client.execute_command(*cmd) == b"OK"

        for key, v in VECTORS.items():
            client.hset(key, mapping={
                "vec": float_to_bytes(v),
                "vec2": float_to_bytes(v),
            })

        query_blob = float_to_bytes(QUERY_VEC)
        # Range radius=1 on vec: doc:0, doc:1 (2 candidates)
        # KNN 10 requests 10 but only 2 candidates available
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 1 $range_blob]=>[KNN 10 @vec2 $knn_blob]",
            "PARAMS", "4", "range_blob", query_blob, "knn_blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2

    # =================================================================
    # 11. Query attributes: $yield_distance_as
    # =================================================================

    def test_query_attr_yield_distance_as(self):
        """
        Custom distance field name via {$yield_distance_as: name}=> prefix.
        Req: 4.1, 4.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "{$yield_distance_as: my_dist}=>@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] == 3
        # Check that the custom distance field name is used
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "my_dist" in fields, f"Expected 'my_dist' field in {key}"

    # =================================================================
    # 12. Query attributes: $epsilon
    # =================================================================

    def test_query_attr_epsilon(self):
        """
        $epsilon query attribute is accepted and does not cause errors.
        Req: 4.2
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "{$epsilon: 0.5}=>@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] >= 1

    # =================================================================
    # 13. Query attributes: both $yield_distance_as and $epsilon
    # =================================================================

    def test_query_attr_both(self):
        """
        Both $yield_distance_as and $epsilon together.
        Req: 4.1, 4.2
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "{$yield_distance_as: dist; $epsilon: 0.1}=>@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] >= 1
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "dist" in fields

    # =================================================================
    # 14. Default distance field naming: __<field>_score
    # =================================================================

    def test_default_distance_field_name(self):
        """
        Without $yield_distance_as, distance field defaults to __vec_score.
        Req: 4.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] >= 1
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "__vec_score" in fields, f"Expected '__vec_score' in {key}, got {list(fields.keys())}"


    # =================================================================
    # 15. Results sorted by ascending distance (default)
    # =================================================================

    def test_results_ascending_distance_order(self):
        """
        Standalone Vector Range results are sorted by ascending distance.
        Req: 5.1, 5.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] == 5
        parsed = parse_result_with_fields(result)
        # Extract distances in result order
        distances = []
        for i in range(1, len(result), 2):
            fields = result[i + 1]
            field_dict = {
                fields[j].decode("utf-8"): fields[j + 1]
                for j in range(0, len(fields), 2)
            }
            dist = float(field_dict["__vec_score"])
            distances.append(dist)
        # Verify non-decreasing order
        for i in range(len(distances) - 1):
            assert distances[i] <= distances[i + 1], (
                f"Distances not in ascending order: {distances}"
            )

    # =================================================================
    # 16. Distance scores are correct
    # =================================================================

    def test_distance_scores_correct(self):
        """
        Returned distance scores match expected L2 distances.
        Req: 5.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
        )
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            actual_dist = float(fields["__vec_score"])
            expected_dist = l2_distance(QUERY_VEC, VECTORS[key])
            assert abs(actual_dist - expected_dist) < 0.01, (
                f"{key}: expected dist {expected_dist}, got {actual_dist}"
            )

    # =================================================================
    # 17. SORTBY overrides default distance ordering
    # =================================================================

    def test_sortby_overrides_distance_order(self):
        """
        SORTBY on a non-distance field overrides default ascending distance.
        Req: 5.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["score", "NUMERIC"])
        extra = {
            "doc:0": {"score": "50"},
            "doc:1": {"score": "10"},
            "doc:2": {"score": "30"},
            "doc:3": {"score": "20"},
            "doc:4": {"score": "40"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "SORTBY", "score", "ASC",
            "RETURN", "1", "score",
        )
        assert result[0] == 5
        # Extract scores in result order
        scores = []
        for i in range(1, len(result), 2):
            fields = result[i + 1]
            field_dict = {
                fields[j].decode("utf-8"): fields[j + 1]
                for j in range(0, len(fields), 2)
            }
            scores.append(float(field_dict["score"]))
        assert scores == sorted(scores), f"Scores not in ASC order: {scores}"

    # =================================================================
    # 18. LIMIT applied after sorting
    # =================================================================

    def test_limit_after_sort(self):
        """
        LIMIT offset/count slicing works correctly with Vector Range results.
        Req: 5.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        # Get all 5 results first
        full_result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert full_result[0] == 5

        # LIMIT 1 2: skip first, take next 2
        limited_result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "LIMIT", "1", "2",
            "NOCONTENT",
        )
        # Total count should still be 5
        assert limited_result[0] == 5
        # But only 2 keys returned
        returned_keys = [limited_result[i] for i in range(1, len(limited_result))]
        assert len(returned_keys) == 2

    # =================================================================
    # 19. LIMIT 0 0 returns only count
    # =================================================================

    def test_limit_zero(self):
        """
        LIMIT 0 0 returns only the total count, no keys.
        Req: 5.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "LIMIT", "0", "0",
        )
        assert result[0] == 5
        assert len(result) == 1  # only count

    # =================================================================
    # 20. NOCONTENT suppresses fields and distance scores
    # =================================================================

    def test_nocontent(self):
        """
        NOCONTENT returns only key names, no fields or distance scores.
        Req: 6.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        count = result[0]
        assert count == 3
        # With NOCONTENT, result is [count, key1, key2, ...]
        assert len(result) == count + 1
        for i in range(1, len(result)):
            assert isinstance(result[i], bytes)  # just key names

    # =================================================================
    # 21. RETURN includes distance score plus specified fields
    # =================================================================

    def test_return_with_fields(self):
        """
        RETURN includes specified fields plus the distance score field.
        Req: 6.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["category", "TAG"])
        extra = {k: {"category": "test"} for k in VECTORS}
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "RETURN", "2", "category", "__vec_score",
        )
        assert result[0] == 3
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "__vec_score" in fields, f"Missing distance score in {key}"
            assert "category" in fields, f"Missing category in {key}"


    # =================================================================
    # 22. Error: missing PARAMS for vector blob
    # =================================================================

    def test_error_missing_blob_param(self):
        """
        Missing PARAMS entry for the vector blob parameter returns error.
        Req: 1.6, 7.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $nonexistent]",
                "PARAMS", "0",
            )

    # =================================================================
    # 23. Error: dimension mismatch
    # =================================================================

    def test_error_dimension_mismatch(self):
        """
        Vector blob with wrong dimensions returns error.
        Req: 1.7
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, dim=3)
        self._load_vector_data(client)

        # Provide a 2D vector for a 3D index
        wrong_blob = float_to_bytes([1.0, 2.0])
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob]",
                "PARAMS", "2", "blob", wrong_blob,
            )

    # =================================================================
    # 24. Error: missing closing bracket
    # =================================================================

    def test_error_missing_closing_bracket(self):
        """
        Missing closing ] in Vector Range clause returns parse error.
        Req: 7.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 25. Error: missing radius
    # =================================================================

    def test_error_missing_radius(self):
        """
        VECTOR_RANGE without radius returns error.
        Req: 7.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError, match="radius"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 26. Error: missing vector blob parameter
    # =================================================================

    def test_error_missing_blob_in_syntax(self):
        """
        VECTOR_RANGE with radius but no blob param returns error.
        Req: 7.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        with pytest.raises(ResponseError, match="blob"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5]",
                "PARAMS", "0",
            )

    # =================================================================
    # 27. Error: negative radius
    # =================================================================

    def test_error_negative_radius(self):
        """
        Negative radius returns error.
        Req: 1.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError, match="non-negative"):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE -1 $blob]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 28. Error: unknown optional parameter
    # =================================================================

    def test_error_unknown_optional_param(self):
        """
        Unknown keyword in optional params section returns error.
        Req: 7.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@vec:[VECTOR_RANGE 5 $blob UNKNOWN_PARAM 42]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 29. Error: non-vector field
    # =================================================================

    def test_error_non_vector_field(self):
        """
        Using VECTOR_RANGE on a non-vector field returns error.
        Req: 1.4
        """
        client = self.server.get_new_client()
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "price", "NUMERIC",
        ]
        assert client.execute_command(*cmd) == b"OK"
        client.hset("doc:0", mapping={"price": "10"})

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "@price:[VECTOR_RANGE 5 $blob]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 30. Error: empty $yield_distance_as
    # =================================================================

    def test_error_empty_yield_distance_as(self):
        """
        Empty $yield_distance_as value returns error.
        Req: 4.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "{$yield_distance_as: }=>@vec:[VECTOR_RANGE 5 $blob]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 31. Error: invalid $epsilon
    # =================================================================

    def test_error_invalid_epsilon(self):
        """
        Invalid $epsilon value returns error.
        Req: 4.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "{$epsilon: notanumber}=>@vec:[VECTOR_RANGE 5 $blob]",
                "PARAMS", "2", "blob", query_blob,
            )

    # =================================================================
    # 32. Error: negative $epsilon
    # =================================================================

    def test_error_negative_epsilon(self):
        """
        Negative $epsilon value returns error.
        Req: 4.5
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        with pytest.raises(ResponseError):
            self._search(
                client, "idx",
                "{$epsilon: -0.5}=>@vec:[VECTOR_RANGE 5 $blob]",
                "PARAMS", "2", "blob", query_blob,
            )


    # =================================================================
    # 33. DIALECT 2 compatibility
    # =================================================================

    def test_dialect_2(self):
        """
        Vector Range query works with DIALECT 2.
        Req: 6.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "DIALECT", "2",
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 34. DIALECT 3 compatibility
    # =================================================================

    def test_dialect_3(self):
        """
        Vector Range query works with DIALECT 3.
        Req: 6.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "DIALECT", "3",
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 35. DIALECT 4 compatibility
    # =================================================================

    def test_dialect_4(self):
        """
        Vector Range query works with DIALECT 4.
        Req: 6.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "DIALECT", "4",
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 36. Parameterized radius via $param
    # =================================================================

    def test_parameterized_radius(self):
        """
        Radius specified as $param is resolved from PARAMS.
        Req: 1.2
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE $rad $blob]",
            "PARAMS", "4", "rad", "5", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 3
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1", "doc:2"}

    # =================================================================
    # 37. Optional params: EF_RUNTIME
    # =================================================================

    def test_optional_param_ef_runtime(self):
        """
        EF_RUNTIME optional parameter is accepted in HNSW Vector Range.
        Req: 1.3
        """
        client = self.server.get_new_client()
        self._create_hnsw_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob EF_RUNTIME 100]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] >= 1

    # =================================================================
    # 38. Optional params: AS (alias for distance field)
    # =================================================================

    def test_optional_param_as(self):
        """
        AS optional parameter sets the distance field name.
        Req: 1.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client)
        self._load_vector_data(client)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob AS my_score]",
            "PARAMS", "2", "blob", query_blob,
        )
        assert result[0] >= 1
        parsed = parse_result_with_fields(result)
        for key, fields in parsed.items():
            assert "my_score" in fields, f"Expected 'my_score' in {key}"

    # =================================================================
    # 39. Multiple Vector Range predicates in one query
    # =================================================================

    def test_multiple_vector_range_predicates(self):
        """
        Multiple Vector Range clauses combined with AND.
        Req: 2.2
        """
        client = self.server.get_new_client()
        # Two vector fields
        cmd = [
            "FT.CREATE", "idx", "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "vec1", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
            "vec2", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2",
        ]
        assert client.execute_command(*cmd) == b"OK"

        # doc:0 close on both, doc:1 close on vec1 only, doc:2 close on vec2 only
        client.hset("doc:0", mapping={
            "vec1": float_to_bytes([0.0, 0.0, 0.0]),
            "vec2": float_to_bytes([0.0, 0.0, 0.0]),
        })
        client.hset("doc:1", mapping={
            "vec1": float_to_bytes([1.0, 0.0, 0.0]),
            "vec2": float_to_bytes([10.0, 0.0, 0.0]),
        })
        client.hset("doc:2", mapping={
            "vec1": float_to_bytes([10.0, 0.0, 0.0]),
            "vec2": float_to_bytes([1.0, 0.0, 0.0]),
        })

        blob1 = float_to_bytes([0.0, 0.0, 0.0])
        blob2 = float_to_bytes([0.0, 0.0, 0.0])
        # Both ranges radius=2: vec1 matches doc:0,doc:1; vec2 matches doc:0,doc:2
        # AND = doc:0
        result = self._search(
            client, "idx",
            "@vec1:[VECTOR_RANGE 2 $b1] @vec2:[VECTOR_RANGE 2 $b2]",
            "PARAMS", "4", "b1", blob1, "b2", blob2,
            "NOCONTENT",
        )
        assert result[0] == 1
        keys = parse_result_keys(result)
        assert keys == {"doc:0"}

    # =================================================================
    # 40. COSINE distance metric
    # =================================================================

    def test_cosine_distance_metric(self):
        """
        Vector Range works with COSINE distance metric.
        Req: 2.1
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, distance="COSINE")

        # Cosine distance: 1 - cos(angle)
        # Same direction = 0, orthogonal = 1, opposite = 2
        client.hset("doc:0", mapping={"vec": float_to_bytes([1.0, 0.0, 0.0])})
        client.hset("doc:1", mapping={"vec": float_to_bytes([0.9, 0.1, 0.0])})
        client.hset("doc:2", mapping={"vec": float_to_bytes([0.0, 1.0, 0.0])})
        client.hset("doc:3", mapping={"vec": float_to_bytes([-1.0, 0.0, 0.0])})

        query_blob = float_to_bytes([1.0, 0.0, 0.0])
        # Small radius should match only very similar vectors
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 0.1 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        # doc:0 (distance=0) and doc:1 (very small distance) should match
        assert result[0] >= 1
        keys = parse_result_keys(result)
        assert "doc:0" in keys

    # =================================================================
    # 41. SORTBY DESC with Vector Range
    # =================================================================

    def test_sortby_desc(self):
        """
        SORTBY field DESC overrides default ascending distance order.
        Req: 5.3
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["priority", "NUMERIC"])
        extra = {
            "doc:0": {"priority": "1"},
            "doc:1": {"priority": "3"},
            "doc:2": {"priority": "2"},
        }
        vectors = {
            "doc:0": [0.0, 0.0, 0.0],
            "doc:1": [1.0, 0.0, 0.0],
            "doc:2": [2.0, 0.0, 0.0],
        }
        self._load_vector_data(client, vectors=vectors, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 100 $blob]",
            "PARAMS", "2", "blob", query_blob,
            "SORTBY", "priority", "DESC",
            "RETURN", "1", "priority",
        )
        assert result[0] == 3
        # Extract priorities in result order
        priorities = []
        for i in range(1, len(result), 2):
            fields = result[i + 1]
            field_dict = {
                fields[j].decode("utf-8"): fields[j + 1]
                for j in range(0, len(fields), 2)
            }
            priorities.append(int(field_dict["priority"]))
        assert priorities == sorted(priorities, reverse=True), (
            f"Priorities not in DESC order: {priorities}"
        )

    # =================================================================
    # 42. Vector Range with tag OR filter in parentheses
    # =================================================================

    def test_vector_range_with_tag_or_in_parens(self):
        """
        Vector Range AND (tag1 OR tag2) using parentheses.
        Req: 2.3, 2.4
        """
        client = self.server.get_new_client()
        self._create_flat_index(client, extra_fields=["color", "TAG"])
        extra = {
            "doc:0": {"color": "red"},
            "doc:1": {"color": "blue"},
            "doc:2": {"color": "green"},
            "doc:3": {"color": "red"},
            "doc:4": {"color": "blue"},
        }
        self._load_vector_data(client, extra_data=extra)

        query_blob = float_to_bytes(QUERY_VEC)
        # radius=5 matches doc:0,1,2; color red|blue matches doc:0,1,3,4
        # AND = doc:0, doc:1
        result = self._search(
            client, "idx",
            "@vec:[VECTOR_RANGE 5 $blob] @color:{red|blue}",
            "PARAMS", "2", "blob", query_blob,
            "NOCONTENT",
        )
        assert result[0] == 2
        keys = parse_result_keys(result)
        assert keys == {"doc:0", "doc:1"}
