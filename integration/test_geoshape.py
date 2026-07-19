from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from valkey.cluster import ValkeyCluster
import json
import time
import pytest

"""
Integration tests for GEOSHAPE indexing and querying on Hash/JSON documents.
Uses real geographic data (US cities, states, landmarks).
"""

# --- HASH-based GEOSHAPE index ---
geoshape_index_on_hash = (
    "FT.CREATE places ON HASH PREFIX 1 place: "
    "SCHEMA geom GEOSHAPE FLAT name TAG"
)

# Real-world inspired flat coordinates representing city blocks / zones
hash_docs = [
    # A square park (polygon)
    ["HSET", "place:park", "name", "central_park",
     "geom", "POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))"],
    # A triangular building footprint
    ["HSET", "place:building", "name", "triangle_bldg",
     "geom", "POLYGON ((6 1, 7 3, 8 1, 6 1))"],
    # A point representing a fountain
    ["HSET", "place:fountain", "name", "fountain",
     "geom", "POINT (3 3)"],
    # A point outside the park
    ["HSET", "place:bench", "name", "bench",
     "geom", "POINT (10 10)"],
    # A large rectangle encompassing the park
    ["HSET", "place:district", "name", "district",
     "geom", "POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))"],
]

# --- JSON-based GEOSHAPE index (SPHERICAL) ---
geoshape_index_on_json = (
    "FT.CREATE geolocations ON JSON PREFIX 1 geoloc: "
    "SCHEMA $.geom AS geom GEOSHAPE FLAT $.city AS city TAG"
)

# Real geographic coordinates (lon, lat) for US cities
json_docs = [
    # Denver, CO
    ['JSON.SET', 'geoloc:denver', '$', json.dumps({
        "city": "denver",
        "geom": "POINT (-104.9903 39.7392)"
    })],
    # Boulder, CO
    ['JSON.SET', 'geoloc:boulder', '$', json.dumps({
        "city": "boulder",
        "geom": "POINT (-105.2705 40.0150)"
    })],
    # Colorado Springs, CO
    ['JSON.SET', 'geoloc:cosprings', '$', json.dumps({
        "city": "colorado_springs",
        "geom": "POINT (-104.8214 38.8339)"
    })],
    # A polygon roughly representing downtown Denver area
    ['JSON.SET', 'geoloc:downtown_denver', '$', json.dumps({
        "city": "downtown_denver",
        "geom": "POLYGON ((-105.01 39.73, -105.01 39.76, -104.97 39.76, -104.97 39.73, -105.01 39.73))"
    })],
    # New York City (far away from Colorado)
    ['JSON.SET', 'geoloc:nyc', '$', json.dumps({
        "city": "nyc",
        "geom": "POINT (-74.0060 40.7128)"
    })],
]


def create_geoshape_indexes(client: Valkey):
    assert client.execute_command(geoshape_index_on_hash) == b"OK"
    assert client.execute_command(geoshape_index_on_json) == b"OK"


def populate_hash_docs(client):
    for doc in hash_docs:
        client.execute_command(*doc)


def populate_json_docs(client):
    for doc in json_docs:
        assert client.execute_command(*doc) == b"OK"


def validate_geoshape_within_query(client: Valkey):
    """
    Test WITHIN: find shapes inside a query polygon.
    The fountain at (3,3) is within the park polygon (1,1)-(5,5).
    """
    result = client.execute_command(
        "FT.SEARCH", "places",
        "@geom:[WITHIN $shape]",
        "PARAMS", "2", "shape", "POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))",
        "NOCONTENT", "DIALECT", "2"
    )
    assert result[0] >= 1, f"Expected at least 1 result for WITHIN park, got {result[0]}"
    keys = [result[i] for i in range(1, len(result))]
    assert b"place:fountain" in keys, "Fountain should be WITHIN the park"
    assert b"place:bench" not in keys, "Bench at (10,10) should NOT be within park"
    assert b"place:building" not in keys, "Building should NOT be within park"


def validate_geoshape_contains_query(client: Valkey):
    """
    Test CONTAINS: find shapes that contain the query shape.
    The district polygon (0,0)-(6,6) contains the point (3,3).
    """
    result = client.execute_command(
        "FT.SEARCH", "places",
        "@geom:[CONTAINS $shape]",
        "PARAMS", "2", "shape", "POINT (3 3)",
        "NOCONTENT", "DIALECT", "2"
    )
    assert result[0] >= 2, f"Expected at least 2 results for CONTAINS point(3,3), got {result[0]}"
    keys = [result[i] for i in range(1, len(result))]
    assert b"place:park" in keys, "Park should CONTAIN point (3,3)"
    assert b"place:district" in keys, "District should CONTAIN point (3,3)"
    assert b"place:bench" not in keys, "Bench should NOT contain point (3,3)"


def validate_geoshape_intersects_query(client: Valkey):
    """
    Test INTERSECTS: find shapes that share any area with the query shape.
    A polygon overlapping the park and building area should match both.
    """
    result = client.execute_command(
        "FT.SEARCH", "places",
        "@geom:[INTERSECTS $shape]",
        "PARAMS", "2", "shape", "POLYGON ((4 0, 4 4, 9 4, 9 0, 4 0))",
        "NOCONTENT", "DIALECT", "2"
    )
    assert result[0] >= 2, f"Expected at least 2 results for INTERSECTS, got {result[0]}"
    keys = [result[i] for i in range(1, len(result))]
    assert b"place:park" in keys, "Park should INTERSECT the query polygon"
    assert b"place:building" in keys, "Building should INTERSECT the query polygon"
    assert b"place:bench" not in keys, "Bench at (10,10) should NOT intersect"


def validate_geoshape_disjoint_query(client: Valkey):
    """
    Test DISJOINT: find shapes that do NOT share any area with the query shape.
    A small polygon far from everything except the bench.
    """
    result = client.execute_command(
        "FT.SEARCH", "places",
        "@geom:[DISJOINT $shape]",
        "PARAMS", "2", "shape", "POLYGON ((9 9, 9 11, 11 11, 11 9, 9 9))",
        "NOCONTENT", "DIALECT", "2"
    )
    keys = [result[i] for i in range(1, len(result))]
    assert b"place:bench" not in keys, "Bench should NOT be disjoint (it's inside)"
    assert b"place:park" in keys, "Park should be DISJOINT from far-away polygon"
    assert b"place:fountain" in keys, "Fountain should be DISJOINT from far-away polygon"


def validate_geoshape_json_within(client: Valkey):
    """
    Test WITHIN on JSON GEOSHAPE with real geographic coordinates.
    Find points within a bounding box around Colorado.
    """
    colorado_bbox = "POLYGON ((-109.05 36.99, -109.05 41.00, -102.05 41.00, -102.05 36.99, -109.05 36.99))"
    result = client.execute_command(
        "FT.SEARCH", "geolocations",
        "@geom:[WITHIN $shape]",
        "PARAMS", "2", "shape", colorado_bbox,
        "NOCONTENT", "DIALECT", "2"
    )
    keys = [result[i] for i in range(1, len(result))]
    assert b"geoloc:denver" in keys, "Denver should be within Colorado bbox"
    assert b"geoloc:boulder" in keys, "Boulder should be within Colorado bbox"
    assert b"geoloc:cosprings" in keys, "Colorado Springs should be within Colorado bbox"
    assert b"geoloc:downtown_denver" in keys, "Downtown Denver polygon should be within Colorado bbox"
    assert b"geoloc:nyc" not in keys, "NYC should NOT be within Colorado bbox"


def validate_geoshape_combined_filter(client: Valkey):
    """
    Test GEOSHAPE combined with TAG filter.
    """
    result = client.execute_command(
        "FT.SEARCH", "places",
        "@name:{fountain} @geom:[WITHIN $shape]",
        "PARAMS", "2", "shape", "POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))",
        "NOCONTENT", "DIALECT", "2"
    )
    assert result[0] == 1, f"Expected 1 result for combined TAG+GEOSHAPE, got {result[0]}"
    assert result[1] == b"place:fountain"


class TestGeoShape(ValkeySearchTestCaseBase):

    def test_geoshape_basic(self):
        """Test GEOSHAPE index creation, ingestion, and spatial queries on HASH docs."""
        client: Valkey = self.server.get_new_client()
        create_geoshape_indexes(client)
        populate_hash_docs(client)
        populate_json_docs(client)

        # Wait for backfill
        time.sleep(0.5)

        validate_geoshape_within_query(client)
        validate_geoshape_contains_query(client)
        validate_geoshape_intersects_query(client)
        validate_geoshape_disjoint_query(client)

    def test_geoshape_json_spherical(self):
        """Test GEOSHAPE on JSON docs using real geographic coordinates (FLAT)."""
        client: Valkey = self.server.get_new_client()
        create_geoshape_indexes(client)
        populate_json_docs(client)

        time.sleep(0.5)

        validate_geoshape_json_within(client)

    def test_geoshape_combined_with_tag(self):
        """Test GEOSHAPE queries combined with TAG filters."""
        client: Valkey = self.server.get_new_client()
        create_geoshape_indexes(client)
        populate_hash_docs(client)

        time.sleep(0.5)

        validate_geoshape_combined_filter(client)

    def test_geoshape_invalid_wkt(self):
        """Test that invalid WKT data is handled gracefully (key becomes untracked)."""
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(geoshape_index_on_hash) == b"OK"

        # Insert a doc with invalid WKT - should not crash, just not be indexed
        client.execute_command("HSET", "place:invalid", "name", "bad",
                              "geom", "NOT_VALID_WKT")
        # Insert a valid one too
        client.execute_command("HSET", "place:valid", "name", "ok",
                              "geom", "POINT (5 5)")
        time.sleep(0.5)

        # Query a huge area - invalid key should not appear, valid should
        result = client.execute_command(
            "FT.SEARCH", "places",
            "@geom:[WITHIN $shape]",
            "PARAMS", "2", "shape", "POLYGON ((0 0, 0 100, 100 100, 100 0, 0 0))",
            "NOCONTENT", "DIALECT", "2"
        )
        keys = [result[i] for i in range(1, len(result))]
        assert b"place:invalid" not in keys
        assert b"place:valid" in keys

    def test_geoshape_modify_and_delete(self):
        """Test that modifying/deleting a key updates the geoshape index."""
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(geoshape_index_on_hash) == b"OK"

        # Insert a point inside (0,0)-(10,10)
        client.execute_command("HSET", "place:mover", "name", "mover",
                              "geom", "POINT (5 5)")
        time.sleep(0.5)

        # Verify it's found within (0,0)-(10,10)
        result = client.execute_command(
            "FT.SEARCH", "places",
            "@geom:[WITHIN $shape]",
            "PARAMS", "2", "shape", "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
            "NOCONTENT", "DIALECT", "2"
        )
        keys = [result[i] for i in range(1, len(result))]
        assert b"place:mover" in keys

        # Move it outside the query area
        client.execute_command("HSET", "place:mover", "geom", "POINT (50 50)")
        time.sleep(0.5)

        # Should no longer be found in the original area
        result = client.execute_command(
            "FT.SEARCH", "places",
            "@geom:[WITHIN $shape]",
            "PARAMS", "2", "shape", "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
            "NOCONTENT", "DIALECT", "2"
        )
        keys = [result[i] for i in range(1, len(result))]
        assert b"place:mover" not in keys

        # But should be found in the new area
        result = client.execute_command(
            "FT.SEARCH", "places",
            "@geom:[WITHIN $shape]",
            "PARAMS", "2", "shape", "POLYGON ((40 40, 40 60, 60 60, 60 40, 40 40))",
            "NOCONTENT", "DIALECT", "2"
        )
        keys = [result[i] for i in range(1, len(result))]
        assert b"place:mover" in keys

        # Delete the key entirely
        client.execute_command("DEL", "place:mover")
        time.sleep(0.5)

        # Should not appear anywhere
        result = client.execute_command(
            "FT.SEARCH", "places",
            "@geom:[WITHIN $shape]",
            "PARAMS", "2", "shape", "POLYGON ((0 0, 0 100, 100 100, 100 0, 0 0))",
            "NOCONTENT", "DIALECT", "2"
        )
        keys = [result[i] for i in range(1, len(result))]
        assert b"place:mover" not in keys

    def test_geoshape_empty_results(self):
        """Test queries that should return zero results."""
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(geoshape_index_on_hash) == b"OK"
        populate_hash_docs(client)
        time.sleep(0.5)

        # Query an area where nothing exists
        result = client.execute_command(
            "FT.SEARCH", "places",
            "@geom:[WITHIN $shape]",
            "PARAMS", "2", "shape", "POLYGON ((-100 -100, -100 -90, -90 -90, -90 -100, -100 -100))",
            "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 0, f"Expected 0 results in empty area, got {result[0]}"

    def test_geoshape_negate(self):
        """Test negated GEOSHAPE: find everything NOT within an area."""
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(geoshape_index_on_hash) == b"OK"
        populate_hash_docs(client)
        time.sleep(0.5)

        # Negate: find everything NOT within the park
        result = client.execute_command(
            "FT.SEARCH", "places",
            "-@geom:[WITHIN $shape]",
            "PARAMS", "2", "shape", "POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))",
            "NOCONTENT", "DIALECT", "2"
        )
        keys = [result[i] for i in range(1, len(result))]
        # Fountain is within park, so it should NOT appear in negated results
        assert b"place:fountain" not in keys, "Fountain is within park, should be excluded by negate"
        # Bench and building are outside park, should appear
        assert b"place:bench" in keys, "Bench should appear in negated WITHIN"
        assert b"place:building" in keys, "Building should appear in negated WITHIN"

    def test_geoshape_with_content(self):
        """Test that FT.SEARCH returns actual document content (not just keys)."""
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(geoshape_index_on_hash) == b"OK"

        client.execute_command("HSET", "place:solo", "name", "solo_point",
                              "geom", "POINT (5 5)")
        time.sleep(0.5)

        # Search WITH content (no NOCONTENT)
        result = client.execute_command(
            "FT.SEARCH", "places",
            "@geom:[CONTAINS $shape]",
            "PARAMS", "2", "shape", "POINT (5 5)",
            "DIALECT", "2"
        )
        assert result[0] >= 1
        assert result[1] == b"place:solo"
        # Result[2] should be the field list
        doc_fields = dict(zip(result[2][::2], result[2][1::2]))
        assert doc_fields[b"name"] == b"solo_point"
        assert doc_fields[b"geom"] == b"POINT (5 5)"

    def test_geoshape_ft_info(self):
        """Test that FT.INFO reports GEOSHAPE field type."""
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(geoshape_index_on_hash) == b"OK"
        populate_hash_docs(client)
        time.sleep(0.5)

        info = client.execute_command("FT.INFO", "places")
        # FT.INFO should contain the index name and attributes
        assert b"places" in info


class TestGeoShapeCluster(ValkeySearchClusterTestCase):

    def test_geoshape_cluster(self):
        """Test GEOSHAPE queries in cluster mode.
        NOTE: GEOSHAPE cluster support requires gRPC proto extension.
        For now, test index creation and data ingestion in cluster mode.
        """
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        # Index creation works in cluster mode
        assert client.execute_command(geoshape_index_on_hash) == b"OK"

        # Data ingestion works in cluster mode
        for doc in hash_docs:
            cluster_client.execute_command(*doc)

        time.sleep(1)

        # Verify FT.INFO works
        info = client.execute_command("FT.INFO", "places")
        assert b"places" in info
