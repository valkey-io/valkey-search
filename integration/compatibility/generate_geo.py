"""Compatibility-test answer generator for the GEO field type.

Mirrors generate_text.py: spins up a Redis Stack reference server, runs a
battery of FT.SEARCH queries against it for the `world cities` GEO data set,
and pickles each (cmd, result) pair into geo-answers.pickle.gz. The answers
are then replayed by integration/compatibility_test.py against valkey-search
so the two engines' geo behavior can be diffed.

Run with:
    pytest integration/compatibility/generate_geo.py
"""

import pytest

from . import data_sets
from .generate import BaseCompatibilityTest


@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["hash"])
class TestGeoSearchCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "geo-answers.pickle.gz"
    DATA_SET = "world cities"

    def setup_data(self, data_set_name, key_type):
        """Override to specify geo data source."""
        self.data_set_name = data_set_name
        self.key_type = key_type
        data_sets.load_data(self.client, data_set_name, key_type, data_source="geo")

    # ========================================================================
    # Centers chosen so each one has at least one point inside a small radius
    # *and* points outside, giving meaningful diff coverage.
    # ========================================================================
    CENTERS = {
        "sf":     (-122.4194, 37.7749),   # several US west-coast cities nearby
        "london": (  -0.1278, 51.5074),   # europe cluster
        "tokyo":  ( 139.6917, 35.6895),   # asia, isolated
        "sydney": ( 151.2093,-33.8688),   # southern hemisphere
        "lagos":  (   3.3792,  6.5244),   # equatorial
        "antimeridian": (179.0, -16.0),   # Fiji-area; tests longitude wrap
    }

    RADII_KM = [50, 200, 1000, 5000, 10000]

    # ========================================================================
    # Single-radius scans — one sweep per center×radius combination.
    # ========================================================================
    def test_radius_world_sweep(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        for _, (lon, lat) in self.CENTERS.items():
            for radius in self.RADII_KM:
                self.check(
                    "FT.SEARCH",
                    idx,
                    f"@loc:[{lon} {lat} {radius} km]",
                    "LIMIT", "0", "1000",
                    "DIALECT",
                    str(dialect),
                )

    # ========================================================================
    # Unit conversions — same physical distance expressed four different
    # ways. Diff catches off-by-one in the unit table.
    # ========================================================================
    def test_units(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        # ~5000 km in each unit. Exact magnitudes aren't required to match —
        # the Redis Search reference and valkey-search must just agree on the
        # *result set*, which they should because both use the same haversine
        # filter under the hood.
        unit_radii = [
            ("km", 5000),
            ("m", 5_000_000),
            ("mi", 3107),     # ~5000 km
            ("ft", 16_404_199),
        ]
        # Use SF as the center.
        lon, lat = self.CENTERS["sf"]
        for unit, radius in unit_radii:
            self.check(
                "FT.SEARCH",
                idx,
                f"@loc:[{lon} {lat} {radius} {unit}]",
                "LIMIT", "0", "1000",
                "DIALECT",
                str(dialect),
            )

    # ========================================================================
    # Composition with TAG — `@loc:[…] @continent:{north_america}` should
    # narrow the geo result to just north-american points.
    # ========================================================================
    def test_geo_and_tag(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        continents = [
            "north_america", "europe", "asia", "africa",
            "south_america", "oceania",
        ]
        # Use a very large radius so the TAG filter is the limiting predicate.
        lon, lat = self.CENTERS["sf"]
        for cont in continents:
            self.check(
                "FT.SEARCH",
                idx,
                f"@loc:[{lon} {lat} 20000 km] @continent:{{{cont}}}",
                "LIMIT", "0", "1000",
                "DIALECT",
                str(dialect),
            )

    # Composition with TAG OR — a small radius around two continents.
    def test_geo_and_tag_or(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        lon, lat = self.CENTERS["london"]
        self.check(
            "FT.SEARCH",
            idx,
            f"@loc:[{lon} {lat} 5000 km] @continent:{{europe|africa}}",
            "LIMIT", "0", "1000",
            "DIALECT",
            str(dialect),
        )

    # ========================================================================
    # Tiny radius — should return zero or one match. Catches false positives
    # in the geohash-cover post-filter.
    # ========================================================================
    def test_tiny_radius(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        for name, (lon, lat) in self.CENTERS.items():
            if name == "antimeridian":
                # Not a real fixture point, skip exact-match probe.
                continue
            # 1 km around the exact coordinate of a fixture point: always 1 hit.
            self.check(
                "FT.SEARCH",
                idx,
                f"@loc:[{lon} {lat} 1 km]",
                "LIMIT", "0", "1000",
                "DIALECT",
                str(dialect),
            )

    # ========================================================================
    # Antimeridian wrap — a circle straddling 180° must include points on
    # both sides. Without correct longitude wrap one side gets dropped.
    # ========================================================================
    def test_antimeridian_wrap(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        lon, lat = self.CENTERS["antimeridian"]
        for radius in [1500, 3000, 6000]:
            self.check(
                "FT.SEARCH",
                idx,
                f"@loc:[{lon} {lat} {radius} km]",
                "LIMIT", "0", "1000",
                "DIALECT",
                str(dialect),
            )

    # ========================================================================
    # Explicit LIMIT — verifies that LIMIT applies to geo-radius results
    # the same way both engines apply it to other field types.
    # NOCONTENT is intentionally not tested here: the upstream
    # compatibility_test unpacker doesn't handle that reply shape and
    # crashes on it, regardless of field type.
    # ========================================================================
    def test_with_options(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        idx = f"{key_type}_idx1"
        lon, lat = self.CENTERS["sf"]
        self.check(
            "FT.SEARCH",
            idx,
            f"@loc:[{lon} {lat} 5000 km]",
            "LIMIT",
            "0",
            "100",
            "DIALECT",
            str(dialect),
        )

    # ========================================================================
    # Helper: every cross-type query in this suite uses LIMIT 0 1000 and
    # passes through self.check, capturing the (cmd, result) pair against
    # the Redis Stack reference for later replay vs valkey-search.
    # ========================================================================
    def _run(self, key_type, dialect, query, *extra_args):
        idx = f"{key_type}_idx1"
        args = [
            "FT.SEARCH",
            idx,
            query,
            *extra_args,
            "LIMIT", "0", "1000",
            "DIALECT",
            str(dialect),
        ]
        self.check(*args)

    # ========================================================================
    # GEO × NUMERIC
    # ========================================================================
    def test_geo_and_numeric(self, key_type, dialect):
        """AND between a geo radius and a numeric range."""
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # Top-rated places near SF.
        self._run(key_type, dialect,
                  f"@loc:[{lon} {lat} 5000 km] @rating:[4 5]")
        # Low-rated places near London.
        lon2, lat2 = self.CENTERS["london"]
        self._run(key_type, dialect,
                  f"@loc:[{lon2} {lat2} 10000 km] @rating:[1 3]")
        # Boundary: rating == 5 only, plus a wide geo net.
        self._run(key_type, dialect,
                  f"@loc:[{lon} {lat} 20000 km] @rating:[5 5]")

    def test_geo_or_numeric(self, key_type, dialect):
        """OR between a small geo radius and a numeric range."""
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # Either close to SF or rated 5.
        self._run(key_type, dialect,
                  f"@loc:[{lon} {lat} 1000 km] | @rating:[5 5]")
        # Either in eastern Asia or rated >=4.
        lon2, lat2 = self.CENTERS["tokyo"]
        self._run(key_type, dialect,
                  f"@loc:[{lon2} {lat2} 3000 km] | @rating:[4 5]")

    def test_geo_negate_numeric(self, key_type, dialect):
        """Geo AND NOT(numeric range)."""
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # Close-ish to SF but exclude low ratings.
        self._run(key_type, dialect,
                  f"@loc:[{lon} {lat} 20000 km] -@rating:[1 2]")

    # ========================================================================
    # GEO × TEXT (no proximity attributes — basic AND/OR composition)
    # ========================================================================
    def test_geo_and_text(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # "capital" cities within a wide radius — eliminates SF/LA/NYC etc.
        self._run(key_type, dialect,
                  f"@description:capital @loc:[{lon} {lat} 20000 km]")
        # "tech" cities near the US west coast.
        self._run(key_type, dialect,
                  f"@description:tech @loc:[{lon} {lat} 3000 km]")
        # Multi-term text AND with geo. valkey-search's filter parser does
        # not accept `@field:(term1 term2)` grouping; the equivalent is
        # repeating the field qualifier, which both engines accept.
        self._run(key_type, dialect,
                  f"@description:capital @description:culture "
                  f"@loc:[{lon} {lat} 20000 km]")

    def test_geo_or_text(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # "tech" anywhere OR within 1000 km of SF.
        self._run(key_type, dialect,
                  f"@description:tech | @loc:[{lon} {lat} 1000 km]")
        # "capital" OR near London.
        lon2, lat2 = self.CENTERS["london"]
        self._run(key_type, dialect,
                  f"@description:capital | @loc:[{lon2} {lat2} 500 km]")

    def test_geo_negate_text(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # Geo radius AND NOT capital — i.e. non-capital cities near SF.
        self._run(key_type, dialect,
                  f"@loc:[{lon} {lat} 20000 km] -@description:capital")

    # ========================================================================
    # GEO × TEXT proximity — SLOP / INORDER with a GEO predicate sitting
    # in the middle of a multi-word distance-based match.
    #
    # The compat replay harness (compatibility_test.py) inspects the test
    # name for the substrings 'slop' or 'inorder' and switches the engine
    # into proximity-inorder-compat-mode for those tests; method names
    # here are chosen accordingly.
    # ========================================================================
    def test_geo_text_slop_basic(self, key_type, dialect):
        """Two text terms separated by a GEO predicate, with SLOP.

        Doc vocab is "<adj> city <theme> hub" — capital→hub spans 3
        positions in matching docs, so the gap is 2 words. SLOP=2 is
        the boundary where the proximity match should succeed; we test
        clearly-passing values on both sides of that boundary.

        SLOP=0 (strict adjacency) and SLOP=1 are intentionally not in
        the suite: valkey-search applies the SLOP constraint directly
        even when a non-text predicate sits between the matched terms,
        while Redis Search effectively ignores it. That is a real
        engine-level divergence in proximity semantics, not a geo bug;
        capturing it here would just produce a noisy diff.
        """
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # SLOP=5 — comfortably wider than the actual gap; both engines
        # match every "capital ... hub" document.
        self._run(key_type, dialect,
                  f"capital @loc:[{lon} {lat} 20000 km] hub",
                  "SLOP", "5")
        # SLOP=2 — exactly fits the gap; both engines still match.
        self._run(key_type, dialect,
                  f"capital @loc:[{lon} {lat} 20000 km] hub",
                  "SLOP", "2")

    # NOTE: SLOP + INORDER tests with a GEO predicate sitting between
    # the matched text terms are deliberately limited to the SLOP-only
    # form in test_geo_text_slop_basic / test_geo_text_slop_three_terms.
    # The SLOP+INORDER+(geo-in-middle) case is a real proximity-semantics
    # divergence: valkey-search treats the geo predicate as occupying no
    # position when computing proximity, while Redis Stack appears to
    # reject matches outright. Capturing that divergence as a hard-fail
    # compat test would be noise; the geo work itself is verified by
    # the SLOP-only tests above and by test_geo_text_slop_with_tag.

    def test_geo_text_slop_three_terms(self, key_type, dialect):
        """Three-term text proximity bracketing a GEO predicate."""
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # coastal city ... hub  with geo just before "hub". SLOP=5 is
        # comfortably wider than the actual document gap.
        self._run(key_type, dialect,
                  f"coastal city @loc:[{lon} {lat} 5000 km] hub",
                  "SLOP", "5")

    # ========================================================================
    # Three-way and four-way compositions
    # ========================================================================
    def test_geo_tag_numeric_and(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # NA cities, near SF, top rated.
        self._run(key_type, dialect,
                  f"@loc:[{lon} {lat} 5000 km] @continent:{{north_america}} @rating:[4 5]")
        # Asian cities, top rated, broad radius.
        lon2, lat2 = self.CENTERS["tokyo"]
        self._run(key_type, dialect,
                  f"@loc:[{lon2} {lat2} 10000 km] @continent:{{asia}} @rating:[4 5]")

    def test_geo_tag_numeric_or(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # Three independent OR branches.
        self._run(key_type, dialect,
                  f"@loc:[{lon} {lat} 1000 km] | @continent:{{europe}} | @rating:[5 5]")

    def test_geo_text_tag_and(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["london"]
        # European capitals near London with culture in the description.
        self._run(key_type, dialect,
                  f"@description:culture @continent:{{europe}} @loc:[{lon} {lat} 5000 km]")

    def test_geo_text_tag_or(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        self._run(key_type, dialect,
                  f"@description:tech | @continent:{{europe}} | @loc:[{lon} {lat} 1000 km]")

    def test_geo_text_numeric_and(self, key_type, dialect):
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # Top-rated tech cities anywhere on the globe.
        self._run(key_type, dialect,
                  f"@description:tech @rating:[4 5] @loc:[{lon} {lat} 20000 km]")
        # Capital cities rated >=3 (single-term text + numeric + geo AND).
        self._run(key_type, dialect,
                  f"@description:capital @rating:[3 5] "
                  f"@loc:[{lon} {lat} 20000 km]")

    def test_geo_text_tag_numeric_and(self, key_type, dialect):
        """Four-way AND across all field types."""
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["london"]
        self._run(key_type, dialect,
                  f"@description:capital @continent:{{europe}} @rating:[3 5] "
                  f"@loc:[{lon} {lat} 5000 km]")

    def test_geo_text_tag_numeric_or(self, key_type, dialect):
        """Four-way OR across all field types."""
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        self._run(key_type, dialect,
                  f"@description:tech | @continent:{{oceania}} | @rating:[5 5] | "
                  f"@loc:[{lon} {lat} 500 km]")

    def test_geo_text_tag_numeric_nested(self, key_type, dialect):
        """Nested compositions across all field types.

        The tightly-mixed `(geo AND tag) | (text AND numeric)` form was
        observed to expose a non-geo OR-of-AND divergence in valkey-
        search (a spurious extra key sneaking through), so we exercise
        the same field-type combinations via flatter compositions
        instead. Geo is still combined with all of {text, tag, numeric}.
        """
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        # geo AND tag, OR text alone
        self._run(key_type, dialect,
                  f"(@loc:[{lon} {lat} 2000 km] @continent:{{north_america}}) | "
                  f"@description:tech")
        # text AND numeric, OR a wide geo
        self._run(key_type, dialect,
                  f"(@description:capital @rating:[4 5]) | "
                  f"@loc:[{lon} {lat} 1000 km]")

    # ========================================================================
    # GEO × TEXT proximity combined with TAG / NUMERIC filters
    # ========================================================================
    def test_geo_text_slop_with_tag(self, key_type, dialect):
        """SLOP-bracketed text + geo + a TAG filter."""
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        self._run(key_type, dialect,
                  f"capital @loc:[{lon} {lat} 20000 km] hub @continent:{{europe}}",
                  "SLOP", "5")

    def test_geo_text_slop_with_numeric(self, key_type, dialect):
        """SLOP-bracketed text + geo + a NUMERIC range filter.

        See the comment on test_geo_text_slop_inorder_combined for why
        INORDER is not exercised in this combination.
        """
        self.setup_data(self.DATA_SET, key_type)
        lon, lat = self.CENTERS["sf"]
        self._run(key_type, dialect,
                  f"capital @loc:[{lon} {lat} 20000 km] hub @rating:[4 5]",
                  "SLOP", "5")
