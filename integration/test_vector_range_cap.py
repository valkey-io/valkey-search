"""
Integration tests for the per-shard VECTOR_RANGE result cap
(search.max-nonvector-search-results-fetched, N below).

Covers the HNSW walk with a ball of exactly N inside a fuller traversal shell,
compound OR at exactly N and N + 1 unique matches, the distances of OR
survivors, and the cap on HNSW cluster shards (walk and exact scan) under
LIMIT. Every retained key is checked against an uncapped FLAT query over the
same keys.
"""

import math
import struct
import time
import numpy as np
from valkey import ResponseError
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper, find_local_key

COUNTER = "search_nonvector_results_fetched_limited_count"
CAP_CONFIG = "search.max-nonvector-search-results-fetched"


def float_to_bytes(floats):
    """Pack a list of floats into a little-endian float32 blob."""
    return struct.pack(f"<{len(floats)}f", *floats)


def parse_result_with_fields(result):
    """Parse an FT.SEARCH reply into {key: {field: value}}."""
    parsed = {}
    for i in range(1, len(result), 2):
        fields = result[i + 1]
        parsed[result[i].decode()] = {
            fields[j].decode(): fields[j + 1]
            for j in range(0, len(fields), 2)
        }
    return parsed


def create_index(client, name, prefix, algo, metric="L2", dim=2, tag=False):
    """Create a vector index on field v, plus a tag field grp if asked."""
    extra = ["grp", "TAG"] if tag else []
    assert client.execute_command(
        "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", prefix,
        "SCHEMA", "v", "VECTOR", algo, "6", "TYPE", "FLOAT32",
        "DIM", str(dim), "DISTANCE_METRIC", metric, *extra,
    ) == b"OK"


def wait_indexed(clients, indexes, timeout=60):
    """Wait until every index is fully indexed on every client's node."""
    deadline = time.monotonic() + timeout
    for client in clients:
        for index in indexes:
            while True:
                try:
                    if IndexingTestHelper.is_indexing_complete_on_node(
                            client, index):
                        break
                except ResponseError:
                    pass  # Not yet created on this node.
                assert time.monotonic() < deadline, f"{index} not indexed"
                time.sleep(0.1)


def range_search(client, index, query, radius, *args, or_clause=None,
                 range_first=True, epsilon=None, limit=(0, 1000)):
    """VECTOR_RANGE query yielding dist, optionally ORed with or_clause
    after or before the range clause. Returns (total, [(key, dist or None)])
    in reply order."""
    attrs = "$yield_distance_as: dist"
    if epsilon is not None:
        attrs = f"$epsilon: {epsilon:.9g}; {attrs}"
    clause = f"@v:[VECTOR_RANGE $r $blob]=>{{{attrs}}}"
    if or_clause is not None:
        clause = (f"{clause} | {or_clause}" if range_first
                  else f"{or_clause} | {clause}")
    result = client.execute_command(
        "FT.SEARCH", index, clause,
        "PARAMS", "4", "r", f"{radius:.9g}", "blob", float_to_bytes(query),
        "RETURN", "1", "dist", *args,
        "LIMIT", str(limit[0]), str(limit[1]), "DIALECT", "2",
    )
    rows = [(key, float(fields["dist"]) if "dist" in fields else None)
            for key, fields in parse_result_with_fields(result).items()]
    assert len(rows) == (len(result) - 1) // 2
    return result[0], rows


def range_map(client, index, query, radius, **kwargs):
    """range_search as {key: dist}, checking the total equals the rows."""
    total, rows = range_search(client, index, query, radius, **kwargs)
    found = dict(rows)
    assert total == len(found) == len(rows)
    return found


def assert_flat_distances(found, flat):
    """Every found key is an uncapped FLAT match at FLAT's distance."""
    assert found.keys() <= flat.keys(), found.keys() - flat.keys()
    for key, dist in found.items():
        assert math.isclose(dist, flat[key], rel_tol=1e-5, abs_tol=1e-6), (
            key, dist, flat[key])


def counter(client):
    return client.info("search").get(COUNTER, 0)


def set_cap(clients, cap):
    for client in clients:
        client.config_set("search.info-developer-visible", "yes")
        client.config_set(CAP_CONFIG, str(cap))


class TestVectorRangeCap(ValkeySearchTestCaseBase):
    """The cap on one standalone node."""

    def test_hnsw_walk_ball_of_n_in_fuller_shell_is_exact(self):
        """
        A ball of exactly N keys inside a traversal shell holding more than N
        keys returns the FLAT ball, untruncated; a ball of N + 1 truncates to
        N FLAT matches. The N + 1 witness must be counted within the radius,
        not within the shell, so distances are distinct and the shell holds
        2N keys.
        """
        client = self.server.get_new_client()
        cap = 4
        create_index(client, "flat", "w:", "FLAT")
        create_index(client, "hnsw", "w:", "HNSW")
        rng = np.random.default_rng(61)
        points = rng.uniform(0.0, 10.0, (150, 2)).astype(np.float32)
        pipe = client.pipeline(transaction=False)
        for i, point in enumerate(points):
            pipe.hset(f"w:{i}", mapping={"v": point.astype("<f4").tobytes()})
        pipe.execute()
        wait_indexed([client], ("flat", "hnsw"))

        queries = rng.uniform(2.0, 8.0, (8, 2)).astype(np.float32)
        cells = []
        for query in queries:
            # Uncapped FLAT: radii midway between neighboring distances, so
            # each ball holds exactly n keys and the shell exactly 2 * cap.
            flat = range_map(client, "flat", query.tolist(), 200.0)
            dists = sorted(flat.values())
            assert len(set(dists[:2 * cap + 1])) == 2 * cap + 1
            radius = {n: (dists[n - 1] + dists[n]) / 2
                      for n in (cap, cap + 1)}
            shell = (dists[2 * cap - 1] + dists[2 * cap]) / 2
            cells.append((query, flat, radius, shell))

        set_cap([client], cap)
        before = counter(client)
        for query, flat, radius, shell in cells:
            epsilon = shell / radius[cap] - 1
            found = range_map(client, "hnsw", query.tolist(), radius[cap],
                              epsilon=epsilon)
            ball = {k: d for k, d in flat.items() if d <= radius[cap]}
            assert found.keys() == ball.keys()
            assert_flat_distances(found, flat)
        assert counter(client) == before

        for query, flat, radius, shell in cells:
            epsilon = shell / radius[cap + 1] - 1
            found = range_map(client, "hnsw", query.tolist(),
                              radius[cap + 1], epsilon=epsilon)
            assert len(found) == cap
            assert all(flat[k] <= radius[cap + 1] for k in found)
            assert_flat_distances(found, flat)
        assert counter(client) == before + len(cells)

    def _or_fixture(self, client):
        """FLAT and HNSW indexes with tag grp over the same keys. v0..v5 lie
        on the x axis at squared distances 0.01 .. 0.36 from the origin;
        t0 and t1 have only a tag. Returns the uncapped FLAT distances."""
        for algo in ("FLAT", "HNSW"):
            create_index(client, algo.lower(), "o:", algo, tag=True)
        tags = {
            "o:v0": "a,d,x", "o:v1": "a,d", "o:v2": "a,b,d", "o:v3": "a,d,x",
            "o:v4": "d,x", "o:v5": "x",
        }
        for i in range(6):
            key = f"o:v{i}"
            client.hset(key, mapping={
                "v": float_to_bytes([0.1 * (i + 1), 0.0]), "grp": tags[key]})
        client.hset("o:t0", mapping={"grp": "b,c,x"})
        client.hset("o:t1", mapping={"grp": "c"})
        wait_indexed([client], ("flat", "hnsw"))
        return range_map(client, "flat", [0.0, 0.0], 1.0)

    def test_vector_range_or_at_n_and_n_plus_one_unique_matches(self):
        """
        OR counts unique keys against N: a union of exactly N keys, even with
        branches that overlap or with a range branch of exactly N, is
        returned whole and not counted; N + 1 unique keys return N and count
        one truncation, whether one, both or neither branch exceeds N.
        """
        client = self.server.get_new_client()
        cap = 4
        flat = self._or_fixture(client)
        tagged = {
            tag: {key.decode() for key in client.execute_command(
                "FT.SEARCH", "flat", f"@grp:{{{tag}}}", "NOCONTENT",
                "LIMIT", "0", "100")[1:]}
            for tag in ("a", "b", "c", "d")
        }
        set_cap([client], cap)
        # (radius, tag) with a union of exactly N: two fully overlapping
        # branches of N each, and a range branch of 3 plus a tag branch
        # sharing one key with it.
        exact = ((0.2, "a"), (0.1, "b"))
        # A union of N + 1: two branches below N, and two above N.
        over = ((0.1, "c"), (0.3, "d"))
        for index in ("flat", "hnsw"):
            for range_first in (True, False):
                for radius, tag in exact + over:
                    union = {k for k, d in flat.items() if d <= radius}
                    union |= tagged[tag]
                    before = counter(client)
                    found = range_map(
                        client, index, [0.0, 0.0], radius,
                        or_clause=f"@grp:{{{tag}}}", range_first=range_first)
                    case = (index, range_first, radius, tag)
                    if (radius, tag) in exact:
                        assert len(union) == cap, case
                        assert found.keys() == union, case
                        assert counter(client) == before, case
                    else:
                        assert len(union) == cap + 1, case
                        assert len(found) == cap, case
                        assert found.keys() <= union, case
                        assert counter(client) == before + 1, case

    def test_vector_range_or_survivors_carry_flat_distances(self):
        """
        Every OR survivor within the radius carries FLAT's distance, whichever
        branch matched it; every other survivor matches the tag branch and
        carries none. Checked uncapped and with the cap truncating.
        """
        client = self.server.get_new_client()
        flat = self._or_fixture(client)
        tagged = {key.decode() for key in client.execute_command(
            "FT.SEARCH", "flat", "@grp:{x}", "NOCONTENT",
            "LIMIT", "0", "100")[1:]}
        radius = 0.2
        ball = {k for k, d in flat.items() if d <= radius}
        # Some tagged keys lie inside the ball, some outside it, one has no
        # vector, and some ball keys are untagged.
        assert ball & tagged and ball - tagged and tagged - ball
        union = ball | tagged
        for cap in (len(union), 3):
            set_cap([client], cap)
            for index in ("flat", "hnsw"):
                for range_first in (True, False):
                    case = (cap, index, range_first)
                    before = counter(client)
                    found = range_map(
                        client, index, [0.0, 0.0], radius,
                        or_clause="@grp:{x}", range_first=range_first)
                    if cap == len(union):
                        assert found.keys() == union, case
                        assert counter(client) == before, case
                    else:
                        assert len(found) == cap, case
                        assert found.keys() <= union, case
                        assert counter(client) == before + 1, case
                    for key, dist in found.items():
                        if key in ball:
                            assert dist is not None, (case, key)
                            assert math.isclose(
                                dist, flat[key], rel_tol=1e-5,
                                abs_tol=1e-6), (case, key, dist)
                        else:
                            assert key in tagged, (case, key)
                            assert dist is None, (case, key, dist)


class TestVectorRangeCapCluster(ValkeySearchClusterTestCase):
    """The cap through the coordinator on 3 primaries, each its own shard."""

    CAP = 4
    # Keys within the radius on each shard: below N, above N, exactly N.
    BALLS = (2, CAP + 3, CAP)
    OVERLOADED = 1

    def _primaries(self):
        return [self.client_for_primary(i)
                for i in range(len(self.replication_groups))]

    def _load(self, metric):
        """FLAT and HNSW indexes over the same keys: BALLS[s] keys on shard s
        within the returned radius, at distinct distances, plus 3 farther
        keys per shard for L2. Returns (primaries, the in-radius keys per
        shard, query, radius, uncapped FLAT distances)."""
        cluster_client = self.new_cluster_client()
        for algo in ("FLAT", "HNSW"):
            create_index(cluster_client, algo.lower(), "c:", algo,
                         metric=metric)
        primaries = self._primaries()
        shard_keys = []
        inside = outside = 0
        for shard, primary in enumerate(primaries):
            tag = find_local_key(primary, f"vr-cap-{shard}:")
            # COSINE radius 2 holds every key, so it gets no farther keys.
            farther = 0 if metric == "COSINE" else 3
            keys = set()
            pipe = primary.pipeline(transaction=False)
            for j in range(self.BALLS[shard] + farther):
                key = f"c:{{{tag}}}:{j}"
                if j < self.BALLS[shard]:
                    inside += 1
                    keys.add(key)
                    squared = 0.5 * inside
                else:
                    outside += 1
                    squared = 20.0 + outside
                # Distinct angles from the query [1, 0] for COSINE.
                angle = 0.37 * (inside + outside)
                norm = 1.0 if metric == "COSINE" else math.sqrt(squared)
                pipe.hset(key, mapping={"v": float_to_bytes(
                    [norm * math.cos(angle), norm * math.sin(angle)])})
            pipe.execute()
            shard_keys.append(keys)
        wait_indexed(primaries, ("flat", "hnsw"))
        radius = 2.0 if metric == "COSINE" else 10.0
        query = [1.0, 0.0] if metric == "COSINE" else [0.0, 0.0]
        flat = range_map(primaries[0], "flat", query, radius)
        assert flat.keys() == set().union(*shard_keys)
        assert len(set(flat.values())) == len(flat)
        return primaries, shard_keys, query, radius, flat

    def _assert_per_shard(self, primaries, shard_keys, found, before):
        """Shards above N keep N; the others keep every match; only the
        overloaded shard counts a truncation."""
        for shard, keys in enumerate(shard_keys):
            retained = found.keys() & keys
            if shard == self.OVERLOADED:
                assert len(retained) == self.CAP
            else:
                assert retained == keys
        assert len(found) == sum(min(b, self.CAP) for b in self.BALLS)
        deltas = [counter(p) - old
                  for p, old in zip(primaries, before, strict=True)]
        assert deltas == [int(i == self.OVERLOADED)
                          for i in range(len(primaries))]

    def test_hnsw_range_cap_is_per_shard_on_the_walk(self):
        """
        On HNSW L2 shards that walk the graph, a shard with more than N keys
        in the ball keeps N FLAT matches and counts one truncation; shards
        with fewer or exactly N keep every match and count nothing.
        """
        primaries, shard_keys, query, radius, flat = self._load("L2")
        set_cap(primaries, self.CAP)
        before = [counter(p) for p in primaries]
        found = range_map(primaries[0], "hnsw", query, radius)
        assert_flat_distances(found, flat)
        self._assert_per_shard(primaries, shard_keys, found, before)

    def test_hnsw_range_cap_is_per_shard_on_the_exact_scan(self):
        """
        COSINE radius 2 takes the HNSW exact scan on every shard: the same
        per-shard split as the walk. With N = 0 each shard holding a match
        returns nothing and counts once.
        """
        primaries, shard_keys, query, radius, flat = self._load("COSINE")
        set_cap(primaries, self.CAP)
        before = [counter(p) for p in primaries]
        found = range_map(primaries[0], "hnsw", query, radius)
        assert_flat_distances(found, flat)
        self._assert_per_shard(primaries, shard_keys, found, before)

        set_cap(primaries, 0)
        before = [counter(p) for p in primaries]
        assert range_map(primaries[0], "hnsw", query, radius) == {}
        assert [counter(p) - old for p, old in zip(primaries, before)] == [
            1, 1, 1]

    def test_limit_pages_the_retained_keys_only(self):
        """
        LIMIT pages through the keys the shards retained: every page reports
        the merged retained total, pages partition the retained set, and a
        key a shard discarded never appears.
        """
        primaries, shard_keys, query, radius, flat = self._load("L2")
        set_cap(primaries, self.CAP)
        page_size = 3
        for index in ("flat", "hnsw"):
            retained = range_map(primaries[0], index, query, radius)
            assert_flat_distances(retained, flat)
            assert len(retained) == sum(min(b, self.CAP) for b in self.BALLS)
            ranked = sorted(retained, key=retained.get)
            for sort in (("SORTBY", "dist", "ASC"), ()):
                seen = []
                for offset in range(0, len(retained) + page_size, page_size):
                    total, rows = range_search(
                        primaries[0], index, query, radius, *sort,
                        limit=(offset, page_size))
                    assert total == len(retained), (index, sort, offset)
                    keys = [key for key, _ in rows]
                    if sort:
                        assert keys == ranked[offset:offset + page_size]
                    assert len(keys) == min(
                        page_size, max(0, len(retained) - offset)), (
                        index, sort, offset)
                    for key, dist in rows:
                        assert key in retained, (index, sort, offset, key)
                        assert dist == retained[key]
                    seen.extend(keys)
                assert sorted(seen) == sorted(retained), (index, sort)
