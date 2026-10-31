"""
A node whose limit is lower than an index created on a peer refuses that
peer's metadata as a whole: it keeps serving the indexes it already had, does
not pick up any other index from the same proposal, and logs the rejection on
every round until an operator raises the limit, at which point it converges.
"""

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters

REJECTED_LOG = "failed validation, nothing applied"


def create_hnsw_index(client, name, m):
    """FT.CREATE an HNSW index; returns the error text, or None on success."""
    try:
        client.execute_command(
            "FT.CREATE", name, "PREFIX", "1", f"{name}:", "SCHEMA",
            "v", "VECTOR", "HNSW", "8", "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2", "M", str(m))
    except ResponseError as e:
        return str(e)
    return None


def index_names(client):
    return {name.decode() for name in client.execute_command("FT._LIST")}


class TestMetadataSyncStuckOnLimit(ValkeySearchClusterTestCase):

    def test_sync_stays_stuck_until_limit_is_raised(self):
        creator = self.new_client_for_primary(0)
        limited = self.new_client_for_primary(1)
        limited_node = self.replication_groups[1].primary
        limited.execute_command("CONFIG SET search.info-developer-visible yes")
        limited.execute_command("CONFIG SET search.max-vector-m 16")

        def completed_rounds():
            return int(limited.info("search")[
                "search_coordinator_metadata_reconciliation_completed_count"])

        assert create_hnsw_index(creator, "served", m=8) is None
        waiters.wait_for_true(lambda: "served" in index_names(limited))
        rounds_before = completed_rounds()

        # Over this node's limit; the creating node cannot reach consistency.
        error = create_hnsw_index(creator, "too_wide", m=32)
        assert error is None or "Unable to contact all cluster members" in error
        waiters.wait_for_true(
            lambda: limited_node.does_logfile_contains(REJECTED_LOG))

        # A within-limit index created afterwards travels in the same
        # proposal, so it is not applied either: the node stays stuck.
        error = create_hnsw_index(creator, "later", m=8)
        assert error is None or "Unable to contact all cluster members" in error
        assert index_names(limited) == {"served"}
        assert completed_rounds() == rounds_before
        for i in (0, 2):
            assert {"served", "too_wide", "later"} <= index_names(
                self.new_client_for_primary(i))

        # The operator raises the limit; the next round converges.
        limited.execute_command("CONFIG SET search.max-vector-m 32")
        error = create_hnsw_index(creator, "trigger", m=8)
        assert error is None or "Unable to contact all cluster members" in error
        waiters.wait_for_true(
            lambda: index_names(limited)
            == {"served", "too_wide", "later", "trigger"},
            timeout=60)
        assert completed_rounds() > rounds_before
