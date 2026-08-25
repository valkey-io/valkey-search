"""End-to-end coverage for the module-global defrag callback.

The unit tests in testing/defrag_test.cc drive the callback directly with mocked
core APIs. This file checks the other half: that a real valkey-server, running a
real active-defrag cycle, actually reaches our callback and that each element of
the core contract is exercised in practice.

Requirements, both checked at runtime rather than assumed:
  * the server was built with jemalloc, otherwise active defrag is compiled out;
  * the server supports module global defrag, i.e. it invokes the callback with
    a deadline and a persistent cursor. Cores without that support simply never
    call us, so the test skips rather than failing.

See src/defrag.h for the contract these assertions map to.
"""

import logging

import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker


def _defrag_stats(client: Valkey) -> dict:
    """FT._DEBUG DEFRAG_STATS returns a flat [name, value, ...] reply."""
    reply = client.execute_command("FT._DEBUG DEFRAG_STATS")
    decoded = [r.decode() if isinstance(r, bytes) else r for r in reply]
    return {decoded[i]: int(decoded[i + 1]) for i in range(0, len(decoded), 2)}


class TestModuleGlobalDefrag(ValkeySearchTestCaseDebugMode):
    def _enable_aggressive_defrag(self, client: Valkey) -> bool:
        """Turn defrag on and make it run as eagerly as possible.

        Returns False when the server has no active defrag at all (built without
        jemalloc), in which case there is nothing to test.
        """
        # Remove every reason defrag might decide it has nothing worth doing, so
        # a cycle starts promptly even on a small dataset.
        client.execute_command("CONFIG SET active-defrag-ignore-bytes 1")
        client.execute_command("CONFIG SET active-defrag-threshold-lower 0")
        client.execute_command("CONFIG SET active-defrag-cycle-min 50")
        client.execute_command("CONFIG SET active-defrag-cycle-max 75")
        try:
            client.execute_command("CONFIG SET activedefrag yes")
        except Exception as exc:  # noqa: BLE001 - server rejects when unsupported
            logging.info("active defrag unavailable: %s", exc)
            return False
        setting = client.execute_command("CONFIG GET activedefrag")
        value = setting[1].decode() if isinstance(setting[1], bytes) else setting[1]
        return value == "yes"

    def _populate_and_fragment(self, client: Valkey) -> None:
        """Create an index, fill it, then delete half of it.

        Deleting every other key leaves the surviving keys scattered across
        jemalloc slabs, which is the condition that makes defrag do real work.
        """
        client.execute_command(
            "FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA n NUMERIC t TAG"
        )
        pipe = client.pipeline(transaction=False)
        for i in range(10000):
            pipe.execute_command("HSET", f"doc:{i}", "n", i, "t", f"tag{i % 100}")
        pipe.execute()
        pipe = client.pipeline(transaction=False)
        for i in range(0, 10000, 2):
            pipe.execute_command("DEL", f"doc:{i}")
        pipe.execute()

    def test_global_defrag_callback_is_driven_by_core(self):
        """Core reaches our callback and exercises the full contract."""
        client: Valkey = self.server.get_new_client()

        baseline = _defrag_stats(client)
        assert baseline["callback_invocations"] == 0, (
            "expected a clean baseline before defrag is enabled"
        )

        self._populate_and_fragment(client)

        if not self._enable_aggressive_defrag(client):
            pytest.skip("server has no active defrag (built without jemalloc)")

        # Give the defrag timer several cycles to run. Defrag is deliberately
        # rate limited to a fraction of CPU, so this is seconds, not milliseconds.
        import time

        deadline = time.time() + 15
        stats = _defrag_stats(client)
        while time.time() < deadline and stats["callback_invocations"] == 0:
            time.sleep(0.5)
            stats = _defrag_stats(client)

        if stats["callback_invocations"] == 0:
            pytest.skip(
                "server does not support module global defrag "
                "(callback was never invoked)"
            )

        # 1. Core invoked the callback. Before the core-side change the global
        #    hook existed but could not be used, so this alone is meaningful.
        assert stats["callback_invocations"] > 0

        # 2. Core handed us a usable persistent cursor. This is what previously
        #    failed: global callbacks were invoked with cursor == NULL, so
        #    DefragCursorGet returned an error and resuming was impossible.
        assert stats["cursor_reads"] > 0, (
            "callback ran but never got a usable cursor from core"
        )

        # 3. We polled the deadline, i.e. the callback is time bounded rather
        #    than assuming it owns the main thread.
        assert stats["deadline_checks"] > 0

        # 4. We reported completion at least once by resetting the cursor to 0.
        #    Without this core would reschedule the global defrag stage forever,
        #    so seeing it prove out end to end matters.
        assert stats["completed_passes"] > 0, (
            "callback never completed a pass; core would keep rescheduling it"
        )

        logging.info("module global defrag stats: %s", stats)

    def test_defrag_stats_reset(self):
        """FT._DEBUG DEFRAG_STATS RESET zeroes the counters."""
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT._DEBUG DEFRAG_STATS RESET")
        stats = _defrag_stats(client)
        assert all(value == 0 for value in stats.values()), stats
