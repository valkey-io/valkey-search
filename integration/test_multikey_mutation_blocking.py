"""Command-wide blocking while multi-key writes update an index."""

import pytest

from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from utils import IndexingTestHelper, run_in_thread


class TestMultiKeyMutationBlocking(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.writer-threads"] = "1"
        return args

    @pytest.mark.parametrize(
        "command, expected_reply, remaining_documents",
        [("DEL", 2, 0), ("UNLINK", 2, 0), ("RENAME", True, 1)],
    )
    def test_multikey_write_waits_for_every_mutation(
        self, command, expected_reply, remaining_documents
    ):
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
        )
        for key in ["doc:1", "doc:2"]:
            client.hset(key, "content", "hello")
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        assert client.execute_command("FT.SEARCH", "idx", "*")[0] == 2

        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        write_thread, result, error = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                command, "doc:1", "doc:2"
            )
        )
        try:
            waiters.wait_for_true(
                lambda: client.execute_command(
                    "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing"
                ) == 1
            )
            # The first task has passed the queue pausepoint. With one writer,
            # enabling it now holds only the second task after the first ends.
            client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "SET", "block_mutation_queue"
            )
            client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing"
            )
            waiters.wait_for_true(
                lambda: client.execute_command(
                    "FT._DEBUG", "PAUSEPOINT", "TEST", "block_mutation_queue"
                ) == 1
            )

            assert error[0] is None
            assert client.info("SEARCH")["search_ingest_hash_blocked"] == 1
            assert write_thread.is_alive()
            assert result[0] is None
        finally:
            client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing"
            )
            client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "RESET", "block_mutation_queue"
            )
            write_thread.join(timeout=10)

        assert not write_thread.is_alive()
        assert error[0] is None
        assert result[0] == expected_reply
        assert (
            client.execute_command("FT.SEARCH", "idx", "*")[0]
            == remaining_documents
        )
        assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0


class TestPendingTransactionMutationBlocking(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.writer-threads"] = "2"
        return args

    def test_normal_write_waits_when_overwriting_pending_transaction(self):
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
        )
        client.hset("doc:1", "content", "initial")
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        initial_batches = client.info("SEARCH")["search_ingest_total_batches"]

        # One transaction mutation stays pending below the two-writer batch
        # threshold. The following normal write must flush it without a search.
        with client.pipeline(transaction=True) as transaction:
            transaction.hset("doc:1", "content", "pending")
            assert transaction.execute() == [0]

        write_thread, result, error = run_in_thread(
            lambda: self.server.get_new_client().hset("doc:1", "content", "latest")
        )
        try:
            write_thread.join(timeout=10)
            assert not write_thread.is_alive()
            assert error[0] is None
            assert result[0] == 0
            info = client.info("SEARCH")
            assert info["search_ingest_total_batches"] == initial_batches + 1
            assert info["search_ingest_hash_blocked"] == 0
        finally:
            search_result = client.execute_command(
                "FT.SEARCH", "idx", "@content:latest"
            )
            write_thread.join(timeout=10)

        assert not write_thread.is_alive()
        assert error[0] is None
        assert result[0] == 0
        assert search_result == [1, b"doc:1", [b"content", b"latest"]]
        assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0
