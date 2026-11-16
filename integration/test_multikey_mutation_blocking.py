"""Command-wide blocking while multi-key writes update an index."""

import os

import pytest

from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from utils import IndexingTestHelper, run_in_thread


class TestMultiKeyMutationBlocking(ValkeySearchTestCaseDebugMode):
    def get_config_file_lines(self, testdir, port):
        lines = super().get_config_file_lines(testdir, port)
        search_module = f"loadmodule {os.environ['MODULE_PATH']}"
        return [
            line + " --writer-threads 1" if line.startswith(search_module) else line
            for line in lines
        ]

    @pytest.mark.parametrize(
        "command, expected_reply, remaining_documents",
        [("DEL", 2, 0), ("UNLINK", 2, 0), ("RENAME", True, 1)],
    )
    @pytest.mark.parametrize("schema_count", [1, 2, 5, 10])
    def test_multikey_write_waits_for_every_mutation(
        self, command, expected_reply, remaining_documents, schema_count
    ):
        client = self.server.get_new_client()
        indexes = [f"idx{i}" for i in range(schema_count)]
        for index in indexes:
            client.execute_command(
                "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "content", "TEXT",
            )
        for key in ["doc:1", "doc:2"]:
            client.hset(key, "content", "hello")
        for index in indexes:
            IndexingTestHelper.wait_for_indexing_complete_on_node(client, index)
            assert client.execute_command("FT.SEARCH", index, "*")[0] == 2

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
        for index in indexes:
            assert (
                client.execute_command("FT.SEARCH", index, "*")[0]
                == remaining_documents
            )
        assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0

    @pytest.mark.parametrize("tracked_first", [True, False])
    def test_delete_mixed_keys_waits_for_indexed_mutation(self, tracked_first):
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
        )
        client.hset("doc:tracked", "content", "hello")
        client.set("doc:string", "untracked")
        client.hset("outside:hash", "content", "untracked")
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        keys = ["doc:tracked", "doc:string", "outside:hash", "doc:missing"]
        if not tracked_first:
            keys.reverse()
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        thread, result, error = run_in_thread(
            lambda: self.server.get_new_client().delete(*keys)
        )
        try:
            waiters.wait_for_true(
                lambda: client.execute_command(
                    "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing"
                ) == 1
            )
            assert client.info("SEARCH")["search_ingest_hash_blocked"] == 1
            assert thread.is_alive()
            assert result[0] is None
            assert error[0] is None
        finally:
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
            thread.join(timeout=10)
        assert not thread.is_alive()
        assert error[0] is None
        assert result[0] == 3
        assert client.execute_command("FT.SEARCH", "idx", "*")[0] == 0
        assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0

    @pytest.mark.parametrize("schema_count", [1, 10])
    def test_json_write_shares_one_block_across_schemas(self, schema_count):
        client = self.server.get_new_client()
        indexes = [f"idx{i}" for i in range(schema_count)]
        for index in indexes:
            client.execute_command(
                "FT.CREATE", index, "ON", "JSON", "PREFIX", "1", "doc:",
                "SCHEMA", "$.content", "AS", "content", "TEXT",
            )
        client.execute_command("JSON.SET", "doc:1", "$", '{"content":"initial"}')
        for index in indexes:
            IndexingTestHelper.wait_for_indexing_complete_on_node(client, index)
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        thread, result, error = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "JSON.SET", "doc:1", "$", '{"content":"latest"}'
            )
        )
        try:
            waiters.wait_for_true(
                lambda: client.execute_command(
                    "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing"
                ) == 1
            )
            info = client.info("SEARCH")
            assert info["search_ingest_json_blocked"] == 1
            assert info["search_ingest_hash_blocked"] == 0
            assert thread.is_alive()
            assert result[0] is None
            assert error[0] is None
        finally:
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
            thread.join(timeout=10)
        assert not thread.is_alive()
        assert result[0] == b"OK"
        assert error[0] is None
        for index in indexes:
            assert client.execute_command("FT.SEARCH", index, "@content:latest")[0] == 1
        assert client.info("SEARCH")["search_ingest_json_blocked"] == 0

    def test_expire_untracked_string_schedules_no_index_work(self):
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
        )
        client.set("doc:string", "untracked")
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "block_mutation_queue")
        try:
            assert client.expire("doc:string", 3600)
            assert client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "block_mutation_queue"
            ) == 0
            assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0
        finally:
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "block_mutation_queue")
        assert client.execute_command("FT.SEARCH", "idx", "*")[0] == 0

    def test_disconnected_writer_preserves_other_client_waiting_on_same_key(self):
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
        )
        client.hset("doc:1", "content", "initial")
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        connection = client.connection_pool.get_connection("HSET")
        connection.send_command("CLIENT", "SETNAME", "disconnecting-writer")
        assert connection.read_response() == b"OK"
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        second_thread = None
        try:
            connection.send_command("HSET", "doc:1", "content", "obsolete")
            waiters.wait_for_true(
                lambda: client.execute_command(
                    "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing"
                ) == 1
            )
            connection.disconnect()
            waiters.wait_for_true(
                lambda: all(
                    entry["name"] != "disconnecting-writer"
                    for entry in client.client_list()
                )
            )
            second_thread, result, error = run_in_thread(
                lambda: self.server.get_new_client().hset(
                    "doc:1", "content", "latest"
                )
            )
            waiters.wait_for_equal(
                lambda: client.info("SEARCH")["search_ingest_hash_blocked"], 2
            )
            assert second_thread.is_alive()
            assert result[0] is None
            assert error[0] is None
        finally:
            connection.disconnect()
            client.connection_pool.release(connection)
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
            if second_thread is not None:
                second_thread.join(timeout=10)
        assert not second_thread.is_alive()
        assert result[0] == 0
        assert error[0] is None
        assert client.execute_command("FT.SEARCH", "idx", "@content:latest")[0] == 1
        assert client.execute_command("FT.SEARCH", "idx", "@content:obsolete")[0] == 0
        waiters.wait_for_equal(
            lambda: client.info("SEARCH")["search_ingest_hash_blocked"], 0
        )


class TestPendingTransactionMutationBlocking(ValkeySearchTestCaseDebugMode):
    def get_config_file_lines(self, testdir, port):
        lines = super().get_config_file_lines(testdir, port)
        search_module = f"loadmodule {os.environ['MODULE_PATH']}"
        return [
            line + " --writer-threads 2" if line.startswith(search_module) else line
            for line in lines
        ]

    @pytest.mark.parametrize("schema_count", [1, 10])
    @pytest.mark.parametrize("executor", ["MULTI", "LUA"])
    def test_normal_write_coalesces_without_waiting_for_pending_transaction(
        self, schema_count, executor
    ):
        client = self.server.get_new_client()
        indexes = [f"idx{i}" for i in range(schema_count)]
        for index in indexes:
            client.execute_command(
                "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "content", "TEXT",
            )
        client.hset("doc:1", "content", "initial")
        for index in indexes:
            IndexingTestHelper.wait_for_indexing_complete_on_node(client, index)
            assert client.execute_command("FT.SEARCH", index, "@content:initial")[0] == 1
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        write_thread = None
        try:
            # One transaction update stays below the two-writer batch threshold.
            if executor == "MULTI":
                with client.pipeline(transaction=True) as transaction:
                    transaction.hset("doc:1", "content", "pending")
                    assert transaction.execute() == [0]
            else:
                assert client.eval(
                    "return redis.call('HSET', KEYS[1], 'content', 'pending')",
                    1, "doc:1",
                ) == 0
            assert client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing"
            ) == 0
            writer = self.server.get_new_client()
            writer_id = writer.client_id()
            write_thread, result, error = run_in_thread(
                lambda: writer.hset("doc:1", "content", "latest")
            )
            # The ordinary reply must not depend on a worker or search flush.
            write_thread.join(timeout=10)
            assert not write_thread.is_alive()
            assert error[0] is None
            assert result[0] == 0
            assert client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing"
            ) == 0
            assert client.hget("doc:1", "content") == b"latest"
            assert not any(
                str(row["id"]) == str(writer_id) and "b" in row["flags"]
                for row in client.client_list()
            )
        finally:
            client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing"
            )
            if write_thread is not None:
                write_thread.join(timeout=10)

        assert not write_thread.is_alive()
        assert error[0] is None
        assert result[0] == 0
        assert not any(
            str(row["id"]) == str(writer_id) and "b" in row["flags"]
            for row in client.client_list()
        )
        # Search is the barrier that applies the deferred latest value.
        for index in indexes:
            assert client.execute_command("FT.SEARCH", index, "@content:latest") == [
                1, b"doc:1", [b"content", b"latest"]
            ]
            assert client.execute_command("FT.SEARCH", index, "@content:initial")[0] == 0

    @pytest.mark.parametrize("schema_count", [1, 10])
    @pytest.mark.parametrize("executor", ["MULTI", "LUA"])
    def test_atomic_duplicate_key_mutations_then_ordinary_write(
        self, schema_count, executor
    ):
        client = self.server.get_new_client()
        indexes = [f"idx{i}" for i in range(schema_count)]
        for index in indexes:
            client.execute_command(
                "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "content", "TEXT",
            )
        for key in ["doc:1", "doc:2"]:
            client.hset(key, "content", "initial")
        for index in indexes:
            IndexingTestHelper.wait_for_indexing_complete_on_node(client, index)

        if executor == "MULTI":
            with client.pipeline(transaction=True) as transaction:
                for value in ["obsoleteone", "obsoletetwo", "obsoletethree"]:
                    transaction.hset("doc:1", "content", value)
                transaction.hset("doc:2", "content", "neighbor")
                assert transaction.execute() == [0, 0, 0, 0]
        else:
            assert client.eval(
                """
                redis.call('HSET', KEYS[1], 'content', 'obsoleteone')
                redis.call('HSET', KEYS[1], 'content', 'obsoletetwo')
                redis.call('HSET', KEYS[1], 'content', 'obsoletethree')
                return redis.call('HSET', KEYS[2], 'content', 'neighbor')
                """,
                2,
                "doc:1",
                "doc:2",
            ) == 0

        assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0
        thread, result, error = run_in_thread(
            lambda: self.server.get_new_client().hset("doc:1", "content", "latest")
        )
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert error[0] is None
        assert result[0] == 0
        for index in indexes:
            assert client.execute_command("FT.SEARCH", index, "*")[0] == 2
            assert client.execute_command("FT.SEARCH", index, "@content:latest")[0] == 1
            assert client.execute_command("FT.SEARCH", index, "@content:neighbor")[0] == 1
            assert client.execute_command(
                "FT.SEARCH", index, "@content:(obsoleteone|obsoletetwo|obsoletethree)"
            )[0] == 0
            assert IndexingTestHelper.is_indexing_complete_on_node(client, index)
        assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0

    @pytest.mark.parametrize("schema_count", [1, 10])
    def test_delete_cancels_pending_transaction_insert(self, schema_count):
        client = self.server.get_new_client()
        indexes = [f"idx{i}" for i in range(schema_count)]
        for index in indexes:
            client.execute_command(
                "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "content", "TEXT",
            )
            IndexingTestHelper.wait_for_indexing_complete_on_node(client, index)
        with client.pipeline(transaction=True) as transaction:
            transaction.hset("doc:pending", "content", "pending")
            assert transaction.execute() == [1]
        # This key has pending work but has never reached the index registry.
        assert client.delete("doc:pending") == 1
        for index in indexes:
            assert client.execute_command("FT.SEARCH", index, "*")[0] == 0
            assert client.execute_command("FT.SEARCH", index, "@content:pending")[0] == 0
            assert IndexingTestHelper.is_indexing_complete_on_node(client, index)
        assert client.info("SEARCH")["search_ingest_hash_blocked"] == 0
