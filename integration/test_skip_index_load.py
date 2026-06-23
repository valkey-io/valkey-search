import base64
import os
import time
import logging

from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseCommon, LOGS_DIR
from valkeytestframework.conftest import resource_port_tracker
import pytest


class TestRDBCorruptedIndex(ValkeySearchTestCaseCommon):
    """Test handling of corrupted index in RDB file"""

    RDB_FILENAME = "dump.rdb"

    # Corrupted RDB with vector index
    RDB_BASE64 = (
        "UkVESVMwMDEx+gp2YWxrZXktdmVyCzI1NS4yNTUuMjU1+gpyZWRpcy1iaXRzwED6BWN0aW1lwrvV"
        "fGj6CHVzZWQtbWVtwjgwZgD6CGFvZi1iYXNlwAD+APsBABAFZG9jOjFAfX0AAAAGAIV0aXRsZQaP"
        "U2FtcGxlIERvY3VtZW50EItkZXNjcmlwdGlvbgyyVGhpcyBpcyBhIHRlc3QgZG9jdW1lbnQgd2l0"
        "aCBhbiBlbWJlZGRpbmcgdmVjdG9yIDEziWVtYmVkZGluZwqQAAAAAHfWiD531gg/s0FNPxH/94FW"
        "T5J5qtyEAQICAoAAAQAAAgEFQJgIARAEGpEBChdpbmRleF90b190ZXN0X3NraXBfbG9hZBIEZG9j"
        "OhgBMhUKBXRpdGxlEgV0aXRsZRoFGgMKASwyIQoLZGVzY3JpcHRpb24SC2Rlc2NyaXB0aW9uGgUa"
        "AwoBLDIuCgllbWJlZGRpbmcSCWVtYmVkZGluZxoWChQIBBABGAMgASiAUDIHCBAQyAEYCjoCCAJA"
        "AAUbCAESFwoVCgV0aXRsZRIFdGl0bGUaBRoDCgEsBQAFJwgBEiMKIQoLZGVzY3JpcHRpb24SC2Rl"
        "c2NyaXB0aW9uGgUaAwoBLAUABTQIARIwCi4KCWVtYmVkZGluZxIJZW1iZWRkaW5nGhYKFAgEEAEY"
        "AyABKIBQMgcIEBDIARgKBSYKJBCAUBiAlOvcAyCcASiMATCEAUgQUCBYEGH+gitlRxXXP2jIAQVA"
        "nwqcAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB31og+d9YIP7NBTT8AAAAAAAAAAAUKCggAAAAAAAAA"
        "AAUABTQIAhowCi4KCWVtYmVkZGluZxIJZW1iZWRkaW5nGhYKFAgEEAEYAyABKIBQMgcIEBDIARgK"
        "BQ4KDAoFZG9jOjEdAACAPwUAAP+auuFfJikt2g=="
    )

    def _setup_rdb_in_testdir(self, testdir):
        """Decode the corrupted RDB and write it into testdir as dump.rdb."""
        os.makedirs(testdir, exist_ok=True)
        rdb_path = os.path.join(testdir, self.RDB_FILENAME)
        rdb_data = base64.b64decode(self.RDB_BASE64)
        with open(rdb_path, "wb") as f:
            f.write(rdb_data)
        logging.info(f"Created corrupted RDB at: {rdb_path} ({len(rdb_data)} bytes)")

    def _start_server(self, test_name, search_module_args="", expect_failure=False):
        """Test that loading corrupted RDB fails without skip option."""
        server_path = os.getenv("VALKEY_SERVER_PATH")
        testdir = f"{LOGS_DIR}/{test_name}"
        port = self.get_bind_port()

        # Place the corrupted RDB in testdir before starting
        self._setup_rdb_in_testdir(testdir)

        lines = [
            "enable-debug-command yes",
            f"dbfilename {self.RDB_FILENAME}",
            f"dir {testdir}",
            f"port {port}",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"loadmodule {os.getenv('MODULE_PATH')} {search_module_args}",
        ]

        conf_file = os.path.join(testdir, f"valkey_{port}.conf")
        with open(conf_file, "w") as f:
            for line in lines:
                f.write(f"{line}\n")

        logging.info(f"Starting server with config: {conf_file}")

        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            port=port,
            conf_file=conf_file,
            args={"logfile": f"logfile_{port}", "dbfilename": self.RDB_FILENAME},
            wait_for_ping=not expect_failure,
            connect_client=not expect_failure,
        )

        logfile = os.path.join(testdir, f"logfile_{port}")
        return server, client, logfile

    @pytest.mark.skipif(
        os.getenv("SAN_BUILD") not in (None, "no"),
        reason="Corrupted RDB triggers expected ASAN heap-buffer-overflow during index load",
    )
    def test_corrupted_rdb_load_fails(self):
        """Test that loading corrupted RDB fails without skip option."""
        server, client, logfile = self._start_server(
            "corrupted_rdb_test_fail", expect_failure=True
        )

        # Wait for the server process to exit
        server.wait_for_shutdown()

        # Check server logs for RDB errors
        with open(logfile, "r") as f:
            log_content = f.read()
        assert (
            "Failed to load ValkeySearch aux section from RDB" in log_content
            or "Short read or OOM loading DB" in log_content
            or "Internal error in RDB reading" in log_content
        ), "Server exited but logfile did not contain expected error message"

    def test_corrupted_rdb_skip_index_load_succeeds(self):
        """Test that loading corrupted RDB succeeds with skip index option and verify schema."""
        server, client, logfile = self._start_server(
            "corrupted_rdb_test_skip", "--skip-rdb-load yes"
        )

        try:
            # Server should start successfully (this is the key test)
            assert server.is_alive(), "Server failed to start with skip index option"

            # Check the server logs for any issues
            if os.path.exists(logfile):
                with open(logfile, "r") as f:
                    log_content = f.read()
                    logging.info(f"Server log content:\n{log_content}")

            # Check what keys exist in the database
            all_keys = client.keys("*")
            logging.info(f"All keys in database: {all_keys}")
            dbsize = client.dbsize()
            logging.info(f"Database size: {dbsize}")

            # Verify the document data was loaded
            doc_data = client.hgetall("doc:1")
            logging.info(f"Document data for doc:1: {doc_data}")
            assert doc_data is not None, "Document not found"
            assert b"title" in doc_data, "Document missing title field"
            assert doc_data[b"title"] == b"Sample Document", "Document title mismatch"

            # Verify the index exists but is empty (skipped during load)
            indices = client.execute_command("FT._LIST")
            assert (
                b"index_to_test_skip_load" in indices
            ), "Index not found in index list"

            # Get index info to verify schema
            info = client.execute_command("FT.INFO", "index_to_test_skip_load")
            info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}

            # Check that we have the expected fields (schema verification)
            attributes = info_dict.get(b"attributes", [])
            field_names = set()

            for attr in attributes:
                if isinstance(attr, list):
                    for i in range(0, len(attr), 2):
                        if attr[i] == b"identifier":
                            field_names.add(attr[i + 1])

            # Verify expected fields are present
            expected_fields = {b"title", b"description", b"embedding"}
            assert expected_fields.issubset(
                field_names
            ), f"Missing fields. Expected: {expected_fields}, Got: {field_names}"

            # Verify vector field configuration
            for attr in attributes:
                if isinstance(attr, list):
                    attr_dict = {attr[i]: attr[i + 1] for i in range(0, len(attr), 2)}
                    if attr_dict.get(b"identifier") == b"embedding":
                        assert attr_dict.get(b"attribute") == b"embedding"
                        assert attr_dict.get(b"type") == b"VECTOR"
                        # Check vector parameters in the index array
                        index_params = attr_dict.get(b"index", [])
                        if isinstance(index_params, list):
                            index_dict = {
                                index_params[i]: index_params[i + 1]
                                for i in range(0, len(index_params), 2)
                            }
                            assert b"dimensions" in index_dict or b"DIM" in index_dict
                            assert (
                                b"distance_metric" in index_dict
                                or b"algorithm" in index_dict
                                or b"ALGORITHM" in index_dict
                            )
                            # Verify it's using HNSW algorithm
                            algorithm = index_dict.get(b"algorithm") or index_dict.get(
                                b"ALGORITHM"
                            )
                            if isinstance(algorithm, list) and len(algorithm) >= 2:
                                # Algorithm is stored as [b'name', b'HNSW', ...other params...]
                                assert (
                                    algorithm[0] == b"name"
                                ), f"Expected algorithm name field, got {algorithm[0]}"
                                assert (
                                    algorithm[1] == b"HNSW"
                                ), f"Expected HNSW algorithm, got {algorithm[1]}"
                            else:
                                assert (
                                    algorithm == b"HNSW"
                                ), f"Expected HNSW algorithm, got {algorithm}"

            logging.info("Index schema verified successfully")

            # Index should exist but have 0 documents initially (since we skipped loading)
            num_docs = int(info_dict.get(b"num_docs", 0))
            logging.info(f"Number of documents in index initially: {num_docs}")

            # Wait for mutation queue to be empty (all mutations processed)
            max_wait_time = 10  # seconds
            start_time = time.time()

            while time.time() - start_time < max_wait_time:
                info = client.execute_command("FT.INFO", "index_to_test_skip_load")
                info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
                mutation_queue_size = int(info_dict.get(b"mutation_queue_size", 0))

                if mutation_queue_size == 0:
                    logging.info("Mutation queue is empty, all mutations processed")
                    break

                logging.info(
                    f"Waiting for mutations to process, queue size: {mutation_queue_size}"
                )
                time.sleep(0.5)

            # After mutations are processed, check final document count
            info = client.execute_command("FT.INFO", "index_to_test_skip_load")
            info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
            num_docs = int(info_dict.get(b"num_docs", 0))

            # After backfill, we should have documents indexed
            # Note: num_docs counts indexed attributes, not unique documents
            # With 3 attributes (title, description, embedding), we expect num_docs to be a multiple of the attribute count
            assert (
                num_docs > 0
            ), f"Expected documents to be indexed after backfill, got {num_docs}"

            # Verify the vector index is functional
            # Search for the document using vector similarity
            embedding_hex = client.hget("doc:1", "embedding")
            assert embedding_hex is not None, "Document missing embedding field"

            # Perform a simple KNN search to verify index is functional
            try:
                # Use the same embedding to find itself (should return distance 0)
                results = client.execute_command(
                    "FT.SEARCH",
                    "index_to_test_skip_load",
                    "*=>[KNN 1 @embedding $vec AS score]",
                    "PARAMS",
                    "2",
                    "vec",
                    embedding_hex,
                    "RETURN",
                    "1",
                    "score",
                    "DIALECT",
                    "2",
                )

                assert results[0] >= 1, f"Expected at least 1 result, got {results[0]}"
                # Check if doc:1 is in results (it should be since we used its embedding)
                result_found = False
                for i in range(1, len(results)):
                    if results[i] == b"doc:1":
                        result_found = True
                        break
                assert result_found, f"Expected doc:1 in results, got: {results}"

                logging.info("Vector search successful after index rebuild")

            except ResponseError as e:
                pytest.fail(f"Vector search failed: {e}")

        finally:
            server.exit()
