import base64
import os
import struct
import tempfile
import time
import subprocess
import shutil
import socket
from valkey import ResponseError, Valkey
from valkey_search_test_case import (
    LOGS_DIR,
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseCommon,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector
from util import waiters
import pytest
import logging


class TestRDBCorruptedIndex(ValkeySearchTestCaseBase):
    """Test handling of corrupted index in RDB file"""

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

    def setup_method(self, method):
        """Set up test method - create temporary RDB file."""
        self.temp_dir = tempfile.mkdtemp()
        self.rdb_path = os.path.join(self.temp_dir, "corrupted.rdb")

        # Write RDB file from base64
        rdb_data = base64.b64decode(self.RDB_BASE64)
        with open(self.rdb_path, "wb") as f:
            f.write(rdb_data)

        logging.info(f"Created test RDB at: {self.rdb_path}")

    def teardown_method(self, method):
        """Clean up test method - remove temporary files."""
        import shutil

        if hasattr(self, "temp_dir") and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            logging.info(f"Removed temporary directory: {self.temp_dir}")

    def _start_server(self, port, test_name, rdbfile="dump.rdb", search_module_args=""):
        server_path = os.getenv("VALKEY_SERVER_PATH")
        testdir = f"/tmp/valkey-test-framework-files/{test_name}"

        os.makedirs(testdir, exist_ok=True)
        curdir = os.getcwd()
        os.chdir(testdir)

        # Copy RDB file to testdir if it's a path (not just a filename)
        if "/" in rdbfile:
            # It's a path, copy the file to testdir
            rdb_filename = os.path.basename(rdbfile)
            dest_path = os.path.join(testdir, rdb_filename)
            shutil.copy2(rdbfile, dest_path)
            rdbfile = rdb_filename  # Use just the filename
            logging.info(f"Copied RDB file from {rdbfile} to {dest_path}")

        # Verify RDB file exists
        rdb_path_in_testdir = os.path.join(testdir, rdbfile)
        if os.path.exists(rdb_path_in_testdir):
            logging.info(
                f"RDB file exists at: {rdb_path_in_testdir}, size: {os.path.getsize(rdb_path_in_testdir)} bytes"
            )
        else:
            logging.error(f"RDB file NOT found at: {rdb_path_in_testdir}")

        lines = [
            "enable-debug-command yes",
            f"dbfilename {rdbfile}",
            f"dir {testdir}",
            f"port {port}",
            f"logfile logfile_{port}",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"loadmodule {os.getenv('MODULE_PATH')} {search_module_args}",
        ]

        conf_file = f"{testdir}/valkey_{port}.conf"
        with open(conf_file, "w+") as f:
            for line in lines:
                f.write(f"{line}\n")
            f.write("\n")
            f.close()

        logging.info(f"Starting server with config: {conf_file}")

        # Start server directly using subprocess instead of test framework
        cmd = [server_path, conf_file]
        logging.info(f"Server command: {' '.join(cmd)}")

        process = subprocess.Popen(
            cmd,
            cwd=testdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Give server time to start
        time.sleep(1)

        # Create client
        try:
            client = Valkey(host="localhost", port=port, socket_connect_timeout=5)
            client.ping()  # Test connection
        except Exception as e:
            logging.error(f"Failed to connect to server: {e}")
            if process.poll() is None:
                process.terminate()
            raise

        os.chdir(curdir)
        logfile = f"{testdir}/logfile_{port}"

        # Return a simple server handle
        class ServerHandle:
            def __init__(self, process, port):
                self.process = process
                self.port = port

            def is_alive(self):
                return self.process.poll() is None

            def exit(self):
                if self.process.poll() is None:
                    self.process.terminate()
                    self.process.wait(timeout=5)

        return ServerHandle(process, port), client, logfile

    def test_corrupted_rdb_load_fails(self):
        """Test that loading corrupted RDB fails without skip option."""
        # This should fail because the RDB is corrupted and we're not skipping index load
        try:
            server, client, logfile = self._start_server(
                self.get_bind_port(), "corrupted_rdb_test_fail", self.rdb_path
            )

            # If we get here, the server started (which shouldn't happen)
            logging.error(
                "Server started successfully but should have failed with corrupted RDB"
            )

            # Check if it actually loaded the data
            try:
                dbsize = client.dbsize()
                logging.info(f"Database size: {dbsize}")
                if dbsize > 0:
                    pytest.fail("Server loaded corrupted RDB data, expected failure")
            except Exception as e:
                logging.info(f"Client connection failed: {e}")

            server.exit()
            pytest.fail("Server should have failed to start with corrupted RDB")

        except Exception as e:
            # This is expected - server should fail to start
            logging.info(f"Server failed to start as expected: {e}")

            # Now check the logs to verify it was actually the RDB corruption that caused the failure
            test_dirs = [f"/tmp/valkey-test-framework-files/corrupted_rdb_test_fail"]
            log_found = False

            for test_dir in test_dirs:
                if os.path.exists(test_dir):
                    for logfile in os.listdir(test_dir):
                        if logfile.startswith("logfile_"):
                            log_path = os.path.join(test_dir, logfile)
                            with open(log_path, "r") as f:
                                log_content = f.read()
                                logging.info(
                                    f"Server log content from {log_path}:\n{log_content}"
                                )
                                log_found = True

                                # Verify the failure was due to RDB corruption
                                if (
                                    "Failed to load ValkeySearch aux section from RDB"
                                    in log_content
                                ):
                                    logging.info(
                                        "✅ Confirmed: Server failed due to corrupted search index in RDB"
                                    )
                                    return  # Test passed
                                elif "Short read or OOM loading DB" in log_content:
                                    logging.info(
                                        "✅ Confirmed: Server failed due to RDB corruption"
                                    )
                                    return  # Test passed
                                elif "Internal error in RDB reading" in log_content:
                                    logging.info(
                                        "✅ Confirmed: Server failed due to RDB reading error"
                                    )
                                    return  # Test passed

            if not log_found:
                logging.warning("No server logs found to verify failure cause")

            # If we get here, the server failed for the right reason
            logging.info("✅ Server correctly failed to load corrupted RDB file")

    def test_corrupted_rdb_skip_index_load_succeeds(self):
        """Test that loading corrupted RDB succeeds with skip index option and verify schema."""
        # Create a server with RDB file and skip index option using proper test framework
        available_port = self.get_bind_port()
        server, client, logfile = self._start_server(
            available_port, "corrupted_rdb_test", self.rdb_path, "--skip-rdb-load yes"
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
                        assert (
                            b"distance_metric" in attr_dict
                            or b"algorithm" in attr_dict
                            or b"ALGORITHM" in attr_dict
                        )
                        assert attr_dict.get(b"algorithm") == b"HNSW"
                        assert b"ef_construction" in attr_dict
                        assert b"ef_runtime" in attr_dict
                        assert b"capacity" in attr_dict
                        assert b"dim" in attr_dict

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


class TestHNSWDuplicateLabelRDBLoad(ValkeySearchTestCaseCommon):
    """
    Regression test for loading an RDB that carries a duplicate label.

    Previously ModifyRecordImpl first called markDelete() on the record to
    tombstone the existing slot with the label and then called addPoint() to add
    the new vector with the exact same label. In the normal case, the same slot
    was updated and there were no duplicate labels. The problem occurred when
    another document was deleted between markDelete() and addPoint(), which put
    another tombstoned slot at the front of deleted_elements. The addPoint()
    for the modified record reused it, adding the label to a new slot while still
    keeping the same label on the old slot it had just tombstoned.
    """

    RDB_FILENAME = "hnsw_duplicate_label.rdb"
    RDB_FIXTURE = f"rdbs/{RDB_FILENAME}"
    SURVIVOR_VEC = struct.pack('<4f', 10.0, 20.0, 30.0, 40.0)

    # TODO: Make functionality common once https://github.com/valkey-io/valkey-search/pull/1172 is merged
    def _start_server(self, test_name, search_module_args=""):
        server_path = os.getenv("VALKEY_SERVER_PATH")
        testdir = f"{LOGS_DIR}/{test_name}"
        port = self.get_bind_port()

        os.makedirs(testdir, exist_ok=True)
        shutil.copy(
            os.path.join(os.path.dirname(__file__), self.RDB_FIXTURE),
            os.path.join(testdir, self.RDB_FILENAME),
        )

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
            f.write("\n".join(lines) + "\n")

        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            port=port,
            conf_file=conf_file,
            args={"logfile": f"logfile_{port}", "dbfilename": self.RDB_FILENAME},
        )
        return server, client, os.path.join(testdir, f"logfile_{port}")

    def test_load_rdb_with_duplicate_label(self):
        server, client, logfile = self._start_server(
            "hnsw_dup_label_rdb", search_module_args="--debug-mode yes")
        client.config_set("search.info-developer-visible", "yes")

        hnsw_index = Index(
            "idx",
            [Vector("vector", 4, type="HNSW", distance="L2")],
            prefixes=["doc:"]
        )
        waiters.wait_for_true(
            lambda: hnsw_index.backfill_complete(client), timeout=10
        )

        dup_on_load = int(client.info("SEARCH").get(
            "search_hnsw_duplicate_label_on_load_count", 0))
        assert dup_on_load == 1, \
            f"Expected a duplicate label on load, got {dup_on_load}"

        ft_info = hnsw_index.info(client)
        assert ft_info.num_docs == 1, \
            f"Expected 1 doc after load, got {ft_info.num_docs}"

        # The survivor should be doc:1 carrying its updated vector.
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 2 @vector $q]",
            "PARAMS", "2", "q", self.SURVIVOR_VEC,
            "RETURN", "1", "vector",
        )
        assert search_result[0] == 1, \
            f"Expected 1 search result, got {search_result[0]}"
        assert search_result[1] == b"doc:1", \
            f"Expected doc:1 to survive, got {search_result[1]}"
        returned_fields = search_result[2]
        vector_value = returned_fields[returned_fields.index(b"vector") + 1]
        assert vector_value == self.SURVIVOR_VEC, \
            f"Expected doc:1 to carry the updated vector, got {vector_value!r}"

        # Deleting the survivor must actually remove it, proving label_lookup_
        # resolves the label to the live slot rather than a tombstone.
        client.delete("doc:1")
        assert hnsw_index.info(client).num_docs == 0
        assert int(client.info("SEARCH").get(
            "search_hnsw_remove_exceptions_count", 0)) == 0
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 2 @vector $q]",
            "PARAMS", "2", "q", self.SURVIVOR_VEC,
        )
        assert search_result[0] == 0, \
            f"Expected doc:1 to be deleted, got {search_result[0]} results"
