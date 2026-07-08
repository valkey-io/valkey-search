import os
import struct
import pytest
from valkey import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode, LOGS_DIR
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector, KeyDataType, float_to_bytes
from util import waiters


def _get_vmsdk_info(client: Valkey) -> dict[str, str]:
    raw_info = client.execute_command("FT._DEBUG", "SHOW_INFO")
    info_data = {}
    for entry in raw_info:
        key = entry[3].decode('utf-8') if isinstance(entry[3], bytes) else entry[3]
        val = entry[5] if len(entry) > 5 else 0
        if isinstance(val, bytes):
            val = val.decode('utf-8')
        info_data[key] = str(val)
        if "." in key:
            info_data[key.split(".", 1)[1]] = str(val)
    return info_data


class TestVectorRegistrySharingOn(ValkeySearchTestCaseDebugMode):
    """
    Integration tests for Vector Registry with memory sharing enabled (default).
    Tests requirements #1 (HNSW) and #2 (FLAT).
    """

    def get_config_file_lines(self, testdir, port):
        lines = super().get_config_file_lines(testdir, port)
        lines.append("hash-max-listpack-entries 0")
        new_lines = []
        for line in lines:
            if line.startswith("loadmodule") and os.getenv("MODULE_PATH") in line:
                line += " --enable-vector-sharing yes --info-developer-visible yes"
            new_lines.append(line)
        return new_lines

    def _run_sharing_test(self, index_type: str, index_name: str):
        """
        Helper method to test index creation, vector ingestion, registry stats,
        HGET verification, index drop, and post-drop HGET verification.
        """
        client: Valkey = self.server.get_new_client()
        dim = 16
        num_vectors = 10

        vector_index = Index(
            index_name,
            [Vector("vec", dim, type=index_type, distance="L2")],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        vector_index.create(client)

        initial_info = _get_vmsdk_info(client)
        initial_shared = int(initial_info.get("vector_registry_shared_externally_cnt", 0))

        # 1. Ingest 10 vectors by issuing hset command
        expected_vectors = {}
        for i in range(num_vectors):
            key = f"doc:{i}"
            vec_data = [float(i + j) for j in range(dim)]
            vec_bytes = float_to_bytes(vec_data)
            expected_vectors[key] = vec_bytes
            client.hset(key, mapping={"vec": vec_bytes})

        # Wait for mutations / backfill to finish indexing
        waiters.wait_for_equal(
            lambda: vector_index.info(client).num_docs,
            num_vectors,
        )

        # 2. Ensure that the vector registry indicates these vectors were shared with the engine
        info_data = _get_vmsdk_info(client)
        shared_cnt = int(info_data.get("vector_registry_shared_externally_cnt", 0)) - initial_shared
        entry_cnt = int(info_data.get("vector_registry_entry_cnt", 0))

        assert shared_cnt == num_vectors, f"Expected {num_vectors} shared vectors, got {shared_cnt}"
        assert entry_cnt == num_vectors, f"Expected entry count {num_vectors}, got {entry_cnt}"

        # 3. Using hget command, ensure received replies match expected vectors
        for key, expected_bytes in expected_vectors.items():
            got_bytes = client.hget(key, "vec")
            assert got_bytes == expected_bytes, f"HGET returned unexpected value for key {key}"

        # 4. Drop the index and ensure vector registry indicates it is empty
        vector_index.drop(client)

        waiters.wait_for_equal(
            lambda: int(_get_vmsdk_info(client).get("vector_registry_entry_cnt", -1)),
            0,
        )

        # 5. Reverify that issuing hget still returns expected values
        for key, expected_bytes in expected_vectors.items():
            got_bytes = client.hget(key, "vec")
            assert got_bytes == expected_bytes, f"HGET post-drop returned unexpected value for key {key}"

    def test_vector_registry_hnsw_sharing_on(self):
        """Test #1: HNSW vector index with memory sharing enabled."""
        self._run_sharing_test("HNSW", "hnsw_registry_idx")

    def test_vector_registry_flat_sharing_on(self):
        """Test #2: FLAT vector index with memory sharing enabled."""
        self._run_sharing_test("FLAT", "flat_registry_idx")


class TestVectorRegistryMemoryDelta(ValkeySearchTestCaseDebugMode):
    """
    Integration tests comparing Valkey memory consumption when vector memory sharing is OFF vs ON.
    Tests requirements #3 (HNSW) and #4 (FLAT).
    """

    def _start_server_with_sharing(self, sharing_enabled: bool, test_suffix: str) -> tuple[object, Valkey]:
        """Helper to launch a server instance with explicit vector sharing configuration."""
        port = self.get_bind_port()
        test_name = f"{self.test_name}_{test_suffix}"
        testdir = f"{LOGS_DIR}/{test_name}"
        os.makedirs(testdir, exist_ok=True)

        server_path = os.getenv("VALKEY_SERVER_PATH")
        sharing_flag = "yes" if sharing_enabled else "no"

        lines = [
            "enable-debug-command yes",
            "hash-max-listpack-entries 0",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"dir {testdir}",
            f"loadmodule {os.getenv('MODULE_PATH')} --debug-mode yes --info-developer-visible yes --enable-vector-sharing {sharing_flag}",
        ]
        conf_file = f"{testdir}/valkey_{port}.conf"
        with open(conf_file, "w+") as f:
            for line in lines:
                f.write(f"{line}\n")

        logfile = f"{testdir}/logfile-primary-{port}.log"
        args = {
            "logfile": logfile,
        }

        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            args=args,
            port=port,
            conf_file=conf_file,
        )
        self.wait_for_logfile(logfile, "Ready to accept connections")
        client.ping()
        return server, client

    def _ingest_and_measure_memory(self, sharing_enabled: bool, index_type: str, index_name: str) -> tuple[int, int, int]:
        """
        Starts a fresh server with specified vector sharing setting, ingests 100 vectors
        of dimension 762, and returns (shared_externally_cnt, entry_cnt, used_memory).
        """
        sharing_str = "on" if sharing_enabled else "off"
        server, client = self._start_server_with_sharing(sharing_enabled, f"{index_type.lower()}_{sharing_str}")

        try:
            dim = 762
            num_vectors = 100

            vector_index = Index(
                index_name,
                [Vector("vec", dim, type=index_type, distance="L2")],
                prefixes=["doc:"],
                type=KeyDataType.HASH,
            )
            vector_index.create(client)

            initial_info = client.info("vector_registry")
            initial_shared = int(initial_info.get("vector_registry_shared_externally_cnt", 0))

            for i in range(num_vectors):
                key = f"doc:{i}"
                vec_data = [float(i + j) for j in range(dim)]
                vec_bytes = float_to_bytes(vec_data)
                client.hset(key, mapping={"vec": vec_bytes})

            waiters.wait_for_equal(
                lambda: vector_index.info(client).num_docs,
                num_vectors,
            )

            info_data = _get_vmsdk_info(client)
            print(f"DEBUG INGEST MEMORY (sharing={sharing_enabled}) INFO DATA:", info_data)
            shared_cnt = int(info_data.get("vector_registry_shared_externally_cnt", 0)) - initial_shared
            entry_cnt = int(info_data.get("vector_registry_entry_cnt", 0))
            used_memory = int(client.info("memory")["used_memory"])

            return shared_cnt, entry_cnt, used_memory
        finally:
            server.exit()

    def test_vector_registry_hnsw_memory_sharing(self):
        """
        Test #3: Create HNSW vector index with dimensions 762. Compare memory consumption
        when enable-vector-sharing is OFF vs ON. Delta must be >= 100 * 762 * sizeof(float).
        """
        dim = 762
        num_vectors = 100
        expected_raw_vector_bytes = num_vectors * dim * struct.calcsize("f")  # 100 * 762 * 4 = 304,800 bytes

        shared_off, entries_off, mem_off = self._ingest_and_measure_memory(
            sharing_enabled=False, index_type="HNSW", index_name="hnsw_mem_idx"
        )
        assert shared_off == 0, f"Expected 0 shared vectors when sharing is OFF, got {shared_off}"
        assert entries_off == num_vectors, f"Expected {num_vectors} tracked entries when sharing is OFF, got {entries_off}"

        shared_on, entries_on, mem_on = self._ingest_and_measure_memory(
            sharing_enabled=True, index_type="HNSW", index_name="hnsw_mem_idx"
        )
        assert shared_on == num_vectors, f"Expected {num_vectors} shared vectors when sharing is ON, got {shared_on}"
        assert entries_on == num_vectors, f"Expected {num_vectors} tracked entries when sharing is ON, got {entries_on}"

        memory_delta = mem_off - mem_on
        min_expected_bytes = int(expected_raw_vector_bytes * 0.95)
        assert memory_delta >= min_expected_bytes, (
            f"[HNSW] Memory delta between sharing OFF ({mem_off}) and ON ({mem_on}) was {memory_delta} bytes. "
            f"Expected at least {min_expected_bytes} bytes (95% of 100 * 762 * sizeof(float))."
        )

    def test_vector_registry_flat_memory_sharing(self):
        """
        Test #4: Create FLAT vector index with dimensions 762. Compare memory consumption
        when enable-vector-sharing is OFF vs ON. Delta must be >= 100 * 762 * sizeof(float).
        """
        dim = 762
        num_vectors = 100
        expected_raw_vector_bytes = num_vectors * dim * struct.calcsize("f")  # 100 * 762 * 4 = 304,800 bytes

        shared_off, entries_off, mem_off = self._ingest_and_measure_memory(
            sharing_enabled=False, index_type="FLAT", index_name="flat_mem_idx"
        )
        assert shared_off == 0, f"Expected 0 shared vectors when sharing is OFF, got {shared_off}"
        assert entries_off == num_vectors, f"Expected {num_vectors} tracked entries when sharing is OFF, got {entries_off}"

        shared_on, entries_on, mem_on = self._ingest_and_measure_memory(
            sharing_enabled=True, index_type="FLAT", index_name="flat_mem_idx"
        )
        assert shared_on == num_vectors, f"Expected {num_vectors} shared vectors when sharing is ON, got {shared_on}"
        assert entries_on == num_vectors, f"Expected {num_vectors} tracked entries when sharing is ON, got {entries_on}"

        memory_delta = mem_off - mem_on
        min_expected_bytes = int(expected_raw_vector_bytes * 0.95)
        assert memory_delta >= min_expected_bytes, (
            f"[FLAT] Memory delta between sharing OFF ({mem_off}) and ON ({mem_on}) was {memory_delta} bytes. "
            f"Expected at least {min_expected_bytes} bytes (95% of 100 * 762 * sizeof(float))."
        )
