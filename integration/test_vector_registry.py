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


def _get_vector_registry_stats(client: Valkey) -> dict[str, int]:
    raw_stats = client.execute_command("FT._DEBUG", "VECTOR_SHARING_STATS")
    stats_data = {}
    for i in range(0, len(raw_stats), 2):
        key = raw_stats[i].decode('utf-8') if isinstance(raw_stats[i], bytes) else raw_stats[i]
        val = raw_stats[i+1]
        stats_data[key] = int(val)
    return stats_data


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
        
        sharing_active = int(initial_info["vector_registry_sharing_active"])
        if not sharing_active:
            pytest.skip("Vector memory sharing is not active/supported on this Valkey server version.")
            
        initial_shared = int(initial_info["vector_registry_shared_externally_cnt"])

        # 1. Ingest 10 vectors by issuing hset command
        expected_vectors = {}
        for i in range(num_vectors):
            key = f"doc:{i}"
            vec_data = [float(i + j) for j in range(dim)]
            vec_bytes = float_to_bytes(vec_data)
            expected_vectors[key] = vec_bytes
            client.hset(key, mapping={"vec": vec_bytes})

        # 2. Ensure that the vector registry indicates these vectors were shared with the engine
        info_data = _get_vmsdk_info(client)
        shared_cnt = int(info_data["vector_registry_shared_externally_cnt"]) - initial_shared
        entry_cnt = int(info_data["vector_registry_entry_cnt"])

        assert shared_cnt == num_vectors, f"Expected {num_vectors} shared vectors, got {shared_cnt}"
        assert entry_cnt == num_vectors, f"Expected entry count {num_vectors}, got {entry_cnt}"

        # 3. Using hget command, ensure received replies match expected vectors
        for key, expected_bytes in expected_vectors.items():
            got_bytes = client.hget(key, "vec")
            assert got_bytes == expected_bytes, f"HGET returned unexpected value for key {key}"

        # 4. Drop the index and ensure vector registry indicates it is empty
        vector_index.drop(client)

        waiters.wait_for_equal(
            lambda: int(_get_vmsdk_info(client)["vector_registry_entry_cnt"]),
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

    def test_vector_registry_advanced_coverage(self):
        """
        Enhance coverage using FT._DEBUG VECTOR_SHARING_STATS.
        Tests overwrites, same vs different vectors, lookup hits/misses, and lifecycle.
        """
        client: Valkey = self.server.get_new_client()
        initial_info = _get_vmsdk_info(client)
        if not int(initial_info["vector_registry_sharing_active"]):
            pytest.skip("Vector memory sharing is not active/supported on this Valkey server version.")
            
        dim = 8
        index_name = "adv_registry_idx"

        vector_index = Index(
            index_name,
            [Vector("vec", dim, type="HNSW", distance="L2")],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        vector_index.create(client)

        # 1. Initial stats should be 0
        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 0
        assert stats["hash_sharing_errors"] == 0
        assert stats["hash_sharing_hits"] == 0
        assert stats["lookup_record_hits"] == 0
        assert stats["lookup_record_misses"] == 0

        # 2. Ingest a vector and verify increments
        key1 = "doc:1"
        vec_data1 = [1.0] * dim
        vec_bytes1 = float_to_bytes(vec_data1)
        client.hset(key1, mapping={"vec": vec_bytes1})

        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 1
        assert stats["hash_sharing_hits"] == 1
        # LookupRecord is called exactly once during AddRecord for the new document
        assert stats["lookup_record_hits"] == 1
        assert stats["lookup_record_misses"] == 0

        # 3. Update document with the EXACT SAME vector
        client.hset(key1, mapping={"vec": vec_bytes1})

        # Since HSET overwrites the reference with a raw string,
        # Track reuses the VectorRecord and re-shares it with Valkey (hash_sharing_hits becomes 2).
        # AddRecord also calls LookupRecord (Hit).
        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 1
        assert stats["hash_sharing_hits"] == 2
        assert stats["lookup_record_hits"] == 2
        assert stats["lookup_record_misses"] == 0

        # 4. Update document with a DIFFERENT vector
        vec_data2 = [3.0] * dim
        vec_bytes2 = float_to_bytes(vec_data2)
        client.hset(key1, mapping={"vec": vec_bytes2})

        # Track sees the content differs, replaces it, and shares it (hash_sharing_hits becomes 3).
        # AddRecord calls LookupRecord (Hit).
        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 1
        assert stats["hash_sharing_hits"] == 3
        assert stats["lookup_record_hits"] == 3
        assert stats["lookup_record_misses"] == 0

        # 5. Delete the document and verify drop in entry count
        client.delete(key1)
        waiters.wait_for_equal(
            lambda: vector_index.info(client).num_docs,
            0,
        )
        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 0

    @pytest.mark.parametrize("index_type,distance_metric", [
        ("HNSW", "L2"),
        ("HNSW", "COSINE"),
        ("FLAT", "L2"),
        ("FLAT", "COSINE"),
    ])
    def test_vector_registry_deletion_coverage(self, index_type: str, distance_metric: str):
        """
        Verify that document deletion correctly erases the registry entry for both index types
        and both distance metrics. By starting from 0 and asserting the count drops to 0,
        we mathematically guarantee that the specific key was the one erased.
        """
        client: Valkey = self.server.get_new_client()
        dim = 8
        index_name = f"del_cov_{index_type}_{distance_metric}"

        vector_index = Index(
            index_name,
            [Vector("vec", dim, type=index_type, distance=distance_metric)],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        vector_index.create(client)

        # 1. Initial stats should be 0
        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 0

        # 2. Ingest a vector and verify increments
        key1 = "doc:1"
        vec_data1 = [1.0] * dim
        vec_bytes1 = float_to_bytes(vec_data1)
        client.hset(key1, mapping={"vec": vec_bytes1})

        waiters.wait_for_equal(
            lambda: vector_index.info(client).num_docs,
            1,
        )

        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 1

        # 3. Delete the document and verify drop to 0
        client.delete(key1)

        waiters.wait_for_equal(
            lambda: vector_index.info(client).num_docs,
            0,
        )

        stats = _get_vector_registry_stats(client)
        assert stats["entry_cnt"] == 0

    def test_hash_sharing_errors_coverage(self):
        """
        Trigger non-zero hash_sharing_errors by forcing ValkeyModule_HashSetStringRef to fail
        using a Controlled Variable via FT._DEBUG.
        """
        client: Valkey = self.server.get_new_client()
        initial_info = _get_vmsdk_info(client)
        if not int(initial_info["vector_registry_sharing_active"]):
            pytest.skip("Vector memory sharing is not active/supported on this Valkey server version.")
            
        dim = 8
        vector_index = Index(
            "err_cov_idx",
            [Vector("vec", dim, type="HNSW", distance="L2")],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        vector_index.create(client)

        try:
            # Enable the forced error injection
            assert client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceHashSharingError 1") == b"OK"

            key1 = "doc:1"
            vec_data1 = [1.0] * dim
            vec_bytes1 = float_to_bytes(vec_data1)
            client.hset(key1, mapping={"vec": vec_bytes1})

            waiters.wait_for_equal(
                lambda: vector_index.info(client).num_docs,
                1,
            )

            stats = _get_vector_registry_stats(client)
            assert stats["hash_sharing_errors"] > 0, f"Expected > 0 errors, got {stats['hash_sharing_errors']}"

        finally:
            # Ensure we reset the control variable even if asserts fail
            client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceHashSharingError 0")

def _get_index_num_docs(client: Valkey, index_name: str) -> str:
    info = client.execute_command("FT.INFO", index_name)
    decoded = {}
    for i in range(0, len(info) - 1, 2):
        k = info[i].decode() if isinstance(info[i], bytes) else str(info[i])
        decoded[k] = info[i + 1]
    val = decoded.get("num_docs", 0)
    return val.decode() if isinstance(val, bytes) else str(val)


class TestVectorRegistryElementTypeIsolation(ValkeySearchTestCaseDebugMode):
    """Two indexes over the same key and attribute but with different element
    types must not share a VectorRegistry record.

    A VectorRecord carries a reciprocal magnitude computed by reading the
    payload as one specific element type. FLOAT16 and BFLOAT16 payloads of the
    same DIM are byte-identical in length, so before the registry key included
    the data type the two indexes shared a single record and whichever
    registered second determined the magnitude. The first index then returned
    wrong cosine distances -- a self-query scored ~0.99 instead of ~0.
    """

    def _self_distance(self, client: Valkey, index_name: str, blob: bytes) -> float:
        res = client.execute_command(
            "FT.SEARCH", index_name, "*=>[KNN 1 @v $q AS sc]",
            "PARAMS", "2", "q", blob, "RETURN", "1", "sc", "DIALECT", "2",
        )
        assert res[0] >= 1, f"{index_name} returned no results"
        fields = res[2]
        attrs = {fields[i]: fields[i + 1] for i in range(0, len(fields), 2)}
        return float(attrs[b"sc"])

    @pytest.mark.parametrize(
        "type_a,type_b,dim_a,dim_b",
        [
            # Same DIM, both 2-byte: byte-identical payloads. The realistic case,
            # e.g. comparing recall between the two low-precision formats.
            ("FLOAT16", "BFLOAT16", 3, 3),
            # Different DIM chosen so DIM_A * 4 == DIM_B * 2.
            ("FLOAT32", "FLOAT16", 3, 6),
        ],
    )
    def test_distinct_element_types_do_not_share_records(
        self, type_a: str, type_b: str, dim_a: int, dim_b: int
    ):
        client: Valkey = self.server.get_new_client()
        values = [3.0, 4.0, 0.0]
        blob = struct.pack(f"<{len(values)}e", *values) if type_a == "FLOAT16" \
            else struct.pack(f"<{len(values)}f", *values)

        # Both indexes cover the same prefix and the same attribute.
        for name, vtype, dim in (("idx_a", type_a, dim_a), ("idx_b", type_b, dim_b)):
            client.execute_command(
                "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", "shared:",
                "SCHEMA", "v", "VECTOR", "FLAT", "6", "DIM", str(dim),
                "TYPE", vtype, "DISTANCE_METRIC", "COSINE",
            )
        client.hset("shared:1", "v", blob)
        waiters.wait_for_equal(
            lambda: int(_get_index_num_docs(client, "idx_a")), 1, timeout=30
        )

        # idx_a queried with the exact bytes it stores must score ~0. If it were
        # handed idx_b's record the magnitude would be wrong and this would not
        # be near zero.
        distance = self._self_distance(client, "idx_a", blob)
        assert abs(distance) < 1e-4, (
            f"{type_a} self-distance was {distance}, expected ~0 -- the record "
            f"was likely shared with the {type_b} index and carries its magnitude"
        )


class TestVectorRegistryMemoryDelta(ValkeySearchTestCaseDebugMode):
    """
    Integration tests comparing Valkey memory consumption when vector memory sharing is OFF vs ON.
    Tests requirements #3 (HNSW) and #4 (FLAT).
    """

    def get_config_file_lines(self, testdir, port) -> list[str]:
        sharing_flag = "yes" if getattr(self, "_sharing_enabled", True) else "no"
        return [
            "enable-debug-command yes",
            "hash-max-listpack-entries 0",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"dir {testdir}",
            f"loadmodule {os.getenv('MODULE_PATH')} --debug-mode yes --info-developer-visible yes --enable-vector-sharing {sharing_flag}",
        ]

    def _start_server_with_sharing(self, sharing_enabled: bool, test_suffix: str) -> tuple[object, Valkey]:
        """Helper to launch a server instance with explicit vector sharing configuration."""
        self._sharing_enabled = sharing_enabled
        server, client, _ = self.start_server(
            port=self.get_bind_port(),
            test_name=f"{self.test_name}_{test_suffix}",
            cluster_enabled=False,
            is_primary=True,
        )
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

            initial_info = _get_vmsdk_info(client)
            initial_shared = int(initial_info["vector_registry_shared_externally_cnt"])

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
            
            sharing_active = int(info_data["vector_registry_sharing_active"])
            if sharing_enabled and not sharing_active:
                pytest.skip("Vector memory sharing is not active/supported on this Valkey server version.")

            shared_cnt = int(info_data["vector_registry_shared_externally_cnt"]) - initial_shared
            entry_cnt = int(info_data["vector_registry_entry_cnt"])
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
        min_expected_bytes = int(expected_raw_vector_bytes * 0.90)
        assert memory_delta >= min_expected_bytes, (
            f"[HNSW] Memory delta between sharing OFF ({mem_off}) and ON ({mem_on}) was {memory_delta} bytes. "
            f"Expected at least {min_expected_bytes} bytes (90% of 100 * 762 * sizeof(float))."
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
        min_expected_bytes = int(expected_raw_vector_bytes * 0.90)
        assert memory_delta >= min_expected_bytes, (
            f"[FLAT] Memory delta between sharing OFF ({mem_off}) and ON ({mem_on}) was {memory_delta} bytes. "
            f"Expected at least {min_expected_bytes} bytes (90% of 100 * 762 * sizeof(float))."
        )
