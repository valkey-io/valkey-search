"""Integration tests for swapdb replication RDB loading and post-swap recovery.

Covers:
1. Full-sync matrix across repl-diskless-load (disabled, on-empty-db, swapdb),
   rdb-write-v2 (yes, no), and dual-channel-replication-enabled (yes, no), plus
   an empty-replica on-empty-db diskless-load case and a fresh-replica
   zero-index swapdb case. Exercises 2 DBs (DB 0 + DB 1), overlapping
   identical/stale/absent keys on the replica, and a pre-sync deleted key on
   the primary.
2. Cluster fan-out (ForceReplicasOnly + ALLSHARDS and LOCALONLY) during paused
   and active post-swapdb recovery.
3. Large multi-index swapdb full sync under tight repl-timeout with concurrent
   writes and PINGs, plus an async-loading (same replid, backlog overflow)
   read-consistency test across pre-swap, active async RDB loading, post-swap
   recovery, and post-recovery.
"""

import os
import struct
import threading
import time
import pytest
from valkey import ResponseError
from valkey.exceptions import BusyLoadingError

from ft_info_parser import FTInfoParser
from valkey_search_test_case import (
    ValkeySearchClusterTestCaseDebugMode,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters


SWAPDB_LOG_MARKER = "Swapping active DB with loaded DB"
LOADING_DB_LOG_MARKER = "PRIMARY <-> REPLICA sync: Loading DB in memory"
DISKLESS_PARSER_LOG_MARKER = "to parser"
DISK_LOAD_LOG_MARKER = "to disk"
RECOVERY_IN_PROGRESS_MSG = "Index is recovering after replication DB swap"
CONSISTENCY_FAILED_MSG = "Index or slot consistency check failed"


def _logfile_offset(node) -> int:
    try:
        return os.path.getsize(node.logfile)
    except OSError:
        return 0


def _logfile_contains_since(node, offset: int, pattern: str) -> bool:
    try:
        with open(node.logfile, "r", errors="replace") as f:
            f.seek(offset)
            return pattern in f.read()
    except OSError:
        return False


def _strip_json_and_configure_search(lines: list[str]) -> list[str]:
    """Omit the JSON module (required for swapdb) and enable vector sharing."""
    json_mod = os.getenv("JSON_MODULE_PATH")
    search_mod = os.getenv("MODULE_PATH")
    out = []
    for line in lines:
        stripped = line.strip()
        if json_mod and stripped == f"loadmodule {json_mod}":
            continue
        if stripped.startswith("loadmodule") and (
            (search_mod and search_mod in stripped) or "libsearch" in stripped
        ):
            if "--enable-vector-sharing" not in line:
                line += " --enable-vector-sharing yes"
            if "--info-developer-visible" not in line:
                line += " --info-developer-visible yes"
        out.append(line)
    out.append("hash-max-listpack-entries 0")
    return out


def _vec(seed: int, dim: int = 4, offset: float = 0.0) -> bytes:
    """Deterministic non-zero FLOAT32 vector (safe for both L2 and COSINE)."""
    return struct.pack(
        f"<{dim}f",
        *[float(seed + 1) + offset + 0.1 * float(d + 1) for d in range(dim)],
    )


def _get_vector_sharing_stats(client) -> dict[str, int]:
    raw = client.execute_command("FT._DEBUG", "VECTOR_SHARING_STATS")
    stats = {}
    for i in range(0, len(raw), 2):
        key = raw[i].decode("utf-8") if isinstance(raw[i], bytes) else raw[i]
        stats[key] = int(raw[i + 1])
    return stats


def _get_ft_info(client, index_name: str) -> FTInfoParser:
    return FTInfoParser(client.execute_command("FT.INFO", index_name))


def _wait_index_ready(client, index_name: str, expected_docs: int, timeout: int = 60):
    def _ready():
        try:
            info = _get_ft_info(client, index_name)
        except ResponseError:
            return False
        return (
            not info.backfill_in_progress
            and info.state.lower() == "ready"
            and info.num_docs == expected_docs
        )

    waiters.wait_for_true(_ready, timeout=timeout)


def _wait_replica_link_up(replica_client, timeout: int = 60):
    def _up():
        try:
            repl = replica_client.info("replication")
            pers = replica_client.info("persistence")
        except BusyLoadingError:
            return False
        return (
            repl.get("role") == "slave"
            and repl.get("master_link_status") == "up"
            and int(repl.get("master_sync_in_progress", 1)) == 0
            and int(pers.get("loading", 1)) == 0
        )

    waiters.wait_for_true(_up, timeout=timeout)


def _search_keys(res) -> list[str]:
    """Extract ordered document keys from an FT.SEARCH NOCONTENT reply."""
    return [
        k.decode("utf-8") if isinstance(k, bytes) else str(k)
        for k in res[1:]
    ]


def _search_key_fields(res) -> dict[str, dict[str, bytes]]:
    """Extract {key: {field: value}} from a full FT.SEARCH reply."""
    docs = {}
    i = 1
    while i < len(res):
        key = res[i].decode("utf-8") if isinstance(res[i], bytes) else str(res[i])
        fields_list = res[i + 1]
        fields = {}
        for j in range(0, len(fields_list), 2):
            fk = (
                fields_list[j].decode("utf-8")
                if isinstance(fields_list[j], bytes)
                else str(fields_list[j])
            )
            fields[fk] = fields_list[j + 1]
        docs[key] = fields
        i += 2
    return docs


def _set_dual_channel_if_supported(client, enabled: str) -> bool:
    """Configure dual-channel-replication-enabled; returns False if unsupported."""
    try:
        client.config_set("dual-channel-replication-enabled", enabled)
        return True
    except ResponseError as exc:
        if "Unsupported CONFIG parameter" in str(exc):
            return False
        raise


class TestFullSyncModeMatrix(ValkeySearchTestCaseDebugMode):
    """Matrix of full-sync replication modes across DB 0 + DB 1 with vector sharing."""

    def get_config_file_lines(self, testdir, port):
        lines = _strip_json_and_configure_search(
            super().get_config_file_lines(testdir, port)
        )
        lines += [
            "repl-diskless-sync yes",
            "repl-diskless-sync-delay 0",
            "repl-timeout 20",
        ]
        return lines

    def _create_db0_index(self, client):
        client.execute_command(
            "FT.CREATE", "idx_db0",
            "ON", "HASH",
            "PREFIX", "1", "doc0:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "8",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
            "INITIAL_CAP", "64",
            "tag", "TAG",
        )

    def _create_db1_index(self, client):
        client.execute_command(
            "FT.CREATE", "idx_db1",
            "ON", "HASH",
            "PREFIX", "1", "doc1:",
            "SCHEMA",
            "vec", "VECTOR", "FLAT", "8",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE",
            "INITIAL_CAP", "64",
            "tag", "TAG",
        )

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    @pytest.mark.parametrize(
        "repl_diskless_load", ["disabled", "on-empty-db", "swapdb"]
    )
    @pytest.mark.parametrize("rdb_write_v2", ["yes", "no"])
    @pytest.mark.parametrize("dual_channel", ["yes", "no"])
    def test_full_sync_mode_matrix(
        self, repl_diskless_load: str, rdb_write_v2: str, dual_channel: str
    ):
        """Full-sync matrix with a pre-populated replica.

        Note: for repl_diskless_load == "on-empty-db", pre-populating the
        replica deliberately exercises the non-empty fallback to disk-based
        loading (verified via DISK_LOAD_LOG_MARKER below), while
        test_on_empty_db_diskless_load_empty_replica covers true diskless
        loading on an empty replica.
        """
        primary = self.rg.primary
        replica = self.rg.replicas[0]

        for node in (primary, replica):
            node.client.config_set("search.rdb-write-v2", rdb_write_v2)
            node.client.config_set("search.rdb-read-v2", rdb_write_v2)
            if not _set_dual_channel_if_supported(node.client, dual_channel):
                if dual_channel == "yes":
                    pytest.skip(
                        "Valkey server does not support dual-channel-replication-enabled"
                    )
        replica.client.config_set("repl-diskless-load", repl_diskless_load)

        # Disconnect replica so we can populate diverging DB 0 + DB 1 state.
        replica.client.execute_command("REPLICAOF", "NO", "ONE")

        p_db0 = primary.server.get_new_client()
        p_db1 = primary.server.get_new_client()
        p_db1.execute_command("SELECT", 1)
        r_db0 = replica.server.get_new_client()
        r_db1 = replica.server.get_new_client()
        r_db1.execute_command("SELECT", 1)

        try:
            # 1. Populate replica with overlapping identical-vector keys,
            #    overlapping differing-vector keys, and stale keys absent from primary.
            self._create_db0_index(r_db0)
            self._create_db1_index(r_db1)
            for i in range(5):
                r_db0.hset(
                    f"doc0:{i}",
                    mapping={"vec": _vec(i, 4, 0.0), "tag": f"grp{i % 2}"},
                )
            for i in range(5, 10):
                r_db0.hset(
                    f"doc0:{i}",
                    mapping={"vec": _vec(i, 4, 100.0), "tag": "stale_val"},
                )
            for i in range(5):
                r_db0.hset(
                    f"doc0:stale{i}",
                    mapping={"vec": _vec(500 + i, 4), "tag": "only_replica"},
                )
            for i in range(5):
                r_db1.hset(
                    f"doc1:{i}",
                    mapping={"vec": _vec(100 + i, 4, 0.0), "tag": f"d1g{i % 2}"},
                )
            for i in range(3):
                r_db1.hset(
                    f"doc1:stale{i}",
                    mapping={"vec": _vec(600 + i, 4), "tag": "only_replica_db1"},
                )
            _wait_index_ready(r_db0, "idx_db0", expected_docs=15)
            _wait_index_ready(r_db1, "idx_db1", expected_docs=8)
            waiters.wait_for_equal(
                lambda: _get_vector_sharing_stats(r_db0)["entry_cnt"], 23, timeout=30
            )
            before_hits = _get_vector_sharing_stats(r_db0)["hash_sharing_hits"]

            # 2. Populate primary DB 0 (20 surviving + 1 pending DEL) and DB 1 (10).
            self._create_db0_index(p_db0)
            self._create_db1_index(p_db1)
            for i in range(20):
                p_db0.hset(
                    f"doc0:{i}",
                    mapping={"vec": _vec(i, 4, 0.0), "tag": f"grp{i % 2}"},
                )
            p_db0.hset(
                "doc0:pending_del",
                mapping={"vec": _vec(999, 4, 0.0), "tag": "pending_del_tag"},
            )
            for i in range(10):
                p_db1.hset(
                    f"doc1:{i}",
                    mapping={"vec": _vec(100 + i, 4, 0.0), "tag": f"d1g{i % 2}"},
                )
            _wait_index_ready(p_db0, "idx_db0", expected_docs=21)
            _wait_index_ready(p_db1, "idx_db1", expected_docs=10)

            # 3. Delete doc0:pending_del on primary and wait for the index to
            #    process it before triggering BGSAVE full resync (a writer task
            #    parked at PAUSEPOINT("block_mutation_queue") would deadlock
            #    AtForkPrepare -> SuspendWorkers during fork(); pending-DEL-in-RDB
            #    reconciliation is covered by unit tests and foreground SAVE in
            #    test_saverestore.py).
            assert p_db0.delete("doc0:pending_del") == 1
            _wait_index_ready(p_db0, "idx_db0", expected_docs=20)

            sync_log_offset = _logfile_offset(replica)
            p_db0.execute_command("DEBUG", "change-repl-id")
            r_db0.execute_command(
                "REPLICAOF", primary.server.bind_ip, str(primary.server.port)
            )
            _wait_replica_link_up(r_db0, timeout=60)

            if repl_diskless_load == "swapdb":
                assert _logfile_contains_since(
                    replica, sync_log_offset, DISKLESS_PARSER_LOG_MARKER
                ), "Expected diskless parser path for swapdb"
                assert _logfile_contains_since(
                    replica, sync_log_offset, SWAPDB_LOG_MARKER
                ), "Expected swapdb replication path to execute"
            else:
                # Both "disabled" and populated "on-empty-db" use disk-based loading.
                assert _logfile_contains_since(
                    replica, sync_log_offset, DISK_LOAD_LOG_MARKER
                ), f"Expected disk-based load path for {repl_diskless_load} on non-empty replica"
                assert not _logfile_contains_since(
                    replica, sync_log_offset, SWAPDB_LOG_MARKER
                )

            # 4. Wait for recovery / backfill completion on both nodes & DBs.
            _wait_index_ready(p_db0, "idx_db0", expected_docs=20)
            _wait_index_ready(p_db1, "idx_db1", expected_docs=10)
            _wait_index_ready(r_db0, "idx_db0", expected_docs=20)
            _wait_index_ready(r_db1, "idx_db1", expected_docs=10)

            # 5. Assert tag + KNN parity in DB 0 and DB 1, and pending-DEL / stale absence.
            for tag_val in ("grp0", "grp1"):
                p_tag = set(
                    _search_keys(
                        p_db0.execute_command(
                            "FT.SEARCH", "idx_db0", f"@tag:{{{tag_val}}}",
                            "NOCONTENT", "LIMIT", "0", "100",
                        )
                    )
                )
                r_tag = set(
                    _search_keys(
                        r_db0.execute_command(
                            "FT.SEARCH", "idx_db0", f"@tag:{{{tag_val}}}",
                            "NOCONTENT", "LIMIT", "0", "100",
                        )
                    )
                )
                assert r_tag == p_tag and len(r_tag) == 10

            # Stale & pending-DEL keys must not appear in either tag or KNN results.
            assert (
                r_db0.execute_command(
                    "FT.SEARCH", "idx_db0", "@tag:{pending_del_tag}", "NOCONTENT"
                )[0]
                == 0
            )
            assert (
                r_db0.execute_command(
                    "FT.SEARCH", "idx_db0", "@tag:{only_replica}", "NOCONTENT"
                )[0]
                == 0
            )
            assert (
                r_db0.execute_command(
                    "FT.SEARCH", "idx_db0", "@tag:{stale_val}", "NOCONTENT"
                )[0]
                == 0
            )

            # KNN query centered near pending_del's vector must NOT return doc0:pending_del.
            del_qvec = _vec(999, 4, 0.0)
            r_knn_del = _search_keys(
                r_db0.execute_command(
                    "FT.SEARCH", "idx_db0", "*=>[KNN 25 @vec $BLOB]",
                    "PARAMS", "2", "BLOB", del_qvec,
                    "NOCONTENT", "LIMIT", "0", "25",
                )
            )
            assert "doc0:pending_del" not in r_knn_del
            assert len(r_knn_del) == 20

            qvec0 = _vec(7, 4, 0.0)
            p_knn0 = _search_keys(
                p_db0.execute_command(
                    "FT.SEARCH", "idx_db0", "*=>[KNN 10 @vec $BLOB]",
                    "PARAMS", "2", "BLOB", qvec0,
                    "NOCONTENT", "LIMIT", "0", "10",
                )
            )
            r_knn0 = _search_keys(
                r_db0.execute_command(
                    "FT.SEARCH", "idx_db0", "*=>[KNN 10 @vec $BLOB]",
                    "PARAMS", "2", "BLOB", qvec0,
                    "NOCONTENT", "LIMIT", "0", "10",
                )
            )
            assert r_knn0 == p_knn0
            assert r_knn0[0] == "doc0:7"

            # DB 1 tag + KNN parity.
            for tag_val in ("d1g0", "d1g1"):
                p_tag1 = set(
                    _search_keys(
                        p_db1.execute_command(
                            "FT.SEARCH", "idx_db1", f"@tag:{{{tag_val}}}",
                            "NOCONTENT", "LIMIT", "0", "100",
                        )
                    )
                )
                r_tag1 = set(
                    _search_keys(
                        r_db1.execute_command(
                            "FT.SEARCH", "idx_db1", f"@tag:{{{tag_val}}}",
                            "NOCONTENT", "LIMIT", "0", "100",
                        )
                    )
                )
                assert r_tag1 == p_tag1 and len(r_tag1) == 5

            assert (
                r_db1.execute_command(
                    "FT.SEARCH", "idx_db1", "@tag:{only_replica_db1}", "NOCONTENT"
                )[0]
                == 0
            )
            qvec1 = _vec(103, 4, 0.0)
            p_knn1 = _search_keys(
                p_db1.execute_command(
                    "FT.SEARCH", "idx_db1", "*=>[KNN 10 @vec $BLOB]",
                    "PARAMS", "2", "BLOB", qvec1,
                    "NOCONTENT", "LIMIT", "0", "10",
                )
            )
            r_knn1 = _search_keys(
                r_db1.execute_command(
                    "FT.SEARCH", "idx_db1", "*=>[KNN 10 @vec $BLOB]",
                    "PARAMS", "2", "BLOB", qvec1,
                    "NOCONTENT", "LIMIT", "0", "10",
                )
            )
            assert set(r_knn1) == set(p_knn1) and len(r_knn1) == 10

            # 6. VectorRegistry parity & sharing assertions.
            waiters.wait_for_equal(
                lambda: _get_vector_sharing_stats(r_db0)["pending_unshare_cnt"],
                0,
                timeout=30,
            )
            waiters.wait_for_equal(
                lambda: _get_vector_sharing_stats(p_db0)["pending_unshare_cnt"],
                0,
                timeout=30,
            )
            p_stats = _get_vector_sharing_stats(p_db0)
            r_stats = _get_vector_sharing_stats(r_db0)
            assert p_stats["entry_cnt"] == 30
            assert r_stats["entry_cnt"] == p_stats["entry_cnt"]
            assert r_stats["hash_sharing_hits"] - before_hits >= 30
            assert r_stats["hash_sharing_errors"] == 0
            assert r_stats["pending_unshare_cnt"] == 0
        finally:
            p_db0.close()
            p_db1.close()
            r_db0.close()
            r_db1.close()

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    @pytest.mark.parametrize("rdb_write_v2", ["yes", "no"])
    @pytest.mark.parametrize("dual_channel", ["yes", "no"])
    def test_on_empty_db_diskless_load_empty_replica(
        self, rdb_write_v2: str, dual_channel: str
    ):
        """Empty replica with repl-diskless-load=on-empty-db takes diskless parser path."""
        primary = self.rg.primary
        replica = self.rg.replicas[0]

        for node in (primary, replica):
            node.client.config_set("search.rdb-write-v2", rdb_write_v2)
            node.client.config_set("search.rdb-read-v2", rdb_write_v2)
            if not _set_dual_channel_if_supported(node.client, dual_channel):
                if dual_channel == "yes":
                    pytest.skip(
                        "Valkey server does not support dual-channel-replication-enabled"
                    )
        replica.client.config_set("repl-diskless-load", "on-empty-db")

        replica.client.execute_command("REPLICAOF", "NO", "ONE")
        replica.client.flushall()
        assert replica.client.dbsize() == 0
        assert replica.client.execute_command("FT._LIST") == []

        p_db0 = primary.server.get_new_client()
        p_db1 = primary.server.get_new_client()
        p_db1.execute_command("SELECT", 1)
        r_db0 = replica.server.get_new_client()
        r_db1 = replica.server.get_new_client()
        r_db1.execute_command("SELECT", 1)

        try:
            self._create_db0_index(p_db0)
            self._create_db1_index(p_db1)
            for i in range(12):
                p_db0.hset(
                    f"doc0:{i}",
                    mapping={"vec": _vec(i, 4, 0.0), "tag": f"grp{i % 2}"},
                )
            for i in range(6):
                p_db1.hset(
                    f"doc1:{i}",
                    mapping={"vec": _vec(100 + i, 4, 0.0), "tag": f"d1g{i % 2}"},
                )
            _wait_index_ready(p_db0, "idx_db0", expected_docs=12)
            _wait_index_ready(p_db1, "idx_db1", expected_docs=6)

            before_hits = _get_vector_sharing_stats(r_db0)["hash_sharing_hits"]
            sync_log_offset = _logfile_offset(replica)

            p_db0.execute_command("DEBUG", "change-repl-id")
            r_db0.execute_command(
                "REPLICAOF", primary.server.bind_ip, str(primary.server.port)
            )
            _wait_replica_link_up(r_db0, timeout=60)

            assert _logfile_contains_since(
                replica, sync_log_offset, DISKLESS_PARSER_LOG_MARKER
            ), "Expected empty replica with on-empty-db to load directly to parser"
            assert not _logfile_contains_since(
                replica, sync_log_offset, DISK_LOAD_LOG_MARKER
            ), "Empty replica with on-empty-db must not fall back to disk"
            assert not _logfile_contains_since(
                replica, sync_log_offset, SWAPDB_LOG_MARKER
            ), "on-empty-db must not use swapdb path"

            _wait_index_ready(r_db0, "idx_db0", expected_docs=12)
            _wait_index_ready(r_db1, "idx_db1", expected_docs=6)

            assert (
                r_db0.execute_command(
                    "FT.SEARCH", "idx_db0", "@tag:{grp0}", "NOCONTENT"
                )[0]
                == 6
            )
            assert (
                r_db1.execute_command(
                    "FT.SEARCH", "idx_db1", "@tag:{d1g1}", "NOCONTENT"
                )[0]
                == 3
            )

            waiters.wait_for_equal(
                lambda: _get_vector_sharing_stats(r_db0)["pending_unshare_cnt"],
                0,
                timeout=30,
            )
            r_stats = _get_vector_sharing_stats(r_db0)
            assert r_stats["entry_cnt"] == 18
            assert r_stats["hash_sharing_hits"] - before_hits >= 18
            assert r_stats["hash_sharing_errors"] == 0
        finally:
            p_db0.close()
            p_db1.close()
            r_db0.close()
            r_db1.close()

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    @pytest.mark.parametrize("rdb_write_v2", ["yes", "no"])
    def test_swapdb_fresh_replica_zero_indexes(self, rdb_write_v2: str):
        """Fresh replica with 0 search indexes undergoing swapdb full sync."""
        primary = self.rg.primary
        replica = self.rg.replicas[0]

        for node in (primary, replica):
            node.client.config_set("search.rdb-write-v2", rdb_write_v2)
            node.client.config_set("search.rdb-read-v2", rdb_write_v2)
        replica.client.config_set("repl-diskless-load", "swapdb")

        replica.client.execute_command("REPLICAOF", "NO", "ONE")
        # Add an unrelated non-indexed key on the replica so it has non-empty DB
        # state (ensuring swapdb executes) while having ZERO search indexes.
        replica.client.set("unrelated:key", "val")
        assert replica.client.execute_command("FT._LIST") == []

        self._create_db0_index(primary.client)
        for i in range(15):
            primary.client.hset(
                f"doc0:{i}", mapping={"vec": _vec(i, 4), "tag": f"t{i % 3}"}
            )
        _wait_index_ready(primary.client, "idx_db0", expected_docs=15)

        before_hits = _get_vector_sharing_stats(replica.client)["hash_sharing_hits"]
        sync_log_offset = _logfile_offset(replica)

        primary.client.execute_command("DEBUG", "change-repl-id")
        replica.client.execute_command(
            "REPLICAOF", primary.server.bind_ip, str(primary.server.port)
        )
        _wait_replica_link_up(replica.client, timeout=60)
        assert _logfile_contains_since(replica, sync_log_offset, SWAPDB_LOG_MARKER)

        _wait_index_ready(replica.client, "idx_db0", expected_docs=15)
        assert replica.client.get("unrelated:key") is None

        # Verify tag + KNN parity and VectorRegistry sharing.
        assert (
            replica.client.execute_command(
                "FT.SEARCH", "idx_db0", "@tag:{t0}", "NOCONTENT"
            )[0]
            == 5
        )
        r_knn = _search_keys(
            replica.client.execute_command(
                "FT.SEARCH", "idx_db0", "*=>[KNN 5 @vec $BLOB]",
                "PARAMS", "2", "BLOB", _vec(4, 4),
                "NOCONTENT",
            )
        )
        assert r_knn[0] == "doc0:4"

        waiters.wait_for_equal(
            lambda: _get_vector_sharing_stats(replica.client)["pending_unshare_cnt"],
            0,
            timeout=30,
        )
        r_stats = _get_vector_sharing_stats(replica.client)
        assert r_stats["entry_cnt"] == 15
        assert r_stats["hash_sharing_hits"] - before_hits >= 15
        assert r_stats["hash_sharing_errors"] == 0


class TestSwapDbClusterFanout(ValkeySearchClusterTestCaseDebugMode):
    """Cluster fan-out queries targeting a replica during paused and active swapdb recovery."""

    CLUSTER_SIZE = 2
    REPLICAS_COUNT = 1

    def get_config_file_lines(self, testdir, port):
        lines = _strip_json_and_configure_search(
            super().get_config_file_lines(testdir, port)
        )
        lines += [
            "repl-diskless-sync yes",
            "repl-diskless-sync-delay 0",
            "repl-diskless-load swapdb",
            "repl-timeout 20",
        ]
        return lines

    def test_swapdb_fanout_during_post_swap_populate(self):
        cluster = self.new_cluster_client()
        target_rg = self.replication_groups[0]
        target_primary = target_rg.primary
        target_replica = target_rg.replicas[0]
        query_node = self.replication_groups[1].primary

        num_docs = 300
        dim = 4
        expected_vectors: dict[str, bytes] = {}

        target_primary.client.execute_command(
            "FT.CREATE", "idx_cluster",
            "ON", "HASH",
            "PREFIX", "1", "cdoc:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "8",
            "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2",
            "INITIAL_CAP", str(num_docs),
            "tag", "TAG",
        )

        for i in range(num_docs):
            key = f"cdoc:{i}"
            v = _vec(i, dim)
            expected_vectors[key] = v
            cluster.hset(key, mapping={"vec": v, "tag": f"grp{i % 4}"})

        # Wait until every primary and replica has indexed its shard's keys.
        def _shard_doc_counts():
            total = 0
            for rg in self.replication_groups:
                p_info = _get_ft_info(rg.primary.client, "idx_cluster")
                r_info = _get_ft_info(rg.replicas[0].client, "idx_cluster")
                if (
                    p_info.backfill_in_progress
                    or r_info.backfill_in_progress
                    or p_info.state.lower() != "ready"
                    or r_info.state.lower() != "ready"
                    or p_info.num_docs != r_info.num_docs
                ):
                    return -1
                total += p_info.num_docs
            return total

        waiters.wait_for_equal(_shard_doc_counts, num_docs, timeout=60)
        shard0_docs = _get_ft_info(target_primary.client, "idx_cluster").num_docs
        assert shard0_docs > 0

        # Force fan-out to route every shard to its replica.
        for node in self.nodes:
            node.client.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "ForceReplicasOnly", "yes"
            )

        # Use a slight fractional query offset + EF_RUNTIME >= shard0_docs so L2
        # distances have zero ties and exact KNN top-20 results are invariant.
        qvec = _vec(10, dim, 0.03)
        baseline_knn = _search_key_fields(
            query_node.client.execute_command(
                "FT.SEARCH", "idx_cluster",
                "*=>[KNN 20 @vec $BLOB EF_RUNTIME 300]",
                "PARAMS", "2", "BLOB", qvec,
                "RETURN", "1", "vec",
                "LIMIT", "0", "20",
                "ALLSHARDS", "CONSISTENT",
            )
        )
        assert len(baseline_knn) == 20
        for k, fields in baseline_knn.items():
            assert fields["vec"] == expected_vectors[k]

        baseline_local_knn = _search_key_fields(
            target_replica.client.execute_command(
                "FT.SEARCH", "idx_cluster",
                "*=>[KNN 20 @vec $BLOB EF_RUNTIME 300]",
                "PARAMS", "2", "BLOB", qvec,
                "RETURN", "1", "vec",
                "LIMIT", "0", "20",
                "LOCALONLY",
            )
        )
        expected_local_k = min(20, shard0_docs)
        assert len(baseline_local_knn) == expected_local_k
        for k, fields in baseline_local_knn.items():
            assert fields["vec"] == expected_vectors[k]

        # Pause post-swap recovery on shard 0's replica and trigger a full resync.
        target_replica.client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "yes"
        )
        stop_backfill_set = True
        try:
            sync_full_before = int(
                target_primary.client.info("stats").get("sync_full", 0)
            )
            swap_log_offset = _logfile_offset(target_replica)

            target_primary.client.execute_command("DEBUG", "change-repl-id")
            target_primary.client.execute_command("CLIENT", "KILL", "TYPE", "replica")

            waiters.wait_for_true(
                lambda: int(
                    target_primary.client.info("stats").get("sync_full", 0)
                )
                >= sync_full_before + 1,
                timeout=60,
            )
            waiters.wait_for_true(
                lambda: _logfile_contains_since(
                    target_replica, swap_log_offset, SWAPDB_LOG_MARKER
                ),
                timeout=60,
            )
            _wait_replica_link_up(target_replica.client, timeout=60)

            # Phase A: Paused recovery — verify fail-closed errors on both
            # cross-shard fan-out (ALLSHARDS) and LOCALONLY on target_replica.
            r_info = _get_ft_info(target_replica.client, "idx_cluster")
            assert r_info.backfill_in_progress
            assert r_info.state.lower() == "backfill_in_progress"

            allowed_fanout_errs = {RECOVERY_IN_PROGRESS_MSG, CONSISTENCY_FAILED_MSG}
            with pytest.raises(ResponseError) as exc_info:
                query_node.client.execute_command(
                    "FT.SEARCH", "idx_cluster",
                    "*=>[KNN 20 @vec $BLOB EF_RUNTIME 300]",
                    "PARAMS", "2", "BLOB", qvec,
                    "ALLSHARDS", "CONSISTENT",
                )
            assert str(exc_info.value) in allowed_fanout_errs

            with pytest.raises(ResponseError) as exc_info:
                query_node.client.execute_command(
                    "FT.SEARCH", "idx_cluster", "@tag:{grp0}",
                    "ALLSHARDS", "CONSISTENT",
                )
            assert str(exc_info.value) in allowed_fanout_errs

            with pytest.raises(ResponseError) as exc_info:
                target_replica.client.execute_command(
                    "FT.SEARCH", "idx_cluster", "@tag:{grp0}", "LOCALONLY"
                )
            assert str(exc_info.value) == RECOVERY_IN_PROGRESS_MSG

            # Phase B: Active recovery drain — hammer both fan-out and LOCALONLY
            # from background threads while unpausing StopBackfill.
            stop_event = threading.Event()
            unexpected_errors: list[str] = []
            observed_recovery_err = [0]
            observed_valid_result = [0]
            observed_local_valid_result = [0]

            def _fanout_worker():
                c = query_node.server.get_new_client()
                try:
                    while not stop_event.is_set():
                        try:
                            res = c.execute_command(
                                "FT.SEARCH", "idx_cluster",
                                "*=>[KNN 20 @vec $BLOB EF_RUNTIME 300]",
                                "PARAMS", "2", "BLOB", qvec,
                                "RETURN", "1", "vec",
                                "LIMIT", "0", "20",
                                "ALLSHARDS", "CONSISTENT",
                            )
                            docs = _search_key_fields(res)
                            if docs != baseline_knn:
                                unexpected_errors.append(
                                    f"Incomplete or mismatched fanout result: "
                                    f"len={len(docs)}, expected={len(baseline_knn)}"
                                )
                                return
                            observed_valid_result[0] += 1
                        except ResponseError as exc:
                            if str(exc) in allowed_fanout_errs:
                                observed_recovery_err[0] += 1
                            else:
                                unexpected_errors.append(f"Fanout error: {exc}")
                                return
                        except Exception as exc:
                            unexpected_errors.append(f"Fanout exception: {exc!r}")
                            return
                finally:
                    c.close()

            def _local_worker():
                c = target_replica.server.get_new_client()
                try:
                    while not stop_event.is_set():
                        try:
                            res = c.execute_command(
                                "FT.SEARCH", "idx_cluster",
                                "*=>[KNN 20 @vec $BLOB EF_RUNTIME 300]",
                                "PARAMS", "2", "BLOB", qvec,
                                "RETURN", "1", "vec",
                                "LIMIT", "0", "20",
                                "LOCALONLY",
                            )
                            docs = _search_key_fields(res)
                            if docs != baseline_local_knn:
                                unexpected_errors.append(
                                    f"LOCALONLY incomplete or mismatched result: "
                                    f"len={len(docs)}, expected={len(baseline_local_knn)}"
                                )
                                return
                            observed_local_valid_result[0] += 1
                        except ResponseError as exc:
                            if str(exc) != RECOVERY_IN_PROGRESS_MSG:
                                unexpected_errors.append(f"LOCALONLY error: {exc}")
                                return
                        except Exception as exc:
                            unexpected_errors.append(f"LOCALONLY exception: {exc!r}")
                            return
                finally:
                    c.close()

            t1 = threading.Thread(target=_fanout_worker)
            t2 = threading.Thread(target=_local_worker)
            t1.start()
            t2.start()
            try:
                time.sleep(0.1)
                target_replica.client.execute_command(
                    "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "no"
                )
                stop_backfill_set = False
                _wait_index_ready(
                    target_replica.client, "idx_cluster", expected_docs=shard0_docs
                )
                # Ensure both workers observe clean post-recovery results before stopping.
                waiters.wait_for_true(
                    lambda: (
                        observed_valid_result[0] >= 1
                        and observed_local_valid_result[0] >= 1
                    )
                    or bool(unexpected_errors),
                    timeout=15,
                )
            finally:
                stop_event.set()
                t1.join(timeout=15)
                t2.join(timeout=15)

            assert not t1.is_alive(), "Fanout worker still running after join"
            assert not t2.is_alive(), "LOCALONLY worker still running after join"
            assert not unexpected_errors, f"Unexpected query errors: {unexpected_errors}"
            assert observed_valid_result[0] >= 1
            assert observed_local_valid_result[0] >= 1

            # Phase C: Full post-recovery parity.
            after_knn = _search_key_fields(
                query_node.client.execute_command(
                    "FT.SEARCH", "idx_cluster",
                    "*=>[KNN 20 @vec $BLOB EF_RUNTIME 300]",
                    "PARAMS", "2", "BLOB", qvec,
                    "RETURN", "1", "vec",
                    "LIMIT", "0", "20",
                    "ALLSHARDS", "CONSISTENT",
                )
            )
            assert after_knn == baseline_knn

            tag_total = query_node.client.execute_command(
                "FT.SEARCH", "idx_cluster", "@tag:{grp0}", "NOCONTENT",
                "ALLSHARDS", "CONSISTENT",
            )[0]
            assert tag_total == num_docs // 4

            waiters.wait_for_equal(
                lambda: _get_vector_sharing_stats(target_replica.client)[
                    "pending_unshare_cnt"
                ],
                0,
                timeout=30,
            )
        finally:
            if stop_backfill_set:
                try:
                    target_replica.client.execute_command(
                        "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "no"
                    )
                except Exception:
                    pass
            cluster.close()


class TestSwapDbLargeIndexAndAsyncLoading(ValkeySearchTestCaseDebugMode):
    """Large multi-index swapdb timeout safety + async-loading read consistency."""

    def get_config_file_lines(self, testdir, port):
        lines = _strip_json_and_configure_search(
            super().get_config_file_lines(testdir, port)
        )
        lines += [
            "repl-diskless-sync yes",
            "repl-diskless-sync-delay 0",
            "repl-diskless-load swapdb",
            "repl-timeout 5",
        ]
        return lines

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_swapdb_large_index_no_repl_timeout(self):
        primary = self.rg.primary
        replica = self.rg.replicas[0]

        dim = 16
        num_primary_docs = 2500
        num_replica_initial_docs = 400

        def _create_indexes(client):
            client.execute_command(
                "FT.CREATE", "idx_hnsw_tag_num",
                "ON", "HASH",
                "PREFIX", "1", "item:",
                "SCHEMA",
                "vec", "VECTOR", "HNSW", "8",
                "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2",
                "INITIAL_CAP", str(num_primary_docs + 200),
                "tag", "TAG",
                "num", "NUMERIC",
            )
            client.execute_command(
                "FT.CREATE", "idx_flat_num",
                "ON", "HASH",
                "PREFIX", "1", "item:",
                "SCHEMA",
                "vec", "VECTOR", "FLAT", "8",
                "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "COSINE",
                "INITIAL_CAP", str(num_primary_docs + 200),
                "num", "NUMERIC",
            )

        replica.client.execute_command("REPLICAOF", "NO", "ONE")
        # Wait until primary processes the intentional REPLICAOF NO ONE disconnect
        # so its log entry precedes primary_sync_offset captured below.
        waiters.wait_for_equal(
            lambda: int(primary.client.info("replication").get("connected_slaves", 1)),
            0,
            timeout=15,
        )

        _create_indexes(primary.client)
        _create_indexes(replica.client)

        # Populate different initial dataset on replica.
        pipe = replica.client.pipeline(transaction=False)
        for i in range(num_replica_initial_docs):
            pipe.hset(
                f"item:{i}",
                mapping={"vec": _vec(i, dim, 50.0), "tag": "r_old", "num": str(i)},
            )
        pipe.execute()
        _wait_index_ready(replica.client, "idx_hnsw_tag_num", num_replica_initial_docs)
        _wait_index_ready(replica.client, "idx_flat_num", num_replica_initial_docs)

        # Populate large dataset on primary.
        pipe = primary.client.pipeline(transaction=False)
        for i in range(num_primary_docs):
            pipe.hset(
                f"item:{i}",
                mapping={"vec": _vec(i, dim, 0.0), "tag": f"cat{i % 10}", "num": str(i)},
            )
            if (i + 1) % 500 == 0:
                pipe.execute()
                pipe = primary.client.pipeline(transaction=False)
        pipe.execute()
        _wait_index_ready(primary.client, "idx_hnsw_tag_num", num_primary_docs, timeout=120)
        _wait_index_ready(primary.client, "idx_flat_num", num_primary_docs, timeout=120)

        sync_full_before = int(primary.client.info("stats").get("sync_full", 0))
        primary_sync_offset = _logfile_offset(primary)
        replica_sync_offset = _logfile_offset(replica)

        # Run concurrent light write load on primary and concurrent PING loop on replica.
        stop_threads = threading.Event()
        sync_started = threading.Event()
        sync_recovery_finished = threading.Event()
        extra_docs_written = [0]
        sync_interval_writes = [0]
        writer_errors: list[str] = []
        ping_errors: list[str] = []
        post_swap_pings_ok = [0]
        max_ping_latency = [0.0]
        max_post_swap_ping_latency = [0.0]

        def _writer_loop():
            c = primary.server.get_new_client()
            try:
                idx = num_primary_docs
                while not stop_threads.is_set():
                    try:
                        c.hset(
                            f"item:{idx}",
                            mapping={
                                "vec": _vec(idx, dim, 0.0),
                                "tag": f"cat{idx % 10}",
                                "num": str(idx),
                            },
                        )
                    except Exception as exc:
                        writer_errors.append(f"Writer exception at idx={idx}: {exc!r}")
                        return
                    idx += 1
                    extra_docs_written[0] += 1
                    if sync_started.is_set() and not sync_recovery_finished.is_set():
                        sync_interval_writes[0] += 1
                    time.sleep(0.05)
            finally:
                c.close()

        def _ping_loop():
            c = replica.server.get_new_client()
            try:
                while not stop_threads.is_set():
                    # Check whether swapdb has already swapped before sending PING:
                    # DEBUG change-repl-id selects non-async RDB receive before the
                    # swap (where -LOADING is expected), whereas once SWAPDB_LOG_MARKER
                    # is emitted stopLoading(1) has completed and PING must succeed.
                    swapped_before = sync_started.is_set() and _logfile_contains_since(
                        replica, replica_sync_offset, SWAPDB_LOG_MARKER
                    )
                    t0 = time.monotonic()
                    try:
                        c.ping()
                    except BusyLoadingError as exc:
                        if swapped_before:
                            ping_errors.append(
                                f"Unexpected post-swap BusyLoadingError on PING: {exc}"
                            )
                        time.sleep(0.02)
                        continue
                    except ResponseError as exc:
                        if "BUSY" in str(exc):
                            ping_errors.append(f"Unexpected -BUSY on PING: {exc}")
                        else:
                            ping_errors.append(f"PING ResponseError: {exc}")
                        time.sleep(0.02)
                        continue
                    except Exception as exc:
                        ping_errors.append(f"PING exception: {exc!r}")
                        return
                    dt = time.monotonic() - t0
                    if dt > max_ping_latency[0]:
                        max_ping_latency[0] = dt
                    if swapped_before:
                        post_swap_pings_ok[0] += 1
                        if dt > max_post_swap_ping_latency[0]:
                            max_post_swap_ping_latency[0] = dt
                    time.sleep(0.02)
            finally:
                c.close()

        # Hold post-swap recovery briefly until at least one primary write is
        # acknowledged while sync/recovery is still active, then release recovery
        # and keep polling PING across the entire recovery drain.
        replica.client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "yes"
        )
        stop_backfill_set = True
        writer_t = threading.Thread(target=_writer_loop)
        ping_t = threading.Thread(target=_ping_loop)
        writer_t.start()
        ping_t.start()
        try:
            primary.client.execute_command("DEBUG", "change-repl-id")
            sync_started.set()
            replica.client.execute_command(
                "REPLICAOF", primary.server.bind_ip, str(primary.server.port)
            )
            waiters.wait_for_true(
                lambda: _logfile_contains_since(
                    replica, replica_sync_offset, SWAPDB_LOG_MARKER
                ),
                timeout=60,
            )
            _wait_replica_link_up(replica.client, timeout=60)
            waiters.wait_for_true(
                lambda: sync_interval_writes[0] >= 1
                or bool(writer_errors)
                or bool(ping_errors),
                timeout=15,
            )
            replica.client.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "no"
            )
            stop_backfill_set = False
            waiters.wait_for_true(
                lambda: (
                    post_swap_pings_ok[0] >= 5
                    and not _get_ft_info(
                        replica.client, "idx_hnsw_tag_num"
                    ).backfill_in_progress
                    and not _get_ft_info(
                        replica.client, "idx_flat_num"
                    ).backfill_in_progress
                )
                or bool(writer_errors)
                or bool(ping_errors),
                timeout=120,
            )
            sync_recovery_finished.set()
        finally:
            sync_recovery_finished.set()
            stop_threads.set()
            writer_t.join(timeout=15)
            ping_t.join(timeout=15)
            if stop_backfill_set:
                try:
                    replica.client.execute_command(
                        "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "no"
                    )
                except Exception:
                    pass

        assert not writer_t.is_alive(), "Writer worker still running after join"
        assert not ping_t.is_alive(), "PING worker still running after join"
        assert not writer_errors, f"Concurrent writer failed: {writer_errors}"
        assert sync_interval_writes[0] >= 1, (
            "Expected at least one acknowledged write during the sync interval"
        )

        expected_total = num_primary_docs + extra_docs_written[0]
        _wait_index_ready(primary.client, "idx_hnsw_tag_num", expected_total, timeout=120)
        _wait_index_ready(primary.client, "idx_flat_num", expected_total, timeout=120)
        _wait_index_ready(replica.client, "idx_hnsw_tag_num", expected_total, timeout=120)
        _wait_index_ready(replica.client, "idx_flat_num", expected_total, timeout=120)

        repl_info = replica.client.info("replication")
        assert repl_info.get("master_link_status") == "up"
        sync_full_after = int(primary.client.info("stats").get("sync_full", 0))
        assert sync_full_after == sync_full_before + 1, (
            f"Expected exactly 1 full sync, saw {sync_full_after - sync_full_before}"
        )
        assert not _logfile_contains_since(
            primary, primary_sync_offset, "Disconnecting timedout replica"
        )
        assert not _logfile_contains_since(
            primary, primary_sync_offset, "Connection with replica"
        )
        assert not _logfile_contains_since(
            replica, replica_sync_offset, "Timeout connecting"
        )
        assert not _logfile_contains_since(
            replica, replica_sync_offset, "repl-timeout"
        )
        assert not _logfile_contains_since(
            replica, replica_sync_offset, "PRIMARY timeout"
        )
        assert not _logfile_contains_since(
            replica, replica_sync_offset, "MASTER timeout"
        )
        assert not _logfile_contains_since(
            replica, replica_sync_offset, "Connection with primary lost"
        )
        assert not ping_errors, f"PING errors during swapdb sync: {ping_errors}"
        assert post_swap_pings_ok[0] >= 5
        assert max_ping_latency[0] < 3.0, (
            f"Replica PING stalled for {max_ping_latency[0]:.2f}s"
        )
        assert max_post_swap_ping_latency[0] < 3.0, (
            f"Post-swap replica PING stalled for {max_post_swap_ping_latency[0]:.2f}s"
        )

        # Verify query and VectorRegistry parity across both indexes.
        p_num = primary.client.execute_command(
            "FT.SEARCH", "idx_flat_num", "@num:[0 99]", "NOCONTENT"
        )[0]
        r_num = replica.client.execute_command(
            "FT.SEARCH", "idx_flat_num", "@num:[0 99]", "NOCONTENT"
        )[0]
        assert p_num == r_num == 100

        waiters.wait_for_equal(
            lambda: _get_vector_sharing_stats(replica.client)["pending_unshare_cnt"],
            0,
            timeout=30,
        )
        assert (
            _get_vector_sharing_stats(replica.client)["entry_cnt"]
            == _get_vector_sharing_stats(primary.client)["entry_cnt"]
        )

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_swapdb_async_loading_read_consistency(self):
        """Async-loading swapdb (same replid, small backlog overflow):

        Verifies:
        (a) pre-trigger control reads and old-view FT.SEARCH reads during active
            async RDB loading succeed and match HGET,
        (b) during post-swap recovery only kSwapDbRecoveryInProgressMsg is
            allowed (never generic error or HGET-inconsistent results), and
        (c) post-recovery FT.SEARCH resumes matching HGET within bounded time.
        """
        primary = self.rg.primary
        replica = self.rg.replicas[0]
        dim = 4
        num_docs = 300

        primary.client.execute_command(
            "FT.CREATE", "idx_async",
            "ON", "HASH",
            "PREFIX", "1", "adoc:",
            "SCHEMA",
            "vec", "VECTOR", "HNSW", "8",
            "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2",
            "INITIAL_CAP", str(num_docs),
            "tag", "TAG",
        )
        pipe = primary.client.pipeline(transaction=False)
        for i in range(num_docs):
            pipe.hset(f"adoc:{i}", mapping={"vec": _vec(i, dim, 0.0), "tag": "old"})
        pipe.execute()

        _wait_index_ready(primary.client, "idx_async", num_docs)
        _wait_index_ready(replica.client, "idx_async", num_docs)
        assert replica.client.hget("adoc:0", "tag") == b"old"

        # Shrink primary's backlog so an atomic disconnect + overwrite forces a
        # full resync while keeping the exact same replication ID (enabling
        # swapdb async-loading reads from the old DB during RDB transfer).
        # Lower loading-process-events-interval-bytes on replica and add a small
        # per-key RDB save delay on primary so processEventsWhileBlocked() serves
        # old-view FT.SEARCH queries deterministically during active async loading.
        primary.client.config_set("repl-backlog-size", "16384")
        primary.client.config_set("rdbcompression", "no")
        primary.client.config_set("rdb-key-save-delay", "2000")
        replica.client.config_set("loading-process-events-interval-bytes", "1024")
        sync_full_before = int(primary.client.info("stats").get("sync_full", 0))
        async_log_offset = _logfile_offset(replica)

        # Hold post-swap recovery paused initially so Phase (b) is guaranteed to
        # be observed deterministically even on fast machines.
        replica.client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "yes"
        )
        stop_backfill_set = True

        stop_reader = threading.Event()
        sync_triggered = threading.Event()
        unpause_started = threading.Event()
        inconsistencies: list[str] = []
        pre_swap_ok = [0]
        during_async_load_ok = [0]
        post_swap_recovery_errs = [0]
        post_recovery_ok = [0]

        def _validate_tag_query(
            label: str,
            expected_old_cnt: int,
            expected_new_cnt: int,
            before_tag,
            cnt,
            err_msg: str | None,
            after_tag,
            allow_post_swap_success: bool,
        ) -> bool:
            if before_tag == b"old" and after_tag == b"old":
                if err_msg is not None:
                    inconsistencies.append(
                        f"Pre-swap {label} search failed while HGET==old: {err_msg}"
                    )
                    return False
                if cnt != expected_old_cnt:
                    inconsistencies.append(
                        f"Pre-swap {label} mismatch: got={cnt}, expected={expected_old_cnt}"
                    )
                    return False
                return True
            if before_tag == b"new" and after_tag == b"new":
                if err_msg is not None:
                    if err_msg != RECOVERY_IN_PROGRESS_MSG:
                        inconsistencies.append(
                            f"Unexpected post-swap {label} error: {err_msg}"
                        )
                        return False
                    return True
                if not allow_post_swap_success:
                    inconsistencies.append(
                        f"Paused post-swap {label} search succeeded before StopBackfill was cleared: cnt={cnt}"
                    )
                    return False
                if cnt != expected_new_cnt:
                    inconsistencies.append(
                        f"Post-swap {label} HGET-inconsistent count: "
                        f"got={cnt}, expected={expected_new_cnt}"
                    )
                    return False
                return True
            # The surrounding HGET calls straddled the DB swap boundary: accept
            # only the complete old count, the explicit recovery error, or (once
            # recovery has been unpaused) the complete new count.
            if err_msg is not None:
                if err_msg != RECOVERY_IN_PROGRESS_MSG:
                    inconsistencies.append(
                        f"Unexpected swap-boundary {label} error: {err_msg}"
                    )
                    return False
                return True
            allowed_counts = (
                (expected_old_cnt, expected_new_cnt)
                if allow_post_swap_success
                else (expected_old_cnt,)
            )
            if cnt not in allowed_counts:
                inconsistencies.append(
                    f"Invalid swap-boundary {label} count: got={cnt}, "
                    f"allowed={allowed_counts}"
                )
                return False
            return True

        def _reader_loop():
            c = replica.server.get_new_client()
            try:
                while not stop_reader.is_set():
                    loading_started_before = (
                        sync_triggered.is_set()
                        and _logfile_contains_since(
                            replica, async_log_offset, LOADING_DB_LOG_MARKER
                        )
                    )
                    try:
                        before_old = c.hget("adoc:0", "tag")
                        try:
                            old_cnt = c.execute_command(
                                "FT.SEARCH", "idx_async", "@tag:{old}", "NOCONTENT"
                            )[0]
                            old_err = None
                        except ResponseError as exc:
                            old_cnt = None
                            old_err = str(exc)
                        after_old = c.hget("adoc:0", "tag")
                        unpause_after_old = unpause_started.is_set()

                        before_new = c.hget("adoc:0", "tag")
                        try:
                            new_cnt = c.execute_command(
                                "FT.SEARCH", "idx_async", "@tag:{new}", "NOCONTENT"
                            )[0]
                            new_err = None
                        except ResponseError as exc:
                            new_cnt = None
                            new_err = str(exc)
                        after_new = c.hget("adoc:0", "tag")
                        unpause_after_new = unpause_started.is_set()
                    except Exception as exc:
                        inconsistencies.append(f"Reader exception: {exc!r}")
                        return

                    swapped_after = (
                        sync_triggered.is_set()
                        and _logfile_contains_since(
                            replica, async_log_offset, SWAPDB_LOG_MARKER
                        )
                    )

                    if not _validate_tag_query(
                        "@tag:{old}",
                        num_docs,
                        0,
                        before_old,
                        old_cnt,
                        old_err,
                        after_old,
                        unpause_after_old,
                    ):
                        return
                    if not _validate_tag_query(
                        "@tag:{new}",
                        0,
                        num_docs,
                        before_new,
                        new_cnt,
                        new_err,
                        after_new,
                        unpause_after_new,
                    ):
                        return

                    if (
                        before_old == b"old"
                        and after_old == b"old"
                        and before_new == b"old"
                        and after_new == b"old"
                    ):
                        if not sync_triggered.is_set():
                            pre_swap_ok[0] += 1
                        elif loading_started_before and not swapped_after:
                            during_async_load_ok[0] += 1
                    elif (
                        before_old == b"new"
                        and after_old == b"new"
                        and before_new == b"new"
                        and after_new == b"new"
                    ):
                        if old_err is None and new_err == RECOVERY_IN_PROGRESS_MSG:
                            inconsistencies.append(
                                "Post-swap index re-entered recovery after ready reply"
                            )
                            return
                        if (
                            old_err == RECOVERY_IN_PROGRESS_MSG
                            and new_err == RECOVERY_IN_PROGRESS_MSG
                        ):
                            post_swap_recovery_errs[0] += 1
                        elif old_err is None and new_err is None:
                            post_recovery_ok[0] += 1
                    time.sleep(0.002)
            finally:
                c.close()

        reader_t = threading.Thread(target=_reader_loop)
        reader_t.start()
        try:
            # Confirm pre-trigger control reads succeed before starting full sync.
            waiters.wait_for_true(lambda: pre_swap_ok[0] >= 3, timeout=15)

            # Atomically disconnect replica (preserving replid) and overflow the
            # 16KB backlog inside a single MULTI/EXEC transaction (CLIENT KILL is
            # NOSCRIPT in Valkey so it runs outside EVAL in the same EXEC).
            overflow_lua = """
                local n = tonumber(ARGV[1])
                local pad = string.rep('x', 4096)
                for i = 0, n - 1 do
                    redis.call('HSET', 'adoc:' .. i, 'tag', 'new')
                end
                for i = 1, 120 do
                    redis.call('SET', 'backlog_pad:' .. i, pad)
                end
                return 1
            """
            async_log_offset = _logfile_offset(replica)
            sync_triggered.set()
            tx = primary.client.pipeline(transaction=True)
            tx.execute_command("CLIENT", "KILL", "TYPE", "replica")
            tx.eval(overflow_lua, 0, str(num_docs))
            killed_cnt, eval_res = tx.execute()
            assert int(killed_cnt) >= 1, (
                f"Expected CLIENT KILL TYPE replica to disconnect replica, killed={killed_cnt}"
            )
            assert int(eval_res) == 1

            # Wait for swapdb full sync to complete and swap the DBs.
            waiters.wait_for_true(
                lambda: _logfile_contains_since(
                    replica, async_log_offset, SWAPDB_LOG_MARKER
                ),
                timeout=60,
            )
            _wait_replica_link_up(replica.client, timeout=60)
            assert int(primary.client.info("stats").get("sync_full", 0)) == (
                sync_full_before + 1
            )

            # Confirm replica HGET now returns "new" and reader observes the
            # explicit recovery error while StopBackfill is paused.
            waiters.wait_for_equal(
                lambda: replica.client.hget("adoc:0", "tag"), b"new", timeout=15
            )
            waiters.wait_for_true(
                lambda: post_swap_recovery_errs[0] >= 3 or bool(inconsistencies),
                timeout=15,
            )

            # Unpause recovery and confirm search resumes matching HGET.
            unpause_started.set()
            replica.client.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "no"
            )
            stop_backfill_set = False
            _wait_index_ready(replica.client, "idx_async", num_docs, timeout=60)
            waiters.wait_for_true(
                lambda: post_recovery_ok[0] >= 3 or bool(inconsistencies),
                timeout=15,
            )
        finally:
            stop_reader.set()
            reader_t.join(timeout=15)
            try:
                primary.client.config_set("rdb-key-save-delay", "0")
                primary.client.config_set("rdbcompression", "yes")
            except Exception:
                pass
            if stop_backfill_set:
                try:
                    replica.client.execute_command(
                        "FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "StopBackfill", "no"
                    )
                except Exception:
                    pass

        assert not reader_t.is_alive(), "Reader worker still running after join"
        assert not inconsistencies, f"Read consistency violations: {inconsistencies}"
        assert pre_swap_ok[0] >= 3
        assert during_async_load_ok[0] >= 1, (
            "Expected at least one successful old-view FT.SEARCH during active async RDB loading"
        )
        assert post_swap_recovery_errs[0] >= 3
        assert post_recovery_ok[0] >= 3
