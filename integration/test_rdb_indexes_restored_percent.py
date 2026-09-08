"""
Verify search_rdb_indexes_restored_percent reports 100% after successful RDB
restore in both standalone (CMD) and cluster/coordinator (CME) modes.

This metric was previously broken in CME mode because the GLOBAL_METADATA RDB
section was counted in the total but never incremented the completed counter,
causing it to report 66.67% with 2 indexes (2/3 sections completed). That was
first worked around by having the metadata section increment the completed
counter too. Both the total and the completed counter now track index schemas
only: the total comes from the RDB_SECTION_SNAPSHOT_INFO section written at
save time (falling back to section arithmetic for older RDBs), and the metadata
section no longer increments the completed counter.
"""

import os
import time
import threading
from valkey_search_test_case import ValkeySearchTestCaseDebugMode, ValkeySearchClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from indexes import *


index_1 = Index("idx_pct_1", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
index_2 = Index("idx_pct_2", [Vector("v", 3, type="HNSW", m=2, efc=1), Tag("t")])
index_3 = Index("idx_pct_3", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
NUM_DOCS = 500

# Wire-format constants for the RDB_SECTION_SNAPSHOT_INFO RDBSection, so the
# tests can assert the section really landed in the RDB rather than trusting
# the INFO metrics that are derived from it.
#
# RDBSection.type is field 1 (varint), and RDB_SECTION_SNAPSHOT_INFO is enum
# value 3. RDBSection.supplemental_count is field 2 and is zero for this
# section, so proto3 omits it. RDBSection.snapshot_info_contents is field 5 and
# is a nested message, giving wire type 2. Protobuf serializes in ascending
# field-number order.
_RDB_SECTION_TYPE_TAG = 0x08          # field 1, varint
_RDB_SECTION_SNAPSHOT_INFO = 0x03     # RDB_SECTION_SNAPSHOT_INFO
_SNAPSHOT_INFO_CONTENTS_TAG = 0x2A    # field 5, wire type 2 -> (5 << 3) | 2
_NUM_INDEXES_TAG = 0x08               # SnapshotInfo.num_indexes, field 1 varint


def _encode_varint(value):
    """Encode an unsigned integer using protobuf base-128 varint encoding."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _snapshot_info_section_bytes(num_indexes):
    """
    Build the exact on-disk byte sequence for a SnapshotInfo RDBSection holding
    num_indexes, including the RDB string-length prefix.

    The section is written with SaveStringBuffer, which prefixes the serialized
    protobuf with an RDB-encoded length. For lengths under 64 that is a single
    byte equal to the length (RDB 6-bit length encoding), so anchoring on it
    makes the pattern specific enough to avoid coincidental matches elsewhere
    in the RDB.
    """
    # SnapshotInfo itself. num_indexes == 0 is the proto3 scalar default and is
    # therefore not serialized, leaving an empty nested message. Presence still
    # works because snapshot_info_contents is a oneof member.
    if num_indexes:
        snapshot_info = bytes([_NUM_INDEXES_TAG]) + _encode_varint(num_indexes)
    else:
        snapshot_info = b""

    section = (
        bytes([_RDB_SECTION_TYPE_TAG, _RDB_SECTION_SNAPSHOT_INFO])
        + bytes([_SNAPSHOT_INFO_CONTENTS_TAG])
        + _encode_varint(len(snapshot_info))
        + snapshot_info
    )
    assert len(section) < 64, "section grew past the 6-bit RDB length encoding"
    return bytes([len(section)]) + section


def _index_schema_name_bytes(index_name):
    """
    Byte sequence for IndexSchema.name (field 1, wire type 2) inside an
    INDEX_SCHEMA RDBSection. Used to prove SnapshotInfo is written first.

    This does not collide with the hash keys for the same index: a key like
    "idx_pct_1:0" is stored as an RDB string whose length prefix is followed
    immediately by 'i', whereas this pattern requires the field tag 0x0a
    followed by the name length.
    """
    encoded = index_name.encode()
    return b"\x0a" + bytes([len(encoded)]) + encoded


def _server_rdb_path(client):
    """Resolve the live RDB path from the server's own configuration."""

    def config_value(name):
        result = client.execute_command("CONFIG", "GET", name)
        if isinstance(result, dict):
            value = list(result.values())[0]
        else:
            value = result[1]
        return value.decode() if isinstance(value, bytes) else value

    return os.path.join(config_value("dir"), config_value("dbfilename"))


def assert_snapshot_info_in_rdb(client, expected_num_indexes, label,
                                index_name_written_after=None,
                                expected_index_names=None):
    """
    Assert the RDB on disk contains exactly one SnapshotInfo section recording
    expected_num_indexes, and that no neighbouring count was written instead.

    Checking the neighbouring counts is what catches an off-by-one, which is
    the whole class of bug this section exists to eliminate: the previous
    implementation inferred the index count from the section count and was
    wrong by exactly one in coordinated mode.

    Pass expected_index_names to tie the recorded number to the index schemas
    actually serialized into the RDB. Without it the expected count would only
    ever be compared against search_number_of_indexes, which is derived from
    the same GetNumberOfIndexSchemas() call that produces num_indexes, so a
    bug in that call would make both agree and go unnoticed.
    """
    rdb_path = _server_rdb_path(client)
    assert os.path.exists(rdb_path), f"[{label}] RDB file not found at {rdb_path}"
    with open(rdb_path, "rb") as handle:
        rdb = handle.read()
    print(f"[{label}] Inspecting RDB at {rdb_path} ({len(rdb)} bytes)")

    expected = _snapshot_info_section_bytes(expected_num_indexes)
    occurrences = rdb.count(expected)
    assert occurrences == 1, (
        f"[{label}] Expected exactly one SnapshotInfo section with "
        f"num_indexes={expected_num_indexes} (bytes {expected.hex()}) in the RDB, "
        f"found {occurrences}"
    )
    print(f"[{label}] Found SnapshotInfo section num_indexes={expected_num_indexes} "
          f"(bytes {expected.hex()})")

    # Guard against an off-by-one in either direction.
    for wrong_count in (expected_num_indexes - 1, expected_num_indexes + 1):
        if wrong_count < 0:
            continue
        wrong = _snapshot_info_section_bytes(wrong_count)
        assert wrong not in rdb, (
            f"[{label}] RDB unexpectedly contains a SnapshotInfo section with "
            f"num_indexes={wrong_count} (bytes {wrong.hex()}); the recorded index "
            f"count is off by one"
        )

    # Independently confirm the recorded number against the index schemas that
    # are actually present in the RDB, rather than against another metric
    # derived from the same in-memory counter.
    if expected_index_names is not None:
        assert expected_num_indexes == len(expected_index_names), (
            f"[{label}] Test setup error: expected_num_indexes="
            f"{expected_num_indexes} disagrees with expected_index_names="
            f"{expected_index_names}"
        )
        for index_name in expected_index_names:
            marker = _index_schema_name_bytes(index_name)
            assert marker in rdb, (
                f"[{label}] Expected an IndexSchema named {index_name} in the "
                f"RDB (bytes {marker.hex()}) but did not find one; the recorded "
                f"num_indexes={expected_num_indexes} does not match the schemas "
                f"on disk"
            )
        print(f"[{label}] Confirmed {len(expected_index_names)} named index "
              f"schemas on disk: {sorted(expected_index_names)}")

    # SnapshotInfo must be the first section, ahead of any index schema.
    if index_name_written_after is not None:
        marker = _index_schema_name_bytes(index_name_written_after)
        marker_offset = rdb.find(marker)
        assert marker_offset != -1, (
            f"[{label}] Could not locate the IndexSchema name for "
            f"{index_name_written_after} in the RDB"
        )
        snapshot_offset = rdb.find(expected)
        assert snapshot_offset < marker_offset, (
            f"[{label}] SnapshotInfo section at offset {snapshot_offset} must "
            f"precede the first index schema at offset {marker_offset}"
        )
        print(f"[{label}] SnapshotInfo at offset {snapshot_offset} precedes the "
              f"first index schema at offset {marker_offset}")


class TestIndexesRestoredPercentStandalone(ValkeySearchTestCaseDebugMode):
    """
    CMD (standalone) mode: the index total comes from SNAPSHOT_INFO and counts
    only index schemas. After restore, the metric should progress monotonically
    and reach 100%.
    """

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.max-mutation-queue-size-on-restore"] = "1"
        return args

    def test_indexes_restored_percent_is_100_after_restore(self):
        # Create indexes and load data
        print(f"[Standalone] Creating indexes and loading {NUM_DOCS} docs each...")
        index_1.create(self.client, True)
        index_2.create(self.client, True)
        index_1.load_data(self.client, NUM_DOCS)
        index_2.load_data(self.client, NUM_DOCS)

        waiters.wait_for_true(lambda: index_1.backfill_complete(self.client))
        waiters.wait_for_true(lambda: index_2.backfill_complete(self.client))

        # Baseline the counters we expect the save/restore cycle to move.
        before = self.client.info("search")
        assert before["search_number_of_indexes"] == 2, \
            f"Expected 2 indexes before save, got {before['search_number_of_indexes']}"
        save_success_before = int(before.get("search_rdb_save_success_cnt", 0))
        load_success_before = int(before.get("search_rdb_load_success_cnt", 0))

        # Save RDB
        self.client.execute_command("SAVE")

        # The save path records the real index count, not the section count.
        assert_snapshot_info_in_rdb(
            self.client, expected_num_indexes=2, label="Standalone",
            index_name_written_after=index_1.name,
            expected_index_names=[index_1.name, index_2.name],
        )

        after_save = self.client.info("search")
        assert int(after_save["search_rdb_save_success_cnt"]) == save_success_before + 1, \
            "Expected search_rdb_save_success_cnt to increment after SAVE"
        assert int(after_save.get("search_rdb_save_failure_cnt", 0)) == 0, \
            "Expected no RDB save failures"

        print("[Standalone] RDB saved, starting DEBUG RELOAD with metric monitoring...")

        # Monitor the metric from a separate connection during DEBUG RELOAD
        monitor_client = self.server.get_new_client()
        monitor_ready = threading.Event()
        done = threading.Event()
        observed_percents = []
        observed_index_counts = []
        monitor_error = [None]

        def monitor():
            try:
                # Signal readiness after confirming the connection works
                monitor_client.info("search")
                monitor_ready.set()

                while not done.is_set():
                    info = monitor_client.info("search")
                    if info.get("search_rdb_restore_in_progress", 0):
                        pct = float(info["search_rdb_indexes_restored_percent"])
                        observed_percents.append(pct)
                        num_indexes = int(info["search_number_of_indexes"])
                        observed_index_counts.append(num_indexes)
                        keys_pct = float(info["search_rdb_current_index_keys_restored_percent"])
                        print(f"[Standalone][Monitor] restore_in_progress=1, "
                              f"indexes_restored_percent={pct:.2f}%, "
                              f"number_of_indexes={num_indexes}, "
                              f"current_index_keys_restored_percent={keys_pct:.2f}%")
                        # Verify the metric never exceeds 100%
                        if pct > 100.0:
                            monitor_error[0] = f"Metric exceeded 100%: {pct}"
                            return
                        if keys_pct > 100.0:
                            monitor_error[0] = f"Key restore metric exceeded 100%: {keys_pct}"
                            return
                        # Indexes must never appear to vanish mid-restore.
                        if num_indexes < 2:
                            monitor_error[0] = (
                                f"number_of_indexes dropped to {num_indexes} during restore"
                            )
                            return
                    done.wait(0.001)
            except Exception as exc:
                monitor_error[0] = f"Monitor exception: {exc}"
                monitor_ready.set()

        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
        assert monitor_ready.wait(timeout=5), "Monitor thread failed to start"

        try:
            self.client.execute_command("DEBUG", "RELOAD")
        finally:
            done.set()
            thread.join(timeout=5)

        assert not thread.is_alive(), "Monitor thread did not exit cleanly"

        print(f"[Standalone] DEBUG RELOAD complete. Observed {len(observed_percents)} progress samples during restore.")
        if observed_percents:
            print(f"[Standalone] Progress samples: {observed_percents}")

        # Verify no errors observed during monitoring
        assert monitor_error[0] is None, monitor_error[0]

        # Verify the metric progressed monotonically (non-decreasing)
        if len(observed_percents) > 1:
            for i in range(1, len(observed_percents)):
                assert observed_percents[i] >= observed_percents[i-1], \
                    f"Metric decreased: {observed_percents[i-1]} -> {observed_percents[i]}"

        if observed_index_counts:
            print(f"[Standalone] number_of_indexes samples: {observed_index_counts}")

        # After restore completes, verify the metric is 100%
        info = self.client.info("search")
        assert info["search_rdb_restore_in_progress"] == 0
        percent = float(info["search_rdb_indexes_restored_percent"])
        print(f"[Standalone] Final: restore_in_progress={info['search_rdb_restore_in_progress']}, "
              f"indexes_restored_percent={percent:.2f}%")
        assert percent == 100.0, \
            f"Expected search_rdb_indexes_restored_percent=100.0 after restore, got {percent}"

        # The remaining restore counters must agree with a clean, complete load.
        assert int(info["search_rdb_load_success_cnt"]) == load_success_before + 1, \
            "Expected search_rdb_load_success_cnt to increment after DEBUG RELOAD"
        assert int(info.get("search_rdb_load_failure_cnt", 0)) == 0, \
            "Expected no RDB load failures"
        assert float(info["search_rdb_current_index_keys_restored_percent"]) == 100.0, \
            "Expected per-index key restore to report 100% after restore"
        assert info["search_number_of_indexes"] == 2, \
            f"Expected 2 indexes after restore, got {info['search_number_of_indexes']}"

    def test_snapshot_info_tracks_actual_index_count(self):
        """
        The recorded count must follow the real number of index schemas rather
        than a fixed value or the section count. Saving at two different index
        counts and re-reading the RDB each time pins that down.
        """
        index_1.create(self.client, True)
        index_2.create(self.client, True)
        waiters.wait_for_true(lambda: index_1.backfill_complete(self.client))
        waiters.wait_for_true(lambda: index_2.backfill_complete(self.client))

        self.client.execute_command("SAVE")
        assert_snapshot_info_in_rdb(
            self.client, expected_num_indexes=2, label="Standalone/2-indexes",
            index_name_written_after=index_1.name,
            expected_index_names=[index_1.name, index_2.name],
        )

        # Add a third index and save again.
        index_3.create(self.client, True)
        waiters.wait_for_true(lambda: index_3.backfill_complete(self.client))
        assert self.client.info("search")["search_number_of_indexes"] == 3

        self.client.execute_command("SAVE")
        assert_snapshot_info_in_rdb(
            self.client, expected_num_indexes=3, label="Standalone/3-indexes",
            index_name_written_after=index_1.name,
            expected_index_names=[index_1.name, index_2.name, index_3.name],
        )

        # Drop back down to one index and confirm the count follows downward too.
        self.client.execute_command("FT.DROPINDEX", index_2.name)
        self.client.execute_command("FT.DROPINDEX", index_3.name)
        assert self.client.info("search")["search_number_of_indexes"] == 1

        self.client.execute_command("SAVE")
        assert_snapshot_info_in_rdb(
            self.client, expected_num_indexes=1, label="Standalone/1-index",
            index_name_written_after=index_1.name,
            expected_index_names=[index_1.name],
        )

        # The dropped schemas must be gone from the RDB too, so the recorded
        # count of 1 is not merely a stale value that happens to be smaller.
        with open(_server_rdb_path(self.client), "rb") as handle:
            rdb_after_drop = handle.read()
        for dropped in (index_2.name, index_3.name):
            marker = _index_schema_name_bytes(dropped)
            assert marker not in rdb_after_drop, (
                f"[Standalone/1-index] Dropped index {dropped} is still present "
                f"in the RDB"
            )

        # The restored total must match, so the metric still completes at 100%.
        self.client.execute_command("DEBUG", "RELOAD")
        info = self.client.info("search")
        assert float(info["search_rdb_indexes_restored_percent"]) == 100.0
        assert info["search_number_of_indexes"] == 1

    def test_no_snapshot_info_section_without_indexes(self):
        """
        With no indexes there is no ValkeySearch aux payload at all, so no
        SnapshotInfo section should be written. This guards the AuxSave2
        "write nothing when empty" behaviour that the new section must not
        disturb.
        """
        assert self.client.info("search")["search_number_of_indexes"] == 0

        self.client.execute_command("SAVE")

        rdb_path = _server_rdb_path(self.client)
        with open(rdb_path, "rb") as handle:
            rdb = handle.read()

        # No SnapshotInfo section for any plausible index count.
        for count in range(0, 4):
            pattern = _snapshot_info_section_bytes(count)
            assert pattern not in rdb, (
                f"[Standalone/no-indexes] RDB unexpectedly contains a SnapshotInfo "
                f"section with num_indexes={count}"
            )
        print("[Standalone/no-indexes] Confirmed no SnapshotInfo section written")

        # Restore is a no-op for search, and the percentage is defined as 100.
        self.client.execute_command("DEBUG", "RELOAD")
        info = self.client.info("search")
        assert info["search_rdb_restore_in_progress"] == 0
        assert float(info["search_rdb_indexes_restored_percent"]) == 100.0
        assert info["search_number_of_indexes"] == 0


class TestIndexesRestoredPercentCluster(ValkeySearchClusterTestCaseDebugMode):
    """
    CME (cluster/coordinator) mode: rdb_section_count includes both
    INDEX_SCHEMA and GLOBAL_METADATA sections. After restore, the metric
    should still be 100%.
    """

    def test_indexes_restored_percent_is_100_after_restore(self):
        node0 = self.new_client_for_primary(0)
        print("[Cluster] Creating 2 indexes in CME mode...")

        # Create indexes on the cluster
        assert node0.execute_command(
            "FT.CREATE", index_1.name,
            "ON", "HASH",
            "PREFIX", "1", f"{index_1.name}:",
            "SCHEMA",
            "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
            "n", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command(
            "FT.CREATE", index_2.name,
            "ON", "HASH",
            "PREFIX", "1", f"{index_2.name}:",
            "SCHEMA",
            "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
            "t", "TAG"
        ) == b"OK"

        # Load some data
        cluster = self.new_cluster_client()
        for i in range(NUM_DOCS):
            cluster.execute_command(
                "HSET", f"{index_1.name}:{i}",
                "v", b'\x00\x00\x80\x3f' * 3,
                "n", str(i)
            )
        print(f"[Cluster] Loaded {NUM_DOCS} docs")

        # Save RDB on all primaries
        for idx, rg in enumerate(self.replication_groups):
            rg.primary.client.execute_command("SAVE")

        # Every primary must record 2 indexes, not the 3 sections it wrote.
        # This is the exact off-by-one that made this metric report 66.67%.
        for idx, rg in enumerate(self.replication_groups):
            assert_snapshot_info_in_rdb(
                rg.primary.client, expected_num_indexes=2,
                label=f"Cluster/node{idx}",
                expected_index_names=[index_1.name, index_2.name],
            )

        # Restart all primaries (triggers RDB load with GLOBAL_METADATA section)
        print(f"[Cluster] Restarting all {len(self.replication_groups)} primaries...")
        for idx, rg in enumerate(self.replication_groups):
            rg.primary.server.restart(remove_rdb=False)
            print(f"[Cluster] Restarted primary node {idx}")

        # Verify the metric on each primary node after restore
        for idx in range(len(self.replication_groups)):
            # Get a fresh client — the Node.client reference is stale after restart
            client = self.new_client_for_primary(idx)

            # Wait for server to be ready and restore to finish
            def server_restored(c=client):
                try:
                    return c.info("search").get("search_rdb_restore_in_progress", 1) == 0
                except Exception:
                    return False

            waiters.wait_for_true(server_restored)

            client.execute_command("CONFIG", "SET", "search.info-developer-visible", "yes")

            info = client.info("search")
            percent = float(info["search_rdb_indexes_restored_percent"])
            rdb_load_success = info.get("search_rdb_load_success_cnt", "N/A")
            num_indexes = info.get("search_number_of_indexes", "N/A")
            print(f"[Cluster] Node {idx}: restore_in_progress={info['search_rdb_restore_in_progress']}, "
                  f"indexes_restored_percent={percent:.2f}%, "
                  f"rdb_load_success_cnt={rdb_load_success}, number_of_indexes={num_indexes}")
            assert percent == 100.0, \
                f"Node {idx}: Expected search_rdb_indexes_restored_percent=100.0 after restore, got {percent}"

            # 100% pins down both halves of the ratio in coordinated mode.
            # If the metadata section were still counted in the total this
            # would read 66.67%; if it still incremented the completed counter
            # it would read 150%.
            assert num_indexes == 2, \
                f"Node {idx}: Expected 2 indexes after restore, got {num_indexes}"
            assert int(info["search_rdb_load_success_cnt"]) >= 1, \
                f"Node {idx}: Expected at least one successful RDB load"
            assert int(info.get("search_rdb_load_failure_cnt", 0)) == 0, \
                f"Node {idx}: Expected no RDB load failures"

            # The SnapshotInfo section survives the restart untouched, so the
            # count the node restored from is still verifiable on disk.
            assert_snapshot_info_in_rdb(
                client, expected_num_indexes=2,
                label=f"Cluster/node{idx}/after-restart",
                expected_index_names=[index_1.name, index_2.name],
            )

        print("[Cluster] All nodes verified: indexes_restored_percent=100.0%")
