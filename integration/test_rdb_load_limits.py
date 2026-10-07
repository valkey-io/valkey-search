"""
FT.CREATE enforces the configurable index limits only when an index is
created. Restoring an RDB re-checks every limit (issue #375): an index that
exceeds a limit lowered since the snapshot was taken fails the load, and
raising the limit lets the same RDB load.
"""

import os
import shutil

import pytest
from valkey_search_test_case import LOGS_DIR, ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401

LOAD_REJECTED = "exceeds a configured limit and cannot be loaded from RDB"


def hnsw_field(name="v", dim=3, extra=()):
    params = ["TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2", *extra]
    return [name, "VECTOR", "HNSW", str(len(params)), *params]


# (id, FT.CREATE commands run before the snapshot, module args the snapshot is
#  restored under, limit message expected in the rejection)
CASES = [
    (
        "max_vector_m",
        [["FT.CREATE", "idx", "SCHEMA", *hnsw_field(extra=("M", "32"))]],
        "--max-vector-m 16",
        "M must be a positive integer greater than or equal to 2 and cannot "
        "exceed 16.",
    ),
    (
        "max_vector_ef_construction",
        [["FT.CREATE", "idx", "SCHEMA",
          *hnsw_field(extra=("EF_CONSTRUCTION", "300"))]],
        "--max-vector-ef-construction 200",
        "EF_CONSTRUCTION must be a positive integer greater than 0 and cannot "
        "exceed 200.",
    ),
    (
        "max_vector_ef_runtime",
        [["FT.CREATE", "idx", "SCHEMA",
          *hnsw_field(extra=("EF_RUNTIME", "50"))]],
        "--max-vector-ef-runtime 20",
        "EF_RUNTIME must be a positive integer greater than 0 and cannot "
        "exceed 20.",
    ),
    (
        "max_vector_dimensions",
        [["FT.CREATE", "idx", "SCHEMA", *hnsw_field(dim=64)]],
        "--max-vector-dimensions 32",
        "The dimensions value must be a positive integer greater than 0 and "
        "less than or equal to 32.",
    ),
    (
        "max_prefixes",
        [["FT.CREATE", "idx", "PREFIX", "3", "a:", "b:", "c:",
          "SCHEMA", "t", "TAG"]],
        "--max-prefixes 2",
        "Number of prefixes (3) exceeds the maximum allowed (2)",
    ),
    (
        # max-attributes itself is not honored on a running server (FT.CREATE
        # included): the legacy max-vector-attributes always takes precedence,
        # so exercise the attribute-count check through the limit in effect.
        "max_vector_attributes",
        [["FT.CREATE", "idx", "SCHEMA", "a", "TAG", "b", "TAG", "c", "TAG"]],
        "--max-vector-attributes 2",
        "The maximum number of attributes cannot exceed 2.",
    ),
    (
        "max_tag_field_length",
        [["FT.CREATE", "idx", "SCHEMA", "a_long_tag_field", "TAG"]],
        "--max-tag-field-length 10",
        "A tag field can have a maximum length of 10.",
    ),
    (
        "max_numeric_field_length",
        [["FT.CREATE", "idx", "SCHEMA", "a_long_num_field", "NUMERIC"]],
        "--max-numeric-field-length 10",
        "A numeric field can have a maximum length of 10.",
    ),
    (
        "max_indexes",
        [["FT.CREATE", f"idx{i}", "SCHEMA", "t", "TAG"] for i in range(3)],
        "--max-indexes 2",
        "Number of indexes (3) exceeds the maximum allowed (2)",
    ),
]


class TestRDBLoadEnforcesLimits(ValkeySearchTestCaseBase):

    def _save_rdb(self) -> str:
        self.client.execute_command("SAVE")
        values = []
        for key in ("dir", "dbfilename"):
            value = self.client.config_get(key)[key]
            values.append(value.decode() if isinstance(value, bytes) else value)
        return os.path.join(*values)

    def _start_from_rdb(self, rdb_path, label, module_args, expect_failure):
        """Start a fresh server on a copy of `rdb_path` with `module_args`."""
        testdir = os.path.join(LOGS_DIR, f"{self.test_name}_{label}")
        os.makedirs(testdir, exist_ok=True)
        shutil.copy(rdb_path, os.path.join(testdir, "dump.rdb"))
        port = self.get_bind_port()
        lines = [
            "enable-debug-command yes",
            "dbfilename dump.rdb",
            f"dir {testdir}",
            f"port {port}",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"loadmodule {os.getenv('MODULE_PATH')} {module_args}".rstrip(),
        ]
        conf_file = os.path.join(testdir, f"valkey_{port}.conf")
        with open(conf_file, "w") as f:
            f.write("\n".join(lines) + "\n")
        logfile = f"logfile_{port}"
        server, client = self.create_server(
            testdir=testdir,
            server_path=os.getenv("VALKEY_SERVER_PATH"),
            port=port,
            conf_file=conf_file,
            args={"logfile": logfile, "dbfilename": "dump.rdb"},
            wait_for_ping=not expect_failure,
            connect_client=not expect_failure,
        )
        return server, client, os.path.join(testdir, logfile)

    def _assert_load_rejected(self, rdb_path, label, module_args, detail):
        server, _, logfile = self._start_from_rdb(
            rdb_path, label, module_args, expect_failure=True)
        server.wait_for_shutdown()
        with open(logfile) as f:
            log = f.read()
        assert LOAD_REJECTED in log, log
        assert detail in log, log

    @pytest.mark.parametrize(
        "commands,module_args,detail",
        [case[1:] for case in CASES],
        ids=[case[0] for case in CASES],
    )
    def test_rdb_load_rejects_index_over_lowered_limit(
            self, commands, module_args, detail):
        for command in commands:
            self.client.execute_command(*command)
        rdb_path = self._save_rdb()
        self._assert_load_rejected(rdb_path, "lowered", module_args, detail)

    def test_rdb_load_succeeds_once_limit_is_raised(self):
        """The rejection is recoverable: the same RDB loads under limits that
        admit it, with every index restored."""
        for i in range(3):
            self.client.execute_command(
                "FT.CREATE", f"idx{i}", "SCHEMA",
                *hnsw_field(extra=("M", "32")))
        rdb_path = self._save_rdb()

        self._assert_load_rejected(
            rdb_path, "lowered", "--max-vector-m 16",
            "M must be a positive integer greater than or equal to 2 and "
            "cannot exceed 16.")
        self._assert_load_rejected(
            rdb_path, "too_few_indexes", "--max-indexes 2",
            "Number of indexes (3) exceeds the maximum allowed (2)")

        server, client, _ = self._start_from_rdb(
            rdb_path, "raised", "--max-vector-m 32 --max-indexes 3",
            expect_failure=False)
        assert sorted(client.execute_command("FT._LIST")) == [
            b"idx0", b"idx1", b"idx2"]
