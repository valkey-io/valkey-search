"""NaN and +/-Inf vector components are rejected end to end.

A single non-finite component makes every distance computed against the
vector NaN. Under -ffast-math the HNSW greedy descent cannot order NaN and,
on aarch64, an insert spun forever and wedged every writer thread behind it.
Such vectors are now invalid data at ingest, dropped at RDB load, and refused
as KNN query vectors.
"""

import os
import shutil
import struct

import pytest
from valkey import ResponseError, Valkey
from valkey_search_test_case import (
    LOGS_DIR,
    ValkeySearchTestCaseCommon,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters

DIM = 4
NUM_VALID = 20

# Built from IEEE-754 bit patterns so the bytes are exact.
NAN = struct.unpack("<f", struct.pack("<I", 0x7FC00000))[0]
POS_INF = struct.unpack("<f", struct.pack("<I", 0x7F800000))[0]
NEG_INF = struct.unpack("<f", struct.pack("<I", 0xFF800000))[0]
NON_FINITE = [NAN, POS_INF, NEG_INF]

ERR_NON_FINITE = "query vector contains NaN or infinite values"


def vec(values):
    return struct.pack(f"<{len(values)}f", *values)


def valid_vector(i):
    return vec([float(i + 1), 1.0, 0.5, float(-i)])


def create_index(client, name, algo, metric):
    algo_args = ["6", "TYPE", "FLOAT32", "DIM", str(DIM),
                 "DISTANCE_METRIC", metric]
    assert client.execute_command(
        "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", f"{name}:",
        "SCHEMA", "n", "NUMERIC",
        "v", "VECTOR", algo, *algo_args) == b"OK"


def info_field(client, name, field):
    info = client.execute_command("FT.INFO", name)
    return int(dict(zip(info[::2], info[1::2]))[field])


def num_docs(client, name):
    return info_field(client, name, b"num_docs")


def indexing_failures(client, name):
    return info_field(client, name, b"hash_indexing_failures")


def knn_keys(client, name, query, k=NUM_VALID + 5):
    reply = client.execute_command(
        "FT.SEARCH", name, f"*=>[KNN {k} @v $q]", "PARAMS", "2", "q", query,
        "NOCONTENT", "LIMIT", "0", str(k), "DIALECT", "2")
    return set(reply[1:])


class TestNonFiniteVectors(ValkeySearchTestCaseDebugMode):

    def populate(self, client, name):
        for i in range(NUM_VALID):
            client.hset(f"{name}:{i}", mapping={"n": i, "v": valid_vector(i)})

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    @pytest.mark.parametrize("metric", ["COSINE", "IP", "L2"])
    def test_ingest_rejects_non_finite(self, algo, metric):
        client: Valkey = self.server.get_new_client()
        name = f"nf_{algo}_{metric}".lower()
        create_index(client, name, algo, metric)
        self.populate(client, name)
        waiters.wait_for_equal(lambda: num_docs(client, name), NUM_VALID)
        bad_keys = set()
        for j, bad in enumerate(NON_FINITE):
            key = f"{name}:bad{j}"
            client.hset(key, mapping={"n": 100 + j,
                                      "v": vec([0.1, bad, 0.2, 0.3])})
            bad_keys.add(key.encode())
        # A key that started valid and is overwritten with NaN leaves the
        # vector index too.
        client.hset(f"{name}:0", "v", vec([NAN] * DIM))
        bad_keys.add(f"{name}:0".encode())

        # Whether the rest of an invalid key stays indexed depends on
        # search.emulate-release, so only the vector index is asserted on.
        waiters.wait_for_true(lambda: indexing_failures(client, name) > 0)
        waiters.wait_for_equal(
            lambda: len(knn_keys(client, name, valid_vector(3))),
            NUM_VALID - 1)
        hits = knn_keys(client, name, valid_vector(3))
        assert not hits & bad_keys
        # The writer threads are still live: a later valid insert is indexed.
        late = f"{name}:late".encode()
        client.hset(late, mapping={"n": 1, "v": valid_vector(99)})
        waiters.wait_for_true(
            lambda: late in knn_keys(client, name, valid_vector(99)))

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    @pytest.mark.parametrize("bad", NON_FINITE, ids=["nan", "posinf", "neginf"])
    def test_knn_query_rejects_non_finite(self, algo, bad):
        client: Valkey = self.server.get_new_client()
        name = f"nfq_{algo}".lower()
        create_index(client, name, algo, "L2")
        self.populate(client, name)
        waiters.wait_for_equal(lambda: num_docs(client, name), NUM_VALID)
        with pytest.raises(ResponseError, match=ERR_NON_FINITE):
            knn_keys(client, name, vec([1.0, 2.0, bad, 3.0]))
        with pytest.raises(ResponseError, match=ERR_NON_FINITE):
            client.execute_command(
                "FT.HYBRID", name, "SEARCH", "@n:[0 100]",
                "VSIM", "@v", "$q", "KNN", "2", "K", "5",
                "PARAMS", "2", "q", vec([bad, 1.0, 1.0, 1.0]))
        with pytest.raises(ResponseError,
                           match="Vector blob contains NaN or infinite"):
            client.execute_command(
                "FT.SEARCH", name, "@v:[VECTOR_RANGE 10 $q]",
                "PARAMS", "2", "q", vec([1.0, bad, 1.0, 1.0]),
                "DIALECT", "2")

    def test_rdb_reload_after_rejected_ingest(self):
        """A key rejected at ingest is not saved as tracked, and reload works.

        The load-time drop of keys saved by an older module is covered by
        TestNonFiniteVectorRDBLoad below.
        """
        client: Valkey = self.server.get_new_client()
        name = "nf_rdb"
        create_index(client, name, "HNSW", "COSINE")
        self.populate(client, name)
        client.hset(f"{name}:bad", mapping={"n": 1, "v": vec([NAN] * DIM)})
        waiters.wait_for_equal(
            lambda: len(knn_keys(client, name, valid_vector(5))), NUM_VALID)

        client.execute_command("SAVE")
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()

        waiters.wait_for_equal(
            lambda: len(knn_keys(client, name, valid_vector(5))), NUM_VALID)
        assert f"{name}:bad".encode() not in knn_keys(
            client, name, valid_vector(5))
        # Ingest still works after the reload.
        client.hset(f"{name}:late", mapping={"n": 2, "v": valid_vector(42)})
        waiters.wait_for_equal(
            lambda: len(knn_keys(client, name, valid_vector(5))),
            NUM_VALID + 1)


def search_keys(client, query):
    reply = client.execute_command(
        "FT.SEARCH", "idx", query, "NOCONTENT", "LIMIT", "0", "100")
    return set(reply[1:])


class TestNonFiniteVectorRDBLoad(ValkeySearchTestCaseCommon):
    """Loads an RDB written by a module that still accepted NaN vectors.

    The fixture holds a FLAT index `idx` over `doc:*` with fields `n` NUMERIC,
    `t` TAG and `v` FLOAT32 DIM 4 L2. doc:0..doc:4 have finite vectors and
    doc:bad has a NaN element; all six have n in [0, 4] and t == "a". It was
    written by a build without non-finite validation, which indexed all six:
    FT.CREATE as above, HSET the six keys, SAVE. FLAT is used because such a
    build can hang inserting a NaN vector into HNSW.
    """

    RDB_FILENAME = "non_finite_vector.rdb"
    RDB_FIXTURE = f"rdbs/{RDB_FILENAME}"
    VALID_KEYS = {f"doc:{i}".encode() for i in range(5)}
    BAD_KEY = b"doc:bad"

    def _start_server(self, test_name, search_module_args):
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
        _, client = self.create_server(
            testdir=testdir,
            server_path=os.getenv("VALKEY_SERVER_PATH"),
            port=port,
            conf_file=conf_file,
            args={"logfile": f"logfile_{port}",
                  "dbfilename": self.RDB_FILENAME},
        )
        return client

    # rdb-read-v2 picks how the loaded keys are re-ingested: by replaying the
    # saved index extension, or by a backfill scan. Both apply the
    # invalid-data policy to the key the vector index dropped.
    @pytest.mark.parametrize("read_v2", ["yes", "no"])
    @pytest.mark.parametrize("release", ["1.2.0", "1.3.0"])
    def test_load_drops_non_finite_vector(self, release, read_v2):
        client = self._start_server(
            f"non_finite_rdb_{release.replace('.', '_')}_v2_{read_v2}",
            f"--debug-mode yes --emulate-release {release} "
            f"--rdb-read-v2 {read_v2}")

        # The vector index never restores the key, whatever the release.
        waiters.wait_for_equal(
            lambda: knn_keys(client, "idx", vec([0.0, 1.0, 2.0, 3.0])),
            self.VALID_KEYS)
        waiters.wait_for_equal(lambda: indexing_failures(client, "idx"), 1)

        # From 1.3.0 an invalid field drops the whole key, as on ingest.
        expected = set(self.VALID_KEYS)
        if release == "1.2.0":
            expected.add(self.BAD_KEY)
        waiters.wait_for_equal(
            lambda: search_keys(client, "@t:{a}"), expected)
        assert search_keys(client, "@n:[0 4]") == expected
