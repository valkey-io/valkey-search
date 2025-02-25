import difflib
import os
import pprint
import time
from typing import Any, List

import numpy as np
import valkey
import valkey.cluster

from absl import flags
from absl.testing import absltest
from absl.testing import parameterized
from testing.integration import utils


FLAGS = flags.FLAGS
flags.DEFINE_string("valkey_server_path", None, "Path to the Valkey server")
flags.DEFINE_string("valkey_cli_path", None, "Path to the Valkey CLI")


def generate_test_vector(dimensions: int, data: int):
    vector = np.zeros(dimensions).astype(np.float32)
    vector[0] = np.float32(1)
    vector[1] = np.float32(data)
    return vector


class VSSOutput:
    """Helper class to parse VSS output."""

    def __init__(
        self, output: Any, embedding_attribute_name: str = "embedding"
    ):
        self.embedding_attribute_name = bytes(embedding_attribute_name, "utf-8")
        if not output:
            return
        self.count = output[0]
        self.keys = dict()
        if len(output) < 3 or not isinstance(output[2], List):
            # NOCONTENT/RETURN 0
            for i in range(1, len(output)):
                self.keys[output[i]] = dict()
            return

        for i in range(1, len(output), 2):
            attrs = output[i + 1]
            attrs_map = dict()
            for j in range(0, len(attrs), 2):
                attrs_map[attrs[j]] = attrs[j + 1]
            if self.embedding_attribute_name in attrs_map:
                attrs_map[self.embedding_attribute_name] = np.frombuffer(
                    attrs_map[self.embedding_attribute_name], dtype=np.float32
                )
            self.keys[output[i]] = attrs_map

    def __eq__(self, other):
        if self.count != other.count:
            return False
        if self.keys.keys() != other.keys.keys():
            return False
        for k in self.keys:
            if self.keys[k].keys() != other.keys[k].keys():
                return False
            for attr in self.keys[k]:
                if attr == self.embedding_attribute_name:
                    if not np.allclose(
                        self.keys[k][attr],
                        other.keys[k][attr],
                    ):
                        return False
                else:
                    if self.keys[k][attr] != other.keys[k][attr]:
                        return False
        return True

    def __str__(self):
        return f"count: {self.count}\nkeys: {pprint.pformat(self.keys)}"


class VSSTestCase(parameterized.TestCase):

    def setUp(self):
        super().setUp()
        self.addTypeEqualityFunc(VSSOutput, "assertVSSOutputEqual")

    def assertVSSOutputEqual(self, a, b, msg=None):
        if a != b:
            diff_msg = "\n" + "\n".join(
                difflib.ndiff(
                    str(a).splitlines(),
                    str(b).splitlines(),
                )
            )
            self.fail(f"VSSOutput not equal: {diff_msg}")


class VectorSearchIntegrationTest(VSSTestCase):
    # Start the valkey cluster once for all tests.
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        valkey_server_stdout_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]

        if FLAGS.valkey_server_path is None:
            raise ValueError(
                "--test_arg=--valkey_server_path=/path/to/valkey_server "
                "is required"
            )
        if FLAGS.valkey_cli_path is None:
            raise ValueError(
                "--test_arg=--valkey_cli_path=/path/to/valkey_cli is "
                "required"
            )

        cls.valkey_cluster_under_test = utils.start_valkey_cluster(
            FLAGS.valkey_server_path,
            FLAGS.valkey_cli_path,
            [6379, 6380, 6381],
            os.environ["TEST_TMPDIR"],
            valkey_server_stdout_dir,
            {
                "loglevel": "debug",
                "enable-debug-command": "yes",
                "repl-diskless-load": "swapdb",
                # tripled, to handle slow test environments
                "cluster-node-timeout": "45000",
            },
            {
                f"{os.path.join(os.environ['PWD'], 'src/libvalkeysearch.so')}": "--threads 2 --log-level notice --use-coordinator",
            },
            0,
        )
        cls.valkey_conn = utils.connect_to_valkey_cluster(
            [
                valkey.cluster.ClusterNode("localhost", port)
                for port in [6379, 6380, 6381]
            ],
            True,
        )
        if not cls.valkey_conn:
            cls.fail("Failed to connect to valkey cluster")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def tearDown(self):
        for index in self.valkey_conn.execute_command("FT._LIST"):
            self.valkey_conn.execute_command("FT.DROPINDEX", index)
        time.sleep(1)
        self.valkey_conn.execute_command(
            "FLUSHDB", target_nodes=self.valkey_conn.ALL_NODES
        )
        terminated = self.valkey_cluster_under_test.get_terminated_servers()
        if terminated:
            self.fail(f"Valkey servers terminated during test, ports: {terminated}")
        try:
            self.valkey_cluster_under_test.ping_all()
        except Exception as e:  # pylint: disable=broad-except
            self.fail(f"Failed to ping all servers in cluster: {e}")
        super().tearDown()

    def test_create_and_drop_index(self):
        self.assertEqual(
            b"OK",
            utils.create_index(
                self.valkey_conn,
                "test_index",
                attributes={
                    "embedding": utils.HNSWVectorDefinition(
                        vector_dimensions=100
                    )
                },
                target_nodes=valkey.ValkeyCluster.RANDOM,
            ),
        )
        time.sleep(1)

        with self.assertRaises(valkey.exceptions.ResponseError) as e:
            utils.create_index(
                self.valkey_conn,
                "test_index",
                attributes={
                    "embedding": utils.HNSWVectorDefinition(
                        vector_dimensions=100
                    )
                },
                target_nodes=valkey.ValkeyCluster.RANDOM,
            ),
        self.assertEqual(
            "Index test_index already exists.",
            e.exception.args[0],
        )

        self.assertEqual(
            [b"test_index"],
            self.valkey_conn.execute_command("FT._LIST"),
        )

        self.assertEqual(
            b"OK",
            self.valkey_conn.execute_command("FT.DROPINDEX", "test_index"),
        )
        time.sleep(1)

        with self.assertRaises(valkey.exceptions.ResponseError) as e:
            self.valkey_conn.execute_command("FT.DROPINDEX", "test_index")
        self.assertEqual(
            "Index with name 'test_index' not found",
            e.exception.args[0],
        )

    @parameterized.named_parameters(
        dict(
            testcase_name="index_not_exists_no_content",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="not_a_real_index",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=None,
                expected_error="Index with name 'not_a_real_index' not found",
                expected_result=None,
                no_content=True,
            ),
        ),
        dict(
            testcase_name="attribute_not_exists_no_content",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="test_index",
                vector_search_attribute="not_a_real_attribute",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=None,
                expected_error="Index field `not_a_real_attribute` does not exist",
                expected_result=None,
                no_content=True,
            ),
        ),
        dict(
            testcase_name="index_not_exists",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="not_a_real_index",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=None,
                expected_error="Index with name 'not_a_real_index' not found",
                expected_result=None,
                no_content=False,
            ),
        ),
        dict(
            testcase_name="attribute_not_exists",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="test_index",
                vector_search_attribute="not_a_real_attribute",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=None,
                expected_error="Index field `not_a_real_attribute` does not exist",
                expected_result=None,
                no_content=False,
            ),
        ),
        dict(
            testcase_name="happy_case_no_content",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="test_index",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=None,
                expected_error=None,
                expected_result=[3, b"2", b"1", b"0"],
                no_content=True,
            ),
        ),
        dict(
            testcase_name="happy_case_returns_0",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="test_index",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=[],
                expected_error=None,
                expected_result=[3, b"2", b"1", b"0"],
                no_content=False,
            ),
        ),
        dict(
            testcase_name="happy_case_return_score",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="test_index",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=["score"],
                expected_error=None,
                expected_result=[
                    3,
                    b"2",
                    [b"score", b"0.552786409855"],
                    b"1",
                    [b"score", b"0.292893230915"],
                    b"0",
                    [b"score", b"0"],
                ],
                no_content=False,
            ),
        ),
        dict(
            testcase_name="happy_case_all_returns",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="test_index",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=None,
                expected_error=None,
                expected_result=[
                    3,
                    b"2",
                    [
                        b"score",
                        b"0.552786409855",
                        b"embedding",
                        generate_test_vector(100, 2).tobytes(),
                        b"numeric",
                        b"2",
                        b"tag",
                        b"2",
                        b"not_indexed",
                        b"2",
                    ],
                    b"1",
                    [
                        b"score",
                        b"0.292893230915",
                        b"embedding",
                        generate_test_vector(100, 1).tobytes(),
                        b"numeric",
                        b"1",
                        b"tag",
                        b"1",
                        b"not_indexed",
                        b"1",
                    ],
                    b"0",
                    [
                        b"score",
                        b"0",
                        b"embedding",
                        generate_test_vector(100, 0).tobytes(),
                        b"numeric",
                        b"0",
                        b"tag",
                        b"0",
                        b"not_indexed",
                        b"0",
                    ],
                ],
                no_content=False,
            ),
        ),
        dict(
            testcase_name="happy_case_just_embeddings",
            config=dict(
                index_name="test_index",
                vector_attribute_name="embedding",
                search_index="test_index",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="*",
                knn=3,
                score_as="score",
                returns=["embedding"],
                expected_error=None,
                expected_result=[
                    3,
                    b"2",
                    [
                        b"embedding",
                        generate_test_vector(100, 2).tobytes(),
                    ],
                    b"1",
                    [
                        b"embedding",
                        generate_test_vector(100, 1).tobytes(),
                    ],
                    b"0",
                    [
                        b"embedding",
                        generate_test_vector(100, 0).tobytes(),
                    ],
                ],
                no_content=False,
            ),
        ),
    )
    def test_vector_search(self, config):
        self.maxDiff = None
        dimensions = 100
        self.assertEqual(
            b"OK",
            utils.create_index(
                self.valkey_conn,
                config["index_name"],
                attributes={
                    config["vector_attribute_name"]: utils.HNSWVectorDefinition(
                        vector_dimensions=dimensions
                    )
                },
            ),
        )
        time.sleep(1)
        for data in range(100):
            vector = generate_test_vector(dimensions, data)
            self.assertEqual(
                4,
                self.valkey_conn.hset(
                    str(data),
                    mapping={
                        "embedding": vector.tobytes(),
                        "tag": str(data),
                        "numeric": str(data),
                        "not_indexed": str(data),
                    },
                ),
            )

        args = [
            "FT.SEARCH",
            config["search_index"],
            (
                f'{config["filter"]}=>[KNN'
                f' {config["knn"]} @{config["vector_search_attribute"]} $vec'
                f' EF_RUNTIME 1 AS {config["score_as"]}]'
            ),
            "params",
            2,
            "vec",
            config["search_vector"].tobytes(),
            "DIALECT",
            2,
        ]
        if config["no_content"]:
            args.append("NOCONTENT")
        if config["returns"] is not None:
            args.extend(
                ["RETURN", str(len(config["returns"]))] + config["returns"]
            )

        if config["expected_error"] is not None:
            with self.assertRaises(valkey.exceptions.ResponseError) as e:
                self.valkey_conn.execute_command(
                    *args, target_nodes=self.valkey_conn.RANDOM
                )
            self.assertEqual(
                config["expected_error"],
                e.exception.args[0],
            )
        else:
            got = VSSOutput(
                self.valkey_conn.execute_command(
                    *args, target_nodes=self.valkey_conn.RANDOM
                ),
                embedding_attribute_name=config["vector_search_attribute"],
            )
            want = VSSOutput(
                config["expected_result"],
                embedding_attribute_name=config["vector_search_attribute"],
            )
            self.assertEqual(want, got)


if __name__ == "__main__":
    absltest.main()
