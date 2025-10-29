import ast
import difflib
import json
import logging
import os
import pprint
import time
from typing import Any, List
import copy

import numpy as np
import valkey
import valkey.cluster

from absl.testing import absltest
from absl.testing import parameterized
import utils

def generate_test_vector(dimensions: int, data: int):
    vector = np.zeros(dimensions).astype(np.float32)
    vector[0] = np.float32(1)
    vector[1] = np.float32(data)
    return vector


def generate_test_cases_basic(test_name):
    test_cases = []
    for store_data_type in utils.StoreDataType:
        for vector_index_type in utils.VectorIndexType:
                test_cases.append(dict(
                    testcase_name = test_name + "_" + store_data_type.name + "_" + vector_index_type.name,
                    store_data_type = store_data_type.name,
                    vector_index_type = vector_index_type.name
                    ))

    return test_cases


def generate_test_cases():
    test_cases_base = [
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
                expected_result=[3, "2", "1", "0"],
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
                expected_result=[3, "2", "1", "0"],
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
                    "2",
                    ["score", "0.552786409855"],
                    "1",
                    ["score", "0.292893230915"],
                    "0",
                    ["score", "0"],
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
                    "2",
                    [
                        "score",
                        "0.552786409855",
                        "embedding",
                        generate_test_vector(100, 2).tobytes(),
                        "numeric",
                        "2",
                        "tag",
                        "2",
                        "not_indexed",
                        "2",
                    ],
                    "1",
                    [
                        "score",
                        "0.292893230915",
                        "embedding",
                        generate_test_vector(100, 1).tobytes(),
                        "numeric",
                        "1",
                        "tag",
                        "1",
                        "not_indexed",
                        "1",
                    ],
                    "0",
                    [
                        "score",
                        "0",
                        "embedding",
                        generate_test_vector(100, 0).tobytes(),
                        "numeric",
                        "0",
                        "tag",
                        "0",
                        "not_indexed",
                        "0",
                    ],
                ],
                no_content=False,
            ),
        ),
        dict(
            testcase_name="happy_case_all_returns_with_alias",
            config=dict(
                index_name="test_index_1",
                vector_attribute_name="embedding",
                search_index="test_index_1",
                vector_search_attribute="embedding",
                search_vector=generate_test_vector(100, 0),
                filter="@numeric_alias:[1 2] @tag_alias:{2}",
                knn=3,
                score_as="score",
                tag_alias="tag_alias",
                numeric_alias="numeric_alias",
                returns=None,
                expected_error=None,
                expected_result=[
                    1,
                    "2",
                    [
                        "score",
                        "0.552786409855",
                        "embedding",
                        generate_test_vector(100, 2).tobytes(),
                        "numeric",
                        "2",
                        "tag",
                        "2",
                        "not_indexed",
                        "2",
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
                    "2",
                    [
                        "embedding",
                        generate_test_vector(100, 2).tobytes(),
                    ],
                    "1",
                    [
                        "embedding",
                        generate_test_vector(100, 1).tobytes(),
                    ],
                    "0",
                    [
                        "embedding",
                        generate_test_vector(100, 0).tobytes(),
                    ],
                ],
                no_content=False,
            ),
        )
        ]
    test_cases = []
    for store_data_type in utils.StoreDataType:
        for vector_index_type in utils.VectorIndexType:
            for test_case in test_cases_base:
                test_case_new = copy.deepcopy(test_case)
                test_case_new["config"]["store_data_type"] = store_data_type.name
                test_case_new["config"]["vector_index_type"] = vector_index_type.name
                test_case_new["testcase_name"] += "_" + store_data_type.name + "_" +vector_index_type.name
                test_cases.append(test_case_new)

    return test_cases


class VSSOutput:
    """Helper class to parse VSS output."""

    def __init__(
        self, output: Any, store_data_type: str, embedding_attribute_name: str = "embedding"
    ):
        self.embedding_attribute_name = embedding_attribute_name
        if not output:
            return
        self.count = output[0]
        self.keys = dict()
        if len(output) < 3 or not isinstance(output[2], List):
            # NOCONTENT/RETURN 0
            for i in range(1, len(output)):
                self.keys[utils.to_str(output[i])] = dict()
            return

        for i in range(1, len(output), 2):
            attrs = output[i + 1]
            attrs_map = dict()
            for j in range(0, len(attrs), 2):
                if store_data_type == utils.StoreDataType.HASH.name:
                    if utils.to_str(attrs[j]) == self.embedding_attribute_name:
                        attrs_map[self.embedding_attribute_name] = np.frombuffer(attrs[j + 1], dtype=np.float32)
                        nps = np.frombuffer(attrs[j + 1], dtype=np.float32)
                        float_list = [float(x) for x in nps]
                    else:
                        attrs_map[utils.to_str(attrs[j])] = utils.to_str(attrs[j + 1])
                else:
                    if utils.to_str(attrs[j]) == "$":
                        data_dict = json.loads(utils.to_str(attrs[j + 1]))
                        for key, value in data_dict.items():
                            if key == self.embedding_attribute_name:
                                list = json.loads(utils.to_str(value))
                                float_list = [float(x) for x in list]
                                value = np.array(float_list, dtype=np.float32)
                            attrs_map[key] = value
                    else:
                        if utils.to_str(attrs[j]) == self.embedding_attribute_name:
                            list = json.loads(utils.to_str(attrs[j + 1]))
                            float_list = [float(x) for x in list]
                            value = np.array(float_list, dtype=np.float32)
                            attrs_map[self.embedding_attribute_name] = value
                        else:
                            attrs_map[utils.to_str(attrs[j])] = utils.to_str(attrs[j + 1])
            self.keys[utils.to_str(output[i])] = attrs_map


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
                    try:
                        if not np.allclose(
                            self.keys[k][attr],
                            other.keys[k][attr],
                        ):
                            return False
                    except Exception:
                        print("__eq__ Exception: {}, {}\n".format(self.keys[k][attr], other.keys[k][attr]))
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
        # Enable logging at DEBUG level
        logging.basicConfig(level=logging.DEBUG)

        # Get the logger used by redis-py
        valkey_logger = logging.getLogger("redis")
        valkey_logger.setLevel(logging.DEBUG)

        # Optional: Stream to stderr explicitly
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        valkey_logger.addHandler(handler)

        valkey_server_stdout_dir = os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]
        valkey_server_path = os.environ["VALKEY_SERVER_PATH"]
        valkey_cli_path = os.environ["VALKEY_CLI_PATH"]
        valkey_search_path = os.environ["VALKEY_SEARCH_PATH"]

        cls.valkey_ports = [6379, 6380, 6381]
        cls.valkey_cluster_under_test = utils.start_valkey_cluster(
            valkey_server_path,
            valkey_cli_path,
            cls.valkey_ports,
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
                f"{valkey_search_path}": "--reader-threads 2 --writer-threads 5 --log-level notice --use-coordinator",
            },
            0,
        )
        cls.valkey_conn = utils.connect_to_valkey_cluster(
            [
                valkey.cluster.ClusterNode("localhost", port)
                for port in cls.valkey_ports
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

    def _set_config(self, name: str, value: str) -> bool:
        name = f"search.{name}"
        try:
            return self.valkey_conn.config_set(name, value)
        except Exception:  # pylint: disable=broad-except
            return False

    def _get_config(self, name: str) -> str:
        name = f"search.{name}"
        val = self.valkey_conn.config_get(name)[name]
        return val

    def test_changing_thread_count(self):
        # Check the default values & existence of the configuration variable
        # search.reader-threads & search.writer-threads
        r_core_count = self._get_config("reader-threads")
        self.assertTrue(
            r_core_count is not None, "reader-threads does not exist!"
        )

        w_core_count = self._get_config("writer-threads")
        self.assertTrue(
            w_core_count is not None, "writer-threads does not exist!"
        )

        # Modify and confirm change
        self.assertTrue(self._set_config("writer-threads", "1"))
        self.assertTrue(self._set_config("reader-threads", "1"))

        self.assertEqual(self._get_config("reader-threads"), "1")
        self.assertEqual(self._get_config("writer-threads"), "1")

    def test_changing_debug_level(self):
        # Check the default values & existence of the configuration variable
        # search.reader-threads & search.writer-threads
        current_log_level = self._get_config("log-level")
        self.assertTrue(
            current_log_level is not None, "log-level does not exist!")
        # Check bad values are not accepted
        self.assertFalse(self._set_config("log-level", "no-such-level"))

        # Check valid values
        self.assertTrue(self._set_config("log-level", "debug"))
        self.assertEqual(self._get_config("log-level"), "debug")

        self.assertTrue(self._set_config("log-level", "verbose"))
        self.assertEqual(self._get_config("log-level"), "verbose")

        self.assertTrue(self._set_config("log-level", "notice"))
        self.assertEqual(self._get_config("log-level"), "notice")

        self.assertTrue(self._set_config("log-level", "warning"))
        self.assertEqual(self._get_config("log-level"), "warning")

    @parameterized.named_parameters(generate_test_cases_basic("test_create_and_drop_index"))
    def test_create_and_drop_index(self, store_data_type, vector_index_type):
        self.assertEqual(
            b"OK",
            utils.create_index(
                self.valkey_conn,
                "test_index",
                utils.StoreDataType.HASH.name,
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
            dimensions = 100
            vector_definitions = utils.HNSWVectorDefinition(vector_dimensions=dimensions) \
                if vector_index_type == utils.VectorIndexType.HNSW.name \
                else utils.FlatVectorDefinition(vector_dimensions=dimensions)
            utils.create_index(
                self.valkey_conn,
                "test_index",
                store_data_type,
                attributes={
                    "embedding": vector_definitions
                },
                target_nodes=valkey.ValkeyCluster.RANDOM,
            )

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


    @parameterized.named_parameters(generate_test_cases())
    def test_vector_search(self, config):
        self.maxDiff = None
        dimensions = 100
        vector_definitions = utils.HNSWVectorDefinition(vector_dimensions=dimensions) \
            if config["vector_index_type"] == utils.VectorIndexType.HNSW.name \
            else utils.FlatVectorDefinition(vector_dimensions=dimensions)

        self.assertEqual(
            b"OK",
            utils.create_index(
                self.valkey_conn,
                config["index_name"],
                config["store_data_type"],
                attributes={
                    config["vector_attribute_name"]: vector_definitions,
                    "tag": utils.TagDefinition(alias=config.get("tag_alias")),
                    "numeric": utils.NumericDefinition(alias=config.get("numeric_alias")),
                },
            ),
        )
        time.sleep(1)
        for data in range(100):
            vector = generate_test_vector(dimensions, data)
            if config["store_data_type"] == utils.StoreDataType.HASH.name:
                vector = vector.tobytes()
            else:
                float_list = [str(float(vec)) for vec in vector]
                vector = "[" + ",".join(float_list) + "]"
            self.assertEqual(
                4,
                utils.store_entry(
                    self.valkey_conn,
                    config["store_data_type"],
                    str(data),
                    mapping={
                        "embedding": vector,
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
            args_str = ""
            for arg in args:
                args_str += str(utils.to_str(arg)) + " "
            got = VSSOutput(
                self.valkey_conn.execute_command(
                    *args, target_nodes=self.valkey_conn.RANDOM
                ),
                embedding_attribute_name=config["vector_search_attribute"],
                store_data_type=config["store_data_type"],
            )
            want = VSSOutput(
                config["expected_result"],
                embedding_attribute_name=config["vector_search_attribute"],
                store_data_type=utils.StoreDataType.HASH.name,
            )
            self.assertEqual(want, got)
            
    def test_coordinator_server_port(self):
        for idx, port in enumerate(self.valkey_ports):
            # Connect to each node in the cluster
            valkey_conn = valkey.Valkey(
                host="localhost",
                port=port,
                socket_timeout=1000,
            )
            if not valkey_conn:
                self.fail("Failed to connect to valkey cluster")
               
            coordinator_port = valkey_conn.info(section="search")["search_coordinator_server_listening_port"]
            assert int(coordinator_port) == port + 20294
            


if __name__ == "__main__":
    absltest.main()
