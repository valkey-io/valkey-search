import os
import time
import traceback

import pytest

from .data_sets import SCORING_NUM_DOCS, load_data
from .generate import BaseCompatibilityTest
from .scoring_query_builder import build_scoring_queries, search_args

'''
Capture RediSearch answers for BM25STD relevance scores.
'''


@pytest.mark.parametrize("schema_type", ["nostem", "docscore"])
@pytest.mark.parametrize("key_type", ["hash", "json"])
class TestScoringCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "scoring-answers.pickle.gz"
    DATA_SET_NAME = "scoring"
    # Built once: the whole set is seeded and read-only, and every test case wants
    # one shape out of the 14.
    QUERIES = build_scoring_queries()

    def setup_method(self):
        # Drops the base class' 1s sleep: setup_data waits on FT.INFO instead, which
        # is both stricter and immediate. Verified to give byte-identical answers.
        self.client.execute_command("FLUSHALL SYNC")

    def setup_data(self, key_type, schema_type):
        """Load the scoring corpus for one index variant."""
        self.data_set_name = self.DATA_SET_NAME
        self.key_type = key_type
        self.schema_type = schema_type
        self.client.execute_command("FLUSHALL SYNC")
        load_data(self.client, self.DATA_SET_NAME, key_type,
                  schema_type=schema_type)
        self._wait_for_indexing(f"{key_type}_idx1")

    def _wait_for_indexing(self, index_name, timeout=30):
        """Wait out Redis' asynchronous indexing."""
        # N and avg_doc_len are corpus-wide, so a partial corpus moves every score.
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.client.ft_info(index_name)
            if int(info["num_docs"]) == SCORING_NUM_DOCS and not int(info["indexing"]):
                return
            time.sleep(0.25)
        assert False, (f"{index_name} still at {info['num_docs']}/"
                       f"{SCORING_NUM_DOCS} docs after {timeout}s")

    def capture(self, cmd):
        """Run one command and build its answer, without recording it."""
        # Not an execute_command override: a shape must see the reply before keeping it.
        print("Cmd:", *cmd)
        return {"cmd": cmd,
                "key_type": self.key_type,
                "data_set_name": self.data_set_name,
                "schema_type": self.schema_type,
                "testname": os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0],
                "traceback": "".join(traceback.format_stack()),
                "result": self.client.execute_command(*cmd),
                "exception": False}

    @staticmethod
    def _scores_by_doc(result):
        """{doc id: score} from a [total, key, score, key, score, ...] reply."""
        return {int(result[i].split(b":")[1]): float(result[i + 1])
                for i in range(1, len(result), 2)}

    def _run_shape(self, shape, key_type, schema_type):
        self.setup_data(key_type, schema_type)
        queries = self.QUERIES[shape]
        for descriptor in queries:
            # A query matching nothing asserts nothing, so it is a builder bug.
            assert descriptor["hits"], \
                f"{shape} query {descriptor['query']!r} predicts no hits"
            answer = self.capture(search_args(f"{key_type}_idx1", descriptor))
            scores = self._scores_by_doc(answer["result"])
            # The replay trusts the captured answer, so only this catches a bad prediction.
            assert set(scores) == descriptor["hits"], (
                f"{shape} query {descriptor['query']!r} matched {sorted(scores)}, "
                f"predicted {sorted(descriptor['hits'])}")
            self.answers.append(answer)
        print(f"{shape}[{key_type}-{schema_type}]: {len(queries)} queries")

    def test_scoring_single_term(self, key_type, schema_type):
        self._run_shape("single_term", key_type, schema_type)

    def test_scoring_and2(self, key_type, schema_type):
        self._run_shape("and2", key_type, schema_type)

    def test_scoring_and3(self, key_type, schema_type):
        self._run_shape("and3", key_type, schema_type)

    def test_scoring_or2(self, key_type, schema_type):
        self._run_shape("or2", key_type, schema_type)

    def test_scoring_or3(self, key_type, schema_type):
        self._run_shape("or3", key_type, schema_type)

    def test_scoring_mixed(self, key_type, schema_type):
        self._run_shape("mixed", key_type, schema_type)

    def test_scoring_cross_field(self, key_type, schema_type):
        self._run_shape("cross_field", key_type, schema_type)

    def test_scoring_leaf_weight(self, key_type, schema_type):
        self._run_shape("leaf_weight", key_type, schema_type)

    def test_scoring_nested_weight(self, key_type, schema_type):
        self._run_shape("nested_weight", key_type, schema_type)

    def test_scoring_tag_only(self, key_type, schema_type):
        self._run_shape("tag_only", key_type, schema_type)

    def test_scoring_numeric_only(self, key_type, schema_type):
        self._run_shape("numeric_only", key_type, schema_type)

    def test_scoring_text_numeric_tag(self, key_type, schema_type):
        self._run_shape("text_numeric_tag", key_type, schema_type)

    def test_scoring_match_all(self, key_type, schema_type):
        self._run_shape("match_all", key_type, schema_type)

    def test_scoring_text_vector(self, key_type, schema_type):
        self._run_shape("text_vector", key_type, schema_type)
