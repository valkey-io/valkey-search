import struct

import pytest

from .data_sets import VECTOR_DIM
from .generate import BaseCompatibilityTest


@pytest.mark.parametrize("key_type", ["hash", "json"])
@pytest.mark.parametrize("algo", ["flat", "hnsw"])
@pytest.mark.parametrize("metric", ["l2", "ip", "cosine"])
class TestFloat64VectorCompatibility(BaseCompatibilityTest):
    """Capture Redis Search FLOAT64 KNN replies for Valkey Search replay."""

    ANSWER_FILE_NAME = "float64-answers.pickle.gz"

    def checkvec(self, key_type, query_vector):
        blob = struct.pack(f"<{VECTOR_DIM}d", *query_vector)
        command = [
            "FT.SEARCH",
            f"{key_type}_idx1",
            "*=>[KNN 3 @v1 $BLOB AS score]",
            "PARAMS",
            "2",
            "BLOB",
            blob,
            "RETURN",
            "2",
            "score",
            "__key",
            "DIALECT",
            "2",
        ]
        self.execute_command(command)

    def test_knn_scores(self, key_type, algo, metric):
        self.setup_data(
            f"vector data {metric} {algo}", key_type, vector_data_type="FLOAT64"
        )
        # The perturbation is below FLOAT32 precision around 1.0, ensuring the
        # reference query is actually encoded and evaluated as FLOAT64.
        self.checkvec(key_type, [1.0 + 2.0 ** -40, -0.75, 0.5])
