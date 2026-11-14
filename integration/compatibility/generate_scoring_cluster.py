from . import generate_scoring


# generate_scoring.py's tests captured from a Redis cluster, where BM25 statistics are per shard.
class TestScoringClusterCompatibility(generate_scoring.TestScoringCompatibility):
    ANSWER_FILE_NAME = "scoring-cluster-answers.pickle.gz"
    CLUSTER = True
