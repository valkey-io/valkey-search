import hashlib
import os

_COMPAT_DIR = os.path.dirname(os.path.abspath(__file__))


# Registry of compatibility generators. To add a new generator, create the
# generate file (subclassing BaseCompatibilityTest with its own
# ANSWER_FILE_NAME) and add an entry here. regenerate.sh and
# compatibility_test.py both read from this list.
GENERATORS = [
    {"generator": "generate.py",         "answers": "aggregate-answers.pickle.gz",   "cluster": True},
    {"generator": "generate_text.py",    "answers": "text-search-answers.pickle.gz", "cluster": False},
    {"generator": "generate_array.py",   "answers": "array-input-answers.pickle.gz",  "cluster": False},
    {"generator": "generate_expr.py",    "answers": "expr-answers.pickle.gz",         "cluster": False},
    {"generator": "generate_sortkey.py", "answers": "sortkey-answers.pickle.gz",      "cluster": False},
    {"generator": "generate_filter.py",  "answers": "filter-answers.pickle.gz",       "cluster": False},
    # "cluster": False for the reason generate_text.py is -- text scores are
    # computed from shard-local corpus statistics, so these standalone-captured
    # answers cannot match a cluster replay. See unsupported_tests.md 5.9. The
    # two mechanical blockers are fixed, so this is a one-word change once
    # distributed text scoring lands.
    {"generator": "generate_hybrid.py",  "answers": "hybrid-answers.pickle.gz",       "cluster": False},
]


def compute_sources_hash():
    """SHA256 of every .py file in this directory.

    Stored inside the generated pickle answer files so compatibility_test.py
    can detect when a pickle is stale relative to the generators and helpers.
    """
    h = hashlib.sha256()
    for fname in sorted(os.listdir(_COMPAT_DIR)):
        if not fname.endswith(".py"):
            continue
        h.update(fname.encode("utf-8"))
        h.update(b"\0")
        with open(os.path.join(_COMPAT_DIR, fname), "rb") as f:
            h.update(f.read())
        h.update(b"\0")
    return h.hexdigest()
