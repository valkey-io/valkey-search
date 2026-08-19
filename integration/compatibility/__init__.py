import hashlib
import os

_COMPAT_DIR = os.path.dirname(os.path.abspath(__file__))


# Registry of compatibility generators. To add a new generator, create the
# generate file (subclassing BaseCompatibilityTest with its own
# ANSWER_FILE_NAME) and add an entry here. regenerate.sh and
# compatibility_test.py both read from this list.
GENERATORS = [
    {"generator": "generate.py",      "answers": "aggregate-answers.pickle.gz",   "cluster": True},
    {"generator": "generate_text.py", "answers": "text-search-answers.pickle.gz", "cluster": False},
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
