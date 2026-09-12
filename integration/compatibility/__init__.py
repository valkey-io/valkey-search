import hashlib
import os

_COMPAT_DIR = os.path.dirname(os.path.abspath(__file__))


# Registry of compatibility generators. To add a new generator, create the
# generate file (subclassing BaseCompatibilityTest with its own
# ANSWER_FILE_NAME) and add an entry here. regenerate.sh and
# compatibility_test.py both read from this list. New generators go in a
# per-command subdirectory (e.g. search/); all paths are relative to this
# directory and ANSWER_FILE_NAME must match the "answers" entry.
GENERATORS = [
    {"generator": "generate.py",      "answers": "aggregate-answers.pickle.gz",   "cluster": True},
    {"generator": "generate_text.py", "answers": "text-search-answers.pickle.gz", "cluster": False},
    {"generator": "generate_array.py", "answers": "array-input-answers.pickle.gz", "cluster": False},
    {"generator": "generate_sortkey.py", "answers": "sortkey-answers.pickle.gz",  "cluster": False},
    {"generator": "search/generate_return.py", "answers": "search/return-answers.pickle.gz", "cluster": False},
]


def compute_sources_hash():
    """SHA256 of every .py file under this directory, recursively.

    Stored inside the generated pickle answer files so compatibility_test.py
    can detect when a pickle is stale relative to the generators and helpers.
    Recursive so generators in per-command subdirectories (e.g. search/) are
    covered by the staleness check.
    """
    h = hashlib.sha256()
    for dirpath, dirnames, filenames in os.walk(_COMPAT_DIR):
        dirnames.sort()
        for fname in sorted(filenames):
            if not fname.endswith(".py"):
                continue
            path = os.path.join(dirpath, fname)
            rel = os.path.relpath(path, _COMPAT_DIR).replace(os.sep, "/")
            h.update(rel.encode("utf-8"))
            h.update(b"\0")
            with open(path, "rb") as f:
                h.update(f.read())
            h.update(b"\0")
    return h.hexdigest()
