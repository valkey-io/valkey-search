#!/bin/bash -e
# Regenerate the compatibility test pickle answer files
# (aggregate-answers.pickle.gz and text-search-answers.pickle.gz).
#
# Requires Docker: the generators spin up redis:latest on a port
# docker picks, in a container named Generate-search-<Class>-NNNN.
# to capture reference answers.
#
# Usage:
#   ./integration/compatibility/regenerate.sh [extra pytest args...]
#
# After it finishes, git add and commit the updated *.pickle.gz files.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPAT_DIR=${ROOT_DIR}/integration/compatibility

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker is required to regenerate pickle files." >&2
    exit 1
fi

# Prefer the integration test venv (created by integration/run.sh) if present;
# it already has pytest and the valkey client installed.
PYTHON=""
for build_dir in .build-release .build-debug \
                 .build-release-asan .build-debug-asan \
                 .build-release-tsan .build-debug-tsan; do
    candidate="${ROOT_DIR}/${build_dir}/integration/env/bin/python3"
    if [ -x "${candidate}" ]; then
        PYTHON="${candidate}"
        break
    fi
done
PYTHON=${PYTHON:-python3}

echo "Using python: ${PYTHON}"
cd "${ROOT_DIR}"

# Source the generator list from compatibility/__init__.py so adding a new
# generator only requires editing one place.
GENERATOR_FILES=()
while IFS= read -r line; do
    GENERATOR_FILES+=("${line}")
done < <(PYTHONPATH=integration "${PYTHON}" -c \
    "from compatibility import GENERATORS
for g in GENERATORS: print(g['generator'])")

ANSWER_FILES=()
while IFS= read -r line; do
    ANSWER_FILES+=("${line}")
done < <(PYTHONPATH=integration "${PYTHON}" -c \
    "from compatibility import GENERATORS
for g in GENERATORS: print(g['answers'])")

cd "${COMPAT_DIR}"
# Each generator collects its answers in one class and writes them in
# teardown_class, so loadscope keeps a generator on a single worker; one
# worker per generator is the most parallelism that stays correct.
XDIST_ARGS=()
if "${PYTHON}" -c "import xdist" 2>/dev/null; then
    XDIST_ARGS=(-n "${#GENERATOR_FILES[@]}" --dist=loadscope)
else
    echo "pytest-xdist not found in ${PYTHON}; running generators serially." >&2
fi
echo "==> Running ${GENERATOR_FILES[*]}"
"${PYTHON}" -m pytest "${XDIST_ARGS[@]}" "${GENERATOR_FILES[@]}" "$@"

echo
echo "Done. Updated files:"
ls -la "${ANSWER_FILES[@]}"
echo
echo "Don't forget to 'git add' and commit them."
