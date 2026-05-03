#!/bin/bash -e
# Regenerate the compatibility test pickle answer files
# (aggregate-answers.pickle.gz and text-search-answers.pickle.gz).
#
# Requires Docker: the generators spin up redis/redis-stack-server on port 6380
# to capture reference answers.
#
# Usage:
#   ./integration/compatibility/regenerate.sh [extra pytest args...]
#
# After it finishes, git add and commit the updated *.pickle.gz files.

ROOT_DIR=$(readlink -f "$(dirname "$0")/../..")
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
mapfile -t GENERATOR_FILES < <(PYTHONPATH=integration "${PYTHON}" -c \
    "from compatibility import GENERATORS
for g in GENERATORS: print(g['generator'])")
mapfile -t ANSWER_FILES < <(PYTHONPATH=integration "${PYTHON}" -c \
    "from compatibility import GENERATORS
for g in GENERATORS: print(g['answers'])")

cd "${COMPAT_DIR}"
for gen in "${GENERATOR_FILES[@]}"; do
    echo "==> Running ${gen}"
    "${PYTHON}" -m pytest "${gen}" "$@"
done

echo
echo "Done. Updated files:"
ls -la "${ANSWER_FILES[@]}"
echo
echo "Don't forget to 'git add' and commit them."
