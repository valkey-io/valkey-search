#!/bin/bash -e
# Print the CI cache key for the ICU libraries that build.sh builds into
# <build-dir>/icu/install/lib. They depend only on the committed ICU sources,
# build_icu_if_needed() in build.sh, and the toolchain.

BUILD_DIR=${1:?usage: icu_cache_key.sh <build-dir>}
cd "$(dirname "$0")/.."

recipe=$(sed -n '/^function build_icu_if_needed()/,/^}/p' build.sh)
if [ -z "${recipe}" ]; then
    echo "build_icu_if_needed() not found in build.sh" >&2
    exit 1
fi

if [ "$(uname -s)" = "Darwin" ]; then
    toolchain=$(cc --version | git hash-object --stdin)
else
    toolchain=$(git rev-parse HEAD:.devcontainer/Dockerfile)
fi
sources=$(git rev-parse HEAD:third_party/icu/source)
recipe=$(printf '%s' "${recipe}" | git hash-object --stdin)

echo "icu-$(uname -s)-$(uname -m)-${BUILD_DIR#.}-${toolchain:0:12}-${sources:0:12}-${recipe:0:12}"
