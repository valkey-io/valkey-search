#!/bin/bash -e

# Locate the top level folder
ROOT_DIR=$(readlink -f $(dirname $(readlink -f $0))/..)

# Regenerating compile_commands.json is done by running `cmake` (build is not required)
cd ${ROOT_DIR}
${ROOT_DIR}/ci/build_ubuntu.sh --no-build --configure

# Detect host OS workspace directory if running inside container
HOST_DIR=$(awk -v target="${ROOT_DIR}" '$5 == target {print $4}' /proc/self/mountinfo 2>/dev/null || true)
if [ -z "${HOST_DIR}" ]; then
  HOST_DIR="${ROOT_DIR}"
fi

# Ensure compile_commands.json root symlink is relative
ln -sf .build-release/compile_commands.json ${ROOT_DIR}/compile_commands.json

# Map container workspace path (${ROOT_DIR}) to host workspace path (${HOST_DIR}) in compile_commands.json
if [ -f "${ROOT_DIR}/.build-release/compile_commands.json" ]; then
  sed -i "s|${ROOT_DIR}|${HOST_DIR}|g" "${ROOT_DIR}/.build-release/compile_commands.json" 2>/dev/null || true
fi
