#!/bin/bash -e

# Locate the top level folder
ROOT_DIR=$(readlink -f $(dirname $(readlink -f $0))/..)

# Regenerating compile_commands.json is done by running `cmake` (build is not required)
cd ${ROOT_DIR}
${ROOT_DIR}/ci/build_ubuntu.sh --no-build --configure
