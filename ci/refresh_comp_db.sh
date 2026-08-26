#!/bin/bash -e

# Locate the top level folder
ROOT_DIR=$(readlink -f $(dirname $(readlink -f $0))/..)

# Regenerating compile_commands.json is done by running `cmake` (build is not required)
cd ${ROOT_DIR}
${ROOT_DIR}/ci/build_ubuntu.sh --skip-prepare-env --no-build --configure

# Create a relative symlink to the newest generated compilation database (.build-debug or .build-release)
latest_db=$(ls -t .build-*/compile_commands.json 2>/dev/null | head -n 1)
if [ -n "$latest_db" ]; then
    # Replace devcontainer paths (/workspaces/...) with ROOT_DIR so IDE indexers on the host can resolve headers and symbols
    sed -i "s|/workspaces/[^/]*|${ROOT_DIR}|g" "$latest_db"
    ln -sf "$latest_db" compile_commands.json
    echo "Updated compile_commands.json symlink and host paths -> $latest_db"
    
    # Restart active clangd language servers so IDEs immediately pick up the updated compilation database
    if command -v pkill >/dev/null 2>&1; then
        pkill -f clangd 2>/dev/null || true
    fi
fi
