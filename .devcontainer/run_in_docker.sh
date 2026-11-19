#!/bin/bash

# Find the workspace root (parent of .devcontainer). The container mounts it at
# the same absolute path as on the host, so paths recorded by the build (e.g.
# compile_commands.json) are valid for host tools such as clangd.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORKSPACE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

# Get the current user's UID and GID to avoid permission mismatch issues
USER_UID=$(id -u)
USER_GID=$(id -g)
USER_NAME=$(id -un)
USER_GNAME=$(id -gn)

# Determine the command to run
COMMAND=("$@")
if [ ${#COMMAND[@]} -eq 0 ]; then
  COMMAND=("bash")
fi

# Detect if running in an interactive terminal (TTY)
INTERACTIVE_FLAGS=""
if [ -t 0 ] && [ -t 1 ]; then
  INTERACTIVE_FLAGS="-it"
fi

CREATED_CONTAINER=false

cleanup() {
  if [ "$CREATED_CONTAINER" = "true" ] && [ -n "$CONTAINER_ID" ]; then
    docker rm -f "$CONTAINER_ID" >/dev/null 2>&1 || true
  fi
}
# Register cleanup trap
trap cleanup EXIT

# Search for a running devcontainer for this workspace
CONTAINER_ID=$(docker ps --filter "label=devcontainer.local_folder=$WORKSPACE_DIR" -q | head -n 1)

if [ -n "$CONTAINER_ID" ] && docker exec "$CONTAINER_ID" test -d "$WORKSPACE_DIR" 2>/dev/null; then
  # Running container found! 
  
  # Copy .gitconfig from host if it exists to ensure git settings are available inside
  if [ -f "$HOME/.gitconfig" ]; then
    docker cp "$HOME/.gitconfig" "$CONTAINER_ID:/home/$USER_NAME/.gitconfig"
    docker exec -u root "$CONTAINER_ID" chown "$USER_UID:$USER_GID" "/home/$USER_NAME/.gitconfig"
  fi

  # Copy non-secret SSH configuration from host if it exists (never private keys)
  if [ -d "$HOME/.ssh" ]; then
    docker exec -u "$USER_NAME" "$CONTAINER_ID" mkdir -p "/home/$USER_NAME/.ssh" 2>/dev/null || true
    for item in config known_hosts known_hosts2; do
      if [ -f "$HOME/.ssh/$item" ]; then
        docker cp "$HOME/.ssh/$item" "$CONTAINER_ID:/home/$USER_NAME/.ssh/$item"
        docker exec -u root "$CONTAINER_ID" chown "$USER_UID:$USER_GID" "/home/$USER_NAME/.ssh/$item"
        docker exec -u "$USER_NAME" "$CONTAINER_ID" chmod 600 "/home/$USER_NAME/.ssh/$item"
      fi
    done
    docker exec -u "$USER_NAME" "$CONTAINER_ID" chmod 700 "/home/$USER_NAME/.ssh" 2>/dev/null || true
  fi
  
  # Execute inside the container
  ENV_FLAGS=("-e" "TERM=$TERM" "-e" "BUILD_DIR_SUFFIX=-container")

  docker exec $INTERACTIVE_FLAGS "${ENV_FLAGS[@]}" -u "$USER_NAME" -w "$WORKSPACE_DIR" "$CONTAINER_ID" "${COMMAND[@]}"
else
  # Fallback: Build and run a new container
  IMAGE_NAME="valkey-search-dev"
  
  # Build/update the docker image using the Dockerfile from .devcontainer
  if ! docker build -q -t "$IMAGE_NAME" \
    --build-arg USER_UID="$USER_UID" \
    --build-arg USER_NAME="$USER_NAME" \
    --build-arg USER_GID="$USER_GID" \
    --build-arg USER_GNAME="$USER_GNAME" \
    -f "$WORKSPACE_DIR/.devcontainer/Dockerfile" \
    "$WORKSPACE_DIR/.devcontainer" >/dev/null; then
    echo "Error: Failed to build Docker image '$IMAGE_NAME'." >&2
    exit 1
  fi

  # Mount .gitconfig if it exists on the host
  GITCONFIG_MOUNT=()
  if [ -f "$HOME/.gitconfig" ]; then
    GITCONFIG_MOUNT=("-v" "$HOME/.gitconfig:/home/$USER_NAME/.gitconfig:ro")
  fi

  # Mount non-secret SSH config files if they exist on the host (never private keys)
  SSH_MOUNTS=()
  if [ -f "$HOME/.ssh/config" ]; then
    SSH_MOUNTS+=("-v" "$HOME/.ssh/config:/home/$USER_NAME/.ssh/config:ro")
  fi
  if [ -f "$HOME/.ssh/known_hosts" ]; then
    SSH_MOUNTS+=("-v" "$HOME/.ssh/known_hosts:/home/$USER_NAME/.ssh/known_hosts:ro")
  fi

  # Mount .ccache if it exists on the host, or create it
  CCACHE_MOUNT=()
  if [ ! -d "$HOME/.ccache-valkey-devcontainer" ]; then
    mkdir -p "$HOME/.ccache-valkey-devcontainer"
  fi
  CCACHE_MOUNT=("-v" "$HOME/.ccache-valkey-devcontainer:/home/$USER_NAME/.ccache")

  ENV_FLAGS=("-e" "TERM=$TERM" "-e" "BUILD_DIR_SUFFIX=-container")
  CREATED_CONTAINER=true
  CONTAINER_ID="valkey-search-runner-$$"

  docker run $INTERACTIVE_FLAGS \
    --name "$CONTAINER_ID" \
    -v "$WORKSPACE_DIR":"$WORKSPACE_DIR" \
    -w "$WORKSPACE_DIR" \
    -u "$USER_UID:$USER_GID" \
    "${GITCONFIG_MOUNT[@]}" \
    "${SSH_MOUNTS[@]}" \
    "${CCACHE_MOUNT[@]}" \
    --network host \
    --security-opt seccomp=unconfined \
    --cap-add SYS_PTRACE \
    "${ENV_FLAGS[@]}" \
    "$IMAGE_NAME" \
    "${COMMAND[@]}"
fi
