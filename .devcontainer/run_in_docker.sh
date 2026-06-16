#!/bin/bash

# Find the workspace root (parent of .devcontainer)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORKSPACE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

# Get the current user's UID and GID to avoid permission mismatch issues
USER_UID=$(id -u)
USER_GID=$(id -g)
USER_NAME=$(id -un)
USER_GNAME=$(id -gn)

# Determine the command to run
CMD="$*"
if [ -z "$CMD" ]; then
  CMD="bash"
fi

# Detect if running in an interactive terminal (TTY)
INTERACTIVE_FLAGS=""
if [ -t 0 ] && [ -t 1 ]; then
  INTERACTIVE_FLAGS="-it"
fi

# Set up SSH Agent socket proxy
PROXY_PID=""
PROXY_SOCKET="$WORKSPACE_DIR/.ssh-agent.sock"
SSH_AUTH_SOCK_CONTAINER=""

if [ -S "$SSH_AUTH_SOCK" ]; then
  # Start the Python socket proxy in the background to forward the SSH agent socket
  python3 "$WORKSPACE_DIR/.devcontainer/socket_proxy.py" "$PROXY_SOCKET" "$SSH_AUTH_SOCK" >/dev/null 2>&1 &
  PROXY_PID=$!
  # Wait briefly to ensure the socket file is created
  sleep 0.1
  # Set the SSH_AUTH_SOCK path to be used inside the container
  SSH_AUTH_SOCK_CONTAINER="/workspaces/valkey-search4/.ssh-agent.sock"
fi

cleanup() {
  if [ -n "$PROXY_PID" ]; then
    kill "$PROXY_PID" 2>/dev/null || true
  fi
  if [ -S "$PROXY_SOCKET" ]; then
    rm -f "$PROXY_SOCKET" 2>/dev/null || true
  fi
}
# Register cleanup trap
trap cleanup EXIT

# Search for a running devcontainer for this workspace
CONTAINER_ID=$(docker ps --filter "label=devcontainer.local_folder=$WORKSPACE_DIR" -q | head -n 1)

if [ -n "$CONTAINER_ID" ]; then
  # Running container found! 
  
  # Copy .gitconfig from host if it exists to ensure git settings are available inside
  if [ -f "$HOME/.gitconfig" ]; then
    docker cp "$HOME/.gitconfig" "$CONTAINER_ID:/home/$USER_NAME/.gitconfig"
    docker exec -u root "$CONTAINER_ID" chown "$USER_UID:$USER_GID" "/home/$USER_NAME/.gitconfig"
  fi

  # Copy .ssh folder from host if it exists to ensure git SSH configs are available inside
  if [ -d "$HOME/.ssh" ]; then
    docker cp "$HOME/.ssh" "$CONTAINER_ID:/home/$USER_NAME/.ssh"
    docker exec -u root "$CONTAINER_ID" chown -R "$USER_UID:$USER_GID" "/home/$USER_NAME/.ssh"
    docker exec -u "$USER_NAME" "$CONTAINER_ID" chmod 700 "/home/$USER_NAME/.ssh"
    docker exec -u "$USER_NAME" "$CONTAINER_ID" find "/home/$USER_NAME/.ssh" -type f -exec chmod 600 {} + 2>/dev/null || true
  fi
  
  # Execute inside the container
  ENV_FLAGS=("-e" "TERM=$TERM")
  if [ -n "$SSH_AUTH_SOCK_CONTAINER" ]; then
    ENV_FLAGS+=("-e" "SSH_AUTH_SOCK=$SSH_AUTH_SOCK_CONTAINER")
  fi

  if [ "$CMD" = "bash" ]; then
    docker exec $INTERACTIVE_FLAGS "${ENV_FLAGS[@]}" -u "$USER_NAME" -w "/workspaces/valkey-search4" "$CONTAINER_ID" bash
  else
    docker exec $INTERACTIVE_FLAGS "${ENV_FLAGS[@]}" -u "$USER_NAME" -w "/workspaces/valkey-search4" "$CONTAINER_ID" bash -c "$CMD"
  fi
else
  # Fallback: Build and run a new container
  IMAGE_NAME="valkey-search-dev"
  
  # Build/update the docker image using the Dockerfile from .devcontainer
  docker build -q -t "$IMAGE_NAME" \
    --build-arg USER_UID="$USER_UID" \
    --build-arg USER_NAME="$USER_NAME" \
    --build-arg USER_GID="$USER_GID" \
    --build-arg USER_GNAME="$USER_GNAME" \
    -f "$WORKSPACE_DIR/.devcontainer/Dockerfile" \
    "$WORKSPACE_DIR/.devcontainer"

  # Mount .gitconfig if it exists on the host
  GITCONFIG_MOUNT=""
  if [ -f "$HOME/.gitconfig" ]; then
    GITCONFIG_MOUNT="-v $HOME/.gitconfig:/home/$USER_NAME/.gitconfig:ro"
  fi

  # Mount .ssh if it exists on the host
  SSH_MOUNT=""
  if [ -d "$HOME/.ssh" ]; then
    SSH_MOUNT="-v $HOME/.ssh:/home/$USER_NAME/.ssh:ro"
  fi

  ENV_FLAGS=("-e" "TERM=$TERM")
  if [ -n "$SSH_AUTH_SOCK_CONTAINER" ]; then
    ENV_FLAGS+=("-e" "SSH_AUTH_SOCK=$SSH_AUTH_SOCK_CONTAINER")
  fi

  if [ "$CMD" = "bash" ]; then
    docker run $INTERACTIVE_FLAGS --rm \
      -v "$WORKSPACE_DIR":"/workspaces/valkey-search4" \
      -w "/workspaces/valkey-search4" \
      -u "$USER_UID:$USER_GID" \
      $GITCONFIG_MOUNT \
      $SSH_MOUNT \
      "${ENV_FLAGS[@]}" \
      "$IMAGE_NAME" \
      bash
  else
    docker run $INTERACTIVE_FLAGS --rm \
      -v "$WORKSPACE_DIR":"/workspaces/valkey-search4" \
      -w "/workspaces/valkey-search4" \
      -u "$USER_UID:$USER_GID" \
      $GITCONFIG_MOUNT \
      $SSH_MOUNT \
      "${ENV_FLAGS[@]}" \
      "$IMAGE_NAME" \
      bash -c "$CMD"
  fi
fi
