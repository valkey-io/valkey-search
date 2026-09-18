#!/usr/bin/env bash
# redisearch-docker.sh — run RediSearch (redis-stack-server) in a throwaway
# Docker container as the parity reference for valkey-search work.
#
#   redisearch-docker.sh up                 # start container, wait for readiness
#   redisearch-docker.sh cli [ARGS...]      # redis-cli against it (interactive if no args)
#   redisearch-docker.sh down               # stop and remove the container
#   redisearch-docker.sh status             # is it up? what version?
#
# Env:
#   RS_IMAGE   image to run (default: redis/redis-stack-server:latest)
#   RS_NAME    container name  (default: redisearch-compat)
#   RS_PORT    host port       (default: 6399, chosen to avoid a local 6379)
set -euo pipefail

RS_IMAGE="${RS_IMAGE:-redis/redis-stack-server:latest}"
RS_NAME="${RS_NAME:-redisearch-compat}"
RS_PORT="${RS_PORT:-6399}"

die() { echo "error: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker not found on PATH"

cmd="${1:-}"; shift || true

case "$cmd" in
  up)
    if docker ps --format '{{.Names}}' | grep -qx "$RS_NAME"; then
      echo "$RS_NAME already running on port $RS_PORT"
      exit 0
    fi
    # Remove a stopped container of the same name, if any.
    docker rm -f "$RS_NAME" >/dev/null 2>&1 || true
    echo "starting $RS_IMAGE as $RS_NAME on host port $RS_PORT ..."
    docker run -d --name "$RS_NAME" -p "${RS_PORT}:6379" "$RS_IMAGE" >/dev/null
    # Wait for readiness (redis-stack takes a moment to load the module).
    for i in $(seq 1 30); do
      if docker exec "$RS_NAME" redis-cli PING 2>/dev/null | grep -qx PONG; then
        # Confirm the search module is actually loaded.
        if docker exec "$RS_NAME" redis-cli FT._LIST >/dev/null 2>&1; then
          echo "ready. RediSearch module version:"
          docker exec "$RS_NAME" redis-cli MODULE LIST 2>/dev/null || true
          echo "connect: $0 cli"
          exit 0
        fi
      fi
      sleep 1
    done
    die "container did not become ready in 30s (check: docker logs $RS_NAME)"
    ;;

  cli)
    docker ps --format '{{.Names}}' | grep -qx "$RS_NAME" \
      || die "$RS_NAME is not running — start it with: $0 up"
    if [ "$#" -eq 0 ]; then
      # Interactive shell.
      docker exec -it "$RS_NAME" redis-cli
    else
      docker exec "$RS_NAME" redis-cli "$@"
    fi
    ;;

  down)
    docker rm -f "$RS_NAME" >/dev/null 2>&1 && echo "removed $RS_NAME" || echo "$RS_NAME not present"
    ;;

  status)
    if docker ps --format '{{.Names}}' | grep -qx "$RS_NAME"; then
      echo "$RS_NAME is UP on host port $RS_PORT"
      docker exec "$RS_NAME" redis-cli MODULE LIST 2>/dev/null || true
    else
      echo "$RS_NAME is not running"
    fi
    ;;

  *)
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
