#!/usr/bin/env bash
# redisearch-docker.sh — run RediSearch (redis-stack-server) in a throwaway
# Docker container as the parity reference for valkey-search work.
#
#   redisearch-docker.sh up                 # start container, wait for readiness
#   redisearch-docker.sh cli [ARGS...]      # redis-cli against it (interactive if no args)
#   redisearch-docker.sh down               # stop and remove the container
#   redisearch-docker.sh status             # is it up? what image/version?
#
# Env:
#   RS_IMAGE   image to run (default: redis/redis-stack-server:latest).
#              For a recorded parity claim, set this to a fixed tag or digest
#              (e.g. redis/redis-stack-server:7.4.0-v1) so the run is repeatable;
#              `latest` is a mutable tag and can resolve to a different build later.
#   RS_NAME    container name  (default: redisearch-compat)
#   RS_PORT    host port       (default: 6399, chosen to avoid a local 6379)
set -euo pipefail

RS_IMAGE="${RS_IMAGE:-redis/redis-stack-server:latest}"
RS_NAME="${RS_NAME:-redisearch-compat}"
RS_PORT="${RS_PORT:-6399}"

die() { echo "error: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker not found on PATH"

# Actual host port the running container publishes for 6379/tcp (empty if none).
running_port() {
  docker inspect -f '{{ (index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort }}' \
    "$RS_NAME" 2>/dev/null || true
}

# Resolved image reference of the running container (repo digest if available,
# else the configured image), so a run is self-documenting even under `latest`.
running_image() {
  local digest
  digest=$(docker inspect -f '{{ range .RepoDigests }}{{ . }}{{ break }}{{ end }}' \
    "$RS_NAME" 2>/dev/null || true)
  if [ -n "$digest" ]; then echo "$digest"; return; fi
  docker inspect -f '{{ .Config.Image }}' "$RS_NAME" 2>/dev/null || true
}

cmd="${1:-}"; shift || true

case "$cmd" in
  up)
    if docker ps --format '{{.Names}}' | grep -qx "$RS_NAME"; then
      # A container of this name is already up. Report its ACTUAL port/image
      # rather than the requested env vars, and warn if they diverge — the
      # early exit does not recreate the container, so a changed RS_PORT/RS_IMAGE
      # would otherwise be silently ignored.
      actual_port="$(running_port)"
      actual_image="$(running_image)"
      # Compare the container's CONFIGURED image reference (.Config.Image) against
      # the requested RS_IMAGE. running_image() may resolve to a digest while
      # RS_IMAGE is a tag, so use the configured reference for the equality check.
      actual_configured_image="$(docker inspect -f '{{ .Config.Image }}' "$RS_NAME" 2>/dev/null || true)"
      echo "$RS_NAME already running (host port ${actual_port:-unknown}, image ${actual_image:-unknown})"
      if [ -n "$actual_port" ] && [ "$actual_port" != "$RS_PORT" ]; then
        echo "warning: requested RS_PORT=$RS_PORT but container is on $actual_port; run '$0 down' first to change it" >&2
      fi
      if [ -n "$actual_configured_image" ] && [ "$actual_configured_image" != "$RS_IMAGE" ]; then
        echo "warning: requested RS_IMAGE=$RS_IMAGE but container uses $actual_configured_image; run '$0 down' first to change it" >&2
      fi
      echo "connect: $0 cli"
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
          echo "ready. image: $(running_image)"
          echo "RediSearch module version:"
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
      # No args: interactive shell when stdin is a terminal, otherwise read a
      # piped script from stdin. A TTY (-t) with piped stdin fails with
      # "the input device is not a TTY", so only allocate one when interactive.
      if [ -t 0 ]; then
        docker exec -it "$RS_NAME" redis-cli
      else
        docker exec -i "$RS_NAME" redis-cli
      fi
    else
      docker exec -i "$RS_NAME" redis-cli "$@"
    fi
    ;;

  down)
    docker rm -f "$RS_NAME" >/dev/null 2>&1 && echo "removed $RS_NAME" || echo "$RS_NAME not present"
    ;;

  status)
    if docker ps --format '{{.Names}}' | grep -qx "$RS_NAME"; then
      echo "$RS_NAME is UP (host port $(running_port), image $(running_image))"
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
