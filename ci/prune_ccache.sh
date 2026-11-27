#!/usr/bin/env bash
# Delete the main-branch Actions cache entries for a key prefix, keeping only
# the entry just saved for this commit. Keeps the repo under its 10 GB Actions
# cache quota, which otherwise evicts the ccache entries other jobs rely on.
#
# Usage: ci/prune_ccache.sh <key-prefix>   (key saved is <key-prefix>${GITHUB_SHA})
# Requires GH_TOKEN with actions:write.
set -euo pipefail

prefix="$1"
current="${prefix}${GITHUB_SHA}"

entries=$(gh cache list --repo "${GITHUB_REPOSITORY}" --ref refs/heads/main \
  --key "${prefix}" --limit 100 --json id,key -q '.[] | "\(.id) \(.key)"')

# If this run's save did not land, keep the older entries so the next run
# still has something to restore.
if ! grep -qxF "${current}" <<<"$(cut -d' ' -f2- <<<"${entries}")"; then
  echo "No cache entry '${current}' found; not pruning."
  exit 0
fi

while read -r id key; do
  [[ -z "${id}" || "${key}" == "${current}" ]] && continue
  echo "Deleting superseded cache ${key} (id ${id})"
  gh cache delete "${id}" --repo "${GITHUB_REPOSITORY}"
done <<<"${entries}"
