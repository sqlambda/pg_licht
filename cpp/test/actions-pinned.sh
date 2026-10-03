#!/usr/bin/env bash
#
# Fails if any GitHub Actions workflow uses an action by a moving reference
# (a tag or a branch) instead of a full commit SHA. release.yml publishes with
# write permissions, and a re-pointed tag would run there; the pins are kept
# current by .github/dependabot.yml. A local action (uses: ./...) needs none.
#
# Usage: actions-pinned.sh <repository root>
set -euo pipefail
root=${1:-.}
bad=$(grep -nE '^[[:space:]]*-?[[:space:]]*uses:[[:space:]]*[^./[:space:]]' "$root"/.github/workflows/*.yml \
        | grep -vE 'uses:[[:space:]]*[^@[:space:]]+@[0-9a-f]{40}([[:space:]]|$)' || true)
if [ -n "$bad" ]; then
  echo "actions not pinned to a full commit SHA:" >&2
  echo "$bad" >&2
  exit 1
fi
echo "every action in $root/.github/workflows is pinned to a commit SHA"
