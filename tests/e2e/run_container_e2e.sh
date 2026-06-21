#!/usr/bin/env bash
# Host wrapper: build a clean container from the current branch and run the
# full-restart e2e inside it (real service restart via a process-backed shim).
# Nothing on the host is touched. Requires docker.
#
# Usage:  tests/e2e/run_container_e2e.sh
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BR="$(git -C "$REPO" rev-parse --abbrev-ref HEAD)"

command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }

echo "building image from branch $BR ..."
git -C "$REPO" archive "$BR" | docker build -q -t herdres-update-e2e -f tests/e2e/Dockerfile -
echo "running container e2e ..."
docker run --rm herdres-update-e2e
