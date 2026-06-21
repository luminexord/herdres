#!/usr/bin/env bash
# End-to-end test for `herdres update` (edge channel).
#
# Runs a REAL update — real git pull, real backup, real atomic file swap, real
# rollback — against a throwaway sandbox $HOME. Nothing on the live machine is
# touched: every install path derives from $HOME, the service restart is skipped
# via --no-restart (systemctl/launchctl are not HOME-scoped), and NO Telegram bot
# or Herdr is required (verification is version-based).
#
# Usage:    tests/e2e/update_sandbox.sh
# Requires: git, python3, sha256sum
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BR="$(git -C "$REPO" rev-parse --abbrev-ref HEAD)"
NEW="9.9.9-e2e"

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT

ok()   { printf '  OK  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ver()  { HOME="$T" HERDR_BIN=/bin/false "$T/.local/bin/herdres" version \
           | python3 -c 'import sys,json;print(json.load(sys.stdin)["version"])'; }

echo "herdres update e2e (branch $BR, sandbox HOME=$T)"

# 1. A local bare "remote" of the current branch + a working clone for the marker.
git clone -q --bare "$REPO" "$T/remote.git"
git clone -q -b "$BR" "$T/remote.git" "$T/src"
OLD="$(grep -oE 'HERDRES_VERSION = "[^"]+"' "$T/src/herdres.py" | head -1 | sed -E 's/.*"([^"]+)".*/\1/')"

# 2. Push a newer version to the remote so the update has something to pull.
git clone -q -b "$BR" "$T/remote.git" "$T/bump"
sed -i -E "s/HERDRES_VERSION = \"[^\"]+\"/HERDRES_VERSION = \"$NEW\"/" "$T/bump/herdres.py"
git -C "$T/bump" -c user.email=e2e@x -c user.name=e2e commit -aqm "bump to $NEW"
git -C "$T/bump" push -q origin "HEAD:$BR"

# 3. Lay down a fake OLD install + herdres.env with a SENTINEL + the source marker.
mkdir -p "$T/.local/bin" "$T/.local/share/herdres" "$T/.config/herdres"
cp "$T/src/herdres.py" "$T/.local/bin/herdres"; chmod +x "$T/.local/bin/herdres"
printf '%s\n' "TELEGRAM_BOT_TOKEN=SENTINEL_DO_NOT_TOUCH" > "$T/.config/herdres/herdres.env"
echo "$T/src" > "$T/.local/share/herdres/source"
ENV_BEFORE="$(sha256sum "$T/.config/herdres/herdres.env" | cut -d' ' -f1)"
[ "$(ver)" = "$OLD" ] || fail "sandbox did not start on OLD ($OLD)"
ok "sandbox installed at version $OLD"

# 4. THE REAL UPDATE (edge, no restart, isolated herdr).
HOME="$T" HERDR_BIN=/bin/false "$T/.local/bin/herdres" update --edge --no-restart >/dev/null
[ "$(ver)" = "$NEW" ] || fail "binary not updated to $NEW (got $(ver))"
ok "real update applied: $OLD -> $NEW"

# 5. The load-bearing guarantee: config untouched.
[ "$(sha256sum "$T/.config/herdres/herdres.env" | cut -d' ' -f1)" = "$ENV_BEFORE" ] \
  || fail "herdres.env was modified!"
ok "herdres.env preserved (sentinel intact)"
ls -d "$T/.local/share/herdres/backups/"*/ >/dev/null 2>&1 || fail "no backup created"
ok "backup created"

# 6. --check runs (now current vs the remote).
HOME="$T" HERDR_BIN=/bin/false "$T/.local/bin/herdres" update --check >/dev/null && ok "update --check runs"

# 7. Rollback restores OLD.
HOME="$T" HERDR_BIN=/bin/false "$T/.local/bin/herdres" update --rollback --no-restart >/dev/null
[ "$(ver)" = "$OLD" ] || fail "rollback did not restore $OLD (got $(ver))"
ok "rollback restored $OLD"

echo "PASS — real update + env-preservation + backup + rollback verified; nothing live touched."
