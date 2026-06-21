#!/usr/bin/env bash
# Runs INSIDE the container (source files at /work). Exercises the FULL
# `herdres update` INCLUDING the real service restart — the step the host
# sandbox test (update_sandbox.sh) skips with --no-restart.
#
# A process-backed `systemctl` shim stands in for systemd: each "unit" is a real
# background process tracked by a pidfile, so disable/enable/restart and the
# gateway is-active health check are genuinely exercised (deterministic, no
# privileges, no real systemd). Two cases: a healthy restart, and a failed
# gateway re-enable that must roll back.
set -euo pipefail

SRC=/work
NEW="9.9.9-e2e"
T="$(mktemp -d)"; export HOME="$T"
SENTINEL="TELEGRAM_BOT_TOKEN=SENTINEL_DO_NOT_TOUCH"

ok()   { printf '  OK  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ver()  { HOME="$T" HERDR_BIN=/bin/false "$T/.local/bin/herdres" version \
           | python3 -c 'import sys,json;print(json.load(sys.stdin)["version"])'; }
envsum() { sha256sum "$T/.config/herdres/herdres.env" | cut -d' ' -f1; }

# --- process-backed systemctl shim on PATH ---
SBIN="$T/sbin"; export SYSTEMCTL_SHIM_RUN="$T/run"; mkdir -p "$SBIN" "$SYSTEMCTL_SHIM_RUN"
cat > "$SBIN/systemctl" <<'SH'
#!/usr/bin/env bash
RUN="${SYSTEMCTL_SHIM_RUN:?}"; FAIL="${SYSTEMCTL_SHIM_FAIL_ENABLE:-}"
a=(); for x in "$@"; do case "$x" in --user|--now) ;; *) a+=("$x");; esac; done
cmd="${a[0]:-}"; unit="${a[1]:-}"; pf="$RUN/$unit.pid"
alive(){ [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; }
start(){ if [ -n "$FAIL" ] && [ "$unit" = "$FAIL" ]; then exit 1; fi
         setsid sleep 1000000 >/dev/null 2>&1 & echo $! > "$pf"; }
stop(){ if alive; then kill "$(cat "$pf")" 2>/dev/null || true; fi; rm -f "$pf"; }
case "$cmd" in
  daemon-reload) : ;;
  is-active)  if alive; then echo active; exit 0; else echo inactive; exit 3; fi ;;
  is-enabled) echo enabled ;;
  enable|start)  start ;;
  disable|stop)  stop ;;
  restart)       stop; start ;;
  *) : ;;
esac
SH
chmod +x "$SBIN/systemctl"; export PATH="$SBIN:$PATH"

OLD="$(grep -oE 'HERDRES_VERSION = "[^"]+"' "$SRC/herdres.py" | head -1 | sed -E 's/.*"([^"]+)".*/\1/')"

install_old() {
  rm -rf "$T/seed" "$T/remote.git" "$T/src" "$T/bump" "$T/.local" "$T/.config"
  mkdir -p "$T/seed"; cp -a "$SRC/." "$T/seed/"; rm -rf "$T/seed/.git" "$T/seed/.claude"
  ( cd "$T/seed" && git init -q -b main && git config user.email e2e@x && git config user.name e2e \
      && git add -A && git commit -qm OLD )
  git clone -q --bare "$T/seed" "$T/remote.git"
  git clone -q "$T/remote.git" "$T/src"          # tracks origin/main
  git clone -q "$T/remote.git" "$T/bump"
  sed -i -E "s/HERDRES_VERSION = \"[^\"]+\"/HERDRES_VERSION = \"$NEW\"/" "$T/bump/herdres.py"
  ( cd "$T/bump" && git -c user.email=e2e@x -c user.name=e2e commit -aqm NEW && git push -q )
  mkdir -p "$T/.local/bin" "$T/.local/share/herdres" "$T/.config/herdres"
  cp "$T/src/herdres.py" "$T/.local/bin/herdres"; chmod +x "$T/.local/bin/herdres"
  printf '%s\n' "$SENTINEL" > "$T/.config/herdres/herdres.env"
  echo "$T/src" > "$T/.local/share/herdres/source"
}

echo "herdres update CONTAINER e2e (real restart via process-backed shim), HOME=$T"

# === Case 1: healthy update — the gateway is really stopped and re-started ===
install_old
systemctl --user enable --now herdres-gateway.service
systemctl --user enable --now herdres.timer
GW_BEFORE="$(cat "$T/run/herdres-gateway.service.pid")"
ENV_BEFORE="$(envsum)"
HOME="$T" HERDR_BIN=/bin/false "$T/.local/bin/herdres" update --edge >/dev/null
[ "$(ver)" = "$NEW" ] || fail "version not bumped (got $(ver))"
ok "real update applied WITH restart: $OLD -> $NEW"
[ "$(envsum)" = "$ENV_BEFORE" ] || fail "herdres.env was modified!"
ok "herdres.env preserved"
GW_AFTER="$(cat "$T/run/herdres-gateway.service.pid")"
[ "$GW_BEFORE" != "$GW_AFTER" ] || fail "gateway not actually restarted (same pid $GW_BEFORE)"
systemctl --user is-active herdres-gateway.service >/dev/null || fail "gateway not active after update"
ok "gateway really restarted (pid $GW_BEFORE -> $GW_AFTER) and is active"

# === Case 2: a failed gateway re-enable must be detected and ROLLED BACK ===
install_old
systemctl --user enable --now herdres-gateway.service     # active before
set +e
SYSTEMCTL_SHIM_FAIL_ENABLE=herdres-gateway.service HOME="$T" HERDR_BIN=/bin/false \
  "$T/.local/bin/herdres" update --edge >/dev/null 2>&1
rc=$?
set -e
[ "$rc" -ne 0 ] || fail "update should FAIL when the gateway cannot re-enable"
[ "$(ver)" = "$OLD" ] || fail "files should have ROLLED BACK to $OLD after gateway failure (got $(ver))"
ok "failed gateway re-enable detected -> files rolled back to $OLD (exit $rc)"

echo "PASS — full update incl. real service restart, env-preservation, and gateway-failure rollback."
