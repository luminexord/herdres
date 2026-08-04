#!/usr/bin/env sh
set -eu

install -Dm755 herdres.py "$HOME/.local/bin/herdres"
install -Dm755 herdres_gateway.py "$HOME/.local/bin/herdres-gateway"
find herdres_connector -type f -name '*.py' | while IFS= read -r f; do
    install -Dm644 "$f" "$HOME/.local/bin/$f"
done

# Migrate only the unit shape that invoked the retired transcript adapter. Preserve custom units,
# back up before replacement, and complete this before removing the old executable below.
TENDWIRED_UNIT="$HOME/.config/systemd/user/tendwired.service"
if [ -f "$TENDWIRED_UNIT" ] && grep -Eq '^Environment=TENDWIRE_HERDR_BIN=.*/herdr_turn_adapter\.py$' "$TENDWIRED_UNIT"; then
    command -v python3 >/dev/null 2>&1 || {
        printf '%s\n' "Cannot migrate tendwired.service without python3; legacy adapter retained." >&2
        exit 1
    }
    cp -a "$TENDWIRED_UNIT" "$TENDWIRED_UNIT.bak-herdres-acp"
    python3 - "$TENDWIRED_UNIT" <<'UNITMIGRATE'
import os
import re
import stat
import sys
import tempfile

path = sys.argv[1]
adapter = re.compile(r"^Environment=TENDWIRE_HERDR_BIN=.*/herdr_turn_adapter\.py$")
real_bin = re.compile(r"^Environment=HERDR_REAL_BIN=.*$")
backend = re.compile(r"^Environment=TENDWIRE_HERDR_BACKEND=.*$")
with open(path, encoding="utf-8") as source:
    lines = source.read().splitlines(keepends=True)
has_backend = any(
    backend.fullmatch(line.rstrip("\r\n"))
    for line in lines
)
rewritten = []
replaced = False
backend_written = False
for line in lines:
    body = line.rstrip("\r\n")
    ending = line[len(body):]
    if adapter.fullmatch(body):
        if not has_backend:
            rewritten.append("Environment=TENDWIRE_HERDR_BACKEND=socket\n")
            backend_written = True
        rewritten.append("Environment=TENDWIRE_HERDR_BIN=%h/.local/bin/herdr" + (ending or "\n"))
        replaced = True
    elif backend.fullmatch(body):
        if not backend_written:
            rewritten.append("Environment=TENDWIRE_HERDR_BACKEND=socket" + (ending or "\n"))
            backend_written = True
    elif not real_bin.fullmatch(body):
        rewritten.append(line)
if not replaced:
    raise SystemExit("legacy adapter line disappeared during tendwired.service migration")
metadata = os.stat(path, follow_symlinks=False)
directory = os.path.dirname(path)
descriptor, temporary = tempfile.mkstemp(prefix=".tendwired.service.", dir=directory)
try:
    with os.fdopen(descriptor, "w", encoding="utf-8") as target:
        target.writelines(rewritten)
        target.flush()
        os.fsync(target.fileno())
    os.chmod(temporary, stat.S_IMODE(metadata.st_mode))
    os.replace(temporary, path)
finally:
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass
print("Migrated tendwired.service from the transcript adapter to raw Herdr.")
UNITMIGRATE
fi
if [ -f "$TENDWIRED_UNIT" ] && grep -q 'herdr_turn_adapter\.py' "$TENDWIRED_UNIT"; then
    printf '%s\n' "Refusing to remove the legacy adapter: migrate custom tendwired.service first." >&2
    exit 1
fi

rm -f \
    "$HOME/.local/bin/herdres_tendwire.py" \
    "$HOME/.local/bin/herdres_routing.py" \
    "$HOME/.local/bin/herdres_gateway.py" \
    "$HOME/.local/bin/herdres-decision-hook" \
    "$HOME/.local/bin/herdres-pending-hook" \
    "$HOME/.local/bin/herdr_turn_adapter.py" \
    "$HOME/.local/bin/herdres-speech" \
    "$HOME/.local/bin/herdr_telegram_topics_install_bridge.py" \
    "$HOME/.local/bin/herdres_connector/formatter.py" \
    "$HOME/.local/bin/herdres_connector/source_state.py"

# Remove registrations for retired Herdres hooks from the real Claude settings. A dangling hook can
# block prompts, so this cleanup is guarded, backed up, and best-effort.
CLAUDE_SETTINGS="$HOME/.claude/settings.json"
if [ -f "$CLAUDE_SETTINGS" ] && grep -Eq "herdres-(decision|pending)-hook" "$CLAUDE_SETTINGS" 2>/dev/null && command -v python3 >/dev/null 2>&1; then
    cp -a "$CLAUDE_SETTINGS" "$CLAUDE_SETTINGS.bak-herdres-uninstall" 2>/dev/null || true
    python3 - "$CLAUDE_SETTINGS" <<'PY' || true
import json, sys
p = sys.argv[1]
try:
    d = json.load(open(p))
except Exception:
    sys.exit(0)
hooks = d.get("hooks")
if not isinstance(hooks, dict):
    sys.exit(0)
for event, groups in list(hooks.items()):
    if not isinstance(groups, list):
        continue
    new_groups = []
    for g in groups:
        inner = g.get("hooks", []) if isinstance(g, dict) else []
        kept = [
            h for h in inner
            if isinstance(h, dict)
            and not any(
                name in str(h.get("command", ""))
                for name in ("herdres-decision-hook", "herdres-pending-hook")
            )
        ]
        if kept:
            g["hooks"] = kept
            new_groups.append(g)
    if new_groups:
        hooks[event] = new_groups
    else:
        del hooks[event]
json.dump(d, open(p, "w"), indent=2)
PY
    printf '%s\n' "Removed stale Herdres hook entries from ~/.claude/settings.json."
fi

HERDRES_ENV_PATH="$HOME/.config/herdres/herdres.env"
[ -f "$HERDRES_ENV_PATH" ] || install -Dm600 .env.example "$HERDRES_ENV_PATH"

# Request IDs must survive connector restarts and Telegram redelivery without
# depending on a bot token. Install the dedicated raw key once, fully written
# before its final pathname becomes visible, and never repair or replace an
# existing identity.
REQUEST_ID_KEY_PATH="${HERDRES_REQUEST_ID_KEY_PATH:-$HOME/.local/share/herdres/request-id.key}"
if ! command -v python3 >/dev/null 2>&1; then
    printf '%s\n' "python3 is required to initialize the Herdres request identity key." >&2
    exit 1
fi
python3 - "$REQUEST_ID_KEY_PATH" <<'KEYPY'
import os
import secrets
import stat
import sys
import tempfile

KEY_BYTES = 32


def fail(message):
    raise SystemExit(f"Cannot initialize Herdres request identity key: {message}")


requested_path = sys.argv[1]
path = os.path.expanduser(requested_path)
if not requested_path or not os.path.isabs(path):
    fail("HERDRES_REQUEST_ID_KEY_PATH must expand to a nonempty absolute path")
parent = os.path.dirname(path)


def safe_key_metadata(metadata):
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and metadata.st_size == KEY_BYTES
    )


def validate_existing():
    try:
        before = os.lstat(path)
    except OSError as exc:
        fail(str(exc))
    if not safe_key_metadata(before):
        fail("existing path must be an owner-owned regular file with mode 0600")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            value = os.read(descriptor, KEY_BYTES + 1)
            after = os.lstat(path)
        finally:
            os.close(descriptor)
    except OSError as exc:
        fail(str(exc))
    if (
        not safe_key_metadata(opened)
        or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        or (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
        or not safe_key_metadata(after)
        or len(value) != KEY_BYTES
    ):
        fail("existing key is malformed or was replaced while being checked")


os.makedirs(parent, mode=0o700, exist_ok=True)
parent_metadata = os.lstat(parent)
if not stat.S_ISDIR(parent_metadata.st_mode) or parent_metadata.st_uid != os.geteuid():
    fail("parent must be an owner-owned directory, not a symlink")
os.chmod(parent, 0o700, follow_symlinks=False)

try:
    os.lstat(path)
except FileNotFoundError:
    old_umask = os.umask(0o077)
    temporary = ""
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".request-id.key.", dir=parent)
        try:
            os.fchmod(descriptor, 0o600)
            value = secrets.token_bytes(KEY_BYTES)
            offset = 0
            while offset < len(value):
                offset += os.write(descriptor, value[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            pass
        directory_descriptor = os.open(
            parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        os.umask(old_umask)
        if temporary:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
else:
    pass

validate_existing()
KEYPY

mkdir -p "$HOME/.config/systemd/user" "$HOME/.local/share/herdres"
cp systemd/user/herdres.service systemd/user/herdres-gateway.service "$HOME/.config/systemd/user/"
rm -f "$HOME/.config/systemd/user/herdres.timer"
rm -f "$HOME/.config/systemd/user/herdres-speech.service"
rm -rf "$HOME/.config/systemd/user/herdres-gateway.service.d"
printf '%s\n' "$PWD" > "$HOME/.local/share/herdres/source"

printf '%s\n' "Installed source-only Herdres."
printf '%s\n' "Edit $HOME/.config/herdres/herdres.env, then run:"
printf '%s\n' "  systemctl --user daemon-reload"
printf '%s\n' "  systemctl --user enable --now herdres.service herdres-gateway.service"
