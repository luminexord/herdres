# Task 2 — env-safe installers + source marker

**Branch:** `feat/issue13-installers` (off `main`)
**Files:** `install-user.sh`, `install-macos.sh`, `tests/test_installers.py` only.

## Why

Re-running `install-user.sh` today **overwrites `herdres.env`** (line 6: `install -Dm644 .env.example "$HOME/.config/herdres/herdres.env"`), wiping live credentials — which is why the manual update is dangerous. `install-macos.sh` is **already guarded** (line 52: `if [ ! -f "$CFG/herdres.env" ]; then …`); mirror that.

Also: `herdres update --edge` needs to know where the source checkout is. Record it.

## Implement

### `install-user.sh`
1. **Guard the env copy** so it only writes when absent:
   ```sh
   [ -f "$HOME/.config/herdres/herdres.env" ] || \
     install -Dm644 .env.example "$HOME/.config/herdres/herdres.env"
   ```
   (Re-install becomes a safe update; an existing config is preserved.)
2. **Write the source marker** (the absolute path of this checkout) so `update --edge` can find it:
   ```sh
   install -d "$HOME/.local/share/herdres"
   printf '%s\n' "$(cd "$(dirname "$0")" && pwd)" > "$HOME/.local/share/herdres/source"
   ```

### `install-macos.sh`
- Env copy is already guarded — leave it. **Add the same source-marker write** (using the script's own directory; macOS layout puts share at `$SHARE` / `$HOME/.local/share/herdres`).

## Tests — `tests/test_installers.py` (CI-safe, content assertions)

- `install-user.sh` guards the `herdres.env` write behind a `[ -f … ]` / `if [ ! -f … ]` check (no unconditional `install … herdres.env`).
- **Both** installers write the source marker to `~/.local/share/herdres/source` (assert the literal `share/herdres/source` path string appears in each).
- (Optional, robust) run `sh -n install-user.sh` / `bash -n install-macos.sh` to assert they still parse.

## Acceptance
- [ ] Re-running `install-user.sh` with an existing `herdres.env` does **not** overwrite it.
- [ ] Both installers write `~/.local/share/herdres/source` with the checkout path.
- [ ] `tests/test_installers.py` + full `pytest tests/` green; `/code-review` clean.
