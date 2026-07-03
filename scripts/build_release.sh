#!/usr/bin/env bash
# Build the herdres release assets: a self-contained install-set tarball plus its
# SHA-256 checksum. This is the producer half of the Phase-3 release contract that
# the `herdres update --channel stable` fetcher consumes:
#
#   herdres-<tag>.tar.gz   - gzip tarball whose members are the install set, rooted
#                            under a single "herdres-<tag>/" prefix so an extraction
#                            yields a directory that `_apply_install_set(repo)` can
#                            treat as a source checkout.
#   herdres-<tag>.tar.gz.sha256 - "<hexdigest>  herdres-<tag>.tar.gz" (two-space,
#                            the `sha256sum -c` format) so the fetcher can verify
#                            the download before applying it.
#
# Usage:
#   scripts/build_release.sh <tag> [out_dir]
#
# <tag> is the release tag (e.g. "v0.2.0"); the leading "v" is kept in the asset
# names so they line up with the git tag and the GitHub release. out_dir defaults
# to "dist/" under the repo root. The script must be run from (or pointed at) a
# herdres checkout root; it packages exactly the files the installed update engine
# replaces, never herdres.env.
set -eu

tag="${1:-}"
if [ -z "$tag" ]; then
    printf '%s\n' "usage: build_release.sh <tag> [out_dir]" >&2
    exit 2
fi

# Resolve the repo root from this script's location so it works regardless of cwd.
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

out_dir="${2:-$repo_root/dist}"
mkdir -p "$out_dir"

name="herdres-$tag"
tarball="$out_dir/$name.tar.gz"
checksum="$out_dir/$name.tar.gz.sha256"

# The install set the update engine applies (mirrors herdres.py:_update_files_plan),
# plus the installer scripts + .env.example so the extracted tree is a complete,
# self-installable checkout. herdres.env is deliberately NOT here: it is per-host
# config that the updater must never overwrite.
files="
herdres.py
herdres_gateway.py
herdres_routing.py
herdres_tendwire.py
herdres_connector/__init__.py
herdres_connector/doctor.py
herdres_connector/formatter.py
herdres_connector/source_state.py
herdres_connector/telegram_delivery.py
herdres_connector/tendwire_client.py
herdres_speech.py
herdres-speech
herdres_decision_hook.py
herdr_turn_adapter.py
herdr_topic_bridge.py
herdres-plugin/herdr-plugin.toml
systemd/user/herdres.service
systemd/user/herdres.timer
systemd/user/herdres-gateway.service
systemd/user/herdres-speech.service
install-user.sh
install-macos.sh
.env.example
"

# Fail closed if anything the engine needs is missing, so a broken tag can never
# ship a tarball the fetcher would then fail to apply.
missing=""
for f in $files; do
    if [ ! -f "$repo_root/$f" ]; then
        missing="$missing $f"
    fi
done
if [ -n "$missing" ]; then
    printf '%s\n' "build_release: missing install-set files:$missing" >&2
    exit 1
fi

# Stage into a "<name>/" dir so every tar member is rooted under one prefix; the
# fetcher extracts and hands that single directory to the apply step as the repo.
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
pkg="$stage/$name"
for f in $files; do
    mkdir -p "$pkg/$(dirname "$f")"
    cp "$repo_root/$f" "$pkg/$f"
done

# Deterministic-ish tarball: sort members, drop owner/mtime noise so the same tag
# rebuilds to a comparable archive. -C "$stage" keeps the "<name>/" prefix.
tar \
    --sort=name \
    --owner=0 --group=0 --numeric-owner \
    --mtime='UTC 1970-01-01' \
    -czf "$tarball" \
    -C "$stage" "$name"

# Emit the checksum in `sha256sum -c` format next to the tarball. The asset name is
# "<tarball>.sha256" (herdres-<tag>.tar.gz.sha256) — the Phase-3 contract the stable
# fetcher's _release_sha_name() resolves.
( cd "$out_dir" && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256" )

printf '%s\n' "$tarball"
printf '%s\n' "$checksum"
