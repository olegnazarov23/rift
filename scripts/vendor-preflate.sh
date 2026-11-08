#!/bin/sh
# Copies preflate-rs (our restart-points branch) from the lab clone into vendor/, so rift builds on its own.
set -eu
src=${1:-../recompress-lab/tools/preflate-rs}
dst=vendor/preflate-rs
rm -rf "$dst" && mkdir -p "$dst"
cp -R "$src/preflate/src" "$dst/src"
cp "$src/LICENSE" "$dst/LICENSE"
# Apache-2.0 asks that changed files say so
base=e97efb0 # the upstream commit our branch starts from
git -C "$src" diff --name-status "$base" HEAD -- preflate/src | while read -r status path; do
    f="$dst/src/${path#preflate/src/}"
    case $status in
        A) note="// Added for rift; not in upstream preflate-rs. See vendor/preflate-rs/README.md." ;;
        *) note="// Changed for rift; see vendor/preflate-rs/README.md." ;;
    esac
    { echo "$note"; cat "$f"; } > "$f.tmp" && mv "$f.tmp" "$f"
done
rev=$(git -C "$src" rev-parse --short HEAD)
branch=$(git -C "$src" rev-parse --abbrev-ref HEAD)
cat > "$dst/Cargo.toml" <<TOML
[package]
name = "preflate-rs"
version = "0.8.0"
edition = "2024"
authors = ["Kristof Roomp <kristofr@microsoft.com>"]
license = "Apache-2.0"
rust-version = "1.89"
repository = "https://github.com/microsoft/preflate-rs"
description = "preflate-rs with restart points and streaming rebuilds (vendored for rift)"
publish = false

[dependencies]
byteorder = "1.4"
default-boxed = "0.2"
bitcode = "0.6.3"
deranged = "0.5"

[lib]
name = "preflate_rs"
TOML
cat > "$dst/README.md" <<MD
# preflate-rs (vendored)

The \`preflate\` crate of [microsoft/preflate-rs](https://github.com/microsoft/preflate-rs), Apache-2.0,
from branch \`$branch\` at $rev. On top of upstream:

- accept a single one-bit Huffman code (Go's encoder; upstream PR #78)
- a model of Go's compress/flate Huffman trees and block size
- independently rebuildable restart-point groups, rebuilt one at a time and resumable

Files changed from upstream \`$base\` say so in their first line.

Refresh with \`scripts/vendor-preflate.sh\`.
MD
echo "vendored preflate-rs $branch@$rev into $dst"
