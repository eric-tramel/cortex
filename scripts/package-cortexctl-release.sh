#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/package-cortexctl-release.sh <target-triple> [output-dir]

examples:
  scripts/package-cortexctl-release.sh x86_64-unknown-linux-gnu dist
  scripts/package-cortexctl-release.sh aarch64-apple-darwin dist
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 64
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is not installed or not on PATH"
  exit 1
fi

TARGET="$1"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${2:-$PROJECT_ROOT/dist}"

mkdir -p "$OUTPUT_DIR"

cargo build \
  --manifest-path "$PROJECT_ROOT/apps/cortexctl/Cargo.toml" \
  --release \
  --locked \
  --target "$TARGET"

BIN_PATH="$PROJECT_ROOT/target/$TARGET/release/cortexctl"
if [[ ! -x "$BIN_PATH" ]]; then
  echo "expected built binary at $BIN_PATH"
  exit 1
fi

ARCHIVE_NAME="cortexctl-$TARGET.tar.gz"
ARCHIVE_PATH="$OUTPUT_DIR/$ARCHIVE_NAME"
CHECKSUM_PATH="$OUTPUT_DIR/cortexctl-$TARGET.sha256"

STAGE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGE_DIR"' EXIT

cp "$BIN_PATH" "$STAGE_DIR/cortexctl"
tar -C "$STAGE_DIR" -czf "$ARCHIVE_PATH" cortexctl

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$ARCHIVE_PATH" | awk -v archive="$ARCHIVE_NAME" '{print $1 "  " archive}' > "$CHECKSUM_PATH"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$ARCHIVE_PATH" | awk -v archive="$ARCHIVE_NAME" '{print $1 "  " archive}' > "$CHECKSUM_PATH"
else
  echo "neither sha256sum nor shasum found; cannot produce checksum file"
  exit 1
fi

echo "packaged: $ARCHIVE_PATH"
echo "checksum: $CHECKSUM_PATH"
