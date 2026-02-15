#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Install cortexctl from GitHub Releases.

usage:
  scripts/install-cortexctl.sh --repo <owner/repo> [options]

options:
  --repo <owner/repo>   GitHub repository hosting release assets (required)
  --version <tag>       Release tag (default: latest)
  --install-dir <path>  Destination directory for binary (default: ~/.local/bin)
  --force               Replace existing binary without prompting
  -h, --help            Show help

examples:
  scripts/install-cortexctl.sh --repo your-org/cortex
  scripts/install-cortexctl.sh --repo your-org/cortex --version v0.1.0
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1"
    exit 1
  fi
}

detect_target_triple() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Darwin) os="apple-darwin" ;;
    Linux) os="unknown-linux-gnu" ;;
    *)
      echo "unsupported OS: $os (supported: Darwin, Linux)"
      exit 1
      ;;
  esac

  case "$arch" in
    x86_64|amd64) arch="x86_64" ;;
    arm64|aarch64) arch="aarch64" ;;
    *)
      echo "unsupported architecture: $arch (supported: x86_64, arm64/aarch64)"
      exit 1
      ;;
  esac

  echo "${arch}-${os}"
}

fetch_latest_tag() {
  local repo="$1"
  local api_url="https://api.github.com/repos/${repo}/releases/latest"
  local tag

  tag="$(curl -fsSL "$api_url" | sed -n 's/^[[:space:]]*"tag_name":[[:space:]]*"\([^"]\+\)".*$/\1/p' | head -n 1)"
  if [[ -z "$tag" ]]; then
    echo "failed to resolve latest release tag from $api_url"
    exit 1
  fi

  echo "$tag"
}

repo=""
version="latest"
install_dir="${HOME}/.local/bin"
force=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    --version)
      version="${2:-}"
      shift 2
      ;;
    --install-dir)
      install_dir="${2:-}"
      shift 2
      ;;
    --force)
      force=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1"
      usage
      exit 64
      ;;
  esac
done

if [[ -z "$repo" ]]; then
  echo "--repo is required"
  usage
  exit 64
fi

require_cmd curl
require_cmd tar

target="$(detect_target_triple)"
if [[ "$version" == "latest" ]]; then
  version="$(fetch_latest_tag "$repo")"
fi

asset_name="cortexctl-${target}.tar.gz"
asset_url="https://github.com/${repo}/releases/download/${version}/${asset_name}"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "installing cortexctl ${version} for ${target}"
echo "downloading: ${asset_url}"
if ! curl -fL "$asset_url" -o "$tmp_dir/$asset_name"; then
  echo "failed to download release asset: $asset_name"
  echo "verify that repo/tag/target exists in GitHub Releases:"
  echo "  https://github.com/${repo}/releases/tag/${version}"
  exit 1
fi

tar -xzf "$tmp_dir/$asset_name" -C "$tmp_dir"

if [[ ! -f "$tmp_dir/cortexctl" ]]; then
  echo "archive did not contain cortexctl binary"
  exit 1
fi

mkdir -p "$install_dir"
dest="$install_dir/cortexctl"

if [[ -e "$dest" && "$force" -ne 1 ]]; then
  echo "destination already exists: $dest"
  echo "re-run with --force to replace it"
  exit 1
fi

install -m 0755 "$tmp_dir/cortexctl" "$dest"
echo "installed: $dest"

if [[ ":$PATH:" != *":$install_dir:"* ]]; then
  echo
  echo "note: $install_dir is not currently on PATH."
  echo "add this to your shell profile:"
  echo "  export PATH=\"$install_dir:\$PATH\""
fi

if ! command -v clickhouse-server >/dev/null 2>&1; then
  echo
  echo "note: clickhouse-server was not found on PATH."
  echo "cortexctl can manage ClickHouse process lifecycle, but ClickHouse must be installed first."
  echo "macOS (Homebrew): brew install clickhouse"
  echo "Linux install docs: https://clickhouse.com/docs/install"
fi

echo
echo "verify install:"
echo "  cortexctl --help"
