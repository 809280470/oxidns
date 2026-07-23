#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$BASE_DIR/../.." && pwd)"
TOOLS_DIR="${BENCH_TOOLS_DIR:-$BASE_DIR/.tools}"
DNSPERF_VERSION="${DNSPERF_VERSION:-2.15.1}"
MOSDNS_VERSION="${MOSDNS_VERSION:-v5.3.4}"
OXIDNS_VERSION="${OXIDNS_VERSION:-latest}"

mkdir -p "$TOOLS_DIR"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "prepare_server.sh supports Linux benchmark hosts only" >&2
  exit 1
fi

if [[ "${BENCH_INSTALL_DEPS:-1}" == "1" ]]; then
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "automatic dependency installation currently supports Debian/Ubuntu (apt-get)" >&2
    exit 1
  fi
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential ca-certificates curl git libcap-dev libck-dev libjson-c-dev libkrb5-dev \
    libnghttp2-dev libssl-dev libxml2-dev pkg-config unzip xz-utils
fi

case "$(uname -m)" in
  x86_64) mosdns_arch="amd64"; oxidns_target="x86_64-unknown-linux-musl" ;;
  aarch64|arm64) mosdns_arch="arm64"; oxidns_target="aarch64-unknown-linux-musl" ;;
  armv7l|armv7) mosdns_arch="armv7"; oxidns_target="armv7-unknown-linux-musleabihf" ;;
  i386|i686) mosdns_arch="386"; oxidns_target="i686-unknown-linux-musl" ;;
  *) echo "unsupported mosdns architecture: $(uname -m)" >&2; exit 1 ;;
esac

if [[ "${BENCH_BUILD_OXIDNS:-0}" == "1" ]]; then
  if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o "$TOOLS_DIR/rustup-init.sh"
    sh "$TOOLS_DIR/rustup-init.sh" -y --profile minimal
    export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
  fi
  echo "building OxiDNS from $(git -C "$REPO_DIR" describe --tags --always --dirty)"
  cargo build --release --manifest-path "$REPO_DIR/Cargo.toml"
  install -m 0755 "$REPO_DIR/target/release/oxidns" "$BASE_DIR/oxidns"
else
  if [[ "$OXIDNS_VERSION" == "latest" ]]; then
    OXIDNS_VERSION="$(curl -fsSL https://api.github.com/repos/SvenShi/oxidns/releases/latest | python3 -c 'import json,sys; print(json.load(sys.stdin)["tag_name"])')"
  fi
  oxidns_archive="$TOOLS_DIR/oxidns-${OXIDNS_VERSION}-${oxidns_target}.tar.gz"
  curl -fL "https://github.com/SvenShi/oxidns/releases/download/${OXIDNS_VERSION}/oxidns-${oxidns_target}.tar.gz" -o "$oxidns_archive"
  mkdir -p "$TOOLS_DIR/oxidns-extract"
  tar -xzf "$oxidns_archive" -C "$TOOLS_DIR/oxidns-extract"
  oxidns_source="$(find "$TOOLS_DIR/oxidns-extract" -type f -name oxidns -perm -u+x -print -quit)"
  if [[ -z "$oxidns_source" ]]; then
    echo "oxidns executable not found in $oxidns_archive" >&2
    exit 1
  fi
  install -m 0755 "$oxidns_source" "$BASE_DIR/oxidns"
fi

mosdns_zip="$TOOLS_DIR/mosdns-linux-${mosdns_arch}.zip"
curl -fL "https://github.com/IrineSistiana/mosdns/releases/download/${MOSDNS_VERSION}/mosdns-linux-${mosdns_arch}.zip" -o "$mosdns_zip"
unzip -jo "$mosdns_zip" 'mosdns' -d "$TOOLS_DIR/mosdns-extract"
install -m 0755 "$TOOLS_DIR/mosdns-extract/mosdns" "$BASE_DIR/mosdns"

dnsperf_archive="$TOOLS_DIR/dnsperf-${DNSPERF_VERSION}.tar.gz"
curl -fL "https://www.dns-oarc.net/files/dnsperf/dnsperf-${DNSPERF_VERSION}.tar.gz" -o "$dnsperf_archive"
tar -xzf "$dnsperf_archive" -C "$TOOLS_DIR"
(
  cd "$TOOLS_DIR/dnsperf-${DNSPERF_VERSION}"
  ./configure --prefix="$TOOLS_DIR/dnsperf-install"
  make -j"$(getconf _NPROCESSORS_ONLN)"
  make install
)

dnsperf_bin="$TOOLS_DIR/dnsperf-install/bin/dnsperf"
if ! "$dnsperf_bin" -h 2>&1 | grep -q -- '-j'; then
  echo "built dnsperf does not expose JSON output (-j)" >&2
  exit 1
fi
if ! "$dnsperf_bin" -H 2>&1 | grep -q 'latency-histogram'; then
  echo "built dnsperf does not expose latency histograms" >&2
  exit 1
fi

cat <<EOF
Benchmark tools are ready:
  OxiDNS: $BASE_DIR/oxidns
  mosdns: $BASE_DIR/mosdns ($MOSDNS_VERSION)
  dnsperf: $dnsperf_bin ($DNSPERF_VERSION)

Run the publishable matrix:
  DNSPERF_BIN_PATH=$dnsperf_bin ./run_publishable_compare.py --publish-docs
EOF
