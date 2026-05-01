#!/usr/bin/env bash
# install.sh — One-line installer for nexrade-cache
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/nexrade/nexrade-cache/main/install.sh | bash
#
# Or with a specific version:
#   curl -fsSL .../install.sh | bash -s -- --version v0.1.0

set -euo pipefail

REPO="nexrade/nexrade-cache"
BIN_DIR="${NEXRADE_BIN_DIR:-/usr/local/bin}"
VERSION="${1:-latest}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RESET='\033[0m'

info()    { echo -e "${BLUE}[nexrade]${RESET} $*"; }
success() { echo -e "${GREEN}[nexrade]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[nexrade]${RESET} $*"; }
error()   { echo -e "${RED}[nexrade]${RESET} $*" >&2; exit 1; }

# ── Detect platform ──────────────────────────────────────────────────────────
detect_platform() {
    local os arch

    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux)
            # Prefer musl (static) if available
            case "$arch" in
                x86_64)  echo "linux-x86_64-musl" ;;
                aarch64) echo "linux-arm64-musl" ;;
                armv7l)  echo "linux-armv7" ;;
                *)        error "Unsupported Linux architecture: $arch" ;;
            esac
            ;;
        Darwin)
            case "$arch" in
                x86_64)  echo "macos-x86_64" ;;
                arm64)   echo "macos-arm64" ;;
                *)        error "Unsupported macOS architecture: $arch" ;;
            esac
            ;;
        MINGW*|MSYS*|CYGWIN*)
            echo "windows-x86_64"
            ;;
        FreeBSD)
            echo "freebsd-x86_64"
            ;;
        *)
            error "Unsupported OS: $os"
            ;;
    esac
}

# ── Resolve latest version ───────────────────────────────────────────────────
resolve_version() {
    if [ "$VERSION" = "latest" ]; then
        info "Fetching latest release version..."
        VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' \
            | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')
        info "Latest version: $VERSION"
    fi
}

# ── Download and install ─────────────────────────────────────────────────────
install() {
    local platform="$1"
    local ext="tar.gz"
    local archive="nexrade-cache-${platform}.${ext}"
    local url="https://github.com/${REPO}/releases/download/${VERSION}/${archive}"
    local tmpdir

    tmpdir="$(mktemp -d)"
    trap "rm -rf $tmpdir" EXIT

    info "Downloading $archive..."
    curl -fsSL --progress-bar "$url" -o "$tmpdir/$archive"

    info "Verifying checksum..."
    curl -fsSL "${url}.sha256" -o "$tmpdir/${archive}.sha256" 2>/dev/null || warn "Could not fetch checksum, skipping verification"

    if [ -f "$tmpdir/${archive}.sha256" ]; then
        cd "$tmpdir"
        sha256sum -c "${archive}.sha256" >/dev/null 2>&1 || error "Checksum verification failed!"
        cd -
    fi

    info "Extracting..."
    tar -xzf "$tmpdir/$archive" -C "$tmpdir"

    info "Installing to $BIN_DIR ..."
    if [ ! -w "$BIN_DIR" ]; then
        warn "Need sudo to write to $BIN_DIR"
        sudo install -m 755 "$tmpdir/nexrade-cache" "$BIN_DIR/nexrade-cache"
        sudo install -m 755 "$tmpdir/nexrade-cli"   "$BIN_DIR/nexrade-cli"
    else
        install -m 755 "$tmpdir/nexrade-cache" "$BIN_DIR/nexrade-cache"
        install -m 755 "$tmpdir/nexrade-cli"   "$BIN_DIR/nexrade-cli"
    fi
}

# ── Main ─────────────────────────────────────────────────────────────────────
main() {
    echo ""
    echo "  nexrade-cache installer"
    echo "  ========================"
    echo ""

    # Parse args
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --version) VERSION="$2"; shift 2 ;;
            --bin-dir) BIN_DIR="$2"; shift 2 ;;
            --help|-h)
                echo "Usage: install.sh [--version v0.1.0] [--bin-dir /usr/local/bin]"
                exit 0
                ;;
            *) warn "Unknown argument: $1"; shift ;;
        esac
    done

    resolve_version

    local platform
    platform="$(detect_platform)"
    info "Platform: $platform"

    install "$platform"

    echo ""
    success "nexrade-cache $VERSION installed!"
    echo ""
    echo "  Start the server:"
    echo "    nexrade-cache"
    echo ""
    echo "  Connect with CLI:"
    echo "    nexrade-cli PING"
    echo ""
    echo "  Or use redis-cli (fully compatible):"
    echo "    redis-cli PING"
    echo ""
}

main "$@"
