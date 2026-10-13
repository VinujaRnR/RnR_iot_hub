#!/usr/bin/env bash
# ==============================================================================
# RnR Linux IoT Hub - Universal Linux CLI Installer
# File: install.sh
#
# Implements Customization 1 specification:
# - Installs the hub on compatible Linux hosts even if Python is not preinstalled.
# - Detects CPU architecture, Linux distribution, and service manager.
# - Installs supported Python 3 runtime, venv, and dependencies automatically.
# - Hands off to install.py for secure configuration, device enrollment, and systemd registration.
#
# Usage:
#   sudo bash install.sh [--repo <URL>] [--ref <TAG/BRANCH>] [--dry-run] [--wizard]
# ==============================================================================

set -euo pipefail

# Color formatting
GREEN="\033[92m"
YELLOW="\033[93m"
CYAN="\033[96m"
RED="\033[91m"
BOLD="\033[1m"
RESET="\033[0m"

log_info() { echo -e "${CYAN}==>${RESET} ${BOLD}$1${RESET}"; }
log_ok()   { echo -e " ${GREEN}[✓]${RESET} $1"; }
log_warn() { echo -e " ${YELLOW}[!]${RESET} $1"; }
log_fail() { echo -e " ${RED}[✗]${RESET} $1"; exit 1; }

echo -e "\n${BOLD}${CYAN}========================================================================${RESET}"
echo -e "${BOLD}${CYAN} RnR Linux IoT Hub — Universal Linux CLI Installer                     ${RESET}"
echo -e "${BOLD}${CYAN} Target: /opt/iothub | /etc/iothub | /var/lib/iothub | iothubd.service  ${RESET}"
echo -e "${BOLD}${CYAN}========================================================================${RESET}\n"

# 1. Root check
if [ "$(id -u)" -ne 0 ]; then
    log_fail "Installer must be run as root. Please run with sudo: sudo bash install.sh"
fi

# 2. System Architecture & Distribution Detection
ARCH="$(uname -m)"
OS="unknown"
DISTRO="unknown"

if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS="$ID"
    DISTRO="$PRETTY_NAME"
fi

log_info "Detected System Architecture: ${ARCH} on ${DISTRO}"

# Verify Systemd is init manager
if [ ! -d /run/systemd/system ]; then
    log_fail "Systemd was not detected. iothubd requires a systemd-managed Linux distribution."
fi
log_ok "Init system verified: systemd"

# Parse CLI options
REPO_URL=""
GIT_REF=""
DRY_RUN=false
WIZARD=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)
            REPO_URL="$2"
            shift 2
            ;;
        --ref)
            GIT_REF="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --no-wizard)
            WIZARD=false
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# 3. Ensure System Dependencies (Python 3, python3-venv, sqlite3)
log_info "Checking system runtime dependencies..."
MISSING_PKGS=()

if ! command -v python3 >/dev/null 2>&1; then
    MISSING_PKGS+=("python3")
fi
if ! python3 -m venv --help >/dev/null 2>&1; then
    MISSING_PKGS+=("python3-venv")
fi
if ! command -v pip3 >/dev/null 2>&1; then
    MISSING_PKGS+=("python3-pip")
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
    MISSING_PKGS+=("sqlite3")
fi

if [ ${#MISSING_PKGS[@]} -gt 0 ]; then
    log_warn "Missing packages detected: ${MISSING_PKGS[*]}"
    log_info "Updating system packages and installing dependencies..."
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -y
        apt-get install -y "${MISSING_PKGS[@]}" curl git
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y "${MISSING_PKGS[@]}" curl git
    elif command -v pacman >/dev/null 2>&1; then
        pacman -Sy --noconfirm "${MISSING_PKGS[@]}" curl git
    else
        log_fail "Unsupported package manager. Please manually install: ${MISSING_PKGS[*]}"
    fi
    log_ok "Installed required system packages."
else
    log_ok "All system prerequisite packages are present."
fi

# 4. Clone or update repository if --repo is specified
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="$SCRIPT_DIR"

if [ -n "$REPO_URL" ]; then
    TARGET_SRC="/tmp/iothub_install_src"
    rm -rf "$TARGET_SRC"
    log_info "Cloning repository from ${REPO_URL} (${GIT_REF:-main})..."
    git clone --depth 1 ${GIT_REF:+--branch "$GIT_REF"} "$REPO_URL" "$TARGET_SRC"
    WORK_DIR="$TARGET_SRC"
fi

# 5. Hand off to install.py
INSTALL_PY="$WORK_DIR/install.py"
if [ ! -f "$INSTALL_PY" ]; then
    log_fail "Could not find install.py in $WORK_DIR."
fi

log_info "Launching Python configuration and installation manager..."
PYTHON_BIN="$(command -v python3)"

INSTALL_ARGS=()
if [ "$DRY_RUN" = true ]; then
    INSTALL_ARGS+=("--dry-run")
fi
if [ "$WIZARD" = true ]; then
    INSTALL_ARGS+=("--wizard")
fi

"$PYTHON_BIN" "$INSTALL_PY" "${INSTALL_ARGS[@]}"

# Symlink iothubctl into /usr/local/bin
if [ -f "/opt/iothub/bin/iothubctl" ]; then
    ln -sf "/opt/iothub/bin/iothubctl" "/usr/local/bin/iothubctl"
    chmod +x "/usr/local/bin/iothubctl" || true
    log_ok "Created global CLI symlink: /usr/local/bin/iothubctl"
fi

echo -e "\n${BOLD}${GREEN}Universal Linux CLI installation finished successfully!${RESET}\n"
