#!/usr/bin/env bash
# ==============================================================================
# RnR Linux IoT Hub - Dedicated Linux CLI Uninstaller
# File: uninstall.sh
#
# Uninstalls iothubd systemd service, CLI symlink, and application files.
#
# Usage:
#   sudo bash uninstall.sh            # Standard uninstall (preserves config & DB)
#   sudo bash uninstall.sh --purge    # Purges all configuration, keys, and SQLite database
#   sudo bash uninstall.sh --dry-run  # Simulates uninstallation without making changes
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
log_warn() { echo -e " ${YELLOW}[!]{RESET} $1"; }
log_fail() { echo -e " ${RED}[✗]${RESET} $1"; exit 1; }

echo -e "\n${BOLD}${CYAN}========================================================================${RESET}"
echo -e "${BOLD}${CYAN} RnR Linux IoT Hub — Dedicated Linux CLI Uninstaller                    ${RESET}"
echo -e "${BOLD}${CYAN} Target: iothubd.service | /usr/local/bin/iothubctl | /opt/iothub      ${RESET}"
echo -e "${BOLD}${CYAN}========================================================================${RESET}\n"

# 1. Parse arguments
PURGE=false
ASSUME_YES=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --purge)
            PURGE=true
            shift
            ;;
        -y|--yes)
            ASSUME_YES=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# 2. Root check (required unless in dry-run mode)
if [ "$(id -u)" -ne 0 ] && [ "$DRY_RUN" = false ]; then
    log_fail "Uninstaller must be run as root. Please run with sudo: sudo bash uninstall.sh"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_PY=""
if [ -f "$SCRIPT_DIR/install.py" ]; then
    INSTALL_PY="$SCRIPT_DIR/install.py"
elif [ -f "/opt/iothub/install.py" ]; then
    INSTALL_PY="/opt/iothub/install.py"
fi

# 3. Prefer Python installer module if present
if [ -n "$INSTALL_PY" ] && command -v python3 >/dev/null 2>&1; then
    ARGS=("--uninstall")
    if [ "$PURGE" = true ]; then
        ARGS+=("--purge")
    fi
    if [ "$ASSUME_YES" = true ]; then
        ARGS+=("--yes")
    fi
    if [ "$DRY_RUN" = true ]; then
        ARGS+=("--dry-run")
    fi
    python3 "$INSTALL_PY" "${ARGS[@]}"
    exit 0
fi

# 4. Fallback Standalone Shell Removal (if install.py is not found)
log_warn "install.py not found. Performing standalone uninstallation..."

if [ "$DRY_RUN" = true ]; then
    log_ok "[DRY-RUN] Would stop and disable iothubd.service."
    log_ok "[DRY-RUN] Would remove /etc/systemd/system/iothubd.service and reload daemon."
    log_ok "[DRY-RUN] Would remove /usr/local/bin/iothubctl."
    log_ok "[DRY-RUN] Would remove /opt/iothub."
    log_ok "[DRY-RUN] Would remove system user and group 'iothub'."
    if [ "$PURGE" = true ]; then
        log_ok "[DRY-RUN] Would purge /etc/iothub and /var/lib/iothub."
    fi
    exit 0
fi

# Stop and disable systemd service
log_info "Stopping and disabling iothubd.service..."
systemctl stop iothubd.service 2>/dev/null || true
systemctl disable iothubd.service 2>/dev/null || true
rm -f /etc/systemd/system/iothubd.service
systemctl daemon-reload 2>/dev/null || true
systemctl reset-failed 2>/dev/null || true
log_ok "Removed systemd service."

# Remove global CLI symlink
rm -f /usr/local/bin/iothubctl
log_ok "Removed global CLI symlink /usr/local/bin/iothubctl."

# Remove application directory
if [ -d "/opt/iothub" ]; then
    rm -rf /opt/iothub
    log_ok "Removed application directory /opt/iothub."
fi

# Remove user and group
userdel iothub 2>/dev/null || true
groupdel iothub 2>/dev/null || true
log_ok "Removed system user and group 'iothub'."

# Purge or preserve configuration and database
if [ "$PURGE" = true ]; then
    if [ "$ASSUME_YES" = false ] && [ -t 0 ]; then
        echo -e "\n${BOLD}${RED}WARNING: Purge mode will permanently delete all configuration and databases!${RESET}"
        read -p "Are you sure you want to proceed? [y/N]: " confirm
        if [[ ! "$confirm" =~ ^[yY](es)?$ ]]; then
            log_warn "Purge aborted. Preserving configuration and databases."
            PURGE=false
        fi
    fi
    if [ "$PURGE" = true ]; then
        rm -rf /etc/iothub /var/lib/iothub
        log_ok "Purged /etc/iothub and /var/lib/iothub."
    fi
fi

if [ "$PURGE" = false ]; then
    echo -e "\n${BOLD}${YELLOW}------------------------------------------------------------------------${RESET}"
    echo -e "${BOLD}${YELLOW} NOTICE: Configuration and databases have been preserved:               ${RESET}"
    echo -e "   • Configuration: /etc/iothub"
    echo -e "   • Storage/DB   : /var/lib/iothub"
    echo -e " To completely remove these, run: sudo bash uninstall.sh --purge"
    echo -e "${BOLD}${YELLOW}------------------------------------------------------------------------${RESET}"
fi

echo -e "\n${BOLD}${GREEN}========================================================================${RESET}"
echo -e "${BOLD}${GREEN} RnR Linux IoT Hub has been successfully uninstalled.                   ${RESET}"
echo -e "${BOLD}${GREEN}========================================================================${RESET}\n"
