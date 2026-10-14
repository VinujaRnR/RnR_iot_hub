#!/usr/bin/env bash
# ==============================================================================
# RnR Linux IoT Hub - Universal Software Updater
# File: update.sh
#
# Pulls the latest release from Git, updates dependencies, creates an automatic
# safety backup, redeploys /opt/iothub, and restarts iothubd.service.
#
# Usage:
#   sudo bash update.sh
#   sudo bash update.sh --ref v1.1.0
#   sudo bash update.sh --repo https://github.com/VinujaRnR/RnR_iot_hub.git
# ==============================================================================

set -euo pipefail

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
echo -e "${BOLD}${CYAN} RnR Linux IoT Hub — Application Software Updater                       ${RESET}"
echo -e "${BOLD}${CYAN} Target: /opt/iothub | iothubd.service                                  ${RESET}"
echo -e "${BOLD}${CYAN}========================================================================${RESET}\n"

if [ "$(id -u)" -ne 0 ]; then
    log_fail "Updater must be run as root. Please run with sudo: sudo bash update.sh"
fi

REPO_URL="https://github.com/VinujaRnR/RnR_iot_hub.git"
GIT_REF="main"

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
        *)
            shift
            ;;
    esac
done

STAGE_DIR="/tmp/iothub_update_pkg"
rm -rf "$STAGE_DIR"

log_info "Downloading latest release from ${REPO_URL} (${GIT_REF})..."
git clone --depth 1 --branch "$GIT_REF" "$REPO_URL" "$STAGE_DIR"
log_ok "Downloaded update package."

# Create pre-update backup of SQLite database if present
if [ -f "/var/lib/iothub/iothub.db" ] && [ -f "$STAGE_DIR/iothubctl.py" ]; then
    log_info "Creating pre-update database backup..."
    /opt/iothub/.venv/bin/python "$STAGE_DIR/iothubctl.py" backup create || log_warn "Pre-update backup skipped or warned."
fi

# Deploy application code using installer
log_info "Deploying updated software to /opt/iothub..."
python3 "$STAGE_DIR/install.py"

rm -rf "$STAGE_DIR"

echo -e "\n${BOLD}${GREEN}========================================================================${RESET}"
echo -e "${BOLD}${GREEN} Software update to '${GIT_REF}' completed successfully!                 ${RESET}"
echo -e "${BOLD}${GREEN} Run health check: sudo iothubctl doctor                                ${RESET}"
echo -e "${BOLD}${GREEN}========================================================================${RESET}\n"
