#!/usr/bin/env bash
set -e

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="/opt/iothub"

if [ "$(id -u)" -ne 0 ]; then
    echo "Error: This script must be run with sudo: sudo ./update_service.sh"
    exit 1
fi

echo "Copying updated modules from $SRC_DIR to $TARGET_DIR..."
cp -r "$SRC_DIR/ingestion" "$TARGET_DIR/"
cp -r "$SRC_DIR/storage" "$TARGET_DIR/"
cp "$SRC_DIR/main.py" "$TARGET_DIR/"
cp "$SRC_DIR/config.py" "$TARGET_DIR/"
chown -R iothub:iothub "$TARGET_DIR/ingestion" "$TARGET_DIR/storage" "$TARGET_DIR/main.py" "$TARGET_DIR/config.py"

echo "Restarting iothubd systemd service..."
systemctl restart iothubd

echo -e "\n[✓] iothubd service successfully updated and restarted!\n"
systemctl status iothubd --no-pager

