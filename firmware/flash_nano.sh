#!/usr/bin/env bash
# Script to compile and upload nano_firmware.ino to Arduino Nano via arduino-cli

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$DIR")"
ARDUINO_CLI="$PROJECT_ROOT/bin/arduino-cli"

if [ ! -f "$ARDUINO_CLI" ]; then
    echo "[!] arduino-cli not found at $ARDUINO_CLI."
    echo "    Run: curl -fsSL https://raw.githubusercontent.com/arduino/arduino-cli/master/install.sh | sh"
    exit 1
fi

echo "=== 1. Checking / Installing Arduino AVR Core ==="
"$ARDUINO_CLI" core update-index
if ! "$ARDUINO_CLI" core list | grep -q "arduino:avr"; then
    echo "[*] Installing arduino:avr core..."
    "$ARDUINO_CLI" core install arduino:avr
else
    echo "[✓] arduino:avr core already installed."
fi

echo ""
echo "=== 2. Detecting Connected Boards ==="
"$ARDUINO_CLI" board list

# Attempt to auto-detect port or accept from argument
PORT="$1"
if [ -z "$PORT" ]; then
    # Look for common serial ports
    for p in /dev/ttyUSB0 /dev/ttyUSB1 /dev/ttyACM0 /dev/ttyACM1; do
        if [ -e "$p" ]; then
            PORT="$p"
            break
        fi
    done
fi

if [ -z "$PORT" ]; then
    echo "[!] No serial port specified and none auto-detected (/dev/ttyUSB0, /dev/ttyACM0)."
    echo "    Usage: $0 [/dev/ttyUSB0]"
    exit 1
fi

echo "[✓] Using port: $PORT"

echo ""
echo "=== 3. Compiling nano_firmware ==="
"$ARDUINO_CLI" compile --fqbn arduino:avr:nano "$DIR/nano_firmware"

echo ""
echo "=== 4. Uploading to Arduino Nano ==="
echo "[*] Attempting upload (Standard ATmega328P bootloader)..."
if "$ARDUINO_CLI" upload -p "$PORT" --fqbn arduino:avr:nano "$DIR/nano_firmware"; then
    echo "[✓] Upload successful!"
else
    echo "[!] Standard bootloader upload failed. Retrying with Old Bootloader (atmega328old)..."
    "$ARDUINO_CLI" upload -p "$PORT" --fqbn arduino:avr:nano:cpu=atmega328old "$DIR/nano_firmware"
    echo "[✓] Upload with Old Bootloader successful!"
fi

echo ""
echo "=== Testing Communication ==="
echo "You can test the LED by running:"
echo "  stty -F $PORT 9600"
echo "  echo -n '1' > $PORT  # Turn ON"
echo "  echo -n '0' > $PORT  # Turn OFF"
