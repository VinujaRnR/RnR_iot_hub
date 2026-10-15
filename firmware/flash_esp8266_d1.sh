#!/usr/bin/env bash
# Script to compile and upload esp8266_d1_firmware.ino to ESP8266 D1 / D1 Mini via arduino-cli

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$DIR")"
ARDUINO_CLI="$PROJECT_ROOT/bin/arduino-cli"

if [ ! -f "$ARDUINO_CLI" ]; then
    if command -v arduino-cli >/dev/null 2>&1; then
        ARDUINO_CLI="$(command -v arduino-cli)"
    else
        echo "[!] arduino-cli not found at $ARDUINO_CLI or in system PATH."
        echo "    Run: curl -fsSL https://raw.githubusercontent.com/arduino/arduino-cli/master/install.sh | sh"
        exit 1
    fi
fi

# ESP8266 Core Index URL
ESP8266_INDEX_URL="https://arduino.esp8266.com/stable/package_esp8266com_index.json"

echo "=== 1. Checking / Installing ESP8266 Core ==="
"$ARDUINO_CLI" core update-index --additional-urls "$ESP8266_INDEX_URL"

if ! "$ARDUINO_CLI" core list | grep -q "esp8266:esp8266"; then
    echo "[*] Installing esp8266:esp8266 core..."
    "$ARDUINO_CLI" core install esp8266:esp8266 --additional-urls "$ESP8266_INDEX_URL"
else
    echo "[✓] esp8266:esp8266 core already installed."
fi

echo ""
echo "=== 2. Detecting Connected Boards ==="
"$ARDUINO_CLI" board list

# Attempt to auto-detect port or accept from argument
PORT="$1"
if [ -z "$PORT" ]; then
    for p in /dev/ttyUSB0 /dev/ttyUSB1 /dev/ttyACM0 /dev/ttyACM1; do
        if [ -e "$p" ]; then
            PORT="$p"
            break
        fi
    done
fi

if [ -z "$PORT" ]; then
    echo "[!] No serial port specified and none auto-detected (/dev/ttyUSB0, /dev/ttyACM0)."
    echo "    Usage: $0 [/dev/ttyUSB0] [FQBN]"
    exit 1
fi

# FQBN: default to esp8266:esp8266:d1_mini (works for both D1 and D1 Mini)
FQBN="${2:-esp8266:esp8266:d1_mini}"

echo "[✓] Using port: $PORT"
echo "[✓] Using FQBN: $FQBN"

echo ""
echo "=== 3. Compiling esp8266_d1_firmware ==="
"$ARDUINO_CLI" compile --fqbn "$FQBN" --additional-urls "$ESP8266_INDEX_URL" "$DIR/esp8266_d1_firmware"

echo ""
echo "=== 4. Uploading to ESP8266 D1 ==="
UPLOAD_OUTPUT=$("$ARDUINO_CLI" upload -p "$PORT" --fqbn "$FQBN" "$DIR/esp8266_d1_firmware" 2>&1) || UPLOAD_EXIT=$?
echo "$UPLOAD_OUTPUT"

if [ "${UPLOAD_EXIT:-0}" -ne 0 ] || echo "$UPLOAD_OUTPUT" | grep -qiE "error|fatal"; then
    echo ""
    echo "[!] Upload failed. Please check the error above."
    if echo "$UPLOAD_OUTPUT" | grep -qi "Input/output error"; then
        echo "    Tip: The USB device encountered a communication/protocol error."
        echo "    Please unplug the USB cable from the PC and plug it back in firmly to reset the hardware."
    fi
    exit 1
fi
echo "[✓] Upload successful!"

echo ""
echo "=== Testing Communication ==="
echo "Configure baud rate:"
echo "  stty -F $PORT 9600"
echo ""
echo "Light Controls (Built-in LED):"
echo "  echo -n '1' > $PORT  # Light ON"
echo "  echo -n '0' > $PORT  # Light OFF"
echo ""
echo "Valve Controls (Digital Pin D1):"
echo "  echo -n 'v' > $PORT  # Valve ON / Open"
echo "  echo -n 'c' > $PORT  # Valve OFF / Close"
echo ""
echo "Status Query:"
echo "  echo -n '?' > $PORT  # Query status"
