#!/usr/bin/env python3
"""
Interactive Serial Monitor for RnR IoT Hub Firmware (ESP8266 / Arduino).
File: bin/monitor.py

Usage:
    python3 bin/monitor.py [/dev/ttyUSB0] [9600]
"""

import sys
import time
import threading
import serial

PORT = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"
BAUD = int(sys.argv[2]) if len(sys.argv) > 2 else 9600

def reader_thread(ser, stop_event):
    while not stop_event.is_set():
        try:
            if ser.in_waiting:
                data = ser.read(ser.in_waiting).decode("utf-8", errors="replace")
                sys.stdout.write(data)
                sys.stdout.flush()
            else:
                time.sleep(0.02)
        except Exception as e:
            if not stop_event.is_set():
                print(f"\n[!] Serial read error: {e}")
            break

def main():
    print("=" * 60)
    print(f" Connecting to Firmware Serial Monitor on {PORT} @ {BAUD} baud...")
    print(" Commands available:")
    print("   1      -> Light ON")
    print("   0      -> Light OFF")
    print("   v      -> Valve ON (Open)")
    print("   c      -> Valve OFF (Close)")
    print("   ?      -> Query Status (States, Uptime, Next Reboot)")
    print("   reboot -> Trigger ESP8266 Software Restart")
    print("   q      -> Quit Monitor")
    print("=" * 60)

    try:
        ser = serial.Serial()
        ser.port = PORT
        ser.baudrate = BAUD
        ser.timeout = 1.0
        ser.dtr = False
        ser.rts = False
        ser.open()
    except serial.SerialException as e:
        print(f"\n[X] Could not open port {PORT}: {e}")
        print("    Tip: If 'iothubd' is holding the port, stop it first:")
        print("         sudo systemctl stop iothubd\n")
        return 1

    # Safe toggle to wake up board without holding in reset
    ser.dtr = False
    ser.rts = True
    time.sleep(0.05)
    ser.rts = False
    time.sleep(0.3)

    stop_event = threading.Event()
    t = threading.Thread(target=reader_thread, args=(ser, stop_event), daemon=True)
    t.start()

    # Query initial status
    ser.write(b"?\n")

    try:
        while True:
            cmd = input()
            if cmd.strip().lower() in ["q", "quit", "exit"]:
                break
            ser.write((cmd + "\n").encode("utf-8"))
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        print("\n[*] Exiting Serial Monitor...")
        stop_event.set()
        ser.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
