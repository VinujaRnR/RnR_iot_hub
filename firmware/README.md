# IoT Firmware Repository

Firmware implementations for the RnR IoT Hub, supporting **Arduino Nano** and **ESP8266 D1 / D1 Mini** boards over USB serial (9600 baud).

---

## 1. ESP8266 D1 Board Firmware (`esp8266_d1_firmware`)

### Hardware Pin Mapping
* **Light**: `LED_BUILTIN` (GPIO 2 / D4 on Wemos D1/D1 Mini, Active-Low)
* **Valve**: Digital Pin `D1` (GPIO 5, Active-High by default, configurable)

### Flashing via `arduino-cli`
Run the automated flashing script:
```bash
./flash_esp8266_d1.sh [/dev/ttyUSB0]
```

### Serial Testing (Linux / Ubuntu Terminal)
First configure the serial port:
```bash
stty -F /dev/ttyUSB0 9600
```

#### Terminal / CLI Commands:
* **Light ON**: `echo -n "1" > /dev/ttyUSB0`
* **Light OFF**: `echo -n "0" > /dev/ttyUSB0`
* **Valve Open / ON**: `echo -n "v" > /dev/ttyUSB0`
* **Valve Close / OFF**: `echo -n "c" > /dev/ttyUSB0`
* **Query Status**: `echo -n "?" > /dev/ttyUSB0` (returns `STATE:LIGHT=<ON|OFF>,VALVE=<ON|OFF>`)
* **Diagnostic Blink**: `echo -n "b" > /dev/ttyUSB0` (blinks onboard LED 5 times)

#### JSON API Commands:
* **Hello Handshake**:
  ```bash
  echo '{"type":"hello"}' > /dev/ttyUSB0
  ```
* **Turn Light ON**:
  ```bash
  echo '{"command_id":"cmd-1","action":"LIGHTS_ON"}' > /dev/ttyUSB0
  ```
* **Turn Light OFF**:
  ```bash
  echo '{"command_id":"cmd-2","action":"LIGHTS_OFF"}' > /dev/ttyUSB0
  ```
* **Open Valve**:
  ```bash
  echo '{"command_id":"cmd-3","action":"VALVE_ON"}' > /dev/ttyUSB0
  ```
* **Close Valve**:
  ```bash
  echo '{"command_id":"cmd-4","action":"VALVE_OFF"}' > /dev/ttyUSB0
  ```
* **Status Query**:
  ```bash
  echo '{"command_id":"cmd-5","action":"STATUS"}' > /dev/ttyUSB0
  ```

---

## 2. Arduino Nano Firmware (`nano_firmware`)

### Hardware Pin Mapping
* Pins **D2 through D13** and **A0 through A5** (digital 14–19) driven simultaneously.

### Flashing via `arduino-cli`
```bash
./flash_nano.sh [/dev/ttyUSB0]
```

### Serial Testing
```bash
stty -F /dev/ttyUSB0 9600
echo -n "1" > /dev/ttyUSB0  # Turn ON
echo -n "0" > /dev/ttyUSB0  # Turn OFF
```