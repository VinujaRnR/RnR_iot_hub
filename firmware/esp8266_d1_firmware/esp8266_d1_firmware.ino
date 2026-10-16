/*
 * RnR Linux IoT Hub - ESP8266 D1 Smart Firmware
 * File: firmware/esp8266_d1_firmware/esp8266_d1_firmware.ino
 *
 * Target Board: ESP8266 D1 / D1 Mini (or generic ESP8266)
 *
 * Channel Mapping:
 * - Light: Relay on D5 (GPIO 14), polarity set by RELAY_ACTIVE_LOW
 *          Mirrored by the indicator LED on LED_BUILTIN (GPIO 2 / D4),
 *          polarity set by LIGHT_ACTIVE_LOW (Active-Low)
 * - Valve: Mapped to Digital Pin D1 (GPIO 5), configurable Active-High / Active-Low
 *
 * Protocol & Features:
 * - Hello Handshake: Identifies node ID, board, firmware version, channels, and capabilities
 * - Unique Node ID: "98a316e6b658" (matches hub device alias & schedule)
 * - Supported Channels: ["relay:light", "relay:valve"]
 * - Supported Actions: ["LIGHTS_ON", "LIGHTS_OFF", "VALVE_ON", "VALVE_OFF", "VALVE_OPEN", "VALVE_CLOSE", "STATUS"]
 * - JSON Command Parsing: Stack-based string inspection (zero dynamic heap allocations)
 * - Physical Execution: Drives the light relay, indicator LED and Valve digital pins with independent logic polarities
 * - EEPROM Persistence: Persists both light and valve states across reboots using ESP8266 flash EEPROM emulation
 * - Periodic Reboot: Automatically restarts the ESP8266 every 1 hour (state restored seamlessly from EEPROM)
 * - Duplicate Command Handling: Deduplicates matching command IDs
 * - Backward Compatibility: Accepts single-char & CLI commands:
 *     '1' / '0' -> Light ON / OFF
 *     'v' / 'c' -> Valve ON (open) / OFF (close)
 *     '?'       -> Query states (LIGHT & VALVE)
 *     'b'       -> Diagnostic blink (indicator LED 5 times, light relay unaffected)
 *     "reboot"  -> Trigger instant software restart
 * - Watchdog Safe: Yields control in main loop to prevent ESP8266 WDT resets
 */

#include <EEPROM.h>

// =====================================================================
// Hardware Pin Configuration
// =====================================================================
// On ESP8266 D1 / D1 Mini:
// LED_BUILTIN is typically mapped to GPIO 2 (D4).
#ifndef LED_BUILTIN
  #define LED_BUILTIN 2
#endif
const uint8_t PIN_INDICATOR = LED_BUILTIN;

// Light relay connected to D5 (GPIO 14)
const uint8_t PIN_LIGHT = 14;

// Valve connected to Digital Pin D1 (GPIO 5 on ESP8266 D1 / D1 Mini)
#ifdef D1
  const uint8_t PIN_VALVE = D1;
#else
  const uint8_t PIN_VALVE = 5; // GPIO 5 fallback
#endif

// Logic polarities:
// ESP8266 onboard LED (PIN_INDICATOR) is typically Active-Low (LOW = ON, HIGH = OFF)
const bool LIGHT_ACTIVE_LOW = true;

// Light relay (PIN_LIGHT) logic: false = Active-High module (HIGH = ON, LOW = OFF), true = Active-Low (LOW = ON)
const bool RELAY_ACTIVE_LOW = false;

// Valve relay logic: false = Active-High (HIGH = ON/Open, LOW = OFF/Closed), true = Active-Low
const bool VALVE_ACTIVE_LOW = false;

// =====================================================================
// Device & Protocol Configuration
// =====================================================================
const char* NODE_ID        = "98a316e6b658";
const char* FIRMWARE_VER   = "1.2.0";
const char* BOARD_TYPE     = "ESP8266_D1";
const char* CHANNEL_LIGHT  = "relay:light";
const char* CHANNEL_VALVE  = "relay:valve";

// Periodic Reboot Configuration:
// Automatically restarts ESP8266 every 1 hour (3600 seconds) for stability.
// State is preserved in EEPROM and seamlessly restored on reboot.
const unsigned long REBOOT_INTERVAL_MS = 3600000UL; // 1 hour = 3,600,000 ms
unsigned long g_last_boot_time = 0;

// EEPROM layout (ESP8266 uses flash-emulated EEPROM)
const int EEPROM_SIZE        = 64;
const int EEPROM_LIGHT_ADDR  = 0;
const int EEPROM_VALVE_ADDR  = 1;
const int EEPROM_MAGIC_ADDR  = 2;
const byte EEPROM_MAGIC_VAL  = 0x5A; // Sentinel to verify valid initialization

// =====================================================================
// State Variables
// =====================================================================
bool g_light_state = false;
bool g_valve_state = false;
char g_last_cmd_id[32] = "";
char g_rx_buffer[128];
byte g_rx_len = 0;

// =====================================================================
// Physical Actuation & EEPROM Persistence
// =====================================================================
void applyLightState(bool turn_on) {
  g_light_state = turn_on;

  // Indicator LED (mirrors the light state)
  int indicator_level;
  if (LIGHT_ACTIVE_LOW) {
    indicator_level = turn_on ? LOW : HIGH;
  } else {
    indicator_level = turn_on ? HIGH : LOW;
  }
  digitalWrite(PIN_INDICATOR, indicator_level);

  // Light relay (own polarity: RELAY_ACTIVE_LOW = false -> HIGH is ON, LOW is OFF)
  int light_level;
  if (RELAY_ACTIVE_LOW) {
    light_level = turn_on ? LOW : HIGH;
  } else {
    light_level = turn_on ? HIGH : LOW;
  }
  digitalWrite(PIN_LIGHT, light_level);

  Serial.print(F("[DEBUG] Light -> "));
  Serial.print(turn_on ? F("ON") : F("OFF"));
  Serial.print(F(" (Indicator Pin "));
  Serial.print(PIN_INDICATOR);
  Serial.print(F(" = "));
  Serial.print(indicator_level == LOW ? F("LOW") : F("HIGH"));
  Serial.print(F(", Light Pin "));
  Serial.print(PIN_LIGHT);
  Serial.print(F(" = "));
  Serial.print(light_level == LOW ? F("LOW") : F("HIGH"));
  Serial.println(F(")"));

  // Persist state in EEPROM if changed
  if (EEPROM.read(EEPROM_LIGHT_ADDR) != (turn_on ? 1 : 0)) {
    EEPROM.write(EEPROM_LIGHT_ADDR, turn_on ? 1 : 0);
    EEPROM.commit();
    Serial.println(F("[DEBUG] EEPROM Light state updated"));
  }
}

void applyValveState(bool turn_on) {
  g_valve_state = turn_on;
  int pin_level;
  if (VALVE_ACTIVE_LOW) {
    pin_level = turn_on ? LOW : HIGH;
  } else {
    pin_level = turn_on ? HIGH : LOW;
  }
  digitalWrite(PIN_VALVE, pin_level);

  Serial.print(F("[DEBUG] Valve -> "));
  Serial.print(turn_on ? F("ON") : F("OFF"));
  Serial.print(F(" (Pin "));
  Serial.print(PIN_VALVE);
  Serial.print(F(" = "));
  Serial.print(pin_level == LOW ? F("LOW") : F("HIGH"));
  Serial.println(F(")"));

  // Persist state in EEPROM if changed
  if (EEPROM.read(EEPROM_VALVE_ADDR) != (turn_on ? 1 : 0)) {
    EEPROM.write(EEPROM_VALVE_ADDR, turn_on ? 1 : 0);
    EEPROM.commit();
    Serial.println(F("[DEBUG] EEPROM Valve state updated"));
  }
}

void initEEPROM() {
  Serial.println(F("[DEBUG] Initializing EEPROM..."));
  EEPROM.begin(EEPROM_SIZE);
  byte magic = EEPROM.read(EEPROM_MAGIC_ADDR);
  Serial.print(F("[DEBUG] Read EEPROM Magic: 0x"));
  Serial.println(magic, HEX);

  if (magic != EEPROM_MAGIC_VAL) {
    // Uninitialized flash / first boot: set safe defaults (both OFF)
    Serial.println(F("[DEBUG] EEPROM uninitialized, saving defaults (Light=OFF, Valve=OFF)..."));
    EEPROM.write(EEPROM_LIGHT_ADDR, 0);
    EEPROM.write(EEPROM_VALVE_ADDR, 0);
    EEPROM.write(EEPROM_MAGIC_ADDR, EEPROM_MAGIC_VAL);
    EEPROM.commit();
    g_light_state = false;
    g_valve_state = false;
  } else {
    g_light_state = (EEPROM.read(EEPROM_LIGHT_ADDR) == 1);
    g_valve_state = (EEPROM.read(EEPROM_VALVE_ADDR) == 1);
    Serial.print(F("[DEBUG] EEPROM restored: Light="));
    Serial.print(g_light_state ? F("ON") : F("OFF"));
    Serial.print(F(", Valve="));
    Serial.println(g_valve_state ? F("ON") : F("OFF"));
  }
}

// Diagnostic blink only toggles the indicator LED; the light relay is left untouched.
void blinkDiagnostic() {
  Serial.println(F("[DEBUG] Starting 5-blink diagnostic routine..."));
  int on_level  = LIGHT_ACTIVE_LOW ? LOW : HIGH;
  int off_level = LIGHT_ACTIVE_LOW ? HIGH : LOW;
  for (int i = 0; i < 5; i++) {
    digitalWrite(PIN_INDICATOR, on_level);
    delay(400);
    digitalWrite(PIN_INDICATOR, off_level);
    delay(400);
  }
  // Restore indicator to the saved light state (light relay was never changed)
  digitalWrite(PIN_INDICATOR, g_light_state ? on_level : off_level);
  Serial.println(F("[DEBUG] Diagnostic blink complete, restored saved state."));
}

// =====================================================================
// Protocol Message Responses
// =====================================================================
void sendHello() {
  Serial.print(F("{\"type\":\"hello\",\"device_id\":\""));
  Serial.print(NODE_ID);
  Serial.print(F("\",\"board\":\""));
  Serial.print(BOARD_TYPE);
  Serial.print(F("\",\"firmware_version\":\""));
  Serial.print(FIRMWARE_VER);
  Serial.print(F("\",\"channels\":[\""));
  Serial.print(CHANNEL_LIGHT);
  Serial.print(F("\",\""));
  Serial.print(CHANNEL_VALVE);
  Serial.print(F("\"],\"actions\":[\"LIGHTS_ON\",\"LIGHTS_OFF\",\"VALVE_ON\",\"VALVE_OFF\",\"VALVE_OPEN\",\"VALVE_CLOSE\",\"STATUS\",\"REBOOT\"],"));
  Serial.print(F("\"state\":\""));
  Serial.print(g_light_state ? F("ON") : F("OFF"));
  Serial.print(F("\",\"states\":{\"relay:light\":\""));
  Serial.print(g_light_state ? F("ON") : F("OFF"));
  Serial.print(F("\",\"relay:valve\":\""));
  Serial.print(g_valve_state ? F("ON") : F("OFF"));
  Serial.println(F("\"}}"));
}

void sendResult(const char* cmd_id, const char* status, const char* action_name) {
  Serial.print(F("{\"type\":\"result\",\"command_id\":\""));
  Serial.print(cmd_id);
  Serial.print(F("\",\"device_id\":\""));
  Serial.print(NODE_ID);
  Serial.print(F("\",\"status\":\""));
  Serial.print(status);
  Serial.print(F("\",\"action\":\""));
  Serial.print(action_name);
  Serial.print(F("\",\"state\":\""));
  Serial.print(g_light_state ? F("ON") : F("OFF"));
  Serial.print(F("\",\"result\":{\"relay:light\":\""));
  Serial.print(g_light_state ? F("ON") : F("OFF"));
  Serial.print(F("\",\"relay:valve\":\""));
  Serial.print(g_valve_state ? F("ON") : F("OFF"));
  Serial.println(F("\"}}"));
}

// =====================================================================
// Simple & Lightweight Substring Matcher (Zero dynamic memory allocations)
// =====================================================================
bool extractField(const char* json, const char* key, char* output, int max_len) {
  output[0] = '\0';
  char needle[32];
  snprintf(needle, sizeof(needle), "\"%s\"", key);
  const char* pos = strstr(json, needle);
  if (!pos) return false;

  pos += strlen(needle);
  // Skip whitespace and colon
  while (*pos == ' ' || *pos == ':') pos++;

  // Check if quoted string or unquoted
  bool in_quotes = (*pos == '\"');
  if (in_quotes) pos++;

  int out_idx = 0;
  while (*pos && out_idx < (max_len - 1)) {
    if (in_quotes && *pos == '\"') break;
    if (!in_quotes && (*pos == ',' || *pos == '}' || *pos == ' ' || *pos == '\r' || *pos == '\n')) break;
    output[out_idx++] = *pos++;
  }
  output[out_idx] = '\0';
  return (out_idx > 0);
}

// =====================================================================
// Command Processor
// =====================================================================
void processBuffer(const char* line) {
  Serial.print(F("[DEBUG] Received packet: \""));
  Serial.print(line);
  Serial.println(F("\""));

  // 1. Single-character & CLI commands for terminal debugging
  // Light controls
  if (strcmp(line, "1") == 0 || strcasecmp(line, "on") == 0 || strcasecmp(line, "light_on") == 0) {
    Serial.println(F("[DEBUG] Command matched: LIGHTS_ON (CLI)"));
    applyLightState(true);
    Serial.println(F("OK:LIGHTS_ON"));
    return;
  }
  if (strcmp(line, "0") == 0 || strcasecmp(line, "off") == 0 || strcasecmp(line, "light_off") == 0) {
    Serial.println(F("[DEBUG] Command matched: LIGHTS_OFF (CLI)"));
    applyLightState(false);
    Serial.println(F("OK:LIGHTS_OFF"));
    return;
  }

  // Valve controls ('v' = Valve Open/ON, 'c' = Valve Close/OFF)
  if (strcasecmp(line, "v") == 0 || strcasecmp(line, "valve_on") == 0 || strcasecmp(line, "valve_open") == 0) {
    Serial.println(F("[DEBUG] Command matched: VALVE_ON (CLI)"));
    applyValveState(true);
    Serial.println(F("OK:VALVE_ON"));
    return;
  }
  if (strcasecmp(line, "c") == 0 || strcasecmp(line, "valve_off") == 0 || strcasecmp(line, "valve_close") == 0) {
    Serial.println(F("[DEBUG] Command matched: VALVE_OFF (CLI)"));
    applyValveState(false);
    Serial.println(F("OK:VALVE_OFF"));
    return;
  }

  // Status query
  if (strcmp(line, "?") == 0 || strcasecmp(line, "status") == 0) {
    Serial.println(F("[DEBUG] Command matched: STATUS query (CLI)"));
    Serial.print(F("STATE:LIGHT="));
    Serial.print(g_light_state ? F("ON") : F("OFF"));
    Serial.print(F(",VALVE="));
    Serial.print(g_valve_state ? F("ON") : F("OFF"));
    unsigned long elapsed_ms = millis() - g_last_boot_time;
    unsigned long uptime_sec = elapsed_ms / 1000UL;
    unsigned long next_reboot_sec = (REBOOT_INTERVAL_MS > elapsed_ms) ? ((REBOOT_INTERVAL_MS - elapsed_ms) / 1000UL) : 0;
    Serial.print(F(",UPTIME="));
    Serial.print(uptime_sec);
    Serial.print(F("s,NEXT_REBOOT="));
    Serial.print(next_reboot_sec);
    Serial.println(F("s"));
    return;
  }

  // Reboot command (CLI)
  if (strcasecmp(line, "reboot") == 0 || strcasecmp(line, "restart") == 0) {
    Serial.println(F("[DEBUG] Command matched: REBOOT (CLI)"));
    Serial.println(F("OK:REBOOTING"));
    Serial.flush();
    delay(100);
    ESP.restart();
    return;
  }

  // Diagnostic blink (indicator LED)
  if (strcasecmp(line, "b") == 0 || strcasecmp(line, "blink") == 0) {
    Serial.println(F("[DEBUG] Command matched: BLINK (CLI)"));
    Serial.println(F("OK:BLINKING_5_TIMES"));
    blinkDiagnostic();
    Serial.println(F("OK:BLINK_COMPLETE"));
    return;
  }

  // 2. Hello / Handshake
  if (strstr(line, "handshake") != NULL || strstr(line, "hello") != NULL || strstr(line, "HELLO") != NULL) {
    Serial.println(F("[DEBUG] Handshake / Hello requested"));
    sendHello();
    return;
  }

  // 3. JSON Command Message Parsing
  char cmd_id[32]  = "";
  char action[32]  = "";
  char channel[32] = "";

  extractField(line, "command_id", cmd_id, sizeof(cmd_id));
  if (cmd_id[0] == '\0') {
    extractField(line, "action_id", cmd_id, sizeof(cmd_id));
  }
  if (cmd_id[0] == '\0') {
    strcpy(cmd_id, "cmd-esp8266");
  }

  extractField(line, "action", action, sizeof(action));
  if (action[0] == '\0') {
    extractField(line, "cmd", action, sizeof(action));
  }

  extractField(line, "channel", channel, sizeof(channel));

  Serial.print(F("[DEBUG] Parsed JSON -> cmd_id: '"));
  Serial.print(cmd_id);
  Serial.print(F("', action: '"));
  Serial.print(action);
  Serial.print(F("', channel: '"));
  Serial.print(channel);
  Serial.println(F("'"));

  // Duplicate command suppression: if same command_id received, ack immediately
  if (cmd_id[0] != '\0' && strcmp(cmd_id, g_last_cmd_id) == 0) {
    Serial.println(F("[DEBUG] Duplicate command ID detected - sending fast ACK"));
    sendResult(cmd_id, "SUCCESS", action);
    return;
  }

  // Action: Light Control
  if (strcasecmp(action, "LIGHTS_ON") == 0 || strcasecmp(action, "LIGHT_ON") == 0 ||
      (strcasecmp(action, "ON") == 0 && (strcmp(channel, "") == 0 || strcasecmp(channel, CHANNEL_LIGHT) == 0 || strcasecmp(channel, "light") == 0))) {
    Serial.println(F("[DEBUG] Executing action: LIGHTS_ON"));
    applyLightState(true);
    strncpy(g_last_cmd_id, cmd_id, sizeof(g_last_cmd_id) - 1);
    sendResult(cmd_id, "SUCCESS", "LIGHTS_ON");
    Serial.println(F("OK:LIGHTS_ON"));

  } else if (strcasecmp(action, "LIGHTS_OFF") == 0 || strcasecmp(action, "LIGHT_OFF") == 0 ||
             (strcasecmp(action, "OFF") == 0 && (strcmp(channel, "") == 0 || strcasecmp(channel, CHANNEL_LIGHT) == 0 || strcasecmp(channel, "light") == 0))) {
    Serial.println(F("[DEBUG] Executing action: LIGHTS_OFF"));
    applyLightState(false);
    strncpy(g_last_cmd_id, cmd_id, sizeof(g_last_cmd_id) - 1);
    sendResult(cmd_id, "SUCCESS", "LIGHTS_OFF");
    Serial.println(F("OK:LIGHTS_OFF"));

  // Action: Valve Control
  } else if (strcasecmp(action, "VALVE_ON") == 0 || strcasecmp(action, "VALVE_OPEN") == 0 ||
             (strcasecmp(action, "ON") == 0 && (strcasecmp(channel, CHANNEL_VALVE) == 0 || strcasecmp(channel, "valve") == 0))) {
    Serial.println(F("[DEBUG] Executing action: VALVE_ON"));
    applyValveState(true);
    strncpy(g_last_cmd_id, cmd_id, sizeof(g_last_cmd_id) - 1);
    sendResult(cmd_id, "SUCCESS", "VALVE_ON");
    Serial.println(F("OK:VALVE_ON"));

  } else if (strcasecmp(action, "VALVE_OFF") == 0 || strcasecmp(action, "VALVE_CLOSE") == 0 ||
             (strcasecmp(action, "OFF") == 0 && (strcasecmp(channel, CHANNEL_VALVE) == 0 || strcasecmp(channel, "valve") == 0))) {
    Serial.println(F("[DEBUG] Executing action: VALVE_OFF"));
    applyValveState(false);
    strncpy(g_last_cmd_id, cmd_id, sizeof(g_last_cmd_id) - 1);
    sendResult(cmd_id, "SUCCESS", "VALVE_OFF");
    Serial.println(F("OK:VALVE_OFF"));

  // Action: Status Query
  } else if (strcasecmp(action, "STATUS") == 0) {
    Serial.println(F("[DEBUG] Executing action: STATUS query"));
    sendResult(cmd_id, "SUCCESS", "STATUS");

  // Action: Reboot / Restart
  } else if (strcasecmp(action, "REBOOT") == 0 || strcasecmp(action, "RESTART") == 0) {
    Serial.println(F("[DEBUG] Executing action: REBOOT"));
    sendResult(cmd_id, "SUCCESS", "REBOOT");
    Serial.println(F("OK:REBOOTING"));
    Serial.flush();
    delay(100);
    ESP.restart();
    return;

  } else {
    Serial.print(F("[DEBUG] Unrecognized action: '"));
    Serial.print(action);
    Serial.println(F("' - reporting FAILED"));
    sendResult(cmd_id, "FAILED", action);
  }
}

// =====================================================================
// Arduino Setup & Loop
// =====================================================================
void setup() {
  Serial.begin(9600);
  delay(150);

  Serial.println();
  Serial.println(F("========================================"));
  Serial.println(F("[DEBUG] RnR IoT Hub - ESP8266 D1 Firmware"));
  Serial.print(F("[DEBUG] Board Model: ")); Serial.println(BOARD_TYPE);
  Serial.print(F("[DEBUG] Hardware ID (Node): ")); Serial.println(NODE_ID);
  Serial.print(F("[DEBUG] Firmware Version: ")); Serial.println(FIRMWARE_VER);
  Serial.print(F("[DEBUG] Indicator Pin: ")); Serial.print(PIN_INDICATOR);
  Serial.print(F(" (Active-")); Serial.print(LIGHT_ACTIVE_LOW ? F("LOW") : F("HIGH"));
  Serial.print(F("), Light Pin: ")); Serial.print(PIN_LIGHT);
  Serial.print(F(" (Active-")); Serial.print(RELAY_ACTIVE_LOW ? F("LOW") : F("HIGH"));
  Serial.print(F("), Valve Pin: ")); Serial.print(PIN_VALVE);
  Serial.print(F(" (Active-")); Serial.print(VALVE_ACTIVE_LOW ? F("LOW") : F("HIGH"));
  Serial.println(F(")"));
  Serial.println(F("========================================"));

  // Restore persisted states first, so the light pin can be set to its
  // final level before it becomes an output (keeps the relay from clicking).
  initEEPROM();

  int initial_light_level = RELAY_ACTIVE_LOW ? (g_light_state ? LOW : HIGH) : (g_light_state ? HIGH : LOW);
  digitalWrite(PIN_LIGHT, initial_light_level);
  pinMode(PIN_LIGHT, OUTPUT);
  pinMode(PIN_INDICATOR, OUTPUT);
  pinMode(PIN_VALVE, OUTPUT);

  applyLightState(g_light_state);
  applyValveState(g_valve_state);

  g_last_boot_time = millis();
  Serial.println(F("[DEBUG] Periodic 1-hour auto-reboot armed (interval: 3600000 ms)."));
  Serial.println(F("[DEBUG] Initialization complete. Listening for commands on Serial 9600 baud..."));
}

void loop() {
  while (Serial.available() > 0) {
    char c = (char)Serial.read();
    if (c == '\r' || c == '\n') {
      if (g_rx_len > 0) {
        g_rx_buffer[g_rx_len] = '\0';
        processBuffer(g_rx_buffer);
        g_rx_len = 0;
      }
    } else {
      if (g_rx_len < (sizeof(g_rx_buffer) - 1)) {
        g_rx_buffer[g_rx_len++] = c;
      } else {
        Serial.println(F("[DEBUG] WARNING: RX buffer overflow, resetting line"));
        g_rx_len = 0;
      }
    }
  }

  // Yield to allow ESP8266 background system tasks and prevent watchdog reset
  // Periodic auto-reboot check (every 1 hour = 3600000 ms)
  if (millis() - g_last_boot_time >= REBOOT_INTERVAL_MS) {
    Serial.println(F("[DEBUG] Periodic 1-hour reboot timer triggered. Restarting ESP8266..."));
    Serial.flush();
    delay(100);
    ESP.restart();
  }

  yield();
}