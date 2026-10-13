/*
 * RnR Linux IoT Hub - Arduino Nano Smart Firmware
 * File: firmware/nano_firmware/nano_firmware.ino
 *
 * Implements Step 10 specification for Arduino Nano:
 * - Hello Handshake: Identifies node ID, channels, actions, and capabilities
 * - Unique Node ID: "98a316e6b658" (matches hub device alias & schedule)
 * - Supported Channels: ["relay:light"]
 * - Supported Actions: ["LIGHTS_ON", "LIGHTS_OFF", "STATUS"]
 * - JSON Command Parsing: Parses newline-delimited JSON commands
 * - Physical Command Execution: Universal pin drive (D2-D13, A0-A5) with configurable Active-Low/High logic
 * - Execution Acknowledgments: Structured JSON result + OK:<action> for backward compatibility
 * - Duplicate Command Handling: Deduplicates matching command IDs
 * - State Exposure: Real-time query of actual relay/LED state
 * - Backward Compatibility: Accepts single-char commands ('1', '0', 'B', '?')
 */

#include <EEPROM.h>

// =====================================================================
// Hardware Configuration
// =====================================================================
const char* NODE_ID = "98a316e6b658";
const char* FIRMWARE_VER = "1.0.0";
const char* CHANNEL_KEY = "relay:light";

const int EEPROM_STATE_ADDR = 0;
const int FIRST_PIN = 2;
const int LAST_PIN = 19; // D2 to D13 (13) + A0 to A5 (14 to 19)

// Relay logic: false = Active-High (1 = ON, 0 = OFF), true = Active-Low
const bool RELAY_ACTIVE_LOW = false;

// =====================================================================
// State Variables
// =====================================================================
bool g_light_state = false;
char g_last_cmd_id[32] = "";
char g_rx_buffer[128];
byte g_rx_len = 0;

// =====================================================================
// Physical Pin Driving & EEPROM
// =====================================================================
void applyPhysicalState(bool turn_on) {
  g_light_state = turn_on;
  int pin_level;
  if (RELAY_ACTIVE_LOW) {
    pin_level = turn_on ? LOW : HIGH;
  } else {
    pin_level = turn_on ? HIGH : LOW;
  }

  for (int pin = FIRST_PIN; pin <= LAST_PIN; pin++) {
    digitalWrite(pin, pin_level);
  }

  // Persist state in EEPROM
  if (EEPROM.read(EEPROM_STATE_ADDR) != (turn_on ? 1 : 0)) {
    EEPROM.write(EEPROM_STATE_ADDR, turn_on ? 1 : 0);
  }
}

void blinkDiagnostic() {
  for (int i = 0; i < 5; i++) {
    applyPhysicalState(true);
    delay(400);
    applyPhysicalState(false);
    delay(400);
  }
  // Restore saved state
  byte saved = EEPROM.read(EEPROM_STATE_ADDR);
  applyPhysicalState(saved == 1);
}

// =====================================================================
// Protocol Message Responses
// =====================================================================
void sendHello() {
  Serial.print(F("{\"type\":\"hello\",\"device_id\":\""));
  Serial.print(NODE_ID);
  Serial.print(F("\",\"firmware_version\":\""));
  Serial.print(FIRMWARE_VER);
  Serial.print(F("\",\"channels\":[\""));
  Serial.print(CHANNEL_KEY);
  Serial.print(F("\"],\"actions\":[\"LIGHTS_ON\",\"LIGHTS_OFF\",\"STATUS\"],\"state\":\""));
  Serial.print(g_light_state ? F("ON") : F("OFF"));
  Serial.println(F("\"}"));
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
  // 1. Single-character legacy commands for terminal & CLI tools
  if (strcmp(line, "1") == 0 || strcasecmp(line, "on") == 0) {
    applyPhysicalState(true);
    Serial.println(F("OK:LIGHTS_ON"));
    return;
  }
  if (strcmp(line, "0") == 0 || strcasecmp(line, "off") == 0) {
    applyPhysicalState(false);
    Serial.println(F("OK:LIGHTS_OFF"));
    return;
  }
  if (strcmp(line, "?") == 0 || strcasecmp(line, "status") == 0) {
    Serial.print(F("STATE:"));
    Serial.println(g_light_state ? F("ON") : F("OFF"));
    return;
  }
  if (strcasecmp(line, "b") == 0 || strcasecmp(line, "blink") == 0) {
    Serial.println(F("OK:BLINKING_5_TIMES"));
    blinkDiagnostic();
    Serial.println(F("OK:BLINK_COMPLETE"));
    return;
  }

  // 2. Hello / Handshake
  if (strstr(line, "handshake") != NULL || strstr(line, "hello") != NULL || strstr(line, "HELLO") != NULL) {
    sendHello();
    return;
  }

  // 3. JSON Command Message Parsing
  char cmd_id[32] = "";
  char action[32] = "";

  extractField(line, "command_id", cmd_id, sizeof(cmd_id));
  if (cmd_id[0] == '\0') {
    extractField(line, "action_id", cmd_id, sizeof(cmd_id));
  }
  if (cmd_id[0] == '\0') {
    strcpy(cmd_id, "cmd-nano");
  }

  extractField(line, "action", action, sizeof(action));
  if (action[0] == '\0') {
    extractField(line, "cmd", action, sizeof(action));
  }

  // Duplicate command suppression: if same command_id received, ack immediately
  if (cmd_id[0] != '\0' && strcmp(cmd_id, g_last_cmd_id) == 0) {
    sendResult(cmd_id, "SUCCESS", action);
    return;
  }

  if (strcasecmp(action, "LIGHTS_ON") == 0 || strcasecmp(action, "ON") == 0) {
    applyPhysicalState(true);
    strncpy(g_last_cmd_id, cmd_id, sizeof(g_last_cmd_id) - 1);
    sendResult(cmd_id, "SUCCESS", "LIGHTS_ON");
    Serial.println(F("OK:LIGHTS_ON"));

  } else if (strcasecmp(action, "LIGHTS_OFF") == 0 || strcasecmp(action, "OFF") == 0) {
    applyPhysicalState(false);
    strncpy(g_last_cmd_id, cmd_id, sizeof(g_last_cmd_id) - 1);
    sendResult(cmd_id, "SUCCESS", "LIGHTS_OFF");
    Serial.println(F("OK:LIGHTS_OFF"));

  } else if (strcasecmp(action, "STATUS") == 0) {
    sendResult(cmd_id, "SUCCESS", "STATUS");

  } else {
    sendResult(cmd_id, "FAILED", action);
  }
}

// =====================================================================
// Arduino Setup & Loop
// =====================================================================
void setup() {
  for (int pin = FIRST_PIN; pin <= LAST_PIN; pin++) {
    pinMode(pin, OUTPUT);
  }

  // Read saved state from EEPROM
  byte saved = EEPROM.read(EEPROM_STATE_ADDR);
  g_light_state = (saved == 1);
  applyPhysicalState(g_light_state);

  Serial.begin(9600);
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
      }
    }
  }
}
