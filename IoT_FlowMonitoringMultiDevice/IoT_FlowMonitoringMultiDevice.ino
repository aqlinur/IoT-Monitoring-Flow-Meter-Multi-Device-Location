// ============================================
// INDUSTRIAL MONITORING SYSTEM - FLOW & POWER
// ============================================
// Features: 
// - RS485 PZEM-016 Power Monitoring (Modbus)
// - YF-B6 Flow Sensor
// - SD Card Data Logging
// - MQTT Cloud Integration
// - OLED Display
// - RTC Time Synchronization
// - Multi-Location Support
// - Daily Volume Reset at 23:59
// ============================================

#include <WiFi.h>
#include <NTPClient.h>
#include <WiFiUdp.h>
#include <PubSubClient.h>
#include <SD.h>
#include <SPI.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <RTClib.h>
#include <ModbusMaster.h>

// ==============================
// INDUSTRIAL CONFIGURATION
// ==============================
#ifndef DEVICE_ID
  #define DEVICE_ID "Plumb-01"
#endif

#ifndef LOCATION_NAME
  #define LOCATION_NAME "PE"
#endif

// ==============================
// HARDWARE CONFIGURATION
// ==============================
// OLED Configuration
#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
#define OLED_RESET -1
#define SCREEN_ADDRESS 0x3C 
Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);

// RTC DS1307 Configuration
RTC_DS1307 rtc;
WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "pool.ntp.org", 7 * 3600);

// RS485 PZEM-016 Configuration
#define RS485_RX_PIN 16     // GPIO16 for RX
#define RS485_TX_PIN 17     // GPIO17 for TX
#define RS485_DE_RE_PIN 4   // GPIO4 for DE & RE Control

// Modbus for PZEM-016
ModbusMaster node;
const uint8_t PZEM_ADDRESS = 0x01;

// Register addresses for PZEM-016
#define PZEM_VOLTAGE_REG     0x0000
#define PZEM_CURRENT_REG     0x0002  
#define PZEM_POWER_REG       0x0004
#define PZEM_ENERGY_REG      0x0006
#define PZEM_FREQUENCY_REG   0x0008
#define PZEM_PF_REG          0x000A

// Pin Configuration (Industrial Grade)
#ifndef FLOW_SENSOR_PIN
  #define FLOW_SENSOR_PIN 12
#endif

#ifndef SD_CHIP_SELECT
  #define SD_CHIP_SELECT 5
#endif

#ifndef BUTTON_FLOW_PIN
  #define BUTTON_FLOW_PIN 14
#endif

#ifndef BUTTON_POWER_PIN
  #define BUTTON_POWER_PIN 27
#endif

// ==============================
// NETWORK CONFIGURATION
// ==============================
const char* ssid = ""; //add wifi SSID
const char* password = ""; //add Password wifi
const char* mqtt_server = ""; //if local mosquitto use ipv4 addres 
const int mqtt_port = 1883;

// ==============================
// INDUSTRIAL VARIABLES
// ==============================
String device_id = DEVICE_ID;
String location_name = LOCATION_NAME;
String mqtt_topic_pub = "industry/sensor/data/" + device_id;
String mqtt_topic_sub = "industry/sensor/control/" + device_id;
const char* mqtt_topic_command = "industry/command/all";

// Hardware Pins
int flowSensorPin = FLOW_SENSOR_PIN;
int sdChipSelect = SD_CHIP_SELECT;
int buttonFlowPin = BUTTON_FLOW_PIN;
int buttonPowerPin = BUTTON_POWER_PIN;
int rs485RxPin = RS485_RX_PIN;
int rs485TxPin = RS485_TX_PIN;
int rs485DeRePin = RS485_DE_RE_PIN;

// ==============================
// DAILY VOLUME RESET CONFIGURATION
// ==============================
// Daily Volume Tracking
float dailyVolume = 0.0;
float previousDayVolume = 0.0;
String lastResetDate = "";
bool dailyResetPending = false;
bool dailyVolumeSaved = false;
const int DAILY_RESET_HOUR = 23;
const int DAILY_RESET_MINUTE = 59;
const int DAILY_RESET_SECOND = 30;

// ==============================
// INDUSTRIAL INTERVALS (milliseconds)
// ==============================
unsigned long sdWriteInterval = 10000;        // 10 seconds
unsigned long sdReadInterval = 30000;         // 30 seconds
unsigned long mqttPublishInterval = 1000;     // 1 seconds
unsigned long oledUpdateInterval = 1000;      // 1 second
unsigned long flowCalcInterval = 1000;        // 1 second
unsigned long powerCalcInterval = 1000;       // 1 seconds
unsigned long flowBackupInterval = 30000;     // 30 seconds
unsigned long powerBackupInterval = 60000;    // 60 seconds
unsigned long pzemRetryInterval = 30000;       // 30 seconds for PZEM retry
unsigned long dailyResetCheckInterval = 1000; // 1 second for checking daily reset

// Timing Variables
unsigned long lastSDWriteTime = 0;
unsigned long lastSDReadTime = 0;
unsigned long lastMQTTPublishTime = 0;
unsigned long lastOLEDUpdate = 0;
unsigned long lastFlowBackupTime = 0;
unsigned long lastPowerBackupTime = 0;
unsigned long lastPowerCalcTime = 0;
unsigned long lastPzemRetryTime = 0;
unsigned long lastDailyResetCheck = 0;

// Industrial Limits
const unsigned long MIN_INTERVAL = 1000;      // 1 second minimum
const unsigned long MAX_INTERVAL = 3600000;   // 1 hour maximum

// ==============================
// SENSOR VARIABLES
// ==============================
volatile unsigned long pulseCount = 0;
float calibrationFactor = 6.0;
float flowRate = 0.0;
float totalVolume = 0.0;
unsigned long oldTime = 0;

// Display Mode
enum DisplayMode { FLOW_MONITORING, POWER_MONITORING };
DisplayMode currentDisplayMode = FLOW_MONITORING;

// PZEM-016 Variables (REAL DATA ONLY)
bool pzemAvailable = false;
bool pzemInitialized = false;
float voltage = 0.0;
float current = 0.0;
float power = 0.0;
float energy = 0.0;
float frequency = 0.0;
float powerFactor = 0.0;
unsigned long lastEnergyUpdate = 0;

// System Status Flags
bool sdCardAvailable = false;
bool wifiConnected = false;
bool mqttConnected = false;
bool systemInitialized = false;
bool oledAvailable = false;
bool rtcAvailable = false;
String systemStatus = "Initializing...";
String sensorType = "YF-B6";

// Network Variables
WiFiClient espClient;
PubSubClient client(espClient);
unsigned long lastWiFiReconnectAttempt = 0;
unsigned long lastMQTTReconnectAttempt = 0;
unsigned long lastMQTTSuccess = 0;
int mqttConnectionAttempts = 0;
const int MAX_MQTT_ATTEMPTS = 10;
const unsigned long wifiReconnectInterval = 10000;
const unsigned long mqttReconnectInterval = 5000;

// Data Buffer for Offline Operation
const int MAX_UNSENT_DATA = 100;  // Increased for industrial use
String unsentData[MAX_UNSENT_DATA];
int unsentDataCount = 0;
int unsentDataIndex = 0;

// Month Names
const char* monthNames[12] = {
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
};

// YF-B6 Calibration Factors
const float YFB6_CALIBRATION_FACTORS[3] = {4.5, 6.0, 7.5};
int currentCalibrationIndex = 1;

// Backup Variables
float lastBackupVolume = 0.0;
float lastBackupEnergy = 0.0;

// Configuration from SD Card
String configDeviceID = "";
String configLocation = "";
String configMqttPrefix = "";
int configFlowPin = 34;
int configButtonFlowPin = 14;
int configButtonPowerPin = 27;
int configRS485_RX = 16;
int configRS485_TX = 17;
int configRS485_DE_RE = 4;

// ==============================
// FORWARD DECLARATIONS
// ==============================
void IRAM_ATTR pulseCounter();
void initializeOLED();
void initializeRTC();
void initializeSDCard();
bool initializePZEM();
bool loadConfigFromSD();
void setupWiFi();
bool checkWiFiStatus();
void reconnectWiFi();
void reconnectMQTT();
void callback(char* topic, byte* payload, unsigned int length);
void calculateFlowRate();
bool readPowerFromPZEM();
void calculatePowerData();
void publishData(float flowRate, float volume, float power, float energy);
void saveToSD(float flowRate, float volume, float power, float energy);
void readSDLog();
void updateOLED();
String getRTCTime();
String getRTCDate();
String getFormattedDateTime();
void changeCalibrationFactor(int index);
void addToUnsentData(const String& data);
void processUnsentData();
bool isMQTTReallyConnected();
void sendSystemStatus();
void backupFlowData();
void recoverFlowData();
void backupPowerData();
void recoverPowerData();
unsigned long parseTimeString(String timeStr);
void handleDeviceSpecificCommand(String message);
void handleGlobalCommand(String message);
void saveIntervalConfig();
void loadIntervalConfig();
void sendIntervalStatus();
void setSDWriteInterval(unsigned long newInterval);
void setSDReadInterval(unsigned long newInterval);
void setMQTTPublishInterval(unsigned long newInterval);
void setAllIntervals(unsigned long sdWrite, unsigned long sdRead, unsigned long mqttPub);
void switchDisplayMode(DisplayMode mode);
void checkButtons();
void mqttDebugInfo();
void loadDeviceConfig();
void applyDeviceConfig();
void createDefaultConfig();
bool resetPZEMEnergy();
void retryPZEMConnection();
void sendPZEMStatus();
void preTransmission();
void postTransmission();
void syncRTCWithNTP();
void logError(String errorMessage);
void logWarning(String warningMessage);
void logInfo(String infoMessage);

// Daily Volume Management Functions
void checkDailyReset();
void saveDailyVolume();
void saveDailyVolumeToSD(String date, float volume);
void loadLastDailyVolume();
void resetDailyVolume();
void checkForMissedReset();

// ==============================
// ERROR LOGGING SYSTEM
// ==============================
void logError(String errorMessage) {
  Serial.println("[ERROR] " + device_id + " - " + errorMessage);
  if (sdCardAvailable) {
    File errorFile = SD.open("/error_log.txt", FILE_APPEND);
    if (errorFile) {
      errorFile.println(getFormattedDateTime() + " - " + device_id + " - ERROR: " + errorMessage);
      errorFile.close();
    }
  }
}

void logWarning(String warningMessage) {
  Serial.println("[WARNING] " + device_id + " - " + warningMessage);
}

void logInfo(String infoMessage) {
  Serial.println("[INFO] " + device_id + " - " + infoMessage);
}

// ==============================
// INTERRUPT SERVICE ROUTINE
// ==============================
void IRAM_ATTR pulseCounter() {
  pulseCount++;
}

// ==============================
// SETUP FUNCTION
// ==============================
void setup() {
  Serial.begin(115200);
  delay(1000);
  Wire.begin(21,22);
  // Initialize OLED
  initializeOLED();
  
  logInfo("Starting Industrial Monitoring System");
  logInfo("Device: " + device_id);
  logInfo("Location: " + location_name);
  
  // Initialize RTC
  initializeRTC();
  
  // Initialize SD Card
  initializeSDCard();
  
  // Load configuration from SD Card
  if (sdCardAvailable) {
    if (!loadConfigFromSD()) {
      logWarning("No config found, creating default");
      createDefaultConfig();
      loadConfigFromSD();
    }
    applyDeviceConfig();
  }
  
  // Initialize buttons
  pinMode(buttonFlowPin, INPUT_PULLUP);
  pinMode(buttonPowerPin, INPUT_PULLUP);
  
  // Initialize RS485 control pin
  pinMode(rs485DeRePin, OUTPUT);
  digitalWrite(rs485DeRePin, LOW);
  
  // Load interval configuration
  if (sdCardAvailable) {
    loadIntervalConfig();
    recoverFlowData();
    recoverPowerData();
    loadLastDailyVolume();  // Load last daily volume from SD
  }
  
  // Initialize flow sensor
  pinMode(flowSensorPin, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(flowSensorPin), pulseCounter, RISING);
  
  // Initialize PZEM-016 (REAL HARDWARE ONLY)
  if (!initializePZEM()) {
    logError("PZEM-016 initialization failed");
  }
  
  // Setup WiFi
  setupWiFi();
  
  // Configure MQTT
  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback);
  client.setKeepAlive(120);
  client.setSocketTimeout(60);
  client.setBufferSize(2048);
  
  // Sync RTC with NTP if WiFi is available
  if (wifiConnected) {
    syncRTCWithNTP();
  }
  
  // Set initial date for daily tracking
  if (rtcAvailable) {
    lastResetDate = getRTCDate();
    logInfo("Daily tracking initialized. Reset date: " + lastResetDate);
    
    // Check for missed reset in case device was restarted
    checkForMissedReset();
  }
  
  // Initialize timing variables
  oldTime = millis();
  lastSDWriteTime = millis();
  lastSDReadTime = millis();
  lastMQTTPublishTime = millis();
  lastWiFiReconnectAttempt = millis();
  lastMQTTReconnectAttempt = millis();
  lastOLEDUpdate = millis();
  lastFlowBackupTime = millis();
  lastPowerBackupTime = millis();
  lastPowerCalcTime = millis();
  lastPzemRetryTime = millis();
  lastDailyResetCheck = millis();
  
  // Initial power reading
  calculatePowerData();
  
  systemInitialized = true;
  
  logInfo("System initialized successfully");
  logInfo("PZEM-016 Status: " + String(pzemAvailable ? "Connected" : "Disconnected"));
  logInfo("Daily Reset Time: 23:59:30");
  logInfo("Current Intervals - SD Write: " + String(sdWriteInterval/1000) + "s, MQTT: " + String(mqttPublishInterval/1000) + "s");
  
  // Initial OLED display
  updateOLED();
}

// ==============================
// LOOP FUNCTION
// ==============================
void loop() {
  unsigned long currentMillis = millis();
  
  // Network management
  if (!checkWiFiStatus()) {
    reconnectWiFi();
  } else {
    if (!isMQTTReallyConnected()) {
      reconnectMQTT();
    } else {
      client.loop();
      processUnsentData();
    }
  }

  // Calculate flow rate
  calculateFlowRate();
  
  // Calculate power data (REAL DATA ONLY)
  calculatePowerData();
  
  // Check button presses
  checkButtons();
  
  // Retry PZEM connection if disconnected
  if (!pzemAvailable && (currentMillis - lastPzemRetryTime >= pzemRetryInterval)) {
    retryPZEMConnection();
    lastPzemRetryTime = currentMillis;
  }
  
  // Check for daily reset at 23:59
  if (currentMillis - lastDailyResetCheck >= dailyResetCheckInterval) {
    checkDailyReset();
    lastDailyResetCheck = currentMillis;
  }
  
  // Publish to MQTT
  if (wifiConnected && mqttConnected && (currentMillis - lastMQTTPublishTime >= mqttPublishInterval)) {
    publishData(flowRate, totalVolume, power, energy);
    lastMQTTPublishTime = currentMillis;
  }
  
  // Save to SD card
  if (sdCardAvailable && (currentMillis - lastSDWriteTime >= sdWriteInterval)) {
    saveToSD(flowRate, totalVolume, power, energy);
    lastSDWriteTime = currentMillis;
  }
  
  // Read SD log periodically
  if (sdCardAvailable && (currentMillis - lastSDReadTime >= sdReadInterval)) {
    readSDLog();
    lastSDReadTime = currentMillis;
  }
  
  // Backup data when offline
  if (sdCardAvailable && !wifiConnected) {
    if (currentMillis - lastFlowBackupTime >= flowBackupInterval) {
      backupFlowData();
      lastFlowBackupTime = currentMillis;
    }
    if (currentMillis - lastPowerBackupTime >= powerBackupInterval) {
      backupPowerData();
      lastPowerBackupTime = currentMillis;
    }
  }
  
  // Update OLED
  if (currentMillis - lastOLEDUpdate >= oledUpdateInterval) {
    updateOLED();
    lastOLEDUpdate = currentMillis;
  }
  
  // System status monitoring
  static unsigned long lastStatusTime = 0;
  if (currentMillis - lastStatusTime >= 60000) {
    lastStatusTime = currentMillis;
    
    logInfo("System Status - Flow: " + String(flowRate, 2) + " L/min, Power: " + String(power, 1) + " W");
    logInfo("Daily Volume: " + String(dailyVolume, 3) + " L, Reset Date: " + lastResetDate);
    logInfo("Network Status - WiFi: " + String(wifiConnected ? "Connected" : "Disconnected") + 
            ", MQTT: " + String(mqttConnected ? "Connected" : "Disconnected"));
    logInfo("Sensor Status - PZEM: " + String(pzemAvailable ? "Connected" : "Disconnected") + 
            ", SD: " + String(sdCardAvailable ? "Available" : "Unavailable"));
  }
  
  delay(10);
}

// ==============================
// OLED FUNCTIONS
// ==============================
void initializeOLED() {
  Wire.begin(21, 22);
  delay(50);

  if (!display.begin(SSD1306_SWITCHCAPVCC, SCREEN_ADDRESS)) {
    oledAvailable = false;
    Serial.println("[ERROR] " + device_id + " - OLED initialization failed!");
    return;
  }

  oledAvailable = true;
  display.clearDisplay();
  display.setTextColor(SSD1306_WHITE);
  display.setTextSize(1);
  display.setCursor(0, 0);
  display.println("Flow MONITOR");
  display.println(device_id);
  display.println(location_name);
  display.println("Daily Reset: 23:59:30");
  display.display();

  Serial.println("[INFO] " + device_id + " - OLED initialized successfully");
}

void updateOLED() {
  if (!oledAvailable) return;
  
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SSD1306_WHITE);

  if (currentDisplayMode == FLOW_MONITORING) {
    // FLOW MONITORING MODE
    display.setCursor(0, 0);
    display.println(device_id + " [FLOW]");

    display.setCursor(0, 10);
    display.println(getRTCTime());

    display.setCursor(0, 20);
    display.print("Flow:");
    display.print(flowRate, 1);
    display.println(" L/m");

    display.setCursor(0, 30);
    display.print("Daily:");
    display.print(dailyVolume, 1);
    display.println(" L");

    display.setCursor(0, 40);
    display.print("Total:");
    display.print(totalVolume, 1);
    display.println(" L");

    display.setCursor(0, 50);
    display.print("Reset:");
    display.print(lastResetDate.substring(0, 5));
    
  } else {
    // POWER MONITORING MODE
    display.setCursor(0, 0);
    display.println(device_id + " [POWER]");

    display.setCursor(0, 10);
    display.println(getRTCTime());

    display.setCursor(0, 20);
    display.print("V:");
    display.print(voltage, 1);
    display.print("V I:");
    display.print(current, 2);
    display.println("A");

    display.setCursor(0, 30);
    display.print("P:");
    display.print(power, 0);
    display.print("W PF:");
    display.print(powerFactor, 2);

    display.setCursor(0, 40);
    display.print("E:");
    display.print(energy, 3);
    display.print("kWh ");
    display.print(frequency, 1);
    display.print("Hz");

    display.setCursor(0, 50);
    display.print("Daily:");
    display.print(dailyVolume, 1);
    display.print("L");
  }
  
  display.display();
}

// ==============================
// RTC FUNCTIONS
// ==============================
void initializeRTC() {
  if (!rtc.begin()) {
    rtcAvailable = false;
    Serial.println("[ERROR] " + device_id + " - RTC initialization failed!");
    return;
  }
  
  rtcAvailable = true;
  
  if (!rtc.isrunning()) {
    logWarning("RTC is not running - Setting time from compile time");
    rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
  }
  
  Serial.println("[INFO] " + device_id + " - RTC initialized successfully");
}

String getRTCTime() {
  if (!rtcAvailable) return "RTC:NA";
  DateTime now = rtc.now();
  char timeString[20];
  sprintf(timeString, "%02d:%02d:%02d", now.hour(), now.minute(), now.second());
  return String(timeString);
}

String getRTCDate() {
  if (!rtcAvailable) return "NO DATE";
  DateTime now = rtc.now();
  char dateString[30];
  sprintf(dateString, "%02d/%02d/%04d", now.day(), now.month(), now.year());
  return String(dateString);
}

String getFormattedDateTime() {
  if (!rtcAvailable) return "RTC UNAVAILABLE";
  DateTime now = rtc.now();
  char dateTimeString[40];
  sprintf(dateTimeString, "%04d-%02d-%02dT%02d:%02d:%02d", 
          now.year(), now.month(), now.day(),
          now.hour(), now.minute(), now.second());
  return String(dateTimeString);
}

void syncRTCWithNTP() {
  if (!wifiConnected) {
    logWarning("Cannot sync NTP - WiFi not connected");
    return;
  }
  
  logInfo("Syncing RTC with NTP");
  
  timeClient.begin();
  if (timeClient.update()) {
    unsigned long epochTime = timeClient.getEpochTime();
    DateTime ntpTime(epochTime);
    rtc.adjust(ntpTime);
    logInfo("RTC synced with NTP: " + getFormattedDateTime());
  } else {
    logError("Failed to sync RTC with NTP");
  }
  timeClient.end();
}

// ==============================
// SD CARD FUNCTIONS
// ==============================
void initializeSDCard() {
  if (!SD.begin(sdChipSelect)) {
    sdCardAvailable = false;
    Serial.println("[ERROR] " + device_id + " - SD Card initialization failed!");
    return;
  }
  
  sdCardAvailable = true;
  uint8_t cardType = SD.cardType();
  
  if (cardType == CARD_NONE) {
    sdCardAvailable = false;
    Serial.println("[ERROR] " + device_id + " - No SD card attached");
    return;
  }
  
  // Create CSV header if file doesn't exist
  if (!SD.exists("/datalog.csv")) {
    File dataFile = SD.open("/datalog.csv", FILE_WRITE);
    if (dataFile) {
      dataFile.println("Timestamp,DeviceID,FlowRate(L/min),TotalVolume(L),DailyVolume(L),Voltage(V),Current(A),Power(W),Energy(kWh),Frequency(Hz),PowerFactor,Status");
      dataFile.close();
    }
  }
  
  // Create daily volume log file if it doesn't exist
  if (!SD.exists("/daily_volume.csv")) {
    File dailyFile = SD.open("/daily_volume.csv", FILE_WRITE);
    if (dailyFile) {
      dailyFile.println("Date,DeviceID,DailyVolume(L),Timestamp");
      dailyFile.close();
    }
  }
  
  Serial.println("[INFO] " + device_id + " - SD Card initialized successfully");
  uint64_t cardSize = SD.cardSize() / (1024 * 1024);
  Serial.println("[INFO] SD Card Size: " + String(cardSize) + "MB");
}

// ==============================
// DAILY VOLUME MANAGEMENT FUNCTIONS
// ==============================
void checkDailyReset() {
  if (!rtcAvailable) return;
  
  DateTime now = rtc.now();
  String currentDate = getRTCDate();
  
  // Check if date has changed (new day)
  if (currentDate != lastResetDate) {
    if (!dailyVolumeSaved && dailyVolume > 0) {
      // Save yesterday's volume before resetting
      saveDailyVolumeToSD(lastResetDate, dailyVolume);
      dailyVolumeSaved = true;
    }
    
    // Reset for new day
    resetDailyVolume();
    lastResetDate = currentDate;
    dailyResetPending = false;
    logInfo("New day detected. Date changed to: " + currentDate);
  }
  
  // Check if it's time for daily reset (23:59:00)
  if (now.hour() == DAILY_RESET_HOUR && 
      now.minute() == DAILY_RESET_MINUTE && 
      now.second() >= DAILY_RESET_SECOND) {
    
    if (!dailyResetPending) {
      dailyResetPending = true;
      logInfo("Daily reset pending at " + getRTCTime());
      
      // Save daily volume 30 seconds before midnight
      if (now.second() >= 30) {
        saveDailyVolume();
      }
    }
  } else {
    dailyResetPending = false;
  }
}

void saveDailyVolume() {
  if (!dailyVolumeSaved && dailyVolume > 0) {
    // Save today's volume
    saveDailyVolumeToSD(lastResetDate, dailyVolume);
    dailyVolumeSaved = true;
    logInfo("Daily volume saved: " + String(dailyVolume, 3) + " L for date: " + lastResetDate);
    
    // Update OLED to show saved status
    updateOLED();
  }
}

void saveDailyVolumeToSD(String date, float volume) {
  if (!sdCardAvailable) return;
  
  File dailyFile = SD.open("/daily_volume.csv", FILE_APPEND);
  if (dailyFile) {
    String dataString = date + "," + device_id + "," + 
                       String(volume, 3) + "," + getFormattedDateTime();
    
    dailyFile.println(dataString);
    dailyFile.close();
    
    logInfo("Daily volume recorded: " + String(volume, 3) + " L on " + date);
  } else {
    logError("Failed to save daily volume to SD");
  }
}

void loadLastDailyVolume() {
  if (!sdCardAvailable) return;
  
  if (!SD.exists("/daily_volume.csv")) {
    logInfo("No daily volume history found");
    return;
  }
  
  File dailyFile = SD.open("/daily_volume.csv", FILE_READ);
  if (!dailyFile) {
    logError("Failed to open daily volume file");
    return;
  }
  
  // Read last line to get last recorded volume
  String lastLine = "";
  while (dailyFile.available()) {
    lastLine = dailyFile.readStringUntil('\n');
  }
  dailyFile.close();
  
  if (lastLine.length() > 0 && !lastLine.startsWith("Date")) {
    // Parse last line: Date,DeviceID,DailyVolume(L),Timestamp
    int firstComma = lastLine.indexOf(',');
    int secondComma = lastLine.indexOf(',', firstComma + 1);
    int thirdComma = lastLine.indexOf(',', secondComma + 1);
    
    if (firstComma != -1 && secondComma != -1 && thirdComma != -1) {
      String savedDate = lastLine.substring(0, firstComma);
      String savedDeviceID = lastLine.substring(firstComma + 1, secondComma);
      String volumeStr = lastLine.substring(secondComma + 1, thirdComma);
      
      if (savedDeviceID == device_id) {
        previousDayVolume = volumeStr.toFloat();
        logInfo("Last daily volume loaded: " + String(previousDayVolume, 3) + " L on " + savedDate);
      }
    }
  }
}

void resetDailyVolume() {
  // Save previous day's volume if not already saved
  if (!dailyVolumeSaved && dailyVolume > 0) {
    saveDailyVolumeToSD(lastResetDate, dailyVolume);
  }
  
  // Reset daily volume
  previousDayVolume = dailyVolume;
  dailyVolume = 0.0;
  dailyVolumeSaved = false;
  
  logInfo("Daily volume reset. Previous: " + String(previousDayVolume, 3) + " L");
  
  // Update OLED
  updateOLED();
}

void checkForMissedReset() {
  if (!rtcAvailable || !sdCardAvailable) return;
  
  DateTime now = rtc.now();
  String currentDate = getRTCDate();
  
  // Check if we have a daily_volume.csv entry for today
  bool todayRecorded = false;
  
  if (SD.exists("/daily_volume.csv")) {
    File dailyFile = SD.open("/daily_volume.csv", FILE_READ);
    while (dailyFile.available()) {
      String line = dailyFile.readStringUntil('\n');
      if (line.startsWith(currentDate + "," + device_id)) {
        todayRecorded = true;
        break;
      }
    }
    dailyFile.close();
  }
  
  // If it's past 00:00 and no record for today, we missed a reset
  if (now.hour() < 12 && !todayRecorded && lastResetDate != currentDate) {
    logWarning("Missed daily reset detected! Performing recovery...");
    
    // Try to save yesterday's volume
    if (dailyVolume > 0) {
      saveDailyVolumeToSD(lastResetDate, dailyVolume);
    }
    
    // Reset for today
    resetDailyVolume();
    lastResetDate = currentDate;
    logInfo("Missed reset recovered. New reset date: " + lastResetDate);
  }
}

// ==============================
// PZEM-016 FUNCTIONS (REAL HARDWARE ONLY)
// ==============================
void preTransmission() {
  digitalWrite(rs485DeRePin, HIGH);
  delay(1);
}

void postTransmission() {
  delay(1);
  digitalWrite(rs485DeRePin, LOW);
}

bool initializePZEM() {
  logInfo("Initializing PZEM-016 RS485 Modbus");
  
  // Initialize Serial2 for RS485 communication
  Serial2.begin(9600, SERIAL_8N1, rs485RxPin, rs485TxPin);
  delay(100);
  
  // Initialize ModbusMaster
  node.begin(PZEM_ADDRESS, Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);
  
  delay(100);
  
  // Test communication by reading voltage
  uint8_t result = node.readHoldingRegisters(PZEM_VOLTAGE_REG, 2);
  
  if (result == node.ku8MBSuccess) {
    pzemAvailable = true;
    pzemInitialized = true;
    
    // Read initial values
    readPowerFromPZEM();
    
    logInfo("PZEM-016 initialized successfully");
    logInfo("Initial Readings - Voltage: " + String(voltage, 1) + "V, Current: " + String(current, 2) + "A");
    return true;
  } else {
    pzemAvailable = false;
    pzemInitialized = false;
    logError("PZEM-016 initialization failed - Modbus Error: " + String(result));
    return false;
  }
}

bool readPowerFromPZEM() {
  if (!pzemInitialized) {
    return false;
  }
  
  // Read all registers at once
  uint8_t result = node.readHoldingRegisters(PZEM_VOLTAGE_REG, 12);
  
  if (result == node.ku8MBSuccess) {
    // Voltage
    uint32_t rawVoltage = (node.getResponseBuffer(0) << 16) | node.getResponseBuffer(1);
    voltage = rawVoltage * 0.1;
    
    // Current
    uint32_t rawCurrent = (node.getResponseBuffer(2) << 16) | node.getResponseBuffer(3);
    current = rawCurrent * 0.001;
    
    // Power
    uint32_t rawPower = (node.getResponseBuffer(4) << 16) | node.getResponseBuffer(5);
    power = rawPower * 0.1;
    
    // Energy
    uint32_t rawEnergy = (node.getResponseBuffer(6) << 16) | node.getResponseBuffer(7);
    energy = rawEnergy * 0.1;
    
    // Frequency
    uint32_t rawFrequency = (node.getResponseBuffer(8) << 16) | node.getResponseBuffer(9);
    frequency = rawFrequency * 0.1;
    
    // Power Factor
    uint32_t rawPF = (node.getResponseBuffer(10) << 16) | node.getResponseBuffer(11);
    powerFactor = rawPF * 0.001;
    
    pzemAvailable = true;
    return true;
  } else {
    pzemAvailable = false;
    logWarning("PZEM communication error - Code: " + String(result));
    return false;
  }
}

void retryPZEMConnection() {
  logInfo("Retrying PZEM-016 connection");
  
  uint8_t result = node.readHoldingRegisters(PZEM_VOLTAGE_REG, 2);
  
  if (result == node.ku8MBSuccess) {
    pzemAvailable = true;
    logInfo("PZEM-016 reconnected successfully");
    readPowerFromPZEM();
  } else {
    logWarning("PZEM-016 still not responding");
  }
}

bool resetPZEMEnergy() {
  if (!pzemAvailable) {
    logWarning("Cannot reset energy - PZEM not connected");
    return false;
  }
  
  // Reset energy by writing 0 to energy register
  uint8_t result = node.writeSingleRegister(PZEM_ENERGY_REG, 0);
  
  if (result == node.ku8MBSuccess) {
    energy = 0;
    lastBackupEnergy = 0;
    logInfo("PZEM energy counter reset successfully");
    return true;
  } else {
    logError("Failed to reset PZEM energy - Error: " + String(result));
    return false;
  }
}

// ==============================
// FLOW SENSOR FUNCTIONS
// ==============================
void calculateFlowRate() {
  unsigned long currentTime = millis();
  
  if (currentTime - oldTime >= flowCalcInterval) {
    noInterrupts();
    unsigned long currentPulseCount = pulseCount;
    pulseCount = 0;
    interrupts();
    
    if (currentPulseCount > 0) {
      float frequency = (float)currentPulseCount / ((float)(currentTime - oldTime) / 1000.0);
      flowRate = (frequency * 60.0) / calibrationFactor;
      
      float timeDiff = (currentTime - oldTime) / 1000.0;
      float volumeIncrement = (flowRate / 60.0) * timeDiff;
      
      // Update both total and daily volume
      totalVolume += volumeIncrement;
      dailyVolume += volumeIncrement;  // Track daily volume separately
    } else {
      flowRate = 0.0;
    }
    
    oldTime = currentTime;
  }
}

// ==============================
// POWER MONITORING FUNCTIONS
// ==============================
void calculatePowerData() {
  unsigned long currentTime = millis();
  
  if (currentTime - lastPowerCalcTime >= powerCalcInterval) {
    if (pzemAvailable) {
      readPowerFromPZEM();
    }
    lastPowerCalcTime = currentTime;
  }
}

// ==============================
// DATA PUBLISHING & STORAGE
// ==============================
void publishData(float flowRate, float volume, float power, float energy) {
  String jsonPayload = "{" +
                       String("\"device_id\":\"") + device_id + "\"," +
                       String("\"location\":\"") + location_name + "\"," +
                       String("\"timestamp\":\"") + getFormattedDateTime() + "\"," +
                       String("\"flow_rate\":") + String(flowRate, 3) + "," +
                       String("\"total_volume\":") + String(volume, 3) + "," +
                       String("\"daily_volume\":") + String(dailyVolume, 3) + "," +
                       String("\"voltage\":") + String(voltage, 2) + "," +
                       String("\"current\":") + String(current, 3) + "," +
                       String("\"active_power\":") + String(power, 2) + "," +
                       String("\"energy\":") + String(energy, 4) + "," +
                       String("\"frequency\":") + String(frequency, 2) + "," +
                       String("\"power_factor\":") + String(powerFactor, 3) + "," +
                       String("\"last_reset_date\":\"") + lastResetDate + "\"," +
                       String("\"daily_reset_pending\":") + String(dailyResetPending ? "true" : "false") + "," +
                       String("\"status\":\"") + (pzemAvailable ? "online" : "offline") + "\"" +
                       "}";
  
  if (isMQTTReallyConnected()) {
    if (client.publish(mqtt_topic_pub.c_str(), jsonPayload.c_str())) {
      lastMQTTSuccess = millis();
      logInfo("Data published to MQTT");
      
      if (unsentDataCount > 0) {
        processUnsentData();
      }
    } else {
      logWarning("MQTT publish failed - Buffering data");
      addToUnsentData(jsonPayload);
    }
  } else {
    logWarning("MQTT not connected - Buffering data");
    addToUnsentData(jsonPayload);
  }
}

void saveToSD(float flowRate, float volume, float power, float energy) {
  if (!sdCardAvailable) return;
  
  String dataString = getFormattedDateTime() + "," + device_id + "," +
                     String(flowRate, 3) + "," + String(volume, 3) + "," +
                     String(dailyVolume, 3) + "," +
                     String(voltage, 2) + "," + String(current, 3) + "," +
                     String(power, 2) + "," + String(energy, 4) + "," +
                     String(frequency, 2) + "," + String(powerFactor, 3) + "," +
                     (pzemAvailable ? "online" : "offline");
  
  File dataFile = SD.open("/datalog.csv", FILE_APPEND);
  if (dataFile) {
    dataFile.println(dataString);
    dataFile.close();
  } else {
    logError("Failed to open SD file for writing");
  }
}

// ==============================
// NETWORK FUNCTIONS
// ==============================
void setupWiFi() {
  logInfo("Connecting to WiFi: " + String(ssid));
  
  WiFi.disconnect(true);
  delay(1000);
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  
  unsigned long startTime = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - startTime < 15000) {
    delay(500);
    Serial.print(".");
  }
  
  if (WiFi.status() == WL_CONNECTED) {
    wifiConnected = true;
    logInfo("WiFi connected - IP: " + WiFi.localIP().toString());
  } else {
    wifiConnected = false;
    logError("WiFi connection failed");
  }
}

bool checkWiFiStatus() {
  wl_status_t status = WiFi.status();
  
  if (status == WL_CONNECTED) {
    if (!wifiConnected) {
      wifiConnected = true;
      logInfo("WiFi reconnected");
    }
    return true;
  } else {
    if (wifiConnected) {
      wifiConnected = false;
      mqttConnected = false;
      logWarning("WiFi connection lost");
    }
    return false;
  }
}

void reconnectWiFi() {
  unsigned long currentMillis = millis();
  
  if (currentMillis - lastWiFiReconnectAttempt >= wifiReconnectInterval) {
    lastWiFiReconnectAttempt = currentMillis;
    
    logInfo("Attempting WiFi reconnection...");
    WiFi.disconnect();
    delay(500);
    WiFi.begin(ssid, password);
  }
}

void reconnectMQTT() {
  unsigned long currentMillis = millis();
  
  if (currentMillis - lastMQTTReconnectAttempt >= mqttReconnectInterval) {
    lastMQTTReconnectAttempt = currentMillis;
    mqttConnectionAttempts++;
    
    if (wifiConnected && mqttConnectionAttempts <= MAX_MQTT_ATTEMPTS) {
      logInfo("Attempting MQTT connection (" + String(mqttConnectionAttempts) + "/" + String(MAX_MQTT_ATTEMPTS) + ")");
      
      String clientId = "INDUSTRIAL_" + device_id + "_" + String(random(0xffff), HEX);
      
      if (client.connect(clientId.c_str())) {
        mqttConnected = true;
        mqttConnectionAttempts = 0;
        lastMQTTSuccess = millis();
        
        client.subscribe(mqtt_topic_sub.c_str());
        client.subscribe(mqtt_topic_command);
        
        logInfo("MQTT connected and subscribed");
        
        String statusMsg = device_id + " - " + location_name + " connected - Daily Reset at 23:59";
        client.publish("industry/status", statusMsg.c_str());
        
      } else {
        mqttConnected = false;
        logWarning("MQTT connection failed - State: " + String(client.state()));
      }
    }
  }
}

// ==============================
// MQTT COMMAND HANDLERS
// ==============================
void callback(char* topic, byte* payload, unsigned int length) {
  String message;
  for (int i = 0; i < length; i++) {
    message += (char)payload[i];
  }
  
  logInfo("MQTT Command received: " + message);
  lastMQTTSuccess = millis();
  
  if (String(topic) == mqtt_topic_sub) {
    handleDeviceSpecificCommand(message);
  } else if (String(topic) == mqtt_topic_command) {
    handleGlobalCommand(message);
  }
}

void handleDeviceSpecificCommand(String message) {
  logInfo("Processing device command: " + message);
  
  if (message == "reset_volume") {
    totalVolume = 0;
    lastBackupVolume = 0;
    logInfo("Volume reset");
    
  } else if (message == "reset_energy") {
    if (resetPZEMEnergy()) {
      logInfo("Energy reset successful");
    }
    
  } else if (message == "system_status") {
    sendSystemStatus();
    
  } else if (message == "pzem_status") {
    sendPZEMStatus();
    
  } else if (message == "reconnect_pzem") {
    initializePZEM();
    
  } else if (message == "display_flow") {
    switchDisplayMode(FLOW_MONITORING);
    
  } else if (message == "display_power") {
    switchDisplayMode(POWER_MONITORING);
    
  } else if (message == "force_daily_reset") {
    saveDailyVolume();
    resetDailyVolume();
    logInfo("Manual daily reset executed");
    
  } else if (message == "get_daily_volume") {
    String response = "{\"device\":\"" + device_id + "\",\"date\":\"" + lastResetDate + 
                     "\",\"daily_volume\":" + String(dailyVolume, 3) + 
                     ",\"previous_day\":" + String(previousDayVolume, 3) + "}";
    if (isMQTTReallyConnected()) {
      client.publish("industry/device/daily_volume", response.c_str());
    }
    
  } else if (message.startsWith("interval:")) {
    String intervalStr = message.substring(9);
    unsigned long newInterval = parseTimeString(intervalStr);
    if (newInterval >= MIN_INTERVAL && newInterval <= MAX_INTERVAL) {
      mqttPublishInterval = newInterval;
      logInfo("MQTT interval set to: " + String(mqttPublishInterval/1000) + "s");
    }
    
  } else if (message == "reboot") {
    logInfo("Rebooting device...");
    delay(1000);
    ESP.restart();
  }
}

// ==============================
// GLOBAL COMMAND HANDLER
// ==============================
void handleGlobalCommand(String message) {
  logInfo("Processing global command: " + message);
  
  if (message == "reboot_all") {
    logInfo("Rebooting all devices...");
    delay(1000);
    ESP.restart();
  } else if (message == "status_all") {
    sendSystemStatus();
  } else if (message == "sync_time_all") {
    if (wifiConnected) {
      syncRTCWithNTP();
      logInfo("Time synced for all devices");
    }
  } else if (message == "reset_all_volume") {
    totalVolume = 0;
    dailyVolume = 0;
    lastBackupVolume = 0;
    logInfo("Volume reset for all devices");
  } else if (message == "reset_all_energy") {
    if (pzemAvailable) {
      resetPZEMEnergy();
    }
    logInfo("Energy reset for all devices");
  } else if (message == "enable_flow_monitoring") {
    switchDisplayMode(FLOW_MONITORING);
    logInfo("Flow monitoring enabled for all");
  } else if (message == "enable_power_monitoring") {
    switchDisplayMode(POWER_MONITORING);
    logInfo("Power monitoring enabled for all");
  } else if (message == "force_daily_reset_all") {
    saveDailyVolume();
    resetDailyVolume();
    logInfo("Manual daily reset executed for all devices");
  } else if (message.startsWith("set_interval:")) {
    // Format: set_interval:sd_write=10000,sd_read=30000,mqtt=2000
    String params = message.substring(13);
    int sdWriteStart = params.indexOf("sd_write=");
    int sdReadStart = params.indexOf("sd_read=");
    int mqttStart = params.indexOf("mqtt=");
    
    if (sdWriteStart != -1) {
      int end = params.indexOf(',', sdWriteStart);
      if (end == -1) end = params.length();
      String valueStr = params.substring(sdWriteStart + 9, end);
      unsigned long newInterval = parseTimeString(valueStr);
      if (newInterval >= MIN_INTERVAL && newInterval <= MAX_INTERVAL) {
        sdWriteInterval = newInterval;
        logInfo("Global SD write interval set to: " + String(sdWriteInterval/1000) + "s");
      }
    }
    
    if (sdReadStart != -1) {
      int end = params.indexOf(',', sdReadStart);
      if (end == -1) end = params.length();
      String valueStr = params.substring(sdReadStart + 8, end);
      unsigned long newInterval = parseTimeString(valueStr);
      if (newInterval >= MIN_INTERVAL && newInterval <= MAX_INTERVAL) {
        sdReadInterval = newInterval;
        logInfo("Global SD read interval set to: " + String(sdReadInterval/1000) + "s");
      }
    }
    
    if (mqttStart != -1) {
      int end = params.indexOf(',', mqttStart);
      if (end == -1) end = params.length();
      String valueStr = params.substring(mqttStart + 5, end);
      unsigned long newInterval = parseTimeString(valueStr);
      if (newInterval >= MIN_INTERVAL && newInterval <= MAX_INTERVAL) {
        mqttPublishInterval = newInterval;
        logInfo("Global MQTT publish interval set to: " + String(mqttPublishInterval/1000) + "s");
      }
    }
  }
}

// ==============================
// SD LOG READER FUNCTION
// ==============================
void readSDLog() {
  if (!sdCardAvailable) {
    logWarning("Cannot read SD log - SD card not available");
    return;
  }
  
  if (!SD.exists("/datalog.csv")) {
    logWarning("No data log file found");
    return;
  }
  
  logInfo("Reading SD card log...");
  
  File dataFile = SD.open("/datalog.csv", FILE_READ);
  if (!dataFile) {
    logError("Failed to open datalog.csv for reading");
    return;
  }
  
  // Read last 5 lines (or as needed)
  const int LINES_TO_READ = 5;
  String lastLines[LINES_TO_READ];
  int lineIndex = 0;
  int totalLines = 0;
  
  while (dataFile.available()) {
    String line = dataFile.readStringUntil('\n');
    line.trim();
    if (line.length() > 0) {
      lastLines[lineIndex % LINES_TO_READ] = line;
      lineIndex++;
      totalLines++;
    }
  }
  dataFile.close();
  
  // Display the last few lines
  logInfo("=== SD Card Log Summary ===");
  logInfo("Total entries in log: " + String(totalLines));
  
  int startIndex = max(0, totalLines - LINES_TO_READ);
  int displayIndex = startIndex;
  
  for (int i = 0; i < min(LINES_TO_READ, totalLines); i++) {
    int actualIndex = (startIndex + i) % LINES_TO_READ;
    if (lastLines[actualIndex].length() > 0) {
      logInfo("Entry " + String(displayIndex + 1) + ": " + lastLines[actualIndex]);
      displayIndex++;
    }
  }
  
  logInfo("=== End of Log Summary ===");
  
  // Also check daily volume file
  if (SD.exists("/daily_volume.csv")) {
    File dailyFile = SD.open("/daily_volume.csv", FILE_READ);
    int dailyEntries = 0;
    while (dailyFile.available()) {
      String line = dailyFile.readStringUntil('\n');
      if (line.length() > 0 && !line.startsWith("Date")) {
        dailyEntries++;
      }
    }
    dailyFile.close();
    logInfo("Daily volume entries: " + String(dailyEntries));
  }
}

// ==============================
// SUPPORT FUNCTIONS
// ==============================
void checkButtons() {
  static unsigned long lastButtonPress = 0;
  const unsigned long debounceDelay = 300;
  
  if (millis() - lastButtonPress < debounceDelay) return;
  
  if (digitalRead(buttonFlowPin) == LOW) {
    currentDisplayMode = FLOW_MONITORING;
    lastButtonPress = millis();
    updateOLED();
  }
  
  if (digitalRead(buttonPowerPin) == LOW) {
    currentDisplayMode = POWER_MONITORING;
    lastButtonPress = millis();
    updateOLED();
  }
}

void switchDisplayMode(DisplayMode mode) {
  currentDisplayMode = mode;
  logInfo("Display mode changed to: " + String(mode == FLOW_MONITORING ? "FLOW" : "POWER"));
}

void addToUnsentData(const String& data) {
  if (unsentDataCount < MAX_UNSENT_DATA) {
    unsentData[unsentDataIndex] = data;
    unsentDataIndex = (unsentDataIndex + 1) % MAX_UNSENT_DATA;
    unsentDataCount++;
    logInfo("Data added to buffer - Count: " + String(unsentDataCount));
  } else {
    logWarning("Buffer full - Dropping data");
  }
}

void processUnsentData() {
  if (unsentDataCount == 0 || !isMQTTReallyConnected()) return;
  
  logInfo("Processing " + String(unsentDataCount) + " buffered messages");
  
  int processed = 0;
  int startIndex = (unsentDataIndex - unsentDataCount + MAX_UNSENT_DATA) % MAX_UNSENT_DATA;
  
  for (int i = 0; i < unsentDataCount; i++) {
    int currentIndex = (startIndex + i) % MAX_UNSENT_DATA;
    
    if (client.publish(mqtt_topic_pub.c_str(), unsentData[currentIndex].c_str())) {
      processed++;
      delay(50); // Prevent flooding
    } else {
      break;
    }
  }
  
  if (processed > 0) {
    unsentDataCount -= processed;
    logInfo("Processed " + String(processed) + " messages - Remaining: " + String(unsentDataCount));
  }
}

bool isMQTTReallyConnected() {
  bool connected = client.connected();
  if (connected != mqttConnected) {
    mqttConnected = connected;
    logInfo("MQTT: " + String(connected ? "Connected" : "Disconnected"));
  }
  return connected && (WiFi.status() == WL_CONNECTED);
}

void sendSystemStatus() {
  if (!isMQTTReallyConnected()) return;
  
  String statusMsg = "{\"device\":\"" + device_id + "\"," +
                    "\"location\":\"" + location_name + "\"," +
                    "\"timestamp\":\"" + getFormattedDateTime() + "\"," +
                    "\"flow_rate\":" + String(flowRate, 3) + "," +
                    "\"total_volume\":" + String(totalVolume, 3) + "," +
                    "\"daily_volume\":" + String(dailyVolume, 3) + "," +
                    "\"previous_day_volume\":" + String(previousDayVolume, 3) + "," +
                    "\"last_reset_date\":\"" + lastResetDate + "\"," +
                    "\"power\":" + String(power, 2) + "," +
                    "\"energy\":" + String(energy, 4) + "," +
                    "\"pzem_status\":\"" + (pzemAvailable ? "connected" : "disconnected") + "\"," +
                    "\"wifi_status\":\"" + (wifiConnected ? "connected" : "disconnected") + "\"," +
                    "\"mqtt_status\":\"" + (mqttConnected ? "connected" : "disconnected") + "\"," +
                    "\"buffer_count\":" + String(unsentDataCount) + "}";
  
  client.publish("industry/device/status", statusMsg.c_str());
}

void sendPZEMStatus() {
  if (!isMQTTReallyConnected()) return;
  
  String pzemStatus = "{\"device\":\"" + device_id + "\"," +
                     "\"pzem_connected\":" + String(pzemAvailable ? "true" : "false") + "," +
                     "\"voltage\":" + String(voltage, 2) + "," +
                     "\"current\":" + String(current, 3) + "," +
                     "\"power\":" + String(power, 2) + "," +
                     "\"energy\":" + String(energy, 4) + "," +
                     "\"frequency\":" + String(frequency, 2) + "," +
                     "\"power_factor\":" + String(powerFactor, 3) + "}";
  
  client.publish("industry/pzem/status", pzemStatus.c_str());
}

// ==============================
// CONFIGURATION FUNCTIONS
// ==============================
bool loadConfigFromSD() {
  if (!sdCardAvailable) return false;
  
  if (!SD.exists("/config.txt")) {
    return false;
  }
  
  File configFile = SD.open("/config.txt", FILE_READ);
  if (!configFile) {
    return false;
  }
  
  while (configFile.available()) {
    String line = configFile.readStringUntil('\n');
    line.trim();
    
    if (line.length() == 0 || line.startsWith("#")) continue;
    
    int separatorIndex = line.indexOf('=');
    if (separatorIndex == -1) continue;
    
    String key = line.substring(0, separatorIndex);
    String value = line.substring(separatorIndex + 1);
    
    key.trim();
    value.trim();
    
    if (key == "device_id") {
      configDeviceID = value;
    } else if (key == "location_name") {
      configLocation = value;
    } else if (key == "mqtt_prefix") {
      configMqttPrefix = value;
    } else if (key == "flow_pin") {
      configFlowPin = value.toInt();
    } else if (key == "button_flow_pin") {
      configButtonFlowPin = value.toInt();
    } else if (key == "button_power_pin") {
      configButtonPowerPin = value.toInt();
    } else if (key == "rs485_rx_pin") {
      configRS485_RX = value.toInt();
    } else if (key == "rs485_tx_pin") {
      configRS485_TX = value.toInt();
    } else if (key == "rs485_de_re_pin") {
      configRS485_DE_RE = value.toInt();
    }
  }
  
  configFile.close();
  return true;
}

void applyDeviceConfig() {
  if (configDeviceID.length() > 0) {
    device_id = configDeviceID;
  }
  
  if (configLocation.length() > 0) {
    location_name = configLocation;
  }
  
  flowSensorPin = configFlowPin;
  buttonFlowPin = configButtonFlowPin;
  buttonPowerPin = configButtonPowerPin;
  rs485RxPin = configRS485_RX;
  rs485TxPin = configRS485_TX;
  rs485DeRePin = configRS485_DE_RE;
}

void createDefaultConfig() {
  if (!sdCardAvailable) return;
  
  File configFile = SD.open("/config.txt", FILE_WRITE);
  if (configFile) {
    configFile.println("# Industrial Monitoring System Configuration");
    configFile.println("# Device Configuration");
    configFile.println("device_id=" + device_id);
    configFile.println("location_name=" + location_name);
    configFile.println("mqtt_prefix=01");
    configFile.println("");
    configFile.println("# Hardware Pin Configuration");
    configFile.println("flow_pin=34");
    configFile.println("button_flow_pin=14");
    configFile.println("button_power_pin=27");
    configFile.println("rs485_rx_pin=16");
    configFile.println("rs485_tx_pin=17");
    configFile.println("rs485_de_re_pin=4");
    configFile.close();
  }
}

unsigned long parseTimeString(String timeStr) {
  timeStr.toLowerCase();
  timeStr.trim();
  
  if (timeStr.length() == 0) return 0;
  
  String numStr = "";
  String unitStr = "";
  
  for (int i = 0; i < timeStr.length(); i++) {
    char c = timeStr.charAt(i);
    if (isdigit(c)) {
      numStr += c;
    } else {
      unitStr = timeStr.substring(i);
      break;
    }
  }
  
  if (numStr.length() == 0) return 0;
  
  unsigned long value = numStr.toInt();
  
  if (unitStr == "s" || unitStr == "sec" || unitStr == "") {
    return value * 1000;
  } else if (unitStr == "m" || unitStr == "min") {
    return value * 60000;
  } else if (unitStr == "h" || unitStr == "hour") {
    return value * 3600000;
  } else if (unitStr == "ms") {
    return value;
  } else {
    return 0;
  }
}

// ==============================
// BACKUP & RECOVERY FUNCTIONS
// ==============================
void backupFlowData() {
  if (!sdCardAvailable) return;
  
  if (abs(totalVolume - lastBackupVolume) < 0.001) {
    return;
  }
  
  File backupFile = SD.open("/flow_backup.txt", FILE_WRITE);
  if (backupFile) {
    String backupData = device_id + ",FLOW," + String(totalVolume, 6) + "," + 
                       String(dailyVolume, 6) + "," +  // Include daily volume in backup
                       getFormattedDateTime() + "," + String(millis());
    backupFile.println(backupData);
    backupFile.close();
    lastBackupVolume = totalVolume;
    
    logInfo("Flow backup: " + String(totalVolume, 3) + " L, Daily: " + String(dailyVolume, 3) + " L");
  }
}

void recoverFlowData() {
  if (!SD.exists("/flow_backup.txt")) {
    logInfo("No flow backup found");
    return;
  }
  
  File backupFile = SD.open("/flow_backup.txt", FILE_READ);
  if (!backupFile) {
    logError("Failed to open flow backup file");
    return;
  }
  
  String lastLine = "";
  while (backupFile.available()) {
    lastLine = backupFile.readStringUntil('\n');
  }
  backupFile.close();
  
  if (lastLine.length() > 0) {
    int firstComma = lastLine.indexOf(',');
    int secondComma = lastLine.indexOf(',', firstComma + 1);
    int thirdComma = lastLine.indexOf(',', secondComma + 1);
    
    if (firstComma != -1 && secondComma != -1) {
      String deviceID = lastLine.substring(0, firstComma);
      String type = lastLine.substring(firstComma + 1, secondComma);
      String volumeStr = lastLine.substring(secondComma + 1);
      
      if (thirdComma != -1) {
        volumeStr = volumeStr.substring(0, volumeStr.indexOf(','));
      }
      
      if (deviceID == device_id && type == "FLOW") {
        totalVolume = volumeStr.toFloat();
        lastBackupVolume = totalVolume;
        logInfo("Recovered total volume: " + String(totalVolume, 3) + " L");
        
        // Try to recover daily volume if available
        int fourthComma = lastLine.indexOf(',', thirdComma + 1);
        if (fourthComma != -1) {
          String dailyVolumeStr = lastLine.substring(thirdComma + 1, fourthComma);
          dailyVolume = dailyVolumeStr.toFloat();
          logInfo("Recovered daily volume: " + String(dailyVolume, 3) + " L");
        }
      }
    }
  }
}

void backupPowerData() {
  if (!sdCardAvailable) return;
  
  if (abs(energy - lastBackupEnergy) < 0.001) {
    return;
  }
  
  File backupFile = SD.open("/power_backup.txt", FILE_WRITE);
  if (backupFile) {
    String backupData = device_id + ",PZEM," + String(energy, 6) + "," +
                       getFormattedDateTime() + "," + String(millis());
    backupFile.println(backupData);
    backupFile.close();
    lastBackupEnergy = energy;
    
    logInfo("Power backup: " + String(energy, 3) + " kWh");
  }
}

void recoverPowerData() {
  if (!SD.exists("/power_backup.txt")) {
    logInfo("No power backup found");
    return;
  }
  
  File backupFile = SD.open("/power_backup.txt", FILE_READ);
  if (!backupFile) {
    logError("Failed to open power backup file");
    return;
  }
  
  String lastLine = "";
  while (backupFile.available()) {
    lastLine = backupFile.readStringUntil('\n');
  }
  backupFile.close();
  
  if (lastLine.length() > 0) {
    int firstComma = lastLine.indexOf(',');
    int secondComma = lastLine.indexOf(',', firstComma + 1);
    
    if (firstComma != -1 && secondComma != -1) {
      String deviceID = lastLine.substring(0, firstComma);
      String type = lastLine.substring(firstComma + 1, secondComma);
      String energyStr = lastLine.substring(secondComma + 1);
      
      int nextComma = energyStr.indexOf(',');
      if (nextComma != -1) {
        energyStr = energyStr.substring(0, nextComma);
      }
      
      if (deviceID == device_id) {
        energy = energyStr.toFloat();
        lastBackupEnergy = energy;
        logInfo("Recovered energy: " + String(energy, 3) + " kWh");
      }
    }
  }
}

// ==============================
// INTERVAL MANAGEMENT FUNCTIONS
// ==============================
void loadIntervalConfig() {
  if (!sdCardAvailable || !SD.exists("/interval_config.txt")) {
    logInfo("No interval config found, using defaults");
    return;
  }
  
  File configFile = SD.open("/interval_config.txt", FILE_READ);
  if (!configFile) {
    logError("Failed to open interval config");
    return;
  }
  
  String lastLine = "";
  while (configFile.available()) {
    lastLine = configFile.readStringUntil('\n');
  }
  configFile.close();
  
  if (lastLine.length() > 0) {
    int firstComma = lastLine.indexOf(',');
    int secondComma = lastLine.indexOf(',', firstComma + 1);
    int thirdComma = lastLine.indexOf(',', secondComma + 1);
    
    if (firstComma != -1 && secondComma != -1 && thirdComma != -1) {
      String deviceID = lastLine.substring(0, firstComma);
      
      if (deviceID == device_id) {
        String writeStr = lastLine.substring(firstComma + 1, secondComma);
        String readStr = lastLine.substring(secondComma + 1, thirdComma);
        String mqttStr = lastLine.substring(thirdComma + 1);
        
        int mqttComma = mqttStr.indexOf(',');
        if (mqttComma != -1) {
          mqttStr = mqttStr.substring(0, mqttComma);
        }
        
        unsigned long writeInterval = writeStr.toInt();
        unsigned long readInterval = readStr.toInt();
        unsigned long mqttInterval = mqttStr.toInt();
        
        if (writeInterval >= MIN_INTERVAL && writeInterval <= MAX_INTERVAL) {
          sdWriteInterval = writeInterval;
        }
        if (readInterval >= MIN_INTERVAL && readInterval <= MAX_INTERVAL) {
          sdReadInterval = readInterval;
        }
        if (mqttInterval >= MIN_INTERVAL && mqttInterval <= MAX_INTERVAL) {
          mqttPublishInterval = mqttInterval;
        }
        
        logInfo("Interval config loaded - SD Write: " + String(sdWriteInterval/1000) + "s, MQTT: " + String(mqttPublishInterval/1000) + "s");
      }
    }
  }
}

// ==============================
// FUNGSI YANG BELUM DIIMPLEMENTASI
// ==============================
void changeCalibrationFactor(int index) {
  if (index >= 0 && index < 3) {
    calibrationFactor = YFB6_CALIBRATION_FACTORS[index];
    currentCalibrationIndex = index;
    logInfo("Calibration factor changed to: " + String(calibrationFactor));
  }
}

void sendIntervalStatus() {
  if (!isMQTTReallyConnected()) return;
  
  String intervalStatus = "{\"device\":\"" + device_id + "\"," +
                        "\"sd_write_interval\":" + String(sdWriteInterval) + "," +
                        "\"sd_read_interval\":" + String(sdReadInterval) + "," +
                        "\"mqtt_publish_interval\":" + String(mqttPublishInterval) + "," +
                        "\"oled_update_interval\":" + String(oledUpdateInterval) + "," +
                        "\"flow_calc_interval\":" + String(flowCalcInterval) + "," +
                        "\"power_calc_interval\":" + String(powerCalcInterval) + "}";
  
  client.publish("industry/device/intervals", intervalStatus.c_str());
}

void setSDWriteInterval(unsigned long newInterval) {
  if (newInterval >= MIN_INTERVAL && newInterval <= MAX_INTERVAL) {
    sdWriteInterval = newInterval;
    logInfo("SD write interval set to: " + String(sdWriteInterval/1000) + "s");
  }
}

void setSDReadInterval(unsigned long newInterval) {
  if (newInterval >= MIN_INTERVAL && newInterval <= MAX_INTERVAL) {
    sdReadInterval = newInterval;
    logInfo("SD read interval set to: " + String(sdReadInterval/1000) + "s");
  }
}

void setMQTTPublishInterval(unsigned long newInterval) {
  if (newInterval >= MIN_INTERVAL && newInterval <= MAX_INTERVAL) {
    mqttPublishInterval = newInterval;
    logInfo("MQTT publish interval set to: " + String(mqttPublishInterval/1000) + "s");
  }
}

void setAllIntervals(unsigned long sdWrite, unsigned long sdRead, unsigned long mqttPub) {
  setSDWriteInterval(sdWrite);
  setSDReadInterval(sdRead);
  setMQTTPublishInterval(mqttPub);
}

void saveIntervalConfig() {
  if (!sdCardAvailable) return;
  
  File configFile = SD.open("/interval_config.txt", FILE_APPEND);
  if (configFile) {
    String configLine = device_id + "," + 
                       String(sdWriteInterval) + "," +
                       String(sdReadInterval) + "," +
                       String(mqttPublishInterval) + "," +
                       getFormattedDateTime();
    configFile.println(configLine);
    configFile.close();
    logInfo("Interval configuration saved");
  }
}

void mqttDebugInfo() {
  logInfo("MQTT Debug Info:");
  logInfo("- Client State: " + String(client.state()));
  logInfo("- Connected: " + String(client.connected() ? "Yes" : "No"));
  logInfo("- WiFi Status: " + String(WiFi.status()));
  logInfo("- Last MQTT Success: " + String(millis() - lastMQTTSuccess) + "ms ago");
  logInfo("- MQTT Attempts: " + String(mqttConnectionAttempts));
}

void loadDeviceConfig() {
  // Fungsi ini sudah diimplementasi sebagai loadConfigFromSD()
  // Di sini kita panggil saja loadConfigFromSD()
  if (!loadConfigFromSD()) {
    logWarning("Failed to load device configuration");
  }
}
