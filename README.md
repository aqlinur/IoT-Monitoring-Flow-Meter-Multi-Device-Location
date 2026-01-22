Before Uploading make sure add this libary :
WiFi.h 
NTPClient.h
WiFiUdp.h
PubSubClient.h
SD.h
SPI.h
Wire.h
Adafruit_GFX.h
Adafruit_SSD1306.h
RTClib.h
ModbusMaster.h

Dont forget Setting Configuration :
Device ID  //"Plumb-01,Plumb-02,Plumb-03,etc"
Location Name //"PE,IQC,Production Line - xx,Testing Room"
SSID
SSID Password 
MQTT Broker Server //for public testing u can use broker.hivemq.com or test.mosquitto.org || for localhost broker u can use ipv4 addres of installed computer mosquitto service 
MQTT Port //1883 (TCP)

jangan lupa atur config.txt dari sdcard

Contoh Json Payload From ESP32 : 
{
  "device_id": "Plumb-01",
  "location": "PE",
  "timestamp": "2024-01-01T12:00:00",
  "flow_rate": 1.234,
  "total_volume": 123.456,
  "daily_volume": 12.345,
  "voltage": 220.0,
  "current": 1.500,
  "active_power": 330.0,
  "energy": 12.3456,
  "frequency": 50.0,
  "power_factor": 0.99,
  "last_reset_date": "01/01/2024",
  "daily_reset_pending": false,
  "status": "online"
}
