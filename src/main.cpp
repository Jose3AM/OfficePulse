#include <Arduino.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <time.h>
#include <sys/time.h>
#include <esp_system.h>

#include "secrets.h"
#include "aws_certs.h"

// ============================================================
// Configuración general
// ============================================================

constexpr char DEVICE_ID[] = "office-node-01";
constexpr char SESSION_ID[] = "baseline_03";
constexpr char FIRMWARE_VERSION[] = "0.3.0";

// ============================================================
// Hardware
// ============================================================

constexpr uint8_t DHT_PIN = 15;
constexpr uint8_t DHT_TYPE = DHT11;

constexpr uint8_t LDR_PIN = 34;
constexpr uint8_t LED_PIN = 23;

constexpr int DARK_THRESHOLD = 200;


// 1 mensaje por minuto
constexpr unsigned long READING_INTERVAL_MS = 60000;

// ============================================================
// AWS IoT
// ============================================================

constexpr char AWS_IOT_CLIENT_ID[] = "office-node-01";

constexpr char AWS_IOT_TOPIC[] =
    "officepulse/office-node-01/telemetry";

// ============================================================
// Objetos
// ============================================================

DHT dht(DHT_PIN, DHT_TYPE);

WiFiClientSecure secureClient;
PubSubClient mqttClient(secureClient);

uint32_t sequenceNumber = 0;
uint32_t bootId = 0;

unsigned long lastReadingTime = 0;

// ============================================================
// Utilidades
// ============================================================

int64_t getDeviceTimestampMs() {

    struct timeval tv;

    gettimeofday(&tv, nullptr);

    return (
        static_cast<int64_t>(tv.tv_sec) * 1000LL
        + static_cast<int64_t>(tv.tv_usec) / 1000LL
    );
}

// ============================================================
// Wi-Fi
// ============================================================

void connectWiFi() {

    if (WiFi.status() == WL_CONNECTED) {
        return;
    }

    Serial.print("Conectando a Wi-Fi");

    WiFi.mode(WIFI_STA);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    while (WiFi.status() != WL_CONNECTED) {
        Serial.print(".");
        delay(500);
    }

    Serial.println();
    Serial.println("Wi-Fi conectado");

    Serial.print("IP: ");
    Serial.println(WiFi.localIP());

    Serial.print("RSSI: ");
    Serial.print(WiFi.RSSI());
    Serial.println(" dBm");
}

// ============================================================
// Sincronización de reloj
// ============================================================

void syncClock() {

    Serial.print("Sincronizando reloj");

    configTime(
        0,
        0,
        "pool.ntp.org",
        "time.nist.gov"
    );

    time_t now = time(nullptr);

    while (now < 1000000000) {
        Serial.print(".");
        delay(500);
        now = time(nullptr);
    }

    Serial.println();
    Serial.println("Reloj sincronizado");

    Serial.print("Epoch actual: ");
    Serial.println(static_cast<long>(now));
}

// ============================================================
// AWS IoT Core
// ============================================================

void connectAWS() {

    secureClient.setCACert(AWS_ROOT_CA);
    secureClient.setCertificate(AWS_DEVICE_CERT);
    secureClient.setPrivateKey(AWS_PRIVATE_KEY);

    mqttClient.setServer(
        AWS_IOT_ENDPOINT,
        8883
    );

    mqttClient.setBufferSize(1024);
    mqttClient.setKeepAlive(60);

    while (!mqttClient.connected()) {

        Serial.print(
            "Conectando a AWS IoT Core... "
        );

        if (
            mqttClient.connect(
                AWS_IOT_CLIENT_ID
            )
        ) {

            Serial.println("OK");

        } else {

            Serial.print(
                "ERROR. MQTT state="
            );

            Serial.println(
                mqttClient.state()
            );

            delay(2000);
        }
    }
}

// ============================================================
// Telemetría
// ============================================================

void publishTelemetry() {

    // --------------------------------------------------------
    // Timestamp de adquisición
    // --------------------------------------------------------

    const int64_t deviceTimestampMs =
        getDeviceTimestampMs();

    // --------------------------------------------------------
    // Sensores
    // --------------------------------------------------------

    const float humidity =
        dht.readHumidity();

    const float temperature =
        dht.readTemperature();

    const int lightRaw =
        analogRead(LDR_PIN);

    const bool dhtValid =
        !isnan(humidity)
        && !isnan(temperature);

    // --------------------------------------------------------
    // Control local
    // --------------------------------------------------------

    const bool isDark =
        lightRaw < DARK_THRESHOLD;

    digitalWrite(
        LED_PIN,
        isDark ? HIGH : LOW
    );

    // --------------------------------------------------------
    // JSON
    // --------------------------------------------------------

    JsonDocument doc;

    // Identidad
    doc["device_id"] = DEVICE_ID;
    doc["session_id"] = SESSION_ID;
    doc["boot_id"] = bootId;
    doc["sequence_number"] = sequenceNumber++;

    // Tiempo
    doc["device_timestamp_ms"] =
        deviceTimestampMs;

    doc["uptime_ms"] =
        millis();

    // Calidad del sensor
    doc["dht_valid"] =
        dhtValid;

    // DHT11
    if (dhtValid) {

        doc["temperature_c"] =
            temperature;

        doc["humidity_pct"] =
            humidity;

    } else {

        doc["temperature_c"] =
            nullptr;

        doc["humidity_pct"] =
            nullptr;
    }

    // LDR
    doc["light_raw"] =
        lightRaw;

    doc["light_state"] =
        isDark
            ? "dark"
            : "bright";

    // Control
    doc["led_active"] =
        isDark;

    doc["dark_threshold"] =
        DARK_THRESHOLD;

    // Operación
    doc["wifi_rssi"] =
        WiFi.RSSI();

    doc["firmware_version"] =
        FIRMWARE_VERSION;

    // --------------------------------------------------------
    // Serializar JSON
    // --------------------------------------------------------

    char payload[768];

    const size_t payloadSize =
        serializeJson(
            doc,
            payload,
            sizeof(payload)
        );

    if (payloadSize == 0) {

        Serial.println(
            "Error serializando JSON"
        );

        return;
    }

    // --------------------------------------------------------
    // Publicar
    // --------------------------------------------------------

    const bool published =
        mqttClient.publish(
            AWS_IOT_TOPIC,
            payload
        );

    if (published) {

        Serial.print("AWS <- ");
        Serial.println(payload);

    } else {

        Serial.println(
            "Error publicando telemetria"
        );
    }
}

// ============================================================
// Setup
// ============================================================

void setup() {

    Serial.begin(115200);
    delay(1000);

    Serial.println();
    Serial.println(
        "============================"
    );

    Serial.println(
        "OfficePulse iniciando"
    );

    Serial.println(
        "============================"
    );

    // ID distinto cada vez que reinicia
    // físicamente el ESP32.
    bootId = esp_random();

    Serial.print("Boot ID: ");
    Serial.println(bootId);

    analogReadResolution(12);

    dht.begin();

    pinMode(
        LED_PIN,
        OUTPUT
    );

    digitalWrite(
        LED_PIN,
        LOW
    );

    connectWiFi();
    syncClock();
    connectAWS();

    Serial.println();
    Serial.println(
        "OfficePulse conectado"
    );

    // Primera medición inmediata
    publishTelemetry();

    lastReadingTime =
        millis();
}

// ============================================================
// Loop
// ============================================================

void loop() {

    // --------------------------------------------------------
    // Recuperación Wi-Fi
    // --------------------------------------------------------

    if (
        WiFi.status()
        != WL_CONNECTED
    ) {

        connectWiFi();
    }

    // --------------------------------------------------------
    // Recuperación MQTT
    // --------------------------------------------------------

    if (
        !mqttClient.connected()
    ) {

        connectAWS();
    }

    mqttClient.loop();

    // --------------------------------------------------------
    // Telemetría periódica
    // --------------------------------------------------------

    const unsigned long now =
        millis();

    if (
        now - lastReadingTime
        >= READING_INTERVAL_MS
    ) {

        lastReadingTime = now;

        publishTelemetry();
    }
}
