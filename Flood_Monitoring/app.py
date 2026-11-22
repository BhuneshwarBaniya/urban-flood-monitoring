import encodings.idna
import os
import json
from datetime import datetime
from flask import Flask, render_template, jsonify
import paho.mqtt.client as mqtt

app = Flask(__name__)

# MQTT Settings (Must match your ESP32 settings)
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "flood_system/telemetry"

# In-memory store for the latest telemetry
latest_data = {
    "rain_raw": 0,
    "wetness": 0,
    "rain_detected": False,
    "distance": 0.0,
    "water_depth": 0.0,
    "rise_rate": 0.0,
    "buzzer": "OFF",
    "risk_code": 0,
    "risk_label": "WAITING FOR DATA",
    "last_updated": "Never"
}

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to MQTT Broker with result code:", rc)
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    global latest_data
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        data["last_updated"] = datetime.now().strftime("%H:%M:%S")
        latest_data.update(data)
        print("Received from ESP32:", latest_data)
    except Exception as e:
        print("Error parsing MQTT message:", e)

# Start MQTT Client in Background Thread
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/data")
def get_data():
    return jsonify(latest_data)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )
