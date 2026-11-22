
import encodings.idna
"test".encode("idna")

import os
import json
import time
from datetime import datetime
from flask import Flask, render_template, jsonify
import paho.mqtt.client as mqtt

app = Flask(__name__)

# =====================================
# MQTT SETTINGS
# =====================================

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "flood_system/telemetry"

# =====================================
# LATEST SENSOR DATA
# =====================================

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

# =====================================
# MQTT CALLBACK FUNCTIONS
# =====================================

def on_connect(client, userdata, flags, reason_code, properties=None):

    print("MQTT CONNECTION RESULT:", reason_code)

    if reason_code == 0:
        print("MQTT CONNECTED SUCCESSFULLY!")

        result = client.subscribe(MQTT_TOPIC)

        print("SUBSCRIBE RESULT:", result)
        print("SUBSCRIBED TO:", MQTT_TOPIC)

    else:
        print("MQTT CONNECTION FAILED:", reason_code)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):

    print("MQTT DISCONNECTED:", reason_code)


def on_message(client, userdata, msg):

    global latest_data

    try:
        data = json.loads(msg.payload.decode("utf-8"))

        data["last_updated"] = datetime.now().strftime("%H:%M:%S")

        latest_data.update(data)

        print("RECEIVED ESP32 DATA:", latest_data)

    except Exception as e:

        print("ERROR PARSING MQTT MESSAGE:", e)


# =====================================
# START MQTT CLIENT
# =====================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="flood_monitor_flask_server"
)

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message

mqtt_client.reconnect_delay_set(
    min_delay=1,
    max_delay=30
)

try:

    mqtt_client.connect_async(
        MQTT_BROKER,
        MQTT_PORT,
        60
    )

    mqtt_client.loop_start()

    print("MQTT CLIENT STARTED")
    print("Connecting to:", MQTT_BROKER)
    print("MQTT TOPIC:", MQTT_TOPIC)

except Exception as e:

    print("MQTT START ERROR:", e)


# =====================================
# FLASK ROUTES
# =====================================

@app.route("/")
def index():

    return render_template("index.html")


@app.route("/api/data")
def get_data():

    return jsonify(latest_data)


@app.route("/api/status")
def mqtt_status():

    return jsonify({
        "mqtt_connected": mqtt_client.is_connected(),
        "topic": MQTT_TOPIC,
        "latest_data": latest_data
    })


# =====================================
# RUN APPLICATION
# =====================================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )
