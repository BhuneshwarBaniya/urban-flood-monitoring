
import encodings.idna
"test".encode("idna")

import os
import json
import socket
from datetime import datetime

from flask import Flask, render_template, jsonify
import paho.mqtt.client as mqtt


app = Flask(__name__)


# ==========================================
# MQTT CONFIGURATION
# ==========================================

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "flood_system/telemetry"

MQTT_CLIENT_ID = "flood_monitor_flask_server"


# ==========================================
# LATEST SENSOR DATA
# ==========================================

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


# ==========================================
# MQTT CALLBACKS
# ==========================================

def on_connect(client, userdata, flags, reason_code, properties=None):

    print("MQTT CONNECTION RESULT:", reason_code)

    if reason_code == 0:
        print("MQTT CONNECTED SUCCESSFULLY!")

        result = client.subscribe(MQTT_TOPIC)

        print("SUBSCRIBE RESULT:", result)
        print("SUBSCRIBED TO:", MQTT_TOPIC)

    else:
        print("MQTT CONNECTION FAILED:", reason_code)


def on_connect_fail(client, userdata):

    print("MQTT CONNECTION ATTEMPT FAILED!")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):

    print("MQTT DISCONNECTED:", reason_code)


def on_message(client, userdata, msg):

    global latest_data

    try:
        print("MQTT MESSAGE RECEIVED:", msg.topic)

        data = json.loads(msg.payload.decode("utf-8"))

        data["last_updated"] = datetime.now().strftime("%H:%M:%S")

        latest_data.update(data)

        print("RECEIVED ESP32 DATA:", latest_data)

    except Exception as e:
        print("ERROR PARSING MQTT MESSAGE:", repr(e))


def on_log(client, userdata, level, buf):

    print("MQTT DEBUG:", buf)


# ==========================================
# CREATE MQTT CLIENT
# ==========================================

mqtt_client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=MQTT_CLIENT_ID,
    protocol=mqtt.MQTTv311
)

mqtt_client.on_connect = on_connect
mqtt_client.on_connect_fail = on_connect_fail
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message
mqtt_client.on_log = on_log

mqtt_client.reconnect_delay_set(
    min_delay=1,
    max_delay=30
)


# ==========================================
# DNS CONNECTIVITY TEST
# ==========================================

print("================================")
print("CHECKING MQTT NETWORK CONNECTION")
print("================================")

try:
    addresses = socket.getaddrinfo(
        MQTT_BROKER,
        MQTT_PORT,
        type=socket.SOCK_STREAM
    )

    print("DNS RESOLUTION SUCCESSFUL")
    print("BROKER ADDRESS:", addresses[0][4])

except Exception as e:
    print("DNS RESOLUTION FAILED:", repr(e))


# ==========================================
# TCP CONNECTIVITY TEST
# ==========================================

try:
    print("TESTING TCP CONNECTION TO MQTT BROKER...")

    connection = socket.create_connection(
        (MQTT_BROKER, MQTT_PORT),
        timeout=8
    )

    print("TCP CONNECTION SUCCESSFUL!")

    connection.close()

except Exception as e:
    print("TCP CONNECTION FAILED:", repr(e))


# ==========================================
# START MQTT CLIENT
# ==========================================

try:
    print("STARTING MQTT CLIENT...")
    print("BROKER:", MQTT_BROKER)
    print("PORT:", MQTT_PORT)
    print("TOPIC:", MQTT_TOPIC)

    mqtt_client.connect_async(
        MQTT_BROKER,
        MQTT_PORT,
        keepalive=60
    )

    mqtt_client.loop_start()

    print("MQTT CLIENT STARTED SUCCESSFULLY")

except Exception as e:
    print("MQTT START ERROR:", repr(e))


# ==========================================
# FLASK ROUTES
# ==========================================

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
        "broker": MQTT_BROKER,
        "port": MQTT_PORT,
        "topic": MQTT_TOPIC,
        "latest_data": latest_data
    })


# ==========================================
# RUN FLASK APPLICATION
# ==========================================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )
