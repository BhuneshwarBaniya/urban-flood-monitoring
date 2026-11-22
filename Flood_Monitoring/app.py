
import os
import json
import uuid
import time
import socket
import threading
import encodings.idna

from datetime import datetime
from flask import Flask, render_template, jsonify
import paho.mqtt.client as mqtt


# ==========================================
# 1. FLASK CONFIGURATION
# ==========================================

app = Flask(__name__)


# ==========================================
# 2. MQTT CONFIGURATION
# ==========================================

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "flood_system/telemetry"

# Unique ID prevents accidental client ID conflicts
MQTT_CLIENT_ID = "flood-dashboard-" + uuid.uuid4().hex[:10]

MQTT_KEEPALIVE = 60

mqtt_client = None
mqtt_connected = False

data_lock = threading.Lock()


# ==========================================
# 3. DEFAULT SENSOR DATA
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
# 4. MQTT CALLBACKS
# ==========================================

def on_connect(client, userdata, flags, reason_code, properties):
    global mqtt_connected

    if reason_code == 0:
        mqtt_connected = True

        print("================================")
        print("MQTT CONNECTED SUCCESSFULLY!")
        print("CLIENT ID:", MQTT_CLIENT_ID)
        print("BROKER:", MQTT_BROKER)

        result, message_id = client.subscribe(MQTT_TOPIC)

        if result == mqtt.MQTT_ERR_SUCCESS:
            print("SUBSCRIBED TO:", MQTT_TOPIC)
        else:
            print("SUBSCRIPTION FAILED:", result)

        print("================================")

    else:
        mqtt_connected = False
        print("MQTT CONNECTION REJECTED:", reason_code)


def on_disconnect(client, userdata, disconnect_flags,
                  reason_code, properties):
    global mqtt_connected

    mqtt_connected = False

    print("================================")
    print("MQTT DISCONNECTED!")
    print("REASON:", reason_code)
    print("================================")


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    print("MQTT SUBSCRIPTION CONFIRMED:", reason_code_list)


def on_message(client, userdata, message):
    global latest_data

    try:
        payload = json.loads(message.payload.decode("utf-8"))

        if not isinstance(payload, dict):
            print("INVALID MQTT DATA FORMAT")
            return

        with data_lock:
            latest_data.update(payload)
            latest_data["last_updated"] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        print("================================")
        print("MQTT MESSAGE RECEIVED!")
        print("TOPIC:", message.topic)
        print("UPDATED SENSOR DATA:")
        print(latest_data)
        print("================================")

    except Exception as error:
        print("MQTT MESSAGE ERROR:", error)


def on_connect_fail(client, userdata):
    print("MQTT CONNECTION ATTEMPT FAILED")


# ==========================================
# 5. START MQTT CLIENT
# ==========================================

def start_mqtt():
    global mqtt_client

    print("\n================================")
    print("STARTING FLOOD MONITORING MQTT")
    print("================================")

    try:
        # Check DNS
        print("CHECKING BROKER DNS...")

        address = socket.gethostbyname(MQTT_BROKER)
        print("DNS SUCCESSFUL:", address)

        # Create MQTT client
        mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv311
        )

        # Register callbacks
        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        mqtt_client.on_message = on_message
        mqtt_client.on_subscribe = on_subscribe
        mqtt_client.on_connect_fail = on_connect_fail

        # Automatic reconnection
        mqtt_client.reconnect_delay_set(
            min_delay=1,
            max_delay=30
        )

        print("CONNECTING TO MQTT BROKER...")

        # Connect to broker
        result = mqtt_client.connect(
            MQTT_BROKER,
            MQTT_PORT,
            MQTT_KEEPALIVE
        )

        print("MQTT CONNECT RETURN CODE:", result)

        # Start background network loop
        mqtt_client.loop_start()

        print("MQTT NETWORK LOOP STARTED!")
        print("CLIENT ID:", MQTT_CLIENT_ID)
        print("TOPIC:", MQTT_TOPIC)

    except Exception as error:
        print("MQTT STARTUP ERROR:", repr(error))


# ==========================================
# 6. DASHBOARD ROUTE
# ==========================================

@app.route("/")
def home():
    return render_template("index.html")


# ==========================================
# 7. SENSOR DATA API
# ==========================================

@app.route("/api/data")
def get_data():

    with data_lock:
        data = dict(latest_data)

    return jsonify(data)


# ==========================================
# 8. MQTT STATUS API
# ==========================================

@app.route("/api/status")
def get_status():

    connected = False

    if mqtt_client is not None:
        try:
            connected = mqtt_client.is_connected()
        except Exception as error:
            print("STATUS CHECK ERROR:", error)

    return jsonify({
        "process_id": os.getpid(),
        "mqtt_connected": connected,
        "broker": MQTT_BROKER,
        "port": MQTT_PORT,
        "topic": MQTT_TOPIC,
        "client_id": MQTT_CLIENT_ID,
        "latest_data": get_latest_data()
    })


def get_latest_data():
    with data_lock:
        return dict(latest_data)


# ==========================================
# 9. START MQTT WHEN APP LOADS
# ==========================================

start_mqtt()


# ==========================================
# 10. LOCAL DEVELOPMENT
# ==========================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
