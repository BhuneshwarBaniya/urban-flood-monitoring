
import os
import json
import uuid
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
MQTT_KEEPALIVE = 60

MQTT_CLIENT_ID = "flood-dashboard-" + uuid.uuid4().hex[:10]

mqtt_client = None
mqtt_connected = False

data_lock = threading.Lock()
mqtt_lock = threading.Lock()


# ==========================================
# 3. SENSOR DATA
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
    "ultrasonic_valid": False,
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
        print("MQTT CONNECTED SUCCESSFULLY")
        print("CLIENT ID:", MQTT_CLIENT_ID)
        print("BROKER:", MQTT_BROKER)
        print("TOPIC:", MQTT_TOPIC)

        result, message_id = client.subscribe(
            MQTT_TOPIC,
            qos=0
        )

        print("SUBSCRIBE RESULT:", result)
        print("MESSAGE ID:", message_id)
        print("================================")

    else:
        mqtt_connected = False
        print("MQTT CONNECTION REJECTED:", reason_code)


def on_disconnect(client, userdata, disconnect_flags,
                  reason_code, properties):
    global mqtt_connected

    mqtt_connected = False

    print("================================")
    print("MQTT DISCONNECTED")
    print("REASON:", reason_code)
    print("================================")


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    print("MQTT SUBSCRIPTION CONFIRMED:", reason_code_list)


def on_message(client, userdata, message):

    try:
        payload = json.loads(
            message.payload.decode("utf-8")
        )

        if not isinstance(payload, dict):
            print("INVALID MQTT PAYLOAD")
            return

        with data_lock:
            latest_data.update(payload)

            latest_data["last_updated"] = (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

        print("================================")
        print("MQTT MESSAGE RECEIVED")
        print("TOPIC:", message.topic)
        print("DATA:", payload)
        print("================================")

    except Exception as error:
        print("MQTT MESSAGE ERROR:", repr(error))


def on_connect_fail(client, userdata):
    print("MQTT CONNECTION ATTEMPT FAILED")


# ==========================================
# 5. START MQTT
# ==========================================

def start_mqtt():

    global mqtt_client

    with mqtt_lock:

        if mqtt_client is not None:
            print("MQTT CLIENT ALREADY CREATED")
            return

        print("================================")
        print("STARTING FLOOD MONITORING MQTT")
        print("PROCESS ID:", os.getpid())
        print("================================")

        try:

            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=MQTT_CLIENT_ID,
                protocol=mqtt.MQTTv311
            )

            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.on_message = on_message
            client.on_subscribe = on_subscribe
            client.on_connect_fail = on_connect_fail

            client.reconnect_delay_set(
                min_delay=1,
                max_delay=30
            )

            # Start asynchronous connection
            client.connect_async(
                MQTT_BROKER,
                MQTT_PORT,
                MQTT_KEEPALIVE
            )

            # Start MQTT background network loop
            client.loop_start()

            mqtt_client = client

            print("MQTT CLIENT STARTED")
            print("CLIENT ID:", MQTT_CLIENT_ID)
            print("BROKER:", MQTT_BROKER)
            print("PORT:", MQTT_PORT)
            print("TOPIC:", MQTT_TOPIC)

        except Exception as error:
            mqtt_client = None
            print("MQTT STARTUP ERROR:", repr(error))


# ==========================================
# 6. GET SENSOR DATA
# ==========================================

def get_latest_data():

    with data_lock:
        return dict(latest_data)


# ==========================================
# 7. DASHBOARD ROUTE
# ==========================================

@app.route("/")
def home():

    return render_template("index.html")


# ==========================================
# 8. SENSOR DATA API
# ==========================================

@app.route("/api/data")
def get_data():

    return jsonify(get_latest_data())


# ==========================================
# 9. MQTT STATUS API
# ==========================================

@app.route("/api/status")
def get_status():

    connected = False

    if mqtt_client is not None:

        try:
            connected = mqtt_client.is_connected()

        except Exception as error:
            print("MQTT STATUS ERROR:", repr(error))

    data = get_latest_data()

    return jsonify({

        "process_id": os.getpid(),

        "mqtt_connected": connected,

        "broker": MQTT_BROKER,

        "port": MQTT_PORT,

        "topic": MQTT_TOPIC,

        "client_id": MQTT_CLIENT_ID,

        "latest_data": data

    })


# ==========================================
# 10. START MQTT CLIENT
# ==========================================

start_mqtt()


# ==========================================
# 11. LOCAL DEVELOPMENT
# ==========================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
