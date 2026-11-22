
import encodings.idna
"test".encode("idna")

import json
import socket
import threading
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

MQTT_CLIENT_ID = "flood_monitor_flask_server"


# ==========================================
# 3. LATEST SENSOR DATA
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

data_lock = threading.Lock()

mqtt_connected = False
mqtt_client = None


# ==========================================
# 4. MQTT CALLBACKS
# ==========================================

def on_connect(client, userdata, flags, reason_code, properties):

    global mqtt_connected

    print("================================")
    print("MQTT CONNECTION CALLBACK")
    print("Connection result:", reason_code)

    if reason_code == 0:

        mqtt_connected = True

        print("MQTT CONNECTED SUCCESSFULLY!")
        print("Broker:", MQTT_BROKER)
        print("Topic:", MQTT_TOPIC)

        result, mid = client.subscribe(MQTT_TOPIC)

        print("SUBSCRIBE RESULT:", result)
        print("SUBSCRIBE MESSAGE ID:", mid)

    else:

        mqtt_connected = False

        print("MQTT CONNECTION REJECTED:", reason_code)

    print("================================")


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties
):

    global mqtt_connected

    mqtt_connected = False

    print("MQTT DISCONNECTED!")
    print("Reason:", reason_code)


def on_message(client, userdata, message):

    global latest_data

    try:

        print("================================")
        print("MQTT MESSAGE RECEIVED!")
        print("Topic:", message.topic)

        received_data = json.loads(
            message.payload.decode("utf-8")
        )

        with data_lock:

            latest_data.update(received_data)

            latest_data["last_updated"] = (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

            current_data = dict(latest_data)

        print("UPDATED SENSOR DATA:")
        print(current_data)

        print("================================")

    except Exception as e:

        print("MESSAGE PROCESSING ERROR:", repr(e))


def on_connect_fail(client, userdata):

    print("MQTT CONNECTION ATTEMPT FAILED!")


# ==========================================
# 5. MQTT CLIENT SETUP
# ==========================================

def start_mqtt():

    global mqtt_client

    print("================================")
    print("STARTING MQTT CLIENT")
    print("BROKER:", MQTT_BROKER)
    print("PORT:", MQTT_PORT)
    print("TOPIC:", MQTT_TOPIC)
    print("================================")

    try:

        # Check DNS resolution

        print("CHECKING BROKER DNS...")

        addresses = socket.getaddrinfo(
            MQTT_BROKER,
            MQTT_PORT,
            type=socket.SOCK_STREAM
        )

        print("DNS SUCCESSFUL:", addresses[0][4])

        # Check TCP connection

        print("CHECKING TCP CONNECTION...")

        connection = socket.create_connection(
            (MQTT_BROKER, MQTT_PORT),
            timeout=8
        )

        connection.close()

        print("TCP CONNECTION SUCCESSFUL!")

    except Exception as e:

        print("NETWORK CHECK ERROR:", repr(e))


    try:

        mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv311
        )

        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        mqtt_client.on_message = on_message
        mqtt_client.on_connect_fail = on_connect_fail

        mqtt_client.reconnect_delay_set(
            min_delay=1,
            max_delay=30
        )

        print("CONNECTING TO MQTT BROKER...")

        # First attempt: direct connection

        try:

            result = mqtt_client.connect(
                MQTT_BROKER,
                MQTT_PORT,
                keepalive=60
            )

            print("MQTT CONNECT RETURN CODE:", result)

            mqtt_client.loop_start()

            print("MQTT NETWORK LOOP STARTED!")

        except Exception as direct_error:

            print(
                "DIRECT CONNECTION FAILED:",
                repr(direct_error)
            )

            # Fallback: asynchronous connection

            print("TRYING ASYNCHRONOUS CONNECTION...")

            mqtt_client.connect_async(
                MQTT_BROKER,
                MQTT_PORT,
                keepalive=60
            )

            mqtt_client.loop_start()

            print("ASYNC MQTT LOOP STARTED!")

    except Exception as e:

        print("MQTT STARTUP ERROR:", repr(e))

        mqtt_client = None


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

        except Exception:

            connected = False

    return jsonify({

        "broker": MQTT_BROKER,

        "port": MQTT_PORT,

        "topic": MQTT_TOPIC,

        "mqtt_connected": connected,

        "latest_data": dict(latest_data)

    })


# ==========================================
# 9. START MQTT
# ==========================================

start_mqtt()


# ==========================================
# 10. RUN FLASK LOCALLY
# ==========================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False
    )
