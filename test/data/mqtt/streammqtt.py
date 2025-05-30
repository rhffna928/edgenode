import streamlit as st
import paho.mqtt.client as mqtt
import pandas as pd
import threading
import time
from collections import defaultdict

# --- 전역 상태 저장용 ---
if "topic_stats" not in st.session_state:
    st.session_state.topic_stats = defaultdict(lambda: {
        "count": 0,
        "last_message": "",
        "last_timestamp": "",
    })

# --- MQTT 콜백 함수 ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT 연결 성공")
        client.subscribe("#")
    else:
        print(f"MQTT 연결 실패: {rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

    stats = st.session_state.topic_stats[topic]
    stats["count"] += 1
    stats["last_message"] = payload
    stats["last_timestamp"] = timestamp

# --- MQTT 스레드 시작 ---
def start_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("223.130.131.234", 31883, 60)
    client.loop_forever()

if __name__ == "__main__":
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()