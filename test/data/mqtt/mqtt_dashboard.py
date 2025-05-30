import streamlit as st
import threading
import queue
import paho.mqtt.client as mqtt
import time

# --- 전역 변수 ---
msg_queue = queue.Queue()
topic_list = set()

# --- MQTT 콜백 함수 ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT 연결 성공")
        client.subscribe("#")  # 모든 토픽 구독
    else:
        print(f"MQTT 연결 실패: {rc}")

def on_message(client, userdata, msg):
    topic_list.add(msg.topic)
    payload = msg.payload.decode("utf-8")
    msg_queue.put((msg.topic, payload, time.strftime('%Y-%m-%d %H:%M:%S')))

# --- MQTT 클라이언트 초기화 ---
def start_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("223.130.131.234", 1883, 60)  # 브로커 주소 및 포트
    client.loop_forever()

# --- MQTT 리스너 스레드 시작 ---
mqtt_thread = threading.Thread(target=start_mqtt)
mqtt_thread.daemon = True
mqtt_thread.start()

# --- Streamlit 대시보드 ---
st.set_page_config(page_title="MQTT 메시지 대시보드", layout="wide")
st.title("📡 실시간 MQTT 대시보드")

col1, col2 = st.columns([2, 5])

with col1:
    st.subheader("📍 구독 중인 토픽")
    topic_box = st.empty()

with col2:
    st.subheader("📥 실시간 메시지")
    log_box = st.empty()

log_lines = []

# --- 메인 루프 ---
while True:
    # 토픽 리스트 갱신
    topic_box.markdown("\n".join(f"- `{t}`" for t in sorted(topic_list)))

    # 메시지 큐에 데이터가 있으면 로그에 출력
    while not msg_queue.empty():
        topic, payload, timestamp = msg_queue.get()
        log_lines.insert(0, f"{topic} ")

    #log_box.markdown("\n".join((len(log_lines))))
    log_box.markdown(f"메시지 수: {len(log_lines)}")
    time.sleep(0.5)
