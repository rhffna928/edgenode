import paho.mqtt.client as mqtt
import time
import threading
from collections import defaultdict

topic_stats = defaultdict(lambda: {"count": 0, "last_message": "", "last_timestamp": ""})

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT 연결 성공")
        client.subscribe("#")  # 모든 토픽 구독
    else:
        print(f"MQTT 연결 실패: {rc}")
        
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

    # 통계 업데이트
    topic_stats[topic]["count"] += 1
    topic_stats[topic]["last_message"] = payload
    topic_stats[topic]["last_timestamp"] = timestamp

def start_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("223.130.131.234", 31883, 60)
    client.loop_forever()

def print_stats_loop(interval=1):
    while True:
        print("\n--- MQTT 토픽 통계 ---")
        print(f"현재 시간: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        if topic_stats:
            for topic, stats in topic_stats.items():
                print(f"토픽: {topic}")
                print(f"  메시지 수: {stats['count']}")
                print(f"  마지막 메시지: {stats['last_message']}")
                print(f"  마지막 타임스탬프: {stats['last_timestamp']}")
            print("-------------------------\n")
        else:
            print("구독 중인 토픽이 없습니다.")
        time.sleep(interval)

if __name__ == "__main__":
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()
    print_stats_loop(1)  # 통계 출력 루프 시작