import paho.mqtt.client as mqtt
import time
import threading
import matplotlib.pyplot as plt
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
    
def plot_data():
    while True:
        if topic_stats:
            topics = list(topic_stats.keys())
            counts = [stats["count"] for stats in topic_stats.values()]

            plt.clf()  # 이전 플롯 지우기
            plt.bar(topics, counts)
            plt.xlabel('토픽')
            plt.ylabel('메시지 수')
            plt.title('MQTT 토픽 메시지 수')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.pause(1)  # 1초 대기 후 다시 그리기
        else:
            plt.clf()
            plt.text(0.5, 0.5, '구독 중인 토픽이 없습니다.', horizontalalignment='center', verticalalignment='center')
            plt.pause(1)    
if __name__ == "__main__":
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()
    
    # 통계 출력 루프 시작
    stats_thread = threading.Thread(target=plot_data, daemon=True)
    stats_thread.start()

    
    plt.show()  # 플롯 창 표시