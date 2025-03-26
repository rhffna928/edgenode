from confluent_kafka import Consumer, KafkaError
import json
from datetime import datetime
from typing import Dict
from dataclasses import dataclass
from enum import Enum
import paho.mqtt.client as mqtt_client
import random
import time

class MessageType(Enum):
    REP = "rep"
    REQ = "req"
    EVENT = "event"
    HEARTBEAT = "heartbeat"

@dataclass
class Message:
    type: MessageType
    timestamp: datetime
    topic: str
    data: Dict

class MessageTypeConsumer:
    def __init__(self, kafka_config: Dict, mqtt_config: Dict):
        self.consumer = Consumer(kafka_config)
        self.topics = ['rep', 'req', 'event', 'heartbeat']
        
        # MQTT 클라이언트 설정
        client_id = f'publish-{random.randint(0, 1000)}'
        self.mqtt_client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_publish = self.on_publish
        
        # MQTT 연결 설정
        self.mqtt_host = mqtt_config.get('host', 'localhost')
        self.mqtt_port = mqtt_config.get('port', 1883)
        self.edgeid = mqtt_config.get('edgeid', '')
        self.edgety = mqtt_config.get('edgety', '')
        
        # MQTT 토픽 설정
        self.topic_subs_base = mqtt_config.get('topic_subs_base', 'edgeplatform/hub')
        self.pub_init_topic = mqtt_config.get('pub_init_topic', 'edgeplatform/hub/init')
        self.pub_edgenode_topic = mqtt_config.get('pub_edgenode_topic', 'edgeplatform/hub/edgenode')
        self.pub_edgenode_mobile_topic = mqtt_config.get('pub_edgenode_mobile_topic', 'edgeplatform/hub/edgenode/mobile')
        
        # MQTT 연결
        self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        self.mqtt_client.loop_start()
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("🟢 MQTT 브로커 연결 성공")
        else:
            print(f"❌ MQTT 브로커 연결 실패 (코드: {rc})")
            
    def on_publish(self, client, userdata, mid):
        print(f"📤 메시지 발행 완료 (ID: {mid})")
        
    def publish_message(self, topic: str, message: str):
        try:
            result = self.mqtt_client.publish(topic, message)
            if result[0] == 0:
                print(f"✅ 메시지 발행 성공: {topic}")
            else:
                print(f"❌ 메시지 발행 실패: {topic}")
        except Exception as e:
            print(f"❌ 메시지 발행 중 오류 발생: {str(e)}")
        
    def start(self):
        # 모든 토픽 구독
        self.consumer.subscribe(self.topics)
        print("🟢 메시지 타입 컨슈머 시작")
        print(f"📥 구독 토픽: {', '.join(self.topics)}")
        
        try:
            while True:
                # 메시지 폴링
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Consumer 오류: {msg.error()}")
                        break
                
                # 메시지 처리
                try:
                    self._process_message(msg)
                except Exception as e:
                    print(f"❌ 데이터 처리 오류: {e}")
                    
        except KeyboardInterrupt:
            print("⏹️ 컨슈머 종료")
        finally:
            self.consumer.close()
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
    
    def _process_message(self, msg):
        """메시지 처리 및 출력"""
        topic = msg.topic()
        timestamp = datetime.now()
        
        try:
            # 메시지 파싱
            message = json.loads(msg.value().decode('utf-8'))
            header = message.get("header", {})
            data = message.get("data", {})
            msg_type = header.get("type", "unknown")
            
            # 메시지 타입별 출력 및 MQTT 발행
            if header["cmd"] == MessageType.REP.value:
                self._print_rep_message(topic, timestamp, data)
                self._publish_rep_message(data)
            elif header["cmd"] == MessageType.REQ.value:
                self._print_req_message(topic, timestamp, data)
                self._publish_req_message(data)
            elif header["cmd"] == MessageType.EVENT.value:
                self._print_event_message(topic, timestamp, data)
                self._publish_event_message(data)
            elif header["cmd"] == MessageType.HEARTBEAT.value:
                self._print_heartbeat_message(topic, timestamp, data)
                self._publish_heartbeat_message(data)
            else:
                print(f"❓ 알 수 없는 메시지 타입: {msg_type}")
                
        except json.JSONDecodeError:
            print(f"❌ JSON 파싱 오류: {msg.value().decode('utf-8')}")
    
    def _publish_rep_message(self, data: Dict):
        self.publish_message(self.pub_edgenode_topic, json.dumps(message, ensure_ascii=False))
    
    def _publish_req_message(self, data: Dict):
        self.publish_message(self.pub_edgenode_topic, json.dumps(message, ensure_ascii=False))
    
    def _publish_event_message(self, data: Dict):
        self.publish_message(self.pub_edgenode_topic, json.dumps(message, ensure_ascii=False))
    
    def _publish_heartbeat_message(self, data: Dict):
        self.publish_message(self.pub_edgenode_topic, json.dumps(message, ensure_ascii=False))
    
    def _print_rep_message(self, topic: str, timestamp: datetime, data: Dict):
        """REP 메시지 출력"""
        print("\n📨 REP 메시지")
        print(f"⏰ 시간: {timestamp}")
        print(f"📌 토픽: {topic}")
        if "vehicle_data" in data:
            print("🚗 차량 데이터:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
        elif "special_data" in data:
            print("🔧 특장 데이터:")
            print(json.dumps(data["special_data"], indent=2, ensure_ascii=False))
        else:
            print("📄 기타 REP 데이터:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
        print("-" * 50)
    
    def _print_req_message(self, topic: str, timestamp: datetime, data: Dict):
        """REQ 메시지 출력"""
        print("\n📤 REQ 메시지")
        print(f"⏰ 시간: {timestamp}")
        print(f"📌 토픽: {topic}")
        print("📄 데이터:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("-" * 50)
    
    def _print_event_message(self, topic: str, timestamp: datetime, data: Dict):
        """이벤트 메시지 출력"""
        print("\n🔔 이벤트 메시지")
        print(f"⏰ 시간: {timestamp}")
        print(f"📌 토픽: {topic}")
        print("📄 데이터:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("-" * 50)
    
    def _print_heartbeat_message(self, topic: str, timestamp: datetime, data: Dict):
        """하트비트 메시지 출력"""
        print("\n💓 하트비트")
        print(f"⏰ 시간: {timestamp}")
        print(f"📌 토픽: {topic}")
        print("📄 데이터:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("-" * 50)

def main():
    # Kafka 설정
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'message_type_consumer_group',
        'auto.offset.reset': 'latest'
    }
    
    # MQTT 설정
    mqtt_config = {
        'host': '223.130.131.234',
        'port': 31883,
        'edgeid': 'EDGE001',
        'edgety': 'TYPE1',
        'topic_subs_base': 'edgeplatform/hub',
        'pub_init_topic': 'edgeplatform/hub/init',
        'pub_edgenode_topic': 'edgeplatform/hub/edgenode',
        'pub_edgenode_mobile_topic': 'edgeplatform/hub/edgenode/mobile'
    }
    
    # 컨슈머 시작
    consumer = MessageTypeConsumer(kafka_config, mqtt_config)
    consumer.start()

if __name__ == "__main__":
    main()