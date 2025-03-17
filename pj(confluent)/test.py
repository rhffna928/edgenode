from confluent_kafka import Consumer, KafkaError
import json
from datetime import datetime
from typing import Dict
from dataclasses import dataclass
from enum import Enum

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
    def __init__(self, kafka_config: Dict):
        self.consumer = Consumer(kafka_config)
        self.topics = ['rep', 'req', 'event', 'heartbeat']
        
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
            
            # 메시지 타입별 출력 형식 지정
            if header["cmd"] == MessageType.REP.value:
                self._print_rep_message(topic, timestamp, data)
            elif header["cmd"] == MessageType.REQ.value:
                self._print_req_message(topic, timestamp, data)
            elif header["cmd"] == MessageType.EVENT.value:
                self._print_event_message(topic, timestamp, data)
            elif header["cmd"] == MessageType.HEARTBEAT.value:
                self._print_heartbeat_message(topic, timestamp, data)
            else:
                print(f"❓ 알 수 없는 메시지 타입: {msg_type}")
                
        except json.JSONDecodeError:
            print(f"❌ JSON 파싱 오류: {msg.value().decode('utf-8')}")
    
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
    
    # 컨슈머 시작
    consumer = MessageTypeConsumer(kafka_config)
    consumer.start()

if __name__ == "__main__":
    main()