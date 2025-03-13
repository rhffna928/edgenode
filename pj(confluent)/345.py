from confluent_kafka import Consumer, KafkaError, KafkaException
import json
from GBUtil import GBUtil
from SqliteController import SqliteController
import random
import logging
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class StreetSweeperConsumer:
    def __init__(self, bootstrap_servers: str):
        self.group_id = random.randint(0, 100)
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.gbutil = GBUtil()
        self.sqlitectrl = SqliteController("./sqlite_db/test.db")

    def connect(self, topics: list):
        try:
            self.consumer.subscribe(topics)
            logging.info(f"🟢 Consumer 4 ({self.group_id}) 시작!")
        except KafkaException as e:
            logging.error(f"Kafka 연결 실패: {e}")
            raise

    def process_cls_data(self, data: Dict[str, Any]) -> bool:
        try:
            snake = self.gbutil.tosnake_dictname(data["data"])
            logging.debug(f"변환된 snake case 데이터: {snake}")
            
            db_in_datas = self.gbutil.dictToSql(snake)
            db_in_datas['VEHICLE_ID'] = f"\"{data['header']['edgeId']}\""
            db_conditions = {'tablename': '"STREET_SWEEPER_INFO"'}
            
            logging.debug(f"DB 입력 데이터: {db_in_datas}")
            try:
                result = self.sqlitectrl.base_insert(db_conditions, db_in_datas)
                logging.info(f"✅ [Consumer 4] cls 데이터 처리 성공: {result}")
                return result
            except Exception as e:
                logging.error(f"데이터 처리 중 오류 발생: {e}")
                return False
            
        except Exception as e:
            logging.error(f"데이터 처리 중 오류 발생: {e}")
            return False

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.warning("파티션 끝에 도달했습니다.")
                    else:
                        logging.error(f"Kafka 에러 발생: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value())
                    
                    if data["header"]["actn"] == "cls":
                        result = self.process_cls_data(data)
                        print("\n" + "="*50)
                        print(f"📍 처리 결과: {result}")
                        print("="*50 + "\n")
                    
                except json.JSONDecodeError as e:
                    logging.error(f"JSON 디코딩 오류: {e}")
                
        except KeyboardInterrupt:
            logging.info("프로그램 종료 요청됨")
        finally:
            self.consumer.close()
            logging.info("Consumer 종료됨")

if __name__ == "__main__":
    consumer = StreetSweeperConsumer(bootstrap_servers='localhost:9092')
    consumer.connect(['shared_topic'])
    consumer.run()