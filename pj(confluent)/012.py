from confluent_kafka import Consumer, KafkaError, KafkaException
import json
from typing import Dict, Any
import logging
from GBUtil import GBUtil
from SqliteController import SqliteController
import datetime
import time
import random  

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class VehicleDataConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str):
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.vehicle_data = {}
        self.status_data = {}
        self.edgeId = None
        self.edgeTy = None
        self.sqlitectrl = SqliteController("./sqlite_db/test.db")
        self.gbutil = GBUtil()
        self.data_received = {
            'vehicle': False,
            'status': False
        }
        
    def connect(self, topics: list):
        try:
            self.consumer.subscribe(topics)
            logging.info(f"🟢 Consumer ({self.consumer_config['group.id']}) 시작!")
        except KafkaException as e:
            logging.error(f"Kafka 연결 실패: {e}")
            raise

    def data_merge(self):
        if not (self.data_received['vehicle'] and self.data_received['status']):
            return None
            
        now = datetime.datetime.now()
        header_repack = dict()

        header_repack["cmd"] = str("rep")
        header_repack["actn"] = str("vehicle")
        header_repack["dtlActn"] = str("common")
        header_repack["strtpnt"] = "N"
        header_repack["dstn"] = "H"

        header_repack["userId"] = ""
        header_repack["edgeId"] = self.edgeId
        header_repack["edgeTy"] = self.edgeTy

        new_timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        header_repack["timestamp"] = new_timestamp    
        header_repack["command"] = "60320"
        
        if self.vehicle_data and self.status_data:
            json_message = dict()
            merge = {**self.vehicle_data, **self.status_data}
            
            snake = self.gbutil.tosnake_dictname(merge)
            db_in_datas = self.gbutil.dictToSql(snake)
            db_in_datas['VEHICLE_ID'] = "\"" + self.edgeId + "\""
            db_in_datas['VEHICLE_TYPE'] = "\"" + self.edgeTy + "\""
            db_conditions = {'tablename': '"VEHICLE_ING_INFO"'}
            try:
                result = self.sqlitectrl.base_insert(db_conditions, db_in_datas)            
            except Exception as e:
                print(e)
            
            # 데이터 초기화
            self.vehicle_data = {}
            self.status_data = {}
            self.data_received = {
                'vehicle': False,
                'status': False
            }
            
            return merge
        return None
    
    def process_message(self, data: Dict[str, Any]):
        try:
            action = data["header"]["actn"]
            if action == "vehicle":
                self.vehicle_data = data["data"]
                self.data_received['vehicle'] = True
                logging.info(f"차량 정보 업데이트: {self.vehicle_data}")
            elif action == "status":
                self.status_data = data["data"]
                self.data_received['status'] = True
                logging.info(f"위치 정보 업데이트: {self.status_data}")
        except KeyError as e:
            logging.error(f"데이터 처리 중 오류 발생: {e}")

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
                    
                    self.edgeId = data["header"]["edgeId"]
                    self.edgeTy = data["header"]["edgeTy"]
                    self.process_message(data)
                    
                    # 두 데이터가 모두 수신되었을 때만 병합
                    merged_data = self.data_merge()
                    if merged_data:
                        print("\n" + "="*50)
                        print(f"📍 병합: {merged_data}")
                        print("="*50 + "\n")
                    
                except json.JSONDecodeError as e:
                    logging.error(f"JSON 디코딩 오류: {e}")
                
        except KeyboardInterrupt:
            logging.info("프로그램 종료 요청됨")
        finally:
            self.consumer.close()
            logging.info("Consumer 종료됨")

if __name__ == "__main__":
    consumer = VehicleDataConsumer(
        bootstrap_servers='localhost:9092',
        group_id=random.randint(0, 100)
    )
    consumer.connect(['rep'])
    consumer.run()