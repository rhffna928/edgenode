from confluent_kafka import Consumer, KafkaError, KafkaException
import json
from typing import Dict, Any
import logging
from GBUtil import GBUtil
from SqliteController import SqliteController
import datetime
import time
import random  
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path
import configparser
import sys
import threading

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG = Path(ROOT_DIR) / 'nodecommsrv.ini' # config.ini 설정
logger = None
file_encoding = 'utf-8'
send_encoding = 'CP949'
recv_encoding = 'CP949'
# 로깅 설정
def create_rotating_log(path, _config):
    _logger_level = _config['LOG_LEVEL']
    _logger_when = _config['LOG_WHEN']
    _logger_interval = _config['LOG_INTERVAL']
    _logger_backupcount = _config['LOG_BACKUPCOUNT']

    set_level = logging.INFO

    if _logger_level == "INFO":
        set_level = logging.INFO
    elif _logger_level == "DEBUG":
        set_level = logging.DEBUG
    elif _logger_level == "NOTICE":
        set_level = logging.NOTICE
    elif _logger_level == "ERR":
        set_level = logging.ERROR
        
    global logger
    """
    Creates a rotating log
    """
    logging.basicConfig(level=set_level, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s')
    logger = logging.getLogger("")
    logger.setLevel(set_level)

    #print("_logger_level: {0}, _logger_backupcount:{1}, _logger_when: {2}".format(_logger_level, _logger_backupcount, _logger_when ));
    
    # add a rotating handler
    """ 
    handler = RotatingFileHandler(path, maxBytes=100000000,
                                  backupCount=5)
    """
    handler = TimedRotatingFileHandler(path,
                                       when=_logger_when,
                                       interval=int(_logger_interval),
                                       backupCount=int(_logger_backupcount),
                                       encoding='utf-8')
                                          
    handler.suffix = "-%Y%m%d_%H-%M-%S"
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s' )    
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)


def get_log():
    return logger
class VehicleDataConsumer:
    
    logger = get_log()
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
        self.edgeId = None
        self.edgeTy = None
        self.timer_interval = 2

    def connect(self, topics: list):
        try:
            self.consumer.subscribe(topics)
        except KafkaException as e:
            logging.error(f"Kafka 연결 실패: {e}")
            raise

    def delete_list_row(self):
        db_conditions = {'tablename': '"VEHICLE_LIST"'}
        try:
            self.sqlitectrl.base_delete(db_conditions)
        except Exception as e:
            logging.error(f"데이터베이스에서 행 삭제 실패: {e}")
            raise
    
    def insert_vehicle_list(self, data):
        db_conditions = {'tablename': '"VEHICLE_LIST"'}
        try:
            for idx, key in enumerate(data):
                
                db_in_datas = {}

                db_in_datas['VEHICLE_ID'] = "\""+data[idx][0]+"\""
                db_in_datas['VEHICLE_NAME'] = "\""+data[idx][1]+"\""
                db_in_datas['VEHICLE_TYPE'] = "\""+data[idx][2]+"\""
                db_in_datas['USER_ID'] = "\""+data[idx][3]+"\""
                self.sqlitectrl.base_insert(db_conditions, db_in_datas)
        except Exception as e:
            logging.error(f"차량 정보 삽입 실패: {e}")
            raise
    
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
                    self.delete_list_row()
                    data = json.loads(str(msg.value().decode('utf-8')))
                    #print(data)
                    #print(type(msg.value()))
                    #print(type(data))
                    self.insert_vehicle_list(data["edgeList"])
                    
                except json.JSONDecodeError as e:
                    logging.error(f"JSON 디코딩 오류: {e}")
                
        except KeyboardInterrupt:
            logging.info("프로그램 종료 요청됨")
        finally:
            self.consumer.close()
            logging.info("Consumer 종료됨")

if __name__ == "__main__":
        
    _config = configparser.ConfigParser()
    _config.read(CONFIG, encoding=file_encoding) # definition.py에 등록된 config.ini
    _kafka_broker = _config['KAFKA']['KAFKA_BROKER']
    _kafka_port = _config['KAFKA']['KAFKA_PORT']
    KAFKA_BROKER = f'{_kafka_broker}:{_kafka_port}'
    full_path = os.path.join(ROOT_DIR, "logs", "nodecommsrv.log")
    create_rotating_log(full_path, _config['LOGGER'])    
    consumer = VehicleDataConsumer(bootstrap_servers=KAFKA_BROKER)
    consumer.connect(['init'])
    
    try:
        merge = threading.Thread(target=VehicleDataConsumer.run, args=(consumer,))
        merge.start()
    except:
        logger.exception("서비스 실행 실패...")
        sys.exit()  