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
    
    def __init__(self, bootstrap_servers: str, group_id: str):
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.vehicle_info = None
        self.vehicle_location = None
        self.edgeId = None
        self.edgeTy = None
        self.timer_interval = 2
        self.sqlitectrl = SqliteController("./sqlite_db/test.db")
        self.gbutil = GBUtil()
       
        
    def connect(self, topics: list):
        try:
            self.consumer.subscribe(topics)
            
            logging.info(f"🟢 Consumer 1({self.consumer_config['group.id']}) 시작!")
        except KafkaException as e:
            logging.error(f"Kafka 연결 실패: {e}")
            raise

    def data_merge(self):
        """스레드 실행메소드"""
        while(True):
            time.sleep(2)
            now = datetime.datetime.now()

            # 공통정보, 위치 머지한값 보내지
            header_repack = dict()

            header_repack["cmd"] = str("rep")
            header_repack["actn"] = str("vehicle")
            header_repack["dtlActn"] = str("common")
            header_repack["strtpnt"] = "N"
            header_repack["dstn"] = "H"

            header_repack["userId"] = ""
            header_repack["edgeId"] = self.edgeId
            header_repack["edgeTy"] = self.edgeTy
            
            new_timestamp = now.strftime("%Y:%m:%d-%H:%M:%S.%f")

            header_repack["timestamp"] = new_timestamp    
            header_repack["command"] = "60320"
            logging.info("************** work_doing_monitor ********************************************** {0}, {1}, {2}, {3}".format(self.vehicle_info, self.vehicle_location,self.edgeId, self.edgeTy ))

            if self.vehicle_info is not None and self.vehicle_location is not None and self.edgeId is not None and self.edgeTy is not None:
                json_message = dict()
                
                #data_message = self.vehicle_info + self.vehicle_location
                data_merge = {**self.vehicle_info, **self.vehicle_location}

                snake = self.gbutil.tosnake_dictname(data_merge)
                db_in_datas = self.gbutil.dictToSql(snake)
                db_in_datas['VEHICLE_ID'] = "\"" + self.edgeId + "\""
                db_in_datas['VEHICLE_TYPE'] = "\"" + self.edgeTy + "\""
                db_conditions = {'tablename': '"VEHICLE_ING_INFO"'}
                try:
                    result = self.sqlitectrl.base_insert(db_conditions, db_in_datas)            
                except Exception as e:
                    print(e)
                logging.info("************** work_doing_monitor ********************")
            # self.vehicle_info = None
            # self.vehicle_location = None

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
                    _actn = data["header"]["actn"]
                    _dtlActn = data["header"]["dtlActn"]
                    
                    if _actn in ["status", "vehicle"]:
                        if _dtlActn == "info":
                            self.vehicle_info = data["data"]
                            #print(data["data"])
                        elif _dtlActn == "position":
                            self.vehicle_location = data["data"]
                            self.data_merge()   

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

    consumer = VehicleDataConsumer(
        bootstrap_servers=KAFKA_BROKER,
        group_id=random.randint(0, 100)
    )
    consumer.connect(['connect'])
    consumer.run()
    try:
        run = threading.Thread(target=VehicleDataConsumer.run, args=())
        run.start()
        #data merge 시작
        merge = threading.Thread(target=VehicleDataConsumer.data_merge, args=("param1", "param2"))
        merge.start()

    except:
        consumer.commit()
        logger.exception("서비스 실행 실패...")
        sys.exit()
