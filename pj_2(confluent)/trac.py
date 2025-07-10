from confluent_kafka import Consumer, KafkaError, KafkaException
import json
from GBUtil import GBUtil
from SqliteController import SqliteController
import random
import logging
from typing import Dict, Any
import os
from pathlib import Path
import configparser
from logging.handlers import TimedRotatingFileHandler

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

class TractorConsumer:
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

    def connect(self, topics: list):
        try:
            self.consumer.subscribe(topics)
            logging.info(f"🟢 Consumer 2 ({self.group_id}) 시작!")
        except KafkaException as e:
            logging.error(f"Kafka 연결 실패: {e}")
            raise

    def process_trc_data(self, data: Dict[str, Any]) -> bool:
        try:
            db_in_datas = {}
            
            if data['header']['command'] == "70500":
                snake = self.gbutil.tosnake_dictname(data["data"])
                #print(data["data"])
                #logging.info(f"변환된 snake case 데이터: {snake}")
                db_in_datas = self.gbutil.dictToSql(snake)
                db_in_datas['"VEHICLE_ID"'] = "\""+data['header']['edgeId']+"\""
                db_in_datas['"WORK_AREA"'] = "\""+db_in_datas['"WORK_AREA"']+"\""
                db_in_datas['"WORK_PATH"'] = "\""+ db_in_datas['"WORK_PATH"']+"\""
                db_conditions = {'tablename': '"WORK_INFO_TRACTOR"'}
                try:
                    result = self.sqlitectrl.base_insert(db_conditions, db_in_datas)
                    logging.info(f" workinfo 데이터 처리: {db_in_datas.keys()}")
                    return result
                except Exception as e:
                    logging.error(f"데이터 처리 중 오류 발생: {e}")
                    return False
                
            if data['header']['command'] == "70300":
                
                snake = self.gbutil.tosnake_dictname(data["data"])
                #print(data["data"])
                #logging.info(f"변환된 snake case 데이터: {snake}")
                db_in_datas = self.gbutil.dictToSql(snake)
                db_in_datas['"VEHICLE_ID"'] = "\""+data['header']['edgeId']+"\""
                
                db_conditions = {'tablename': '"TRACTOR_INFO"'}
                try:
                    result = self.sqlitectrl.base_insert(db_conditions, db_in_datas)
                    logging.info(f" trac_info 데이터 처리: {db_in_datas.keys()}")
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
                    
                    if data["header"]["actn"] == "tractor":
                        self.process_trc_data(data)
                    
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
    
    consumer = TractorConsumer(bootstrap_servers=KAFKA_BROKER)
    consumer.connect(['connect'])
    consumer.run()