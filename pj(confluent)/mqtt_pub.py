import sys
import os
import random
import time
import json
import time
import datetime
from pathlib import Path
import configparser
import logging
import paho.mqtt.client as mqtt_client
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
from constant import Constant
# java jar 임포트
import jpype
import jpype.imports
from jpype.types import *
from confluent_kafka import Consumer, KafkaError, KafkaException
import threading

client_sockets_1 = []
client_sockets_2 = []
mqtt = None

g_work_info = None

command_tbl = None

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG = Path(ROOT_DIR) / 'nodecommsrv.ini' # config.ini 설정
logger = None
file_encoding = 'utf-8'
send_encoding = 'CP949'
recv_encoding = 'CP949'

# Kafka Consumer 설정 변경
consumer_config = {
    'bootstrap.servers': '172.30.1.20:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
}
consumer = Consumer(consumer_config)
consumer.subscribe(['shared_topic'])

# JVM 시작
jpype.startJVM()
# 원본 암호화 모듈
#jpype.addClassPath("watosysEncrypt_v1.0.0.jar")

# AES256 암호화 모듈
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")

# 자바 클래스 로드
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")
#jvm_aes_class = jpype.JClass("watosys.utils.eg.encrypt.AES")

def sendDisconnectAll(client_sockets):
    logger.info("모든 접속자와의 연결을 끊음")
    msg = 'Disconnect'

    for client in client_sockets:
        conn = client[0]
        conn.sendall(msg.encode())
        conn.close()

    client_sockets.clear()

def sendAll(client_sockets, msg):
    msg += '\r\n';

    for client in client_sockets:        
        conn = client[0]
        conn.sendall(msg.encode(encoding=send_encoding))

def sendString(conn, msg):
    msg += '\r\n'
    conn.sendall(msg.encode(encoding=send_encoding))

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
        set_level = logging.ERR

    global logger
    """
    Creates a rotating log
    """
    logging.basicConfig(level=set_level, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s')
    logger = logging.getLogger("")
    logger.setLevel(set_level)

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

class Mqtt:
    logger = get_log()
    
    
    def __init__(self):
        self.host = None
        self.port = None
        self.recvmessage = None
        self.onmessage_topic = None

        self.strtpnt = None
        self.dstn = None

        
        self.TOPIC_SUBS = None
        self.TOPIC_SUBS_BASE = None
        self.PUB_INIT_TOPIC = None
        self.PUB_EDGENODE_TOPIC = None
        self.PUB_EDGENODE_MOBILE_TOPIC = None

        self.edgeid = None
        self.edgeTy = None
        self.userid = None

        self.notOkAddSub = True

        self.subcribes = None


        client_id = f'publish-{random.randint(0, 1000)}'
        # Old
        #self.client = mqtt_client.Client()

        #self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1,client_id)

        try:
            client_mqtt = self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1)
        except:
            client_mqtt = self.client = mqtt_client.Client()

        return

    def __del__(self):
        
        # JVM 종료
        jpype.shutdownJVM()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Mqtt Broker connected OK")

            # EH init 보낸다.
            _header = dict()
            _data = dict()

            _json_message = dict()

            now = datetime.datetime.now()
            now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")

            _header["cmd"] = "req"
            _header["actn"] = "init"
            _header["dtlActn"] = "all"
            _header["strtpnt"] = "N"
            _header["dstn"] = "H"
            _header["edgeId"] = self.edgeid
            _header["userId"] = ""
            _header["command"] = "00001"
            _header["edgeTy"] = self.edgeTy
            _header["timestamp"] = now_str

            _json_message["header"] = _header

            _data["edgeId"] = self.edgeid

            data_message = str(json.dumps(_data, ensure_ascii=False))
            send_data = jvm_msg_encrypt_class.encode(now_str, data_message)

            _json_message["data"] = str(send_data)

            send_message = json.dumps(_json_message, ensure_ascii=False)

            time.sleep(0.02)

            # edgeId로 Mqtt.subscribe 등록
            self.addSub(self.edgeid)

            #while self.notOkAddSub:
            #    logging.info("subcribe 대기")

            self.pubHub4Init(send_message)

        else:
            logging.info("Mqtt Broker Bad connection Returned code=", rc)

    def on_disconnect(self,client, userdata, flags, rc=0):
        logging.info("Disconnected with result code: %s", rc)
        reconnect_count = 0
        reconnect_delay = 0 
        MAX_RECONNECT_COUNT = 1000

        while reconnect_count < MAX_RECONNECT_COUNT:
            logging.info("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                self.client.reconnect()
                logging.info("Reconnected successfully!")

                #self.start(self.subcribes, True)

                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1

        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)

    def pubHub4Init(self, send_message):
        #self.client.publish(self.PUB_INIT_TOPIC, send_message) 
        self.publish(self.client, self.PUB_INIT_TOPIC, send_message) 


    def pubHub4Node(self, send_message):
        #self.client.publish(self.PUB_EDGENODE_TOPIC, send_message) 
        self.publish(self.client, self.PUB_EDGENODE_TOPIC, send_message) 

    def pubHub4Mobile(self, send_message):
        #self.client.publish(self.PUB_EDGENODE_MOBILE_TOPIC, send_message) 
        self.publish(self.client, self.PUB_EDGENODE_MOBILE_TOPIC, send_message)

    def addSub(self, topic):
        topic = self.TOPIC_SUBS_BASE + "/" + topic
        #topic = "edgeplatform/hub/" + topic
        
        self.client.subscribe(topic, 0)

        self.notOkAddSub = True

    def setLogger(self, logger):
        self.logger = logger

    def ready(self, _host, _port, _edgeid, _edgety, _topic_subs_base, _pub_init_topic, _pub_edgenode_topic):
        self.edgeid = _edgeid
        self.edgeTy = _edgety
        self.host = _host
        self.port = _port
        self.TOPIC_SUBS_BASE = _topic_subs_base
        self.PUB_INIT_TOPIC = _pub_init_topic
        self.PUB_EDGENODE_TOPIC = _pub_edgenode_topic + "/" + self.edgeid

    def start(self, subcribes, retry=False):   
        try:
            # 콜백 함수 설정 on_connect(브로커에 접속), on_disconnect(브로커에 접속중료), on_publish(메세지 발행)
            self.client.on_connect = self.on_connect
            
            #publish용 콜백
            self.client.on_publish = self.on_publish

            #subscribe용 콜백
            self.client.on_subscribe = self.on_subscribe

            if retry is False:
                self.subcribes = subcribes
                # address : localhost, port: 1883 에 연결
                self.client.connect(self.host, self.port, keepalive=60)

            self.client.on_disconnect = self.on_disconnect

            # edge로부터 init 과정에서 받은 edgeId로 구독이 이루어지도록 해야한다.
            for topic in self.subcribes:  
                if topic is None or topic == "":
                    continue
                topic = "".join(topic.split())
                logging.info("구독토픽 추가 [{0}]".format(topic))
                self.client.subscribe(topic, 0)
            
            # client.loop_start() client.loop_stop()
            self.client.loop_forever()
        except KeyboardInterrupt as err:
            logging.exception("KeyboardInterrupt %s", err)
            sendDisconnectAll(client_sockets_1)
            sendDisconnectAll(client_sockets_2)
            self.client.close()


    def stop(self):
        # 연결 종료
        self.client.disconnect()
    
    def publish(self, client, topic, msg):
        result = client.publish(topic, msg) 

        # result: [0, 1]
        status = result[0]
        
        if status == 0:
            #logging.info(f"Send `{msg}` to topic `{topic}`")
            pass            
        else:
            logging.error(f"Failed to send message to topic {topic}")

    def on_publish(self,client, userdata, mid):
        #logging.info("In on_publish callback mid= {0}".format(mid))
        pass

    def on_subscribe(self, client, userdata, mid, granted_qos):
        logging.info("In on_subscribe: " + str(mid) + " " + str(granted_qos))

        self.notOkAddSub = False

    def process_kafka_message(self, message):
        try:
            if not message.value():
                return False
            
            # JSON 디코딩
            try:
                msg_data = json.loads(message.value().decode('utf-8'))
            except json.JSONDecodeError:
                logging.error("JSON 디코딩 실패")
                return False
            
            if not isinstance(msg_data, dict):
                logging.error("메시지 형식이 올바르지 않습니다.")
                return False
            
            msg_type = msg_data.get("header", {}).get("actn")
            
            if not msg_type:
                logging.error("메시지 타입이 없습니다.")
                return False
            
            # 메시지 JSON 문자열로 변환
            send_message = json.dumps(msg_data, ensure_ascii=False)
            
            # 메시지 타입에 따라 적절한 토픽으로 발행
            if msg_type == "init":
                self.pubHub4Init(send_message)
                logging.info(f"Init 메시지 {send_message} 발행 완료")
            elif msg_type in ["vehicle", "status", "alarm", "globalpath", "tractor", "cls"]:
                self.pubHub4Node(send_message)
                logging.info(f"Node 메시지 {send_message} 발행 완료")
            elif msg_type == "mobile":
                self.pubHub4Mobile(send_message)
                logging.info(f"Mobile 메시지 {send_message} 발행 완료")
            else:
                logging.warning(f"알 수 없는 메시지 타입: {msg_type}")
                return False
            
            return True
        except Exception as e:
            logging.error(f"메시지 처리 중 오류 발생: {str(e)}")
            return False

def start_kafka_consumer(mqtt_instance):
    try:
        while True:
            try:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.warning("파티션 끝에 도달했습니다.")
                    else:
                        logging.error(f"Kafka 에러 발생: {msg.error()}")
                    continue
                
                if mqtt_instance.process_kafka_message(msg):
                    consumer.commit()
                else:
                    logging.error("메시지 처리 실패")
                    
            except Exception as e:
                logging.error(f"Kafka 메시지 처리 중 오류 발생: {str(e)}")
                
    except KeyboardInterrupt:
        logging.info("Kafka Consumer 종료")
        consumer.close()
    except Exception as e:
        logging.error(f"Kafka Consumer 실행 중 오류 발생: {str(e)}")
        consumer.close()

if __name__ == "__main__":

    _config = configparser.ConfigParser()
    _config.read(CONFIG, encoding=file_encoding) # definition.py에 등록된 config.ini

    _command_file_path = _config['APP']['CMD_FILE'] # CMD_FILE
    

    _host1 = _config['SOCKET']['SERVER_HOST1'] # Server IP
    _port1 = int(_config['SOCKET']['SERVER_PORT1']) # Server Port

    _host2 = _config['SOCKET']['SERVER_HOST2'] # Server IP  
    _port2 = int(_config['SOCKET']['SERVER_PORT2']) # Server Port

    _mqhost = _config['MQTT']['BROKER_HOST'] # Server IP  
    _mqport = int(_config['MQTT']['BROKER_PORT']) # Server Port

    _subscribes = _config['MQTT']['TOPIC_SUBS'] # Topic subscribes
    subcribes = _subscribes.split(',')

    _topic_subs_base = _config['MQTT']['TOPIC_SUBS_BASE']

    _pub_init_topic = _config['MQTT']['PUB_INIT_TOPIC']
    _pub_edgenode_topic = _config['MQTT']['PUB_EDGENODE_TOPIC']

    _edgeid = _config['APP']['EDGEID'] 
    _edgety = _config['APP']['EDGETY'] 

    #Logger
    _logger_level = _config['LOGGER']['LOG_LEVEL']
    _logger_when = _config['LOGGER']['LOG_WHEN']
    _logger_interval = _config['LOGGER']['LOG_INTERVAL']
    _logger_backupcount = _config['LOGGER']['LOG_BACKUPCOUNT']


    full_path = os.path.join(ROOT_DIR, "logs", "nodecommsrv.log")
    create_rotating_log(full_path, _config['LOGGER'])

        
    logger.info("구성정보파일 읽기 {0}".format(ROOT_DIR))
    logger.info("구성정보파일 읽기 {0}".format(_config))
    logger.info("구성정보파일 읽기 서버1:{0}, {1}, 서버2:{2}, {3}".format(_host1, _port1, _host2, _port2))

    HOST1, PORT1 = _host1, _port1
    HOST2, PORT2 = _host2, _port2


    # Edge <-> Node 메시지 정합을 위해 command 정의 파일 읽어온다.
    with open(_command_file_path, 'r', encoding='utf-8') as file:
        command_tbl = json.load(file)

    # ############################################
    mqtt = Mqtt()

    mqtt.setLogger(logger)

    mqtt.ready(_mqhost, _mqport, _edgeid, _edgety, _topic_subs_base, _pub_init_topic, _pub_edgenode_topic)

    try:
        # MQTT 클라이언트 시작
        mqtt_thread = threading.Thread(target=mqtt.start, args=(subcribes, False))
        mqtt_thread.start()
        
        # Kafka Consumer 시작
        kafka_thread = threading.Thread(target=start_kafka_consumer, args=(mqtt,))
        kafka_thread.start()
        
        # 스레드 종료 대기
        mqtt_thread.join()
        kafka_thread.join()
    except:
        logger.exception("서비스 실행 실패...")
        sys.exit()

