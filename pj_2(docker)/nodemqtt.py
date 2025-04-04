import sys
import os
import random
import time
import json
import datetime
from pathlib import Path
import configparser
import logging
import paho.mqtt.client as mqtt_client
from logging.handlers import TimedRotatingFileHandler
from constant import Constant
import jpype
import jpype.imports
from jpype.types import *
from confluent_kafka import Consumer, KafkaError, Producer
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
CONFIG = Path(ROOT_DIR) / 'nodecommsrv.ini'
logger = None
file_encoding = 'utf-8'
send_encoding = 'CP949'
recv_encoding = 'CP949'

# Kafka Producer 설정
producer_config = {'bootstrap.servers': '172.30.1.20:9092'}
producer = Producer(producer_config)
# Kafka Consumer 설정

consumer_config = {
    'bootstrap.servers': '172.30.1.20:9092',
    'group.id': f'mqtt_consumer_{os.getpid()}',  # 프로세스 ID 기반 그룹 ID
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000,
    'session.timeout.ms': 10000,
    'heartbeat.interval.ms': 3000,
    'max.poll.interval.ms': 300000
}
consumer = Consumer(consumer_config)
consumer.subscribe(['rep', 'req', 'event', 'heartbeat'])

# JVM 시작
jpype.startJVM()
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

def sendDisconnectAll(client_sockets):
    logger.info("모든 접속자와의 연결을 끊음")
    msg = 'Disconnect'
    for client in client_sockets:
        conn = client[0]
        conn.sendall(msg.encode())
        conn.close()
    client_sockets.clear()

def sendAll(client_sockets, msg):
    msg += '\r\n'
    for client in client_sockets:        
        conn = client[0]
        try:
            conn.sendall(msg.encode(encoding=send_encoding))
            logging.info(f"메시지 전송 성공: {client[1]}")  # 전송 성공 로그
        except Exception as e:
            logging.error(f"메시지 전송 실패: {client[1]}, 오류: {e}")  # 전송 실패 로그

def sendString(conn, msg):
    msg += '\r\n'
    conn.sendall(msg.encode(encoding=send_encoding))

def send_kafka_msg(topic, message):
    #카프카 메시지 전송
    producer.produce(topic, value=json.dumps(message).encode('utf-8'))
    
    
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
    logging.basicConfig(level=set_level, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s')
    logger = logging.getLogger("")
    logger.setLevel(set_level)

    handler = TimedRotatingFileHandler(path,
                                     when=_logger_when,
                                     interval=int(_logger_interval),
                                     backupCount=int(_logger_backupcount),
                                     encoding='utf-8')
    handler.suffix = "-%Y%m%d_%H-%M-%S"
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s')    
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
        self.edgeId = None
        self.edgeTy = None
        self.userId = None
        self.notOkAddSub = True
        self.subcribes = None
        self.vehicle_data = {}
        self.status_data = {}
        self.data_received = {
            'vehicle': False,
            'status': False
        }

        try:
            client_mqtt = self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1)
        except:
            client_mqtt = self.client = mqtt_client.Client()

    def __del__(self):
        try:
            if jpype.isJVMStarted():
                jpype.shutdownJVM()
        except Exception as e:
            print(f"JVM 종료 중 오류 발생: {e}")


    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Mqtt Broker connected OK")
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
            _header["edgeId"] = self.edgeId
            _header["userId"] = "1"
            _header["command"] = "00001"
            _header["edgeTy"] = self.edgeTy
            _header["timestamp"] = now_str

            _json_message["header"] = _header
            _data["edgeId"] = self.edgeId
            data_message = str(json.dumps(_data, ensure_ascii=False))
            send_data = jvm_msg_encrypt_class.encode(now_str, data_message)
            _json_message["data"] = str(send_data)
            send_message = json.dumps(_json_message, ensure_ascii=False)

            time.sleep(0.02)
            self.addSub(self.edgeId)
            self.pubHub4Init(send_message)
        else:
            logging.info("Mqtt Broker Bad connection Returned code=", rc)

    def on_disconnect(self, client, userdata, flags, rc=0):
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
                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1

        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)


    def pubHub4Init(self, send_message):
        self.publish(self.client, self.PUB_INIT_TOPIC, send_message)
        
    def pubHub4Node(self, send_message):
        self.publish(self.client, self.PUB_EDGENODE_TOPIC, send_message)

    def pubHub4Mobile(self, send_message):
        self.publish(self.client, self.PUB_EDGENODE_MOBILE_TOPIC, send_message)

    def addSub(self, topic):
        topic = self.TOPIC_SUBS_BASE + "/" + topic
        self.client.subscribe(topic, 0)
        self.notOkAddSub = True

    def setLogger(self, logger):
        self.logger = logger

    def ready(self, _host, _port, _edgeid, _edgety, _topic_subs_base, _pub_init_topic, _pub_edgenode_topic):
        self.edgeId = _edgeid
        self.edgeTy = _edgety
        self.host = _host
        self.port = _port
        self.TOPIC_SUBS_BASE = _topic_subs_base
        self.PUB_INIT_TOPIC = _pub_init_topic
        self.PUB_EDGENODE_TOPIC = _pub_edgenode_topic + "/" + self.edgeId

    def start(self, subcribes, retry=False):   
        try:
            self.client.on_connect = self.on_connect
            self.client.on_publish = self.on_publish
            self.client.on_subscribe = self.on_subscribe
            self.client.on_message = self.on_message

            if retry is False:
                self.subcribes = subcribes
                self.client.connect(self.host, self.port, keepalive=60)

            self.client.on_disconnect = self.on_disconnect

            for topic in self.subcribes:  
                if topic is None or topic == "":
                    continue
                topic = "".join(topic.split())
                logging.info("구독토픽 추가 [{0}]".format(topic))
                self.client.subscribe(topic, 0)
            
            self.client.loop_forever()
        except KeyboardInterrupt as err:
            logging.exception("KeyboardInterrupt %s", err)
            sendDisconnectAll(client_sockets_1)
            sendDisconnectAll(client_sockets_2)
            self.client.close()

    def stop(self):
        self.client.disconnect()
    
    def publish(self, client, topic, msg):
        result = client.publish(topic, msg)
        status = result[0]
        if status == 0:
            pass            
        else:
            logging.error(f"Failed to send message to topic {topic}")

    def on_publish(self, client, userdata, mid):
        pass

    def on_subscribe(self, client, userdata, mid, granted_qos):
        logging.info("In on_subscribe: " + str(mid) + " " + str(granted_qos))
        self.notOkAddSub = False

    def on_message(self, client, userdata, message):
        global g_work_info
        self.onmessage_topic = message.topic
            
        try:
            message = str(message.payload.decode(recv_encoding))
        except UnicodeDecodeError as err:
            logging.exception("UnicodeDecodeError %s", err)
            return
        except Exception as err:
            logging.exception("Exception %s. ", err)
            return
        
        send_message = ""
        json_message = dict()

        try:
            res = json.loads(message)
            _cmd = str(res["header"]["cmd"])
            _actn = str(res["header"]["actn"])
            _dtlActn = str(res["header"]["dtlActn"])
            _strtpnt = str(res["header"]["strtpnt"])
            _dstn = str(res["header"]["dstn"])
            _userId = str(res["header"]["userId"])
            _edgeId = str(res["header"]["edgeId"])
            _edgeTy = str(res["header"]["edgeTy"])
            _timestamp = str(res["header"]["timestamp"])
            _command = str(res["header"]["command"])

            now = datetime.datetime.now()
            end_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
            start_time = datetime.datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            delay_time = (now - start_time) * 1000
            logging.info("%%%%%%%%%%%%%%%%%%%%%%%%% EdgeHub(Mobile) -> EdgeNode delay_time: {0}ms , ({1} - {2})".format(delay_time, now, start_time))

            _cmd_req = _cmd
            resheader = res["header"]
            resdata = dict()
            send_direction = Constant.NONE

        except json.decoder.JSONDecodeError as err:
            logging.exception("json.decoder.JSONDecodeError %s", err)
            return
        except Exception as err:
            logging.exception("Exception %s. ", err)
            return

        logging.info("MQTT 메시지 수신 : on topic: {0}  헤더 : {1} {2} {3} {4} {5} {6} {7} \n {8}".format(self.onmessage_topic, _cmd, _actn, _dtlActn, _strtpnt, _dstn, _edgeId, _userId, json.dumps(res, ensure_ascii=False, indent=3)))
        #N
        _strtpnt_res = _dstn
        #H
        _dstn_res = _strtpnt

        topic_define = self.PUB_EDGENODE_TOPIC.replace("#", '')

        if "init" in self.onmessage_topic:
            if _strtpnt == "N":
                if _cmd == "rep":
                    pass
                elif _cmd == "alarm":
                    pass
                elif _cmd == "req":
                    _cmd = "res"
            elif _strtpnt == "M":
                if _cmd == "rep":
                    resdata = res["data"]
                    pass
                elif _cmd == "alarm":
                    resdata = res["data"]
                    pass
                elif _cmd == "req":
                    _cmd_req = "res"

                    if _actn == "init":
                        data = res["data"]
                        _initId = data["initId"]
                        
                        db_userId = "specialuser"
                        db_edgeList = []

                        db_edgeList.append(("트랙터1", "123456A"))
                        db_edgeList.append(("트랙터2", "123456B"))
                        db_edgeList.append(("트랙터3", "123456C"))
                        db_edgeList.append(("트랙터4", "ABCD1234"))

                        resdata = dict({'resultCd': 0, 'resultMssage': "init 성공 응답", 'userId':db_userId, 'edgeList':db_edgeList})   
                        resdata = resdata
                    elif _actn == "globalpath":
                        data = res["data"]
            elif _strtpnt == "H":
                resdata = res["data"]
                if _cmd == "res":
                    if _actn == "init":
                        data = res["data"]
                        _initId = data["initId"]
                    elif _actn == "globalpath":
                        data = res["data"]
        else:
            if _strtpnt == "H":
                if _cmd == "rep":
                    send_direction = Constant.EDGEHUB
                    _dstn_res = "H"
                    if _actn == "alarm":
                        resdata = res["data"]
                elif _cmd == "cmd":
                    send_direction = Constant.EDGE
                    _dstn_res = "E"
                    resdata4edge = res["data"]
                elif _cmd == "req":
                    reqdata = res["data"]
                    
                    if _actn == "tractor":
                        _cmd_req = "res"

                        if _dtlActn == "workinfo" and g_work_info is not None:
                            _strtpnt_res = "N"
                            _dstn_res = "H"
                            send_direction = Constant.EDGEHUB
                            resdata_string = g_work_info

                            start_time = time.time()
                            send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                            end_time = time.time()
                            execution_time = (end_time - start_time) * 1000
                            logging.info("\t코드 실행 시간: {0}ms".format(execution_time))

                            resdata = str(send_data)
                    elif _actn == "globalpath":
                        _cmd_req = "res"
                        resheader["cmd"] = _cmd_req
                        resheader["strtpnt"] = _strtpnt_res
                        resheader["dstn"] = _dstn_res
                        json_message["header"] = resheader

                        if _dtlActn == "planwrite":
                            send_direction = Constant.EDGE_EDGEHUB
                            ret_data = dict({'resultCd': 0, 'resultMssage': "globalpath write 성공"})   
                            resdata_string = str(ret_data)
                            
                            send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                            resdata = str(send_data)
                            decoded_data = jvm_msg_encrypt_class.decode(_timestamp, reqdata)
                            resdata4edge = decoded_data
                    elif _actn == "event":
                        send_direction = Constant.EDGE
                        decoded_data = jvm_msg_encrypt_class.decode(_timestamp, reqdata)
                        resdata4edge = decoded_data
                elif _cmd == "res":
                    send_direction = Constant.EDGE
                    _dstn_res = "E"
                    
                    if _actn == "init":
                        resdata4edge = str(json.dumps(res["data"], ensure_ascii=False))
                    elif _actn == "globalpath":
                        resdata4edge = str(json.dumps(res["data"], ensure_ascii=False))
            elif _strtpnt == "M":
                pass

        resheader["cmd"] = _cmd_req
        resheader["strtpnt"] = _strtpnt_res
        resheader["dstn"] = _dstn_res        

        if send_direction == Constant.EDGEHUB or send_direction == Constant.EDGE_EDGEHUB:   
            json_message["header"] = resheader
            try:
                json_message["data"] = resdata
            except Exception as err:
                logging.error("%s. ", err)

            send_message = json.dumps(json_message, ensure_ascii=False)
            self.pubHub4Node(send_message)
            logging.info("#1-1 edgeHub로 보낼 메시지 {0}->{1}\n {2}".format(_strtpnt_res, _dstn_res, send_message))

        elif send_direction == Constant.EDGE or send_direction == Constant.EDGE_EDGEHUB:
            json_message_edge = dict()
            resheader["command"] = _command
            resheader["VID"] = _edgeId
            resheader["timestamp"] = _timestamp
            resheader["edgeTy"] = _edgeTy
            json_message_edge["header"] = resheader

            try:
                data_message = resdata4edge
                start_time = time.time()
                if _actn == "init":
                    send_data = jvm_msg_encrypt_class.decode(_timestamp, str(data_message))
                else:
                    send_data = jvm_msg_encrypt_class.decode(_timestamp, _edgeId, str(data_message))

                end_time = time.time()
                execution_time = (end_time - start_time) * 1000
                logging.info("#1 DATA 인/디코드 실행 시간: {0}ms \n 보낼 메시지 : {1}".format(execution_time, data_message))

                if send_data == "":
                    json_message_edge["data"] = ""
                else:
                    json_message_edge["data"] = json.loads(str(send_data), strict=True)
                
                send_message = json.dumps(json_message_edge, ensure_ascii=False)
                #print(f"client_sockets_1############# : {client_sockets_1}")
                #sendAll(client_sockets_1, send_message)
                send_kafka_msg("connect", send_message)
                logging.info("#1-2 edge로 보낼 메시지 {0}->{1}\n {2}".format(_strtpnt_res, _dstn_res, send_message))

            except TypeError as err:
                logging.exception("%s. ", err)
            except Exception as err:                
                logging.exception("%s. ", err)
            
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
            header_repack["data"] = {**self.vehicle_data, **self.status_data}
            
            # 데이터 초기화
            self.vehicle_data = {}
            self.status_data = {}
            self.data_received = {
                'vehicle': False,
                'status': False
            }
            
            return header_repack
        return None

    def process_message(self, msg_data):
        try:
            action = msg_data["header"]["actn"]
            if action == "vehicle":
                self.vehicle_data = msg_data["data"]
                self.data_received['vehicle'] = True
                #logging.info(f"차량 정보 업데이트: {self.vehicle_data}")
            elif action == "status":
                self.status_data = msg_data["data"]
                self.data_received['status'] = True
                #logging.info(f"위치 정보 업데이트: {self.status_data}")
        except KeyError as e:
            logging.error(f"데이터 처리 중 오류 발생: {e}")
            
    def process_kafka_message(self, message):
        try:
            if not message.value():
                return False
            try:
                msg_data = json.loads(message.value().decode('utf-8'))
                #print(f"msg_data : {msg_data}")
            except json.JSONDecodeError:
                logging.error("JSON 디코딩 실패")
                return False
            
            if not isinstance(msg_data, dict):
                logging.error(f"메시지 형식이 올바르지 않습니다.{message.topic()}")
                return False
            
            _cmd = msg_data["header"]["cmd"]
            _actn = msg_data["header"]["actn"]
            _timestamp = msg_data["header"]["timestamp"]
            _edgeId = msg_data["header"]["edgeId"]
            _edgeTy = msg_data["header"]["edgeTy"]
            _command = msg_data["header"]["command"]
            json_message = dict()
            
            now = datetime.datetime.now()
            end_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
            start_time = datetime.datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            delay_time = (now - start_time).total_seconds() * 1000
            logging.info("%%%%%%%%%%%%%%%%%%%%%%%%% Edgesocket -> nodemqtt delay_time: {0}ms , ({1} - {2})".format(rount((delay_time),4), now, start_time))
            
            #print(f"_cmd: {_cmd}, _actn: {_actn}")
            # 메시지 타입에 따라 mqtt발행
            if _cmd == "rep":
                if _actn in ["status","vehicle"]:
                    self.process_message(msg_data)
                    merged_data = self.data_merge()
                    
                    json_message["header"] = msg_data["header"]                    
                    data_message = str(json.dumps(merged_data, ensure_ascii=False))                    
                    send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)                    
                    json_message["data"] = str(send_data)                    
                    send_message = json.dumps(json_message, ensure_ascii=False)                    
                    self.pubHub4Node(send_message)
                elif _actn == "alarm":
                    json_message["header"] = msg_data["header"]                
                    data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))                    
                    send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)                    
                    json_message["data"] = str(send_data)                    
                    send_message = json.dumps(json_message, ensure_ascii=False)         
                    self.pubHub4Node(send_message)
                elif _actn == "event":
                    json_message["header"] = msg_data["header"]                
                    data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))                    
                    send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)                    
                    json_message["data"] = str(send_data)                    
                    send_message = json.dumps(json_message, ensure_ascii=False)         
                    self.pubHub4Node(send_message)
                elif _actn == "globalpath":
                    json_message["header"] = msg_data["header"]                
                    data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))                    
                    send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)                    
                    json_message["data"] = str(send_data)                    
                    send_message = json.dumps(json_message, ensure_ascii=False)         
                    self.pubHub4Node(send_message)
                    
                    json_message_edge = dict()

                    json_message_edge["command"] = _command
                    json_message_edge["VID"] = _edgeId
                    json_message_edge["timestamp"] = _timestamp
                    json_message_edge["edgeTy"] = _edgeTy

                    data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))
                    start_time = time.time()
                    send_data = jvm_msg_encrypt_class.decode(_timestamp, str(data_message))
                    end_time = time.time()
                    execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                    logging.info("#1 DATA 인/디코드 실행 시간: {0}ms".format(execution_time))

                    if send_data == "":
                        json_message_edge["data"] = ""
                    else:
                        json_message_edge["data"] = json.loads(str(send_data), strict=True)
                    
                    send_message = json.dumps(json_message_edge, ensure_ascii=False)


                    sendAll(client_sockets_1, send_message)
                    logging.info(f"######### 특장차 전송 완료 ######### {send_message}")
                elif _actn in ["tractor", "cls"]:
                    json_message["header"] = msg_data["header"]                
                    data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))                    
                    send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)                    
                    json_message["data"] = str(send_data)                    
                    send_message = json.dumps(json_message, ensure_ascii=False)                    
                    self.pubHub4Node(send_message)
            elif _cmd == "req":
                if _actn == "init":
                    json_message["header"] = msg_data["header"]
                    data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))
                    send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)
                    json_message["data"] = str(send_data)
                    send_message = json.dumps(json_message, ensure_ascii=False)
                    self.pubHub4Init(send_message)
                elif _actn == "globalpath":
                    json_message["header"] = msg_data["header"]
                    data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))
                    send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)
                    json_message["data"] = str(send_data)
                    send_message = json.dumps(json_message, ensure_ascii=False)
                    self.pubHub4Node(send_message)
            elif _cmd == "event":
                json_message["header"] = msg_data["header"]
                data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))
                send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)
                json_message["data"] = str(send_data)
                send_message = json.dumps(json_message, ensure_ascii=False)
                self.pubHub4Node(send_message)
            elif _cmd == "heartbeat":
                send_message = json.dumps(msg_data, ensure_ascii=False)
                self.pubHub4Node(send_message)
            elif _cmd == "cmd":
                if _actn == "power":
                    send_message = json.dumps(msg_data, ensure_ascii=False)
                    self.pubHub4Node(send_message)
            return True
        except Exception as e:
            logging.error(f"메시지 처리 중 오류 발생: {str(e)}")
            return False        
def start_kafka_consumer(mqtt_instance):
    retry_count = 0
    max_retries = 3
    try:
        while retry_count < max_retries:
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
                retry_count = 0
                if mqtt_instance.process_kafka_message(msg):
                    consumer.commit()
                else:
                    logging.error("메시지 처리 실패")
            except KafkaError as e:
                logging.error(f"Kafka 처리 중 오류 발생: {str(e)}")
                break
            except Exception as e:
                logging.error(f"Kafka 메시지 처리 중 오류 발생: {str(e)}")
                continue
    
    except Exception as e:
        retry_count += 1
        logging.error(f"Kafka Consumer 생성 실패(시도 {retry_count}/{max_retries}): {str(e)}")
        if retry_count < max_retries:
            time.sleep(3)
        else:
            logging.error("Kafka Consumer 최대 재시도 횟수 초과")
    try:
        consumer.close()
        logging.info("Kafka Consumer 정상 종료")
    except Exception as e:
        logging.error(f"Kafka Consumer 종료 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    _config = configparser.ConfigParser()
    _config.read(CONFIG, encoding=file_encoding)

    _command_file_path = _config['APP']['CMD_FILE']
    #_host1 = _config['SOCKET']['SERVER_HOST1']
    #_port1 = int(_config['SOCKET']['SERVER_PORT1'])
    #_host2 = _config['SOCKET']['SERVER_HOST2']
    #_port2 = int(_config['SOCKET']['SERVER_PORT2'])
    _mqhost = _config['MQTT']['BROKER_HOST']
    _mqport = int(_config['MQTT']['BROKER_PORT'])
    _subscribes = _config['MQTT']['TOPIC_SUBS']
    subcribes = _subscribes.split(',')
    _topic_subs_base = _config['MQTT']['TOPIC_SUBS_BASE']
    _pub_init_topic = _config['MQTT']['PUB_INIT_TOPIC']
    _pub_edgenode_topic = _config['MQTT']['PUB_EDGENODE_TOPIC']
    _edgeid = _config['APP']['EDGEID']
    _edgety = _config['APP']['EDGETY']

    _logger_level = _config['LOGGER']['LOG_LEVEL']
    _logger_when = _config['LOGGER']['LOG_WHEN']
    _logger_interval = _config['LOGGER']['LOG_INTERVAL']
    _logger_backupcount = _config['LOGGER']['LOG_BACKUPCOUNT']

    full_path = os.path.join(ROOT_DIR, "logs", "nodecommsrv.log")
    create_rotating_log(full_path, _config['LOGGER'])

    logger.info("구성정보파일 읽기 {0}".format(ROOT_DIR))
    logger.info("구성정보파일 읽기 {0}".format(_config))
    #logger.info("구성정보파일 읽기 서버1:{0}, {1}, 서버2:{2}, {3}".format(_host1, _port1, _host2, _port2))

    #HOST1, PORT1 = _host1, _port1
    #HOST2, PORT2 = _host2, _port2

    with open(_command_file_path, 'r', encoding='utf-8') as file:
        command_tbl = json.load(file)

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