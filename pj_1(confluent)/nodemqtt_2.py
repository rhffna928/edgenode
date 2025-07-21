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
        self.timer_interval = 1
        self.vehicle_info = None
        self.vehicle_location = None
        self.send_time = 0
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
            _header["userId"] = ""
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
            delay_time = (now - start_time).total_seconds() * 1000
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
                            decoded_data = reqdata
                            
                            resdata4edge = decoded_data
                            ret_data = dict({'resultCd': 0, 'resultMssage': "globalpath write 성공"})
                            resdata_string = str(ret_data)
                            
                            send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                            resdata = str(send_data)                           
                    elif _actn == "event":
                        send_direction = Constant.EDGE
                        decoded_data = jvm_msg_encrypt_class.decode(_timestamp, reqdata)
                        resdata4edge = decoded_data
                elif _cmd == "res":
                    send_direction = Constant.EDGE
                    _dstn_res = "E"
                    
                    if _actn == "init":
                        resdata4edge = str(res["data"])
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

        if send_direction == Constant.EDGE or send_direction == Constant.EDGE_EDGEHUB:
            
            json_message_edge = dict()
            json_message_edge["header"] = dict()
            json_message_edge["header"]["command"] = _command
            json_message_edge["header"]["VID"] = _edgeId
            json_message_edge["header"]["edgeId"] = _edgeId
            json_message_edge["header"]["timestamp"] = _timestamp
            json_message_edge["header"]["edgeTy"] = _edgeTy

            try:
                data_message = resdata4edge
                start_time = time.time()
                if _actn == "init":
                    send_data = jvm_msg_encrypt_class.decode(_timestamp, str(data_message))
                    send_message = json.loads(str(send_data), strict=True)
                    send_kafka_msg("init", send_message)
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
                send_kafka_msg("special", send_message)
                logging.info("#1-2 edge로 보낼 메시지 {0}->{1}\n {2}".format(_strtpnt_res, _dstn_res, send_message))

            except TypeError as err:
                logging.exception("%s. ", err)
            except Exception as err:                
                logging.exception("%s. ", err)

    def data_merge(self):
        """스레드 실행메소드"""
        while(1):
            time.sleep(self.timer_interval)

            now = datetime.datetime.now()

            # 공통정보, 위치 머지한값 보내지
            header_repack = dict()

            header_repack["cmd"] = str("rep")
            header_repack["actn"] = str("vehicle")
            header_repack["dtlActn"] = str("common")
            header_repack["strtpnt"] = "N"
            header_repack["dstn"] = "H"
            header_repack["userId"] = self.userId
            header_repack["edgeId"] = self.edgeId
            header_repack["edgeTy"] = self.edgeTy
            new_timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")
            header_repack["timestamp"] = new_timestamp    
            header_repack["command"] = "60320"

            #logging.info("************** work_doing_monitor ********************************************** {0}, {1}, {2}, {3}".format(self.vehicle_info, self.vehicle_location,self.edgeId, self.edgeTy ))

            if self.vehicle_info is not None and self.vehicle_location is not None and self.edgeId is not None and self.edgeTy is not None:
                json_message = dict()

                #data_message = self.vehicle_info + self.vehicle_location
                data_merge = {**self.vehicle_info, **self.vehicle_location}

                data_message = str(json.dumps(data_merge, ensure_ascii=False))

                send_data = jvm_msg_encrypt_class.encode(new_timestamp, self.edgeId, data_message)

                json_message["header"] = header_repack

                json_message["data"] = str(send_data)

                send_message = json.dumps(json_message, ensure_ascii=False)
                
                logging.info("************** work_doing_monitor ******************** {0}".format(data_message))

                mqtt.pubHub4Node(send_message)
                send_kafka_msg("mobile", send_message)
            self.vehicle_info = None
            self.vehicle_location = None

    def encoded_message(self, msg_data, _timestamp, _edgeId):
        """메시지 인코딩"""
        try:
            json_message = dict()
            json_message["header"] = msg_data["header"]
            data_message = str(json.dumps(msg_data["data"], ensure_ascii=False))
            send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, data_message)
            json_message["data"] = str(send_data)
            send_message = json.dumps(json_message, ensure_ascii=False)
        except Exception as e:
            logging.error(f"메시지 인코딩 중 오류 발생: {str(e)}")
        return send_message

    def special_data(self, msg_data, _timestamp, _edgeId):
        """작업정보 설정"""
        current_time = time.time()
        if current_time - self.send_time >= 1.0:
            send_message = self.encoded_message(msg_data, _timestamp, _edgeId)
            self.pubHub4Node(send_message)
            self.send_time = current_time

    def process_kafka_message(self, message):
        try:
            if not message.value():
                return False
            try:
                msg_data = json.loads(message.value().decode('utf-8'))
                now = datetime.datetime.now()
            except json.JSONDecodeError:
                logging.error("JSON 디코딩 실패")
                return False

            if not isinstance(msg_data, dict):
                logging.error(f"메시지 형식이 올바르지 않습니다.{message.topic()}")
                return False

            _cmd = msg_data["header"]["cmd"]
            _actn = msg_data["header"]["actn"]
            _dtlActn = msg_data["header"]["dtlActn"]
            _timestamp = msg_data["header"]["timestamp"]
            _strtpnt = msg_data["header"]["strtpnt"]
            _dstn = msg_data["header"]["dstn"]
            _edgeId = msg_data["header"]["edgeId"]
            _userId = msg_data["header"]["userId"]
            _edgeTy = msg_data["header"]["edgeTy"]
            _command = msg_data["header"]["command"]
            json_message = dict()

            end_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
            start_time = datetime.datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            delay_time = (now - start_time).total_seconds() * 1000
            logging.info("%%%%%%%%%%%%%%%%%%%%%%%%% Edgesocket -> nodemqtt delay_time : {0}ms , ({1} - {2})".format(round((delay_time), 4), now, start_time))
            logging.info("EDGE 수신 : {0} {1} {2} {3} {4} {5} {6}".format(_cmd, _actn, _dtlActn, _strtpnt, _dstn, _edgeId, _userId))
            
            #print(f"_cmd: {_cmd}, _actn: {_actn}")
            # 메시지 타입에 따라 mqtt발행
            if _cmd == "rep":
                if _actn in ["status", "vehicle"]:
                    if _dtlActn == "info":
                        self.vehicle_info = msg_data["data"]
                    elif _dtlActn == "position":
                        self.vehicle_location = msg_data["data"]
                    elif _dtlActn == "location":
                        self.vehicle_location = msg_data["data"]
                elif _actn == "alarm":
                    send_message = self.encoded_message(msg_data, _timestamp, _edgeId)
                    self.pubHub4Node(send_message)
                elif _actn == "event":
                    send_message = self.encoded_message(msg_data, _timestamp, _edgeId)    
                    self.pubHub4Node(send_message)
                elif _actn == "globalpath":
                    send_message = self.encoded_message(msg_data, _timestamp, _edgeId)     
                    self.pubHub4Node(send_message)
                    
                    json_message_edge = dict()

                    json_message_edge["command"] = _command
                    json_message_edge["VID"] = _edgeId
                    json_message_edge["timestamp"] = _timestamp
                    json_message_edge["edgeTy"] = _edgeTy
                    json_message_edge["userId"] = _userId

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
                    self.special_data(msg_data, _timestamp, _edgeId)

            elif _cmd == "req":
                if _actn == "init":
                    send_message = self.encoded_message(msg_data, _timestamp, _edgeId)
                    self.pubHub4Init(send_message)
                elif _actn == "globalpath":
                    send_message = self.encoded_message(msg_data, _timestamp, _edgeId)
                    self.pubHub4Node(send_message)
            elif _cmd == "event":
                send_message = self.encoded_message(msg_data, _timestamp, _edgeId)
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

    _kafka_broker = _config['KAFKA']['KAFKA_BROKER']
    _kafka_port = _config['KAFKA']['KAFKA_PORT']

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
    
    # Kafka Producer 설정
    KAFKA_BROKER = f'{_kafka_broker}:{_kafka_port}'
    producer_config = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(producer_config)
    # Kafka Consumer 설정
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': random.randint(0, 1000),
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(['connect'])
    
    try:
        # MQTT 클라이언트 시작
        mqtt_thread = threading.Thread(target=mqtt.start, args=(subcribes, False))
        mqtt_thread.start()
        
        #data merge 시작
        merge = threading.Thread(target=mqtt.data_merge, args=())
        merge.start()
        
        # Kafka Consumer 시작
        kafka_thread = threading.Thread(target=start_kafka_consumer, args=(mqtt,))
        kafka_thread.start()
        
        # 스레드 종료 대기
        mqtt_thread.join()
        kafka_thread.join()
    except Exception as e:
        consumer.commit()
        logger.exception("서비스 실행 실패...")
        sys.exit()
