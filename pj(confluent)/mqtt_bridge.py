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
            self.client.on_message = self.on_message

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

        #sendAll(client_sockets_1, message)

        try:
            res=json.loads(message)
           
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
            delay_time = (now - start_time) * 1000  # 밀리세컨드 단위로 변환
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

        _strtpnt_res = _dstn
        _dstn_res = _strtpnt

        topic_define = self.PUB_EDGENODE_TOPIC.replace("#", '')

        if "init" in self.onmessage_topic:
            if _strtpnt == "N":
                if _cmd == "rep":
                    # 자체처리
                    pass
                elif _cmd == "alarm":
                    #M 으로 토픽발행
                    pass
                elif _cmd == "req":
                    _cmd = "res"
            elif _strtpnt == "M":
                # Mobile로부터 온 메시지
                if _cmd == "rep":
                    # 자체처리
                    resdata = res["data"]
                    pass
                elif _cmd == "alarm":
                    #M 으로 토픽발행
                    resdata = res["data"]
                    pass
                elif _cmd == "req":
                    _cmd_req = "res"

                    if _actn == "init":
                        data = res["data"]
                        _initId = data["initId"]

                        
                        # DB에서 edgeId로 사용자 아이디 찾고
                        # 찾은 사용자 아이디로 edgeId 목록을 가져와 보낸다.
                        db_userId = "specialuser"
                        db_edgeList = []

                        db_edgeList.append(("트랙터1", "123456A"))
                        db_edgeList.append(("트랙터2", "123456B"))
                        db_edgeList.append(("트랙터3", "123456C"))
                        db_edgeList.append(("트랙터4", "ABCD1234"))


                        resdata = dict({'resultCd': 0, 'resultMssage': "init 성공 응답", 'userId':db_userId, 'edgeList':db_edgeList})   
                        resdata = resdata
                elif _cmd == "res":
                    resdata = res["data"]
                    if _actn == "init":
                        data = res["data"]
                        _db_userId = data["db_userId"]
                    elif _actn == "globalpath":
                        data = res["data"]
            elif _strtpnt == "H":
                resdata = res["data"]
                if _cmd == "res":
                    if _actn == "init":
                        data = res["data"]
                        _initId = data["initId"]
        else:
            # EdgeHub로부터 온 메시지
            if _strtpnt == "H":
                if _cmd == "rep":
                    send_direction = Constant.EDGEHUB
                    _dstn_res = "H"
                    if _actn == "alarm":
                        pass
                        #M 으로 토픽발행
                        resdata = res["data"]
                elif _cmd == "cmd":
                    send_direction = Constant.EDGE
                    _dstn_res = "E"

                    resdata = res["data"]
                    # E 로 전송
                    #sendAll(client_sockets_1, message)
                
                elif _cmd == "req":
                    reqdata = res["data"]

                    if _actn == "tractor":
                        _cmd_req = "res"

                        if _dtlActn == "workinfo" and g_work_info is not None:
                            _strtpnt_res = "N"
                            _dstn_res = "H"

                            send_direction = Constant.EDGEHUB

                            #resdata_string = str(json.dumps(g_work_info, ensure_ascii=False))
                            resdata_string = g_work_info

                            start_time = time.time()
                            send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                            end_time = time.time()
                            execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                            logging.info("\t코드 실행 시간: {0}ms".format(execution_time))

                            resdata = str(send_data)
                    elif _actn == "globalpath":
                        _cmd_req = "res"

                        resheader["cmd"] = _cmd_req
                        resheader["strtpnt"] = _strtpnt_res
                        resheader["dstn"] = _dstn_res

                        json_message["header"] = resheader

                        if _dtlActn == "planwrite":
                            # edge로 보내고, hub로 응답 보내고
                            send_direction = Constant.EDGE_EDGEHUB

                            ret_data = dict({'resultCd': 0, 'resultMssage': "globalpath write 성공"})   
                            resdata_string = str(ret_data)

                            #logging.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$  {0}".format(resdata_string))


                            # hub로 응답 보낼꺼 먼저
                            send_data = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)

                            resdata = str(send_data)
                            
                            #send_message = json.dumps(json_message, ensure_ascii=False)

                            #mqtt.pubHub4Node(send_message)

                            # edge로 보낼꺼

                            # edge 에 보내기 위해 data 디코딩
                            decoded_data = jvm_msg_encrypt_class.decode(_timestamp, reqdata)
                            
                            resdata4edge = decoded_data

                    elif _actn == "event":
                        # edge로 보내고, hub로 응답 보내고
                        send_direction = Constant.EDGE

                        # edge 에 보내기 위해 data 디코딩
                        decoded_data = jvm_msg_encrypt_class.decode(_timestamp, reqdata)

                        # edge로 보낼꺼
                        resdata4edge = decoded_data

                    else:
                        pass

                    # 반드시 N 으로 res 준다(N으로 토픽발행)
                    pass
                elif _cmd == "res":
                    send_direction = Constant.EDGE
                    _dstn_res = "E"
                    
                    if _actn == "init":
                        resdata4edge = str(json.dumps(res["data"], ensure_ascii=False))

                    elif _actn == "globalpath":
                        resdata4edge = str(json.dumps(res["data"], ensure_ascii=False))
                   
            
            # Mobile로부터 온 메시지
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

            #client.publish(self.PUB_EDGENODE_TOPIC, send_message) 
            self.pubHub4Node(send_message)

            logging.info("#1-1 edgeHub로 보낼 메시지 {0}->{1}\n {2}".format(_strtpnt_res, _dstn_res, send_message))

        elif send_direction == Constant.EDGE or send_direction == Constant.EDGE_EDGEHUB:
            json_message_edge = dict()

            json_message_edge["command"] = _command
            json_message_edge["VID"] = _edgeId
            json_message_edge["timestamp"] = _timestamp
            json_message_edge["edgeTy"] = _edgeTy


            try:
                data_message = resdata4edge

                start_time = time.time()
                if _actn == "init":
                    send_data = jvm_msg_encrypt_class.decode(_timestamp, str(data_message))
                else:
                    send_data = jvm_msg_encrypt_class.decode(_timestamp, _edgeId, str(data_message))

                end_time = time.time()
                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                logging.info("#1 DATA 인/디코드 실행 시간: {0}ms".format(execution_time))

                #logging.info("==============> {0}".format(send_data))

                #json_message["data"] = str(send_data)
                if send_data == "":
                    json_message_edge["data"] = ""
                else:
                    json_message_edge["data"] = json.loads(str(send_data), strict=True)
                
                send_message = json.dumps(json_message_edge, ensure_ascii=False)


                sendAll(client_sockets_1, send_message)

                logging.info("#1-2 edge로 보낼 메시지 {0}->{1}\n {2}".format(_strtpnt_res, _dstn_res, send_message))

            except TypeError as err:
                logging.exception("%s. ", err)
            except Exception as err:                
                logging.exception("%s. ", err)
        
        #logging.info("#1 보낼 메시지 {0}->{1}\n  {2}".format(_strtpnt_res, _dstn_res, json.dumps(json_message_edge, ensure_ascii=False, indent=3)))
        #logging.info("#1 보낼 메시지 {0}->{1}\n {2}".format(_strtpnt_res, _dstn_res, send_message))
        #logging.info("#1 보낼 메시지 {0}->{1}".format(_strtpnt_res, _dstn_res))
        
        
        
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
    print("Socket bound on {0}, {1}".format(PORT1, PORT2))

    # Edge <-> Node 메시지 정합을 위해 command 정의 파일 읽어온다.
    with open(_command_file_path, 'r', encoding='utf-8') as file:
        command_tbl = json.load(file)

    # ############################################
    mqtt = Mqtt()

    mqtt.setLogger(logger)

    mqtt.ready(_mqhost, _mqport, _edgeid, _edgety, _topic_subs_base, _pub_init_topic, _pub_edgenode_topic)

    try:
        mqtt.start(subcribes, False)
    except :
        logger.exception("MQTT Connect Fail...")
        sys.exit()

