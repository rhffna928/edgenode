import paho.mqtt
import paho.mqtt.client as mqtt_client
#from paho.mqtt import client as mqtt_client

import json
import random
import time
import os
import datetime
import glob

from pathlib import Path

import configparser
import queue
import threading
import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler

import pandas as pd

from constant import Constant
#from MysqlController import MysqlController
from PostgreController import PostgreController

from GBUtil import GBUtil

# java jar 임포트
import jpype
import jpype.imports
from jpype.types import *

from GBUtil import GBUtil

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG = Path(ROOT_DIR) / 'hubmqttsrv.ini' # mqtt.ini 설정

class Mqtt:
    logger = None
    

    def __init__(self):
        self.data_queue = queue.Queue()
        # Create threads
        self.threads = []

        self.host = None
        self.port = None
        self.recvmessage = None
        self.onmessage_topic = None

        self.strtpnt = None
        self.dstn = None

        self.PUB_INIT_TOPIC = None
        self.PUB_EDGENODE_TOPIC = None
        self.PUB_CTRLCENTER_TOPIC = None

        self.client_list = dict()

        self.subcribes = None


        self.dbctrl = None

        self.gbutil = GBUtil()


        # JVM 시작
        jpype.startJVM()
        
        # 원본 암호화 모듈
        #jpype.addClassPath("watosysEncrypt_v1.0.0.jar")

        # AES256 암호화 모듈
        jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")

        # 자바 클래스 로드
        self.jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")


        client_id = f'publish-{random.randint(0, 1000)}'
        # Old
        #self.client = mqtt_client.Client()

        #self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1,client_id)

        try:
            if paho.mqtt.__version__[0] > '1':
                self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1)
            else:
                self.client = mqtt_client.Client()

            #self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1)
        except Exception as err:
            print("앗 mqtt_client.Client(mqttClient.CallbackAPIVersion.VERSION1,  방식은 문제가 있네")
            print("%s. Reconnect failed. Retrying...", err)
            self.client = mqtt_client.Client()

        return


    def __del__(self):
        if self.dbctrl is not None:
            self.dbctrl.close()

        # 모든 스레드 종료 대기
        for thread in self.threads:
            thread.join()
        
        # JVM 종료
        jpype.shutdownJVM()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("connected OK")
        else:
            logging.info("Bad connection Returned code=", rc)

    def on_disconnect(self,client, userdata, flags, rc=0):
        logging.info("Disconnected with result code: %s", rc)
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY

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

    def get_jar_file_list(self, path):
        return glob(path + "*.jar")

    def setLogger(self, logger):
        self.logger = logger

    def ready(self, _host, _port, _pub_init_topic, _pub_edgenode_topic, _pub_ctrlcenter_topic, _dbinfo):
        self.host = _host
        self.port = _port
        self.PUB_INIT_TOPIC = _pub_init_topic
        self.PUB_EDGENODE_TOPIC = _pub_edgenode_topic
        self.PUB_CTRLCENTER_TOPIC = _pub_ctrlcenter_topic

        #print("{0}, {1}".format(self.PUB_EDGENODE_TOPIC, self.PUB_CTRLCENTER_TOPIC))

        #self.dbctrl = MysqlController(_dbinfo["DB_HOST"], int(_dbinfo["DB_PORT"]), _dbinfo["DB_USER"], _dbinfo["DB_PWD"], _dbinfo["DB_DB"])
        self.dbctrl = PostgreController(_dbinfo["DB_HOST"], int(_dbinfo["DB_PORT"]), _dbinfo["DB_USER"], _dbinfo["DB_PWD"], _dbinfo["DB_DB"])
        
    def start(self, subcribes, retry=False):   

        if retry is False:
            self.subcribes = subcribes
            # address : localhost, port: 1883 에 연결
            self.client.connect(self.host, self.port, keepalive=60)

        # 콜백 함수 설정 on_connect(브로커에 접속), on_disconnect(브로커에 접속중료), on_publish(메세지 발행)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        
        #publish용 콜백
        self.client.on_publish = self.on_publish

        #subscribe용 콜백
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message

        for topic in self.subcribes:  
            topic = "".join(topic.split())
            logging.info("구독토픽 추가 [{0}]".format(topic))
            self.client.subscribe(topic, 0)
        
        for i in range(10):  # Number of threads
            thread = threading.Thread(target=self.handle_data, args=(i,))
            thread.start()
            self.threads.append(thread)

        # client.loop_start() client.loop_stop()
        self.client.loop_forever()
       
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

    def on_subscribe(self,client, userdata, mid, granted_qos):
        #logging.info("In on_subscribe: " + str(mid) + " " + str(granted_qos))
        pass
    
    def decode_and_insert(self, _timestamp, _edgeId, res, table_name):
        start_time = time.time()
        decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
        execution_time = (time.time() - start_time) * 1000
        logging.info(f"\t복호화대상 {len(res)}byte, 복호화 실행시간: {execution_time:.2f}ms")

        snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))
        db_in_datas = self.gbutil.dictToSql(snake_case_vector)
        db_in_datas['"VEHICLE_ID"'] = _edgeId

        db_conditions = {'tablename': f'public."{table_name}"'}
        return self.dbctrl.base_insert(db_conditions, db_in_datas)
    
    def handle_data(self, thread_id):        
        while True:
            send_message = ""
            json_message = dict()
            conditions = dict()
            replay_ok = False
            other_pub_ok = False

            try:                
                queue_att = self.data_queue.get()

                now = datetime.datetime.now()

                client = queue_att['client']
                res = queue_att['data']
                topic = queue_att['topic']
                
                if res is None:
                    continue
            
                _cmd = str(res["header"]["cmd"])
                _actn = str(res["header"]["actn"])
                _dtlActn = str(res["header"]["dtlActn"])
                _strtpnt = str(res["header"]["strtpnt"])
                _dstn = str(res["header"]["dstn"])

                _userId = str(res["header"]["userId"])
                _edgeId = str(res["header"]["edgeId"])
                _edgeTy = str(res["header"]["edgeTy"])
                _timestamp = str(res["header"]["timestamp"])
                print(_edgeId)
                if _cmd == "heartbeat":
                    continue

                #end_time = now.strftime("%Y:%m:%d-%H:%M:%S.%f")
                start_time = datetime.datetime.strptime(_timestamp, '%Y:%m:%d-%H:%M:%S.%f')
                delay_time = (now - start_time).total_seconds() * 1000  # 밀리세컨드 단위로 변환
                start_str = str(now)
                end_str = str(start_time)
                logging.info("$$$ 큐에서 꺼냄  N(M)->H DelayTime: {1}ms ({2} - {3}, ThreadId:{0})".format(thread_id, round(delay_time,4), start_str[13:], end_str[13:]))

                _cmd_req = _cmd
                resheader = res["header"]
                resdata = dict()
                _encoded_data = ""

                _strtpnt_res = _dstn
                _dstn_res = _strtpnt

                #logging.info("메시지 수신 : on topic: {0},  헤더 : {1} {2} {3} {4} {5} {6}\n {7}".format(topic, _cmd, _actn, _strtpnt, _dstn, _edgeId, _userId, json.dumps(res, ensure_ascii=False, indent=3)))
                logging.info("\t수신정보 헤더 : {1} {2} {3} {4} {5} {6} {7}, on topic: {0}".format(topic, _cmd, _actn, _dtlActn, _strtpnt, _dstn, _edgeId, _userId))

                # data 부분의 값을 변경해야 할 때 Decode하고 -> 수정하고 -> Encode해야 한다면 아래와 같이
                # 운영 적용시 이건 꾝 주석처리 하세요.
                """"""
                start_time = time.time()
                decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                end_time = time.time()

                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                logging.info("\n")
                logging.info("\t 운영 적용시 이건 꾝 주석처리 하세요.")
                logging.info("\t DATA 디코드 실행 시간: {0}ms".format(execution_time))
                logging.info("\t decoded_data: {0} {1} :\n [{2}]".format(_edgeId,_timestamp, str(decoded_data)))
                
                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId
                
                topic_define = self.PUB_EDGENODE_TOPIC.replace("#", '')
                
                if "edgeplatform/ctrlsub" in topic:
                    logging.info("\t관제로부터의 메시지")

                    replay_ok = False
                    RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId

                    _cmd_req = _cmd
                    _strtpnt_res = "H"
                    _dstn_res = "N"

                    if _cmd == "rep":
                        if _actn == "globalpath":

                            if _dtlActn == "globalpath":
                                pass
                    elif _cmd == "req":
                        if _actn == "tractor":
                            if _dtlActn == "workinfo":
                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId

                                _encoded_data = str(res["data"])
                        elif _actn == "globalpath":
                            if _dtlActn == "planwrite":
                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId

                                _encoded_data = str(res["data"])
                        elif _actn == "event":
                            RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId
                            _encoded_data = str(res["data"])
                        elif _actn == "simdata":
                            RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId
                            _encoded_data = str(res["data"])
                        elif _actn == "simsendterm":
                            RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId
                            _encoded_data = str(res["data"])
                        elif _actn == "simrand":
                            RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId
                            _encoded_data = str(res["data"])

                # topic이 초기화 관련인지 아닌지
                elif "edgeplatform/node" in topic:
                    logging.info("\tEdgeNode로부터 메시지")
                    # EdgeNode로부터 온 메시지
                    if _strtpnt == "N":
                        if _cmd == "req":                            
                            replay_ok = True
                            if _actn == "globalpath":
                                RES_TOPIC = self.PUB_CTRLCENTER_TOPIC

                                _dstn_res = "C"

                                _encoded_data = res["data"]

                                """
                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId


                                _dstn_res = _strtpnt

                                _userId = db_userId = "specialuser"
                                db_globalPathList = []

                                db_globalPathList.append(("경로1", "ABCD1231"))
                                db_globalPathList.append(("경로2", "123456B"))
                                db_globalPathList.append(("경로3", "123456C"))
                                db_globalPathList.append(("경로4", "ABCD1234"))
                                
                                data_message = dict()

                                data_message["resultCd"] = 0
                                data_message["resultMssage"] = "globalpath 성공 응답"
                                data_message["userId"] = db_userId
                                #data_message["globalPathList"] = db_globalPathList

                                resdata_string = json.dumps(data_message, ensure_ascii=False)
                                
                                start_time = time.time()
                                send_data = self.jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                                end_time = time.time()
                                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                logging.info("#2 DATA 인코드 실행 시간: {0}ms".format(execution_time))

                                _encoded_data = str(send_data)
                                """
                        elif _cmd == "res":
                            if _actn == "tractor":
                                if _dtlActn == "workinfo":
                                    other_pub_ok = True
                                    RES_TOPIC_OTHER = self.PUB_CTRLCENTER_TOPIC

                                    _dstn_res = "C"

                                    _encoded_data = str(res["data"])
                            elif _actn == "globalpath":
                                if _dtlActn == "planwrite":
                                    #other_pub_ok = True
                                    RES_TOPIC = self.PUB_CTRLCENTER_TOPIC

                                    _dstn_res = "C"

                                    _encoded_data = str(res["data"])

                        elif _cmd == "cmd":
                            _strtpnt_res = "H"
                            _dstn_res = "N"

                            RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId

                            if _actn == "power":
                                if _dtlActn == "true":
                                    # 이력남김
                                    pass
                                elif _dtlActn == "false":
                                    pass
                            elif _actn == "race":
                                if _dtlActn == "true":
                                    # 이력남김
                                    pass
                                elif _dtlActn == "false":
                                    pass
                        elif _cmd == "rep":
                            if _actn == "alarm":
                                other_pub_ok = True
                                RES_TOPIC_OTHER = self.PUB_CTRLCENTER_TOPIC

                                #M 으로 토픽발행
                                _dstn_res = "M"
                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _userId

                                """
                                # data 부분 재구성
                                
                                data_message = str(json.dumps(res["data"]))
                                send_data = self.jvm_msg_encrypt_class.encode(_timestamp, _edgeId, decoded_data)

                                _encoded_data = str(send_data)
                                """
                                
                                _encoded_data = res["data"]

                                replay_ok = True
                            elif _actn == "status" or _actn == "vehicle":
                                #M 으로 토픽발행
                                _dstn_res = "M"
                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _userId

                                other_pub_ok = True
                                RES_TOPIC_OTHER = self.PUB_CTRLCENTER_TOPIC

                                #decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                                #logging.info("decoded_data: {0}, {1} => {2} ".format(_timestamp, _edgeId, str(decoded_data)))

                                _encoded_data = res["data"]

                                replay_ok = True

                                if _dtlActn == "info":
                                    pass
                                elif _dtlActn == "location":
                                    pass
                                elif _dtlActn == "common":
                                    replay_ok = False
                                    other_pub_ok = True

                                    # data 디코딩
                                    start_time = time.time()
                                    decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                                    end_time = time.time()
                                    execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    logging.info("\t#1 복호화대상 {}byte, 복호화 실행시간: {}ms".format(len(res["data"]), execution_time))

                                    # DB 변수형으로 변환
                                    snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))

                                    # 다시 sql문 생성위한 위한 변환
                                    db_in_datas= self.gbutil.dictToSql(snake_case_vector)

                                    # DB Insert
                                    db_conditions = {'tablename': 'public.\"VEHICLE_ING_INFO\"'}
                                    # 추가할 필드 추가
                                    db_in_datas['"VEHICLE_ID"'] = _edgeId
                                    db_in_datas['"VEHICLE_TYPE"'] = _edgeTy[0]
                                         
                                    result = self.dbctrl.base_insert(db_conditions,db_in_datas)
                            elif _actn == "tractor":
                                other_pub_ok = True
                                RES_TOPIC_OTHER = self.PUB_CTRLCENTER_TOPIC

                                _encoded_data = res["data"]

                                if _dtlActn == "info":
                                    # data 디코딩
                                    # start_time = time.time()
                                    # decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                                    # end_time = time.time()
                                    # execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    # logging.info("\t#2 복호화대상 {}byte, 복호화 실행시간: {}ms".format(len(res["data"]), execution_time))

                                    # # DB 변수형으로 변환
                                    # snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))

                                    # # 다시 sql문 생성위한 위한 변환
                                    # db_in_datas= self.gbutil.dictToSql(snake_case_vector)

                                    # # DB Insert
                                    # db_conditions = {'tablename': 'public.\"TRACTOR_INFO\"'}

                                    # # 추가할 필드 추가
                                    # db_in_datas['"VEHICLE_ID"'] = _edgeId
                                         
                                    # result = self.dbctrl.base_insert(db_conditions,db_in_datas)
                                    self.decode_and_insert(_timestamp, _edgeId, res, '\"TRACTOR_INFO\"')
                                elif _dtlActn == "workinfo":
                                    
                                    # data 디코딩
                                    # start_time = time.time()
                                    # decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                                    # end_time = time.time()
                                    # execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    # logging.info("\t#3 복호화대상 {}byte, 복호화 실행시간: {}ms".format(len(res["data"]), execution_time))

                                    # # DB 변수형으로 변환
                                    # snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))

                                    # # 다시 sql문 생성위한 위한 변환
                                    # db_in_datas= self.gbutil.dictToSql(snake_case_vector)
                                    # db_in_datas['"WORK_AREA"'] = db_in_datas['"WORK_AREA"'].replace("'", '"')
                                    # db_in_datas['"WORK_PATH"'] = db_in_datas['"WORK_PATH"'].replace("'", '"')

                                    # # DB Insert
                                    # db_conditions = {'tablename': 'public.\"WORK_INFO_TRACTOR\"'}

                                    # # 추가할 필드 추가
                                    # db_in_datas['"VEHICLE_ID"'] = _edgeId
                                         
                                    # result = self.dbctrl.base_insert(db_conditions,db_in_datas)   
                                    self.decode_and_insert(_timestamp, _edgeId, res, '\"WORK_INFO_TRACTOR\"')
                                elif _dtlActn == "tractorinfo":
                                    
                                    # data 디코딩
                                    # start_time = time.time()
                                    # decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                                    # end_time = time.time()
                                    # execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    # logging.info("\t#4 복호화대상 {}byte, 복호화 실행시간: {}ms".format(len(res["data"]), execution_time))

                                    # # DB 변수형으로 변환
                                    # snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))

                                    # # 다시 sql문 생성위한 위한 변환
                                    # db_in_datas= self.gbutil.dictToSql(snake_case_vector)

                                    # # DB Insert
                                    # db_conditions = {'tablename': 'public.\"WORK_INFO_TRACTOR\"'}

                                    # # 추가할 필드 추가
                                    # db_in_datas['"VEHICLE_ID"'] = _edgeId

                                    # result = self.dbctrl.base_insert(db_conditions,db_in_datas)   
                                    self.decode_and_insert(_timestamp, _edgeId, res, '\"WORK_INFO_TRACTOR\"')
                            elif _actn == "cls":
                                if _dtlActn == "ctrlinfo":
                                    # start_time = time.time()
                                    # decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                                    # end_time = time.time()
                                    # execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    # logging.info("\t#5 복호화대상 {}byte, 복호화 실행시간: {}ms".format(len(res["data"]), execution_time))

                                    # decoded_data_obj = json.loads(str(decoded_data))

                                    # # DB 변수형으로 변환
                                    # snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))

                                    # # 다시 sql문 생성위한 위한 변환
                                    # db_in_datas= self.gbutil.dictToSql(snake_case_vector)

                                    # # DB Insert
                                    # db_conditions = {'tablename': 'public.\"STREET_SWEEPER_INFO\"'}
                                    # # 추가할 필드 추가
                                    # db_in_datas['"VEHICLE_ID"'] = _edgeId

                                    # result = self.dbctrl.base_insert(db_conditions,db_in_datas)
                                    self.decode_and_insert(_timestamp, _edgeId, res, '\"STREET_SWEEPER_INFO\"')
                                    # data의 값이 배열로 들어가 있어서 일단 json 객체로 변환해서 하나씩 꺼내와서 DB 넣거나, 그냥 텍스트로 통채로 넣어야 함
                                    """
                                    logging.info("################################ decoded_data: {0}".format(decoded_data))

                                    # DB 변수형으로 변환
                                    snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))

                                    # 다시 sql문 생성위한 위한 변환
                                    db_in_datas= self.gbutil.dictToSql(snake_case_vector)

                                    # DB Insert
                                    db_conditions = {'tablename': 'public.\"STREET_SWEEPER_INFO\"'}

                                    # 추가할 필드 추가
                                    db_in_datas['"VEHICLE_ID"'] = _edgeId

                                    result = self.dbctrl.base_insert(db_conditions,db_in_datas)
                                    """
                                elif _dtlActn == "false":
                                    pass
                        elif _cmd == "event":
                            other_pub_ok = True
                            RES_TOPIC_OTHER = self.PUB_CTRLCENTER_TOPIC

                            _encoded_data = str(res["data"])
                            # DB에 넣으려면 풀고 작업하세요
                            """
                            # data 디코딩
                            decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])

                            # DB 변수형으로 변환
                            snake_case_vector = self.gbutil.tosnake_dictname(json.loads(str(decoded_data)))

                            # 다시 sql문 생성위한 위한 변환
                            db_in_datas= self.gbutil.dictToSql(snake_case_vector)
                            db_in_datas['"WORK_AREA"'] = db_in_datas['"WORK_AREA"'].replace("'", '"')
                            db_in_datas['"WORK_PATH"'] = db_in_datas['"WORK_PATH"'].replace("'", '"')

                            # DB Insert
                            db_conditions = {'tablename': 'public.\"WORK_INFO_TRACTOR\"'}

                            # 추가할 필드 추가
                            db_in_datas['"VEHICLE_ID"'] = _edgeId

                            result = self.dbctrl.base_insert(db_conditions,db_in_datas)
                            """
                        elif _cmd == "hist":
                            #decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, _edgeId, res["data"])
                            #logging.info("decoded_data: {0}, {1} => {2} ".format(_timestamp, _edgeId, str(decoded_data)))
                            pass

                    # Mobile로부터 온 메시지
                    # ###################################################
                    elif _strtpnt == "M":
                        logging.info("\tMobile로부터 메시지")
                        if _cmd == "cmd":
                            _dstn_res = "N" 
                            if _actn == "power":
                                if _dtlActn == "true":
                                    # 이력남기고 N로 보냄
                                    pass
                                elif _dtlActn == "false":
                                    # 이력남기고 N로 보냄
                                    pass

                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId

                                resultMssage = _actn + " " + _dtlActn + " 요청 응답"

                                data_message = dict({'resultCd': 0, 'resultMssage': resultMssage})   
                                resdata_string = json.dumps(data_message, ensure_ascii=False)

                                start_time = time.time()
                                send_data = self.jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                                end_time = time.time()
                                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                logging.info("\t#5 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                                _encoded_data = str(send_data)
                            elif _actn == "race":
                                if _dtlActn == "true":
                                    # 이력남기고 N로 보냄
                                    pass
                                elif _dtlActn == "false":
                                    # 이력남기고 N로 보냄
                                    pass

                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId

                                """
                                resultMssage = _actn + " " + _dtlActn + " 요청 응답"

                                data_message = dict({'resultCd': 0, 'resultMssage': resultMssage})   
                                resdata_string = json.dumps(data_message, ensure_ascii=False)

                                start_time = time.time()
                                send_data = self.jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                                end_time = time.time()
                                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                logging.info("#4 DATA 인코드 실행 시간: {0}ms".format(execution_time))
                                """

                                _encoded_data = str(res["data"])

                        elif _cmd == "req":
                            _cmd_req = "res"
                            if _actn == "alarm":
                                if _dtlActn == "list":
                                    RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _userId
                                    resultMssage = _actn + " " + _dtlActn + " 요청 응답"

                                    data_message = dict({'resultCd': 0, 'resultMssage': resultMssage})  
                                    resdata_string = json.dumps(data_message, ensure_ascii=False)

                                    start_time = time.time()
                                    send_data = self.jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                                    end_time = time.time()
                                    execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    logging.info("\t#6 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                                    _encoded_data = str(send_data)
                            elif _actn == "status":
                                if _dtlActn == "list":
                                    RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _userId
                                    resultMssage = _actn + " " + _dtlActn + " 요청 응답"

                                    data_message = dict({'resultCd': 0, 'resultMssage': resultMssage})  
                                    resdata_string = json.dumps(data_message, ensure_ascii=False)

                                    start_time = time.time()
                                    send_data = self.jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                                    end_time = time.time()
                                    execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    logging.info("\t#7 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                                    _encoded_data = str(send_data)
                            elif _actn == "edge":
                                if _dtlActn == "list":
                                    RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _userId
                                    resultMssage = _actn + " " + _dtlActn + " 요청 응답"

                                    data_message = dict({'resultCd': 0, 'resultMssage': resultMssage})  
                                    resdata_string = json.dumps(data_message, ensure_ascii=False)

                                    start_time = time.time()
                                    send_data = self.jvm_msg_encrypt_class.encode(_timestamp, _edgeId, resdata_string)
                                    end_time = time.time()
                                    execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    logging.info("\t#8 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                                    _encoded_data = str(send_data)
                        elif _cmd == "res":   
                            if _actn == "globalpath":
                                logging.info("요기 globalpath ")
                                RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _edgeId

                                _encoded_data = str(res["data"])

                # topic이 init인 경우
                # ###################################################
                else:
                    logging.info("\t초기화 init 토픽 메시지")
                    if _cmd == "req":
                        _cmd_req = "res"
                        replay_ok = True

                        if _actn == "init":
                            data_string = res["data"]
                            #logging.info("datadata: {0} ".format(data_string))
                            #logging.info("_timestamp: {0} {1} ".format(_timestamp, str(res["sendOptKey"])))

                            connect_point = 0

                            start_time = time.time()
                            decoded_data = self.jvm_msg_encrypt_class.decode(_timestamp, data_string)
                            end_time = time.time()
                            execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                            logging.info("\t#6 복호화대상 {}byte, 복호화 실행시간: {}ms".format(len(data_string), execution_time))

                            #문자열을 객체로 변환하기
                            datadata = json.loads(str(decoded_data))

                            if _strtpnt == "M":
                                _initId = datadata["initId"]
                                _userId = _clientId = datadata["userId"]
                                _userPwd = datadata["userPassword"]
                                connect_point = 1

                                # _userId, _userPwd 로 DB 에서 edge목록을 가져온다.
                            else:
                                _initId = datadata["edgeId"]
                                _edgeId = _clientId = datadata["edgeId"]
                                connect_point = 0

                                # _edgeId 로 사용자 찾아 _userId 찾고 다음 edge목록을 가져온다.
                                #_userId = DB["userId']

                            self.client_list[_clientId] = (connect_point)
                            #logging.info(self.client_list)

                            # DB에서 edgeId로 사용자 아이디 찾고
                            # 찾은 사용자 아이디로 edgeId 목록을 가져와 보낸다.
                            #_userId = db_userId = "specialuser"
                            db_userId = _userId
                            db_edgeList = []

                            """ 기능확인되서 일단 막는다.
                            # DB 
                            # ####################################################
                            """

                            if _userId  != "":
                                db_conditions = {'tablename': 'vehicles as a'}
                                db_conditions['select'] = "a.edge_exp, a.edge_id, a.edge_ty"
                                db_conditions['orderby'] = " order by edge_id "
                                db_conditions['and_where'] = {}
                                db_conditions['and_where']['user_id'] = _userId
                                db_conditions['offset'] = "1"
                                db_conditions['limit'] = "10"

                                result = self.dbctrl.base_select(db_conditions)
                                #print(result['datas'])
                                data_len = len(result['datas'])
                                #print(data_len)
                                #pd_dt = pd.DataFrame(result['datas'])
                                #print(pd_dt)

                            """
                            # DB  edgeId값이 있으면 edgeId로 UserId가져와서 edgeId목록 가져온다. 
                            # ####################################################
                            """
                            if _edgeId != "":
                                db_conditions = {}
                                db_conditions['select_count'] = "SELECT count(*) FROM vehicles WHERE user_id = (SELECT user_id FROM vehicles WHERE edge_id='"+_edgeId+"')"
                                db_conditions['select_string'] = "SELECT edge_exp, edge_id, edge_ty FROM vehicles WHERE user_id = (SELECT user_id FROM vehicles WHERE edge_id='"+_edgeId+"')"
                                
                                result = self.dbctrl.base_select(db_conditions)

                            """
                            # DB Insert
                            # ####################################################
                            db_in_datas = {}
                            db_in_datas['level'] = "7"
                            db_in_datas['leveltype'] = 'a'
                            db_in_datas['url'] = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                            db_in_datas['hanjakey'] = _initId + ""
                            db_in_datas['sitename'] = _userId+ ""

                            db_conditions = {'tablename': 'levelhanjalist'}
                            self.dbctrl.base_insert(db_conditions, db_in_datas)

                            # DB Update
                            # ####################################################
                            db_conditions2 = {'tablename': 'levelhanjalist'}
                            db_conditions2['and_where'] = {}
                            db_conditions2['and_where']['level'] = '1'

                            db_in_datas2 = {}
                            db_in_datas2['leveltype'] = 'F'
                            db_in_datas2['url'] = 'ccccccccc'
                            db_in_datas2['hanjakey'] = _initId + ""
                            db_in_datas2['sitename'] = _userId+ ""

                            self.dbctrl.base_update(db_conditions2, db_in_datas2)

                            # DB Delete
                            # ####################################################
                            db_conditions3 = {'tablename': 'levelhanjalist'}
                            db_conditions3['and_where'] = {}
                            db_conditions3['and_where']['level'] = '3'

                            self.dbctrl.base_delete(db_conditions3)
                            """

                            for item in result['datas']:
                                atts = []
                                for attribute, value in item.items():
                                    atts.append(value)
                                    #print(attribute, value) # example usage

                                db_edgeList.append(atts)
            
                            """
                            db_edgeList.append(("트랙터1", "123456ABCD"))
                            db_edgeList.append(("트랙터2", "123456ABCE"))
                            db_edgeList.append(("트랙터3", "123456ABCF"))
                            db_edgeList.append(("트랙터4", "ABCD123456"))
                            """

                            RES_TOPIC = self.PUB_EDGENODE_TOPIC + "/" + _initId

                            #resdata = dict({'resultCd': 0, 'resultMssage': "init 성공 응답 22", 'userId':db_userId, 'edgeList':result['datas']})   
                            resdata = dict({'resultCd': 0, 'resultMssage': "init 성공 응답 22", 'userId':db_userId, 'edgeList':db_edgeList})   
                            resdata_string = json.dumps(resdata, ensure_ascii=False)

                            #logging.info("{0}".format(resdata_string))

                            start_time = time.time()
                            send_data = self.jvm_msg_encrypt_class.encode(_timestamp, resdata_string)
                            end_time = time.time()
                            execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                            logging.info("\t#1 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                            _encoded_data = str(send_data)
                        elif _actn == "alarm":
                            if _dtlActn == "list":
                                _resultMssage = _actn + " " + _dtlActn + " 성공"
                                resdata = dict({'resultCd': _resultCd, 'resultMssage': _resultMssage })   

                                resdata_string = json.dumps(resdata, ensure_ascii=False)

                                start_time = time.time()
                                send_data = self.jvm_msg_encrypt_class.encode(_timestamp, resdata_string)
                                end_time = time.time()
                                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                logging.info("\t#2 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                                _encoded_data = str(send_data)

                        elif _actn == "status":
                            if _dtlActn == "list":
                                _resultMssage = _actn + " " + _dtlActn + " 성공"
                                resdata = dict({'resultCd': _resultCd, 'resultMssage': _resultMssage})   

                                resdata_string = json.dumps(resdata, ensure_ascii=False)

                                start_time = time.time()
                                send_data = self.jvm_msg_encrypt_class.encode(_timestamp, resdata_string)
                                end_time = time.time()
                                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                logging.info("\t#3 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                                _encoded_data = str(send_data)
                        elif _actn == "edge":
                            if _dtlActn == "list":
                                _resultMssage = _actn + " " + _dtlActn + " 성공"
                                resdata = dict({'resultCd': _resultCd, 'resultMssage': _resultMssage})   

                                resdata_string = json.dumps(resdata, ensure_ascii=False)

                                start_time = time.time()
                                send_data = self.jvm_msg_encrypt_class.encode(_timestamp, resdata_string)
                                end_time = time.time()
                                execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                logging.info("\t#4 암호화대상 {}byte,  코드 실행 시간: {}ms".format(len(resdata_string), execution_time))

                                _encoded_data = str(send_data)
     
                if _cmd == "req" or _cmd == "cmd" or _cmd == "res" or replay_ok is True:                
                    resheader["cmd"] = _cmd_req
                    resheader["strtpnt"] = _strtpnt_res
                    resheader["dstn"] = _dstn_res
                    resheader["edgeId"] = _edgeId

                    json_message["header"] = resheader
                    json_message["data"] = _encoded_data
                    
                    send_message = json.dumps(json_message, ensure_ascii=False)
                    #logging.info("\n$$$$$$$$$>>> #12 보낼 메시지 : 대상 {0},  {1}->{2},  {3}".format(RES_TOPIC, _strtpnt_res, _dstn_res, json.dumps(json_message, ensure_ascii=False, indent=3)))
                    logging.info("\t$$$$$$$$$>>> {} 으로 보냄 : {} {} {}, 대상 {} -> {},  {}".format(resheader["dstn"], _cmd_req, _actn, _dtlActn, resheader["strtpnt"], resheader["dstn"], RES_TOPIC))
            
                    self.publish(client, RES_TOPIC, send_message) 

                if other_pub_ok is True:
                    
                    resheader["cmd"] = _cmd_req
                    resheader["strtpnt"] = _strtpnt_res
                    resheader["dstn"] = "C"
                    resheader["edgeId"] = _edgeId

                    json_message["header"] = resheader
                    json_message["data"] = _encoded_data
                    
                    send_message = json.dumps(json_message, ensure_ascii=False)
                    #logging.info("\n$$$$$$$$$>>> #12 보낼 메시지 : 대상 {0},  {1}->{2},  {3}".format(RES_TOPIC, _strtpnt_res, _dstn_res, json.dumps(json_message, ensure_ascii=False, indent=3)))
                    logging.info("\t$$$$$$$$$>>> 또다른 대상 {} 한테도 보냄 : {} {} {}, 대상 {} -> {},  {}".format(resheader["dstn"], _cmd_req, _actn, _dtlActn, resheader["strtpnt"], resheader["dstn"], RES_TOPIC_OTHER))
            
                    self.publish(client, RES_TOPIC_OTHER, send_message) 
                                
            except queue.Empty as err:
                logging.exception("스레드 {0}가 대기 중입니다. {1}",thread_id, err)
            except json.decoder.JSONDecodeError as err:
                logging.exception("json.decoder.JSONDecodeError %s", err)
                #return            
            except KeyError as err:
                logging.exception("KeyError %s. ", err)                
            except TypeError as err:
                logging.exception("TypeError %s. ", err)
            except Exception as err:
                logging.exception("Exception %s. ", err)
                #return                   
            finally:
                #logging.info("finally IN handle_data")
                self.data_queue.task_done()

    def on_message(self, client, userdata, message):
        topic = self.onmessage_topic = message.topic
        
        try:
            message = str(message.payload.decode("utf-8"))
        except UnicodeDecodeError as err:
            logging.exception("UnicodeDecodeError %s", err)
            return
        except Exception as err:
            logging.exception("Exception %s. ", err)
            return
        
        #logging.exception("message : {0}".format(message))
        send_message = ""
        json_message = dict()
        conditions = dict()
        replay_ok = False
        
        try:
            res=json.loads(str(message))

            queue_att = dict()
            queue_att['client'] = client
            queue_att['data'] = res
            queue_att['topic'] = topic

            logging.info("")
            self.data_queue.put(queue_att)
            #logging.info("$$$ 큐에 메시지 넣음 {0}, {1}, {2}".format(client, res, topic))
            logging.info("$$$ 큐에 메시지 넣음 {0}, {1}".format(topic, client))
            
        except json.decoder.JSONDecodeError as err:
            logging.exception("json.decoder.JSONDecodeError %s", err)
            return        
        except KeyError as err:
            logging.exception("KeyError %s. ", err)
            return
        except TypeError as err:
            logging.exception("TypeError %s. ", err)
            return
        except Exception as err:
            logging.exception("Exception %s. ", err)
            return
        finally:
            #logging.info("finally IN on_message")
            return
    
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
        
    """
    Creates a rotating log
    """
    logging.basicConfig(level=set_level, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s')
    logger = logging.getLogger("")
    logger.setLevel(set_level)

    print("_logger_level: {0}, _logger_backupcount:{1}, _logger_when: {2}".format(_logger_level, _logger_backupcount, _logger_when ));
    
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
                                          
    logger.addHandler(handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s' )        
    handler.setFormatter(formatter) # 핸들러에 로깅 포맷 할당    

    return logger

if __name__ == "__main__":
    _config = configparser.ConfigParser()

    _config.read(CONFIG, encoding='utf-8') 
    _mqhost = _config['MQTT']['BROKER_HOST'] # Server IP  
    _mqport = int(_config['MQTT']['BROKER_PORT']) # Server Port

    _subscribes = _config['MQTT']['TOPIC_SUBS'] # 발행 TOPIC
    subcribes = _subscribes.split(',')

    _pub_init_topic = _config['MQTT']['PUB_INIT_TOPIC'] # 구도 TOPIC
    _pub_edgenode_topic = _config['MQTT']['PUB_EDGENODE_TOPIC']
    _pub_ctrlcenter_topic = _config['MQTT']['PUB_CTRLCENTER_TOPIC']

    # DB
    _db_host = _config['DB']['DB_HOST']
    _db_port = _config['DB']['DB_PORT']
    _db_db = _config['DB']['DB_DB']
    _db_user = _config['DB']['DB_USER']
    _db_pwd = _config['DB']['DB_PWD']

    #Logger
    _logger_level = _config['LOGGER']['LOG_LEVEL']
    _logger_when = _config['LOGGER']['LOG_WHEN']
    _logger_interval = _config['LOGGER']['LOG_INTERVAL']

    _logger_backupcount = _config['LOGGER']['LOG_BACKUPCOUNT']

    full_path = os.path.join(ROOT_DIR, "logs", "hubmqttsrv.log")
    
    logger = create_rotating_log(full_path, _config['LOGGER'])
    logging.info("logger path {0}".format(full_path))

    mqtt = Mqtt()

    mqtt.setLogger(logger)

    mqtt.ready(_mqhost, _mqport, _pub_init_topic, _pub_edgenode_topic, _pub_ctrlcenter_topic, _config['DB'])

    try:
        mqtt.start(subcribes)
    except :
        logger.exception("MQTT Connect Fail...")
        sys.exit()