import socketserver

import sys
import os
import random
import time
import json
import time
import datetime
import threading
import socket
import paho.mqtt.client as mqtt_client
from pathlib import Path
import configparser
import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
from confluent_kafka import Producer
from constant import Constant

# java jar 임포트
import jpype
import jpype.imports
from jpype.types import *


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

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

def send_kafka_msg(topic, message):
    #카프카 메시지 전송
    producer.produce(topic, value=json.dumps(message).encode('utf-8'))
    

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

class MyTCPHandler2(socketserver.BaseRequestHandler):
    logger = get_log()

    def __init__(self, request, client_address, server):
        logging.info("__init__ 호출 MyTCPHandler2")
        
        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)
       
        return

    def setup(self):
        logging.info("사용자(모바일) setup 호출")

        return socketserver.BaseRequestHandler.setup(self)

    def handle(self):
        conn = self.request  # 접속한 클라이언트 소켓
        addr = self.client_address[0]
        recv_len = 4

        client_sockets_2.append((conn, addr))

        buf = ""
        

        logging.info("사용자(모바일) 접속 Client {0}".format(len(client_sockets_2)))

        """
        클라이언트와 연결될 때 호출되는 함수
        상위 클래스에는 handle() 메서드가 정의되어 있지 않기 때문에
        여기서 오버라이딩을 해야함
        """
        cur_thread = threading.current_thread()
        logging.info("사용자(모바일) 클라이언트 접속 : {} was started for {}".format(cur_thread.getName(), self.client_address[0]))

        answer = random.randint(1, 9)

        while True:
            try:
                data = conn.recv(recv_len).decode(encoding=recv_encoding)

                if not data:
                    logger.info('>> 사용자(모바일)  Disconnected by ' + addr)
                    break
                
                #logger.info(f'{cur_thread} - 데이터 수신:{data}   {len(data)}')

                buf += data

                index = buf.find("\r\n")

                #logger.info(f'버퍼링 데이터 :[{buf}]   {len(buf)} , index 위치는: ' + str(index) )

                if index == -1:
                    #logger.info("완전체가 없다. 버퍼에 추가하고 수신대기로 " + buf)
                    continue
                else:  
                    data = buf[0:index + 2]

                    #data = buf + data   

                    data = data.replace("\r\n", '')
                    #logger.info("====> 사용자(모바일) 로부터 완전체가 있다. 수신데이터:[" + data + "] , " + str(len(data)))

                    buf = data.replace(data, '')

                    #logger.info("사용자 버퍼링에 남은 수신데이터:[" + buf + "] , " + str(len(buf)))

                    send_message = ""
                    json_message = dict()
                    json_message2 = dict()

                    try:
                        res=json.loads(str(data))
                    
                        _cmd = str(res["header"]["cmd"])
                        _actn = str(res["header"]["actn"])
                        _dtlActn = str(res["header"]["dtlActn"])
                        _strtpnt = str(res["header"]["strtpnt"])
                        _dstn = str(res["header"]["dstn"])
                        _userId = str(res["header"]["userId"])
                        _edgeId = str(res["header"]["edgeId"])
                        _edgeTy = str(res["header"]["edgeTy"])
                        _timestamp = str(res["header"]["sndngDt"])

                        now = datetime.datetime.now()
                        end_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
                        start_time = datetime.datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S.%f')
                        delay_time = (now - start_time).total_seconds() * 1000  # 밀리세컨드 단위로 변환

                        logging.info("\n")
                        logging.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
                        logging.info("%%%%%%%%%%%% Mobile -> EdgeNode delay_time: {0}ms , ({1} - {2})".format(round(delay_time,4), now, start_time))

                        _cmd_req = _cmd
                        _strtpnt_res = _dstn
                        _dstn_res = _strtpnt

                        resheader = res["header"]
                        resdata = dict()
                        send_direction = Constant.MOBILESOCK2
                        send_direction_other = Constant.NONE

                        otherheader = res["header"]
                        otherresdata = dict()

                        #logging.info("MOBILE수신헤더 : {0} {1} {2} {3} {4} {5} {6} {7}".format(_cmd, _actn, _dtlActn, _strtpnt, _dstn, _edgeId, _userId, _timestamp))
                        logging.info("MOBILE 수신 : {0} {1} {2} {3} {4} {5}\n {6} ".format( _cmd, _actn, _strtpnt, _dstn, _edgeId, _userId, json.dumps(res, ensure_ascii=False, indent=3)))

                        _resultCd = 0
                        _resultMssage = "성공"

                        if _cmd == "cmd":
                            if _actn == "power":
                                if _dtlActn == "on":
                                    # 이력남기고 N로 보냄
                                    pass
                                elif _dtlActn == "off":
                                    pass

                                resdata = res["data"]
                                send_direction = Constant.MOBILESOCK1

                                # 명령 이력을 쌓기 위해 HUB로도 보내자
                                send_direction_other = Constant.EDGEHUB

                                otherheader["cmd"] = "hist"
                                otherheader["strtpnt"] = "N"
                                otherheader["dstn"] = "H"
                                otherheader["timestamp"] = _timestamp

                                otherresdata = res["data"]                                

                        elif _cmd == "heartbeat":
                            send_direction = Constant.NONE
                        elif _cmd == "req":
                            _cmd_req = "res"
                            
                            if _actn == "init":
                                data_message = str(res["data"])        
                                #decoded_data = jvm_msg_encrypt_class.decode(_timestamp, data_message)

                                objheader = res["header"]
                                
                                obj = dict()
                                obj["header"] = objheader
                                obj["data"] = data_message

                                logging.info("================== {0}".format(obj))
                                _userId = str(objheader["userId"])

                                db_edgeList = []

                                db_edgeList.append(("트랙터1", "123456ABCD"))
                                db_edgeList.append(("트랙터2", "123456EFGH"))
                                db_edgeList.append(("트랙터3", "123456IJKL"))
                                db_edgeList.append(("트랙터4", "ABCD123456"))

                                if _userId != "specialuser":
                                    _resultCd = 1000
                                    _resultMssage = "사용자 정보 없음"
                                else:
                                    _resultCd = 0
                                    _resultMssage = "init 성공"

                                resdata = dict({'resultCd': _resultCd, 'resultMssage': _resultMssage, 'userId':_userId, 'edgeList':db_edgeList})
                            elif _actn == "alarm":
                                if _dtlActn == "list":
                                    _resultMssage = _actn + " " + _dtlActn + " 성공"
                                    resdata = dict({'resultCd': _resultCd, 'resultMssage': _resultMssage })   
                            elif _actn == "status":
                                if _dtlActn == "list":
                                    _resultMssage = _actn + " " + _dtlActn + " 성공"
                                    resdata = dict({'resultCd': _resultCd, 'resultMssage': _resultMssage})   
                            elif _actn == "edge":
                                if _dtlActn == "list":
                                    _resultMssage = _actn + " " + _dtlActn + " 성공"
                                    resdata = dict({'resultCd': _resultCd, 'resultMssage': _resultMssage})   
                        elif _cmd == "rep":
                            if _actn == "alarm":
                                resdata = res["data"]

                        resheader["cmd"] = _cmd_req
                        resheader["strtpnt"] = _strtpnt_res
                        resheader["dstn"] = _dstn_res
                        resheader["edgeId"] = _edgeId
                        resheader["userId"] = _edgeid
                        resheader["timestamp"] = _timestamp
                        resheader["command"] = "05421"
                        
                        json_message["header"] = resheader
                        json_message["data"] = resdata

                        send_message = json.dumps(json_message, ensure_ascii=False)

                        if send_direction == Constant.MOBILESOCK1:
                            #sendAll(client_sockets_1, send_message)
                            send_kafka_msg('connect',send_message)
                            pass
                        elif send_direction == Constant.MOBILESOCK2:
                            sendAll(client_sockets_2, send_message)

                        if send_direction_other == Constant.EDGEHUB:
                            json_message2["header"] = otherheader
                            json_message2["data"] = otherresdata

                            send_message2 = json.dumps(json_message2, ensure_ascii=False)

                            send_kafka_msg('event',json_message2)

                    except json.decoder.JSONDecodeError as err:
                        logging.exception("json.decoder.JSONDecodeError %s", err)
                        continue
                    except KeyError as err:
                        logging.exception("KeyError %s. ", err)
                        continue
                    except TypeError as err:
                        logging.exception("TypeError %s. ", err)
                        continue
                    except Exception as err:
                        logging.exception("Exception %s. ", err)
                        continue

            except ConnectionResetError:
                logger.exception("==> 사용자 ConnectionResetError")
                break
            except KeyboardInterrupt as e:
                logger.exception("==> 사용자 KeyboardInterrupt")
                break
            except OSError:
                logger.exception("==> 사용자 OSError")   
                break
            except UnicodeDecodeError:
                logger.exception("==> 사용자 UnicodeDecodeError")   

        conn.close()         

    def finish(self):
        logger.info("finish")
        conn = self.request
        addr = self.client_address[0]
        client_sockets_2.remove((conn, addr))

        logging.info("현재 사용자 Client 접속수 : {0}".format(len(client_sockets_2)))

        return socketserver.BaseRequestHandler.finish(self)

class ThreadedTCPRequestHandler(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

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
    logger.info("구성정보파일 읽기 서버2:{0}, {1}".format(_host2, _port2))

    HOST2, PORT2 = _host2, _port2

    # 소켓 객체 생성
    try:
        server2 = ThreadedTCPRequestHandler((socket.gethostbyname(HOST2), PORT2), MyTCPHandler2)
    except ValueError:
        logger.exception("ValueError Closing...")
        sys.exit()
    except :
        logger.exception("Bind failed. Closing...")
        # print "Error code: %s \nError Message: %s"\   % (str(msg[0]), msg[1])
        sys.exit()


    server_thread2 = threading.Thread(target=server2.serve_forever)
    server_thread2.daemon = True
    server_thread2.start()

    # Edge <-> Node 메시지 정합을 위해 command 정의 파일 읽어온다.
    with open(_command_file_path, 'r', encoding='utf-8') as file:
        command_tbl = json.load(file)


    print("Server2 loop running in thread:", server_thread2.name)
    
    try:
        server2.serve_forever()
    except KeyboardInterrupt:
        sendDisconnectAll(client_sockets_2)
        print("==> Main KeyboardInterrupt")
    except OSError:
        print("==> Main OSError")            
    
    server2.server_close()
    server2.shutdown()