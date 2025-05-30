import socketserver
import json
import socket
import time
import sys
import configparser
import threading
import os
from pathlib import Path
import datetime
from confluent_kafka import Consumer, KafkaError, Producer
import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
from constant import Constant
import jpype
import jpype.imports
from jpype.types import *
import random


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
CONFIG = Path(ROOT_DIR) / 'nodecommsrv.ini'  # config.ini 설정
_command_file_path = Path(ROOT_DIR) / 'command_tbl.txt'  # config.ini 설정
logger = None
file_encoding = 'utf-8'
send_encoding = 'CP949'
recv_encoding = 'CP949'

# jpype.startJVM()
# jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
# jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

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
    
    #logging.info(f"####카프카 {topic} - {message} 전송완료########### ")


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


class MyTCPHandler1(socketserver.BaseRequestHandler):

    logger = get_log()
    
    def __init__(self, request, client_address, server):
        logging.info("__init__ 호출 MyTCPHandler1")
        # Create a lock object
        self.lock = threading.Lock()

        self.vehicle_info = None
        self.vehicle_location = None
        self.edgeId = None
        self.edgeTy = None
        self.userId = None
        self.timer_interval = 3  # 초 간격으로 작업 실행

        socketserver.BaseRequestHandler.__init__(self, request, client_address, server)
        
        return

    def periodic_task(self):
        print("타이머 작업이 실행되었습니다.")

    def start_timer(self, interval):
        print("start_timer 타이머 시작")
        timer = threading.Timer(interval, self.periodic_task)
        timer.start()
        return timer

    def setup(self):
        logging.info("특장차 setup 호출")
        
        return socketserver.BaseRequestHandler.setup(self)

    def handle(self):
        global g_work_info
        conn = self.request  # 접속한 클라이언트 소켓
        addr = self.client_address[0]
        recv_len = 1024
        client_sockets_1.append((conn, addr))
        
        buf = ""

        #cur_thread = threading.current_thread()
        logging.info("특장차 접속 :  {}".format( self.client_address[0]))
        logging.info("현재 특장차 Client 접속수 : {0}".format(len(client_sockets_1)))
        while True:
            try:
                data = conn.recv(recv_len).decode(encoding=recv_encoding)
                #print(f"data : {data}")
                if not data:
                    break

                buf += data

                index = buf.find("\n")
                if index == -1:
                    continue
                else:
                    with self.lock: 
                        now = datetime.datetime.now()
                        data = buf[0:index + 1]
                        data = data.replace("\n", '')
                        buf = buf[index + 1:]

                        json_message = dict()
                        res_repack = dict()

                        try:
                            res = json.loads(str(data))
                            print(res)
                            edge_command = (res["command"])
                            print(f"edge_command : {edge_command}")
                            try:
                                cmd_mapping = command_tbl[str(edge_command)]
                            except KeyError as e:
                                print(e)
                                continue

                            header_repack = dict()
                            header_repack["cmd"] = str(cmd_mapping["cmd"])
                            header_repack["actn"] = str(cmd_mapping["actn"])
                            header_repack["dtlActn"] = str(cmd_mapping["dtlActn"])

                            header_repack["strtpnt"] = "E"
                            header_repack["dstn"] = "N"

                            header_repack["userId"] = ""
                            self.edgeId = header_repack["edgeId"] = (res["VID"])
                            self.edgeTy = header_repack["edgeTy"] = (res["edgeTy"])
                            header_repack["timestamp"] = (res["timestamp"])
                            header_repack["command"] = edge_command

                            res_repack["header"] = header_repack
                            res_repack["data"] = res["data"]

                            ########################################################################

                            _command = str(res_repack["header"]["command"])
                            _cmd = str(res_repack["header"]["cmd"])
                            _actn = str(res_repack["header"]["actn"])
                            _dtlActn = str(res_repack["header"]["dtlActn"])

                            _strtpnt = str(res_repack["header"]["strtpnt"])
                            _dstn = str(res_repack["header"]["dstn"])

                            _userId = str(res_repack["header"]["userId"])
                            _edgeId = str(res_repack["header"]["edgeId"])
                            _edgeTy = str(res_repack["header"]["edgeTy"])
                            _timestamp = str(res_repack["header"]["timestamp"])
                            start_time = datetime.datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S.%f')                        
                            delay_time = (now - start_time).total_seconds() * 1000  # 밀리세컨드 단위로 변환
                            logging.info("%%%%%%%%%%%% Edge -> EdgeNode delay_time : {0}ms, ({1} - {2})".format(round(delay_time,4), now, start_time))

                            _cmd_req = _cmd
                            _strtpnt_res = _dstn
                            _dstn_res = "H"

                            resheader = res_repack["header"]
                            resdata = dict()
                            send_direction = Constant.NONE

                            logging.info("EDGE 수신 : {0} {1} {2} {3} {4} {5} {6}".format(_cmd, _actn, _dtlActn, _strtpnt, _dstn, _edgeId, _userId))

                            if _cmd in ["rep", "req", "heartbeat", "event"]:

                                if _actn == "init":
                                    if _strtpnt == "M":
                                        _edgeId = _userId
                                    else:
                                        _edgeId = _edgeId

                                resheader["cmd"] = _cmd_req
                                resheader["strtpnt"] = _strtpnt_res
                                resheader["dstn"] = _dstn_res
                                resheader["edgeId"] = _edgeId

                                json_message["header"] = resheader

                                json_message["data"] = res["data"]


                                #if _actn == "globalpath":
                                    # if _dtlActn == "planwrite":
                                    #     json_message_edge = dict()

                                    #     json_message_edge["command"] = edge_command
                                    #     json_message_edge["VID"] = _edgeId
                                    #     json_message_edge["timestamp"] = _timestamp
                                    #     json_message_edge["edgeTy"] = _edgeTy

                                    #     data_message = str(json.dumps(res["data"], ensure_ascii=False))
                                    #     start_time = time.time()

                                    #     end_time = time.time()
                                    #     execution_time = (end_time - start_time) * 1000  # 밀리세컨드 단위로 변환
                                    #     logging.info("#1 DATA 인/디코드 실행 시간: {0}ms".format(execution_time))
                                        
                                    #     if data_message == "":
                                    #         json_message_edge["data"] = ""
                                    #     else:
                                    #         json_message_edge["data"] = json.loads(str(data_message), strict=True)
                                        
                                    #     send_message = json.dumps(json_message_edge, ensure_ascii=False)
                                    #     sendAll(client_sockets_1, send_message)
                                    #     logging.info(f"######### 특장차 전송 완료 ######### {send_message}")
                                send_kafka_msg('connect', message=json_message)
                                
                        except json.decoder.JSONDecodeError as err:
                            logging.exception("json.decoder.JSONDecodeError {0}, [{1}]".format(err, data))
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
                logger.exception("==> 특장차 ConnectionResetError")
                break
            except KeyError as e:
                logger.exception("==> 특장차 KeyError")
            except KeyboardInterrupt as e:
                logger.exception("==> 특장차 KeyboardInterrupt")
                #break
            except OSError:
                logger.exception("==> 특장차 OSError")   
                break
            except UnicodeDecodeError:
                logger.exception("==> 특장차 UnicodeDecodeError")   
                buf = ''
            except json.decoder.JSONDecodeError:
                logger.exception("==> 특장차 json.decoder.JSONDecodeError")   
                buf = ''
                
    def finish(self):
        logger.info("finish")
        conn = self.request
        addr = self.client_address[0]
        client_sockets_1.remove((conn, addr))
        logging.info("현재 특장차 Client 접속수 : {0}".format(len(client_sockets_1)))

        return socketserver.BaseRequestHandler.finish(self)  
class ThreadedTCPRequestHandler(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

def start_kafka_consumer():
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("파티션 끝")
                else:
                    logging.error(f"Kafka 에러 발생: {msg.error()}")
                continue
            try: 
                msg_value = json.loads(msg.value().decode('utf-8'))
                sendAll(client_sockets_1, msg_value)
                logging.info(f"특장차 메시지 전송 완료: {msg_value}")
            except json.JSONDecodeError as e:
                logging.error(f"JSON 디코드 오류: {e}")
            except Exception as e:
                logging.error(f"카프카 메시지 처리 오류: {e}")
        except Exception as e:
            logging.error(f"카프카 메시지 처리 중 오류 발생: {e}")

if __name__ == "__main__":
    _config = configparser.ConfigParser()
    _config.read(CONFIG, encoding=file_encoding) # definition.py에 등록된 config.ini

    _config['APP']['CMD_FILE'] # CMD_FILE
    

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

    _kafka_broker = _config['KAFKA']['KAFKA_BROKER']
    _kafka_port = _config['KAFKA']['KAFKA_PORT']
    #_edgeid = _config['APP']['EDGEID'] 
    #_edgety = _config['APP']['EDGETY'] 
    
    full_path = os.path.join(ROOT_DIR, "logs", "nodecommsrv.log")
    create_rotating_log(full_path, _config['LOGGER'])
    
    HOST1, PORT1 = _host1, _port1
    
    KAFKA_BROKER = f'{_kafka_broker}:{_kafka_port}'

    producer_config = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(producer_config)

    consumer_config = {'bootstrap.servers': KAFKA_BROKER, 'group.id': random.randint(0, 100), 'auto.offset.reset': 'latest'}
    consumer = Consumer(consumer_config)
    consumer.subscribe(['special'])
    
    try:
        server1 = ThreadedTCPRequestHandler((socket.gethostbyname(HOST1), PORT1), MyTCPHandler1)
    except ValueError:
        sys.exit()
    except:
        sys.exit()
    print("Socket bound on {0}".format(PORT1))
    ip, port = server1.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread1 = threading.Thread(target=server1.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread1.daemon = True
    server_thread1.start()
    
    # 카프카 소비자 스레드 시작
    consumer_thread = threading.Thread(target=start_kafka_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    with open(_command_file_path, 'r', encoding='utf-8') as file:
        command_tbl = json.load(file)    

    
    try:
        server1.serve_forever()
    except KeyboardInterrupt:
        print("==> Main KeyboardInterrupt")
    except OSError:
        print("==> Main OSError")            
    server1.server_close()
    server1.shutdown()
    consumer.close()
