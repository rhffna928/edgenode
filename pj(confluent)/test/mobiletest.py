import socket
import json
import time
import jpype
import jpype.imports
from jpype.types import *
from datetime import datetime
import socketserver
import threading

# JVM 시작
jpype.startJVM()
jpype.addClassPath("../watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

# 설정
HOST = '0.0.0.0'  # 모든 IP에서 접속 가능
PORT = 31900      # 서버 포트
RECV_ENCODING = 'CP949'

class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            # 클라이언트 정보 출력
            client_address = self.client_address[0]
            print(f"📥 새로운 클라이언트 연결: {client_address}")
            
            while True:
                # 데이터 수신
                data = self.request.recv(1024).decode(RECV_ENCODING)
                if not data:
                    break

                try:
                    # JSON 파싱
                    res = json.loads(data)
                    now = datetime.now()
                    now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")

                    _cmd = str(res["header"]["cmd"])
                    _actn = str(res["header"]["actn"])
                    _dtlActn = str(res["header"]["dtlActn"])
                    _strtpnt = str(res["header"]["strtpnt"])
                    _dstn = str(res["header"]["dstn"])

                    _userId = str(res["header"]["userId"])
                    _edgeId = str(res["header"]["edgeId"])
                    _edgeTy = str(res["header"]["edgeTy"])
                    _timestamp = str(res["header"]["sndngDt"])


                    print(f"📥 수신된 데이터: {res}")

                    # 데이터 디코딩
                    if res["data"] == "":
                        decoded_data = res
                    else:
                        decoded_data = jvm_msg_encrypt_class.decode(now_str, _edgeId, res["data"])

                    print(res['data'])
                    # 파싱된 데이터 출력
                    print(f"🔍 파싱된 데이터:")
                    print(f"  - data: {decoded_data}")

                except json.JSONDecodeError as e:
                    print(f"❌ JSON 파싱 실패: {e}")
                except Exception as e:
                    print(f"❌ 데이터 처리 중 에러 발생: {str(e)}")
                    print(f"  - 에러 타입: {type(e)}")
                    print(f"  - 디코딩된 데이터: {decoded_data}")

        except Exception as e:
            print(f"❌ 핸들러 에러 발생: {str(e)}")
        finally:
            print(f"👋 클라이언트 연결 종료: {client_address}")
            self.request.close()

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

def main():
    try:
        # 서버 시작
        server = ThreadedTCPServer((HOST, PORT), MyTCPHandler)
        print(f"✅ 서버 시작: {HOST}:{PORT}")
        
        # 서버 실행
        server.serve_forever()
        
    except KeyboardInterrupt:
        print("\n👋 서버 종료")
    except Exception as e:
        print(f"❌ 서버 에러 발생: {str(e)}")
    finally:
        server.server_close()

if __name__ == "__main__":
    main()