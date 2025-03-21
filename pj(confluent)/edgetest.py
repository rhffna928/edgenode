import socket
import json
import time
import jpype
import jpype.imports
from jpype.types import *
from datetime import datetime

# JVM 시작
jpype.startJVM()
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")
# 설정
HOST = '172.30.1.20'  # 서버 IP
PORT = 5000          # 서버 포트
RECV_ENCODING = 'CP949'

def receive_data(conn):
    try:
        # 데이터 수신
        data = json.loads(conn.recv(1024).decode(RECV_ENCODING))
        if not data:
            return None
        
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"📥 수신된 데이터: {data}")
        if data["data"] == "":
            data["data"] = ""
        else:
            data = jvm_msg_encrypt_class.decode(now_str,data["data"])
        # JSON 파싱
        try:
            print(f"🔍 파싱된 데이터:")
            print(f"  - command: {data.get('command')}")
            print(f"  - VID: {data.get('VID')}")
            print(f"  - timestamp: {data.get('timestamp')}")
            print(f"  - data: {data.get('data')}")
        except json.JSONDecodeError:
            print("❌ JSON 파싱 실패")
            
        return data
        
    except Exception as e:
        print(f"❌ 데이터 수신 중 에러 발생: {str(e)}")
        return None

def main():
    try:
        # 소켓 생성 및 연결
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((HOST, PORT))
        print(f"✅ 서버 연결 성공: {HOST}:{PORT}")
        
        while True:
            # 데이터 수신
            data = receive_data(conn)
            if data:
                print("✅ 데이터 수신 성공")
            else:
                print("❌ 데이터 수신 실패")
                break
                
            time.sleep(1)  # 1초 대기
            
    except KeyboardInterrupt:
        print("\n👋 프로그램 종료")
    except Exception as e:
        print(f"❌ 에러 발생: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()