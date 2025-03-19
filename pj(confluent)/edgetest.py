import socket
import json
import time

# 설정
HOST = '172.30.1.20'  # 서버 IP
PORT = 5000          # 서버 포트
RECV_ENCODING = 'CP949'

def receive_data(conn):
    try:
        # 데이터 수신
        data = conn.recv(1024).decode(RECV_ENCODING)
        if not data:
            return None
            
        # \r\n 제거
        data = data.strip('\r\n')
        
        print(f"📥 수신된 데이터: {data}")
        
        # JSON 파싱
        try:
            msg_data = json.loads(data)
            print(f"🔍 파싱된 데이터:")
            print(f"  - command: {msg_data.get('command')}")
            print(f"  - VID: {msg_data.get('VID')}")
            print(f"  - timestamp: {msg_data.get('timestamp')}")
            print(f"  - data: {msg_data.get('data')}")
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