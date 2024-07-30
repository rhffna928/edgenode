import socket
import json
import time
import random

def generate_data():
    value = 50.0  # 시작값
    while True:
        # 랜덤값 생성
        delta = random.uniform(-0.5, 0.5)
        value += delta
        yield value

def send_data():
    port = 17799  # 엣지노드의 포트 번호

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', port))
        s.listen()
        print(f"Listening on port {port}")
        conn, addr = s.accept()
        with conn:
            print(f"Connected by {addr}")
            data_generator = generate_data()
            last_send_time = time.time()  # 마지막 데이터 전송 시간
            try:
                while True:
                    # 데이터 생성
                    data = next(data_generator)

                    # JSON 형식으로 데이터 변환
                    data_json = json.dumps({"value": data})

                    # 데이터 전송
                    conn.sendall((data_json + '\n').encode('utf-8'))  # 줄 바꿈 문자 추가

                    # 1/1000초 대기 (1밀리초)
                    time.sleep(0.001)
                    
                    # 3초마다 로그 출력
                    current_time = time.time()
                    if current_time - last_send_time >= 3:
                        print("sending")  # 3초마다 로그 출력
                        last_send_time = current_time

            except Exception as e:
                print(f"Error: {e}")
            finally:
                conn.close()
                print("Connection closed")

if __name__ == "__main__":
    send_data()
