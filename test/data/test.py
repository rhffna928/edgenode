import socket
import json


HOST = '0.0.0.0'
PORT = 5000
LOG_FILE = 'trac_data.log'

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print(f"Listening on {HOST}:{PORT}...")

        while True:
            conn, addr = server_socket.accept()
            print(f"Connected by {addr}")
            with conn:
                buf = ""
                while True:
                    data = conn.recv(4096).decode(encoding='CP949')
                    if not data:
                        break
                    buf += data
                    index = buf.find("\n")
                    if index == -1:
                        continue
                    else:
                    
                        data = buf[0:index +1]
                        
                        print(data)
                        buf = buf[index + 1:]
                    
                        try:
                            res = json.loads(str(data))
                            with open(LOG_FILE, 'a') as f:
                                f.write(str(res))
                                f.write("\n")
                            #buf = ""  # 초기화
                        except UnicodeDecodeError:
                            # 데이터가 아직 완전히 안 들어온 경우 (부분 전송)
                            continue
                        except json.decoder.JSONDecodeError as err:
                            #print("json.decoder.JSONDecodeError {0}, [{1}]".format(err, data))
                            continue

if __name__ == "__main__":
    start_server()
