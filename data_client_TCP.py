import socket
import json
import time
from statistics import mean
import psycopg2
import paho.mqtt.client as mqtt
from datetime import datetime

def receive_data():
    host = '192.168.56.1'
    port = 17799

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host, port))
            print(f"Connected to {host}:{port}")
        except ConnectionRefusedError as e:
            print(f"Connection refused: {e}")
            return
        except Exception as e:
            print(f"Failed to connect: {e}")
            return

        buffer = ''
        while True:
            try:
                data = s.recv(1024).decode('utf-8')
                if not data:
                    break

                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    try:
                        data_json = json.loads(line)
                        print(f"Received data: {data_json}")  # 로그 추가
                        yield data_json
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error: {e}")
                        continue
            except Exception as e:
                print(f"Error receiving data: {e}")
                break


def save_to_db(data):
    try:
        conn = psycopg2.connect(
            host="192.168.56.1",
            database="postgres",
            user="postgres",
            password="1234"
        )
        cur = conn.cursor()
        
        # 현재 시간으로 datetime 객체 생성
        timestamp = datetime.now()
        
        query = """
            INSERT INTO vehicle_time_data (timestamp, value)
            VALUES (%s, %s)
            """
        cur.execute(query, (timestamp, data['value']))  # 데이터 포맷 확인
        conn.commit()
        cur.close()
        conn.close()
        print("Data saved to database successfully.")
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error saving to database: {e}")

def send_to_cloud(data):
    try:
        broker_address = "223.130.131.234"
        port = 31883
        topic = "special_vehicle/data"

        client = mqtt.Client("EdgeNode")
        client.connect(broker_address, port)
        client.publish(topic, json.dumps(data))
        client.disconnect()
        print("Data sent to cloud successfully.")
    except Exception as e:
        print(f"MQTT error: {e}")

def calculate_average(data_points):
    if data_points:
        avg_value = mean(data_points)
        return {"timestamp": time.time(), "average_value": avg_value}
    return {}

def process_data():
    data_points = []
    start_time = time.time()
    for data in receive_data():
        if data is None:
            continue
        data_points.append(data['value'])
        save_to_db(data)
        if time.time() - start_time >= 1:
            avg_data = calculate_average(data_points)
            if avg_data:
                send_to_cloud(avg_data)
            data_points = []
            start_time = time.time()

if __name__ == "__main__":
    process_data()
