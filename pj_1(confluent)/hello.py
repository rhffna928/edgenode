import paho.mqtt.client as mqtt

broker_address = "223.130.131.234"     # 브로커 IP 또는 호스트네임
broker_port = 31883               # MQTT 기본 포트 (필요시 변경)
topic = "edgeplatform/node/STSW000001"

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(broker_address, broker_port)

client.publish(topic, "hello")

client.disconnect()
    