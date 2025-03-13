from confluent_kafka import Consumer
import json

group_id = "consumer_group_3"

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': group_id,
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['shared_topic'])

print(f"🟢 Consumer 3 ({group_id}) 시작!")

while True:
    msg = consumer.poll(1.0)
    if msg and msg.value():
        data = json.loads(msg.value().decode('utf-8'))
        if data["header"]["actn"] == "globalpath":
            print(f"✅ [Consumer 3] globalpath: {data}")
        
