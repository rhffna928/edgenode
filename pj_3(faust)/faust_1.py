import faust
import logging

logging.basicConfig(level=logging.INFO)

app = faust.App(
    'myapp',
    broker='kafka://172.30.1.20:9092',
    broker_api_version='2.5.0',  # Kafka 버전 명시
    broker_request_timeout=40.0,  # 타임아웃 증가
    broker_session_timeout=30.0,
    value_serializer='json',
)

consumer_topic = app.topic('rep')

# 메모리에서 카운트 관리
message_count = 0

@app.agent(consumer_topic)
async def process(messages):
    global message_count
    async for message in messages:
        # 메시지를 받을 때마다 카운트 증가
        message_count += 1
        logging.info(f'Received message: {message}')
        logging.info(f'Total messages received: {message_count}')

if __name__ == '__main__':
    app.main()
