
import sys
try:
    from confluent_kafka import Producer
    p = Producer({'bootstrap.servers': 'localhost'})
    p.produce(None, value='val')
except Exception as e:
    print(f"Kafka Topic Error: {e}")
