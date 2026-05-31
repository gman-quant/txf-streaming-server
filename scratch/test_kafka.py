
import sys
try:
    from confluent_kafka import Producer
    p = Producer({'bootstrap.servers': 'localhost'})
    p.produce('test', value='val', key=None)
except Exception as e:
    print(f"Kafka Error: {e}")
