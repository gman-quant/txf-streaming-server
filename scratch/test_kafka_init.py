
try:
    from confluent_kafka import Producer
    p = Producer({'bootstrap.servers': None})
    print("Producer Init Success")
except Exception as e:
    print(f"Producer Init Error: {e}")
