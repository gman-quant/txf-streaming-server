
import sys
from src import txf_data_pb2

try:
    tick = txf_data_pb2.Tick()
    tick.code = None
except Exception as e:
    print(f"Protobuf Error: {e}")

try:
    s = None
    s.encode('utf-8')
except Exception as e:
    print(f"Encode Error: {e}")
