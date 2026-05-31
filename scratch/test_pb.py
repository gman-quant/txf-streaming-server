
import sys
import os
sys.path.append(os.getcwd())
from src import txf_data_pb2

try:
    tick = txf_data_pb2.Tick()
    tick.code = None
except Exception as e:
    print(f"Protobuf Error: {e}")
