# src/config.py

import os

from dotenv import load_dotenv

# Load Environment Variables 
load_dotenv()

# Shioaji API Credentials 
SHIOAJI_API_KEY = os.environ.get("SHIOAJI_API_KEY")
SHIOAJI_SECRET_KEY = os.environ.get("SHIOAJI_SECRET_KEY")

# Kafka Config 
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
TICK_TOPIC = os.environ.get("TICK_TOPIC")
TICK_R2_TOPIC = os.environ.get("TICK_R2_TOPIC")
BIDASK_TOPIC = os.environ.get("BIDASK_TOPIC")

# 完整行情 raw 總線(JSON,2026-07-26 新增)。
# 為什麼有預設值而其他 topic 沒有:TICK_R2_TOPIC 曾因「.env 忘了加 + 無預設」
# 造成每筆 R2 tick 靜默發送失敗。這裡給預設 + producer 啟動時斷言,兩道防線。
MD_RAW_TOPIC = os.environ.get("MD_RAW_TOPIC", "txf-md-raw")