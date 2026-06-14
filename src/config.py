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