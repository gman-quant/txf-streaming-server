"""
TXF Streaming Consumer (Kafka -> Console Inspector)
---------------------------------------------------
Description: 
    從 Kafka 接收 TXF Tick 與 BidAsk 數據 (Protobuf 格式)。
    採用 Batch Consume 模式，計算並拆解延遲 (Latency) 為：
    1. 外部延遲 (網路/Queue Lag)
    2. 內部延遲 (Python 解析與處理開銷)
    
Architecture:
    - Deserialization: Google Protobuf
    - Transport: confluent-kafka (Batch Consume)
    - Precision: Nanosecond (ns) based time calculation

Author: Garrett & Gemini
Last Updated: 2025-11-28
"""

import sys
import time
from datetime import datetime

# --- Third-party Imports ---
from confluent_kafka import Consumer, KafkaError

# --- Local Imports ---
from .. import txf_data_pb2
from ..config import (
    KAFKA_BOOTSTRAP_SERVERS, 
    TICK_TOPIC, 
    BIDASK_TOPIC
)

# ==========================================
# Global Constants
# ==========================================

SCALE = 10000.0
GROUP_ID = 'txf-console-inspector'

# ==========================================
# Helper Functions
# ==========================================

def to_decimal(scaled_int: int) -> float:
    """將 int64 (x10000) 還原回 float。"""
    return scaled_int / SCALE

def format_list(data_list, is_price=False) -> str:
    """將列表格式化為易讀字串 (顯示前3檔)。"""
    if not data_list: return "[]"
    if is_price:
        items = [str(to_decimal(x)) for x in data_list[:3]]
        return f"[{', '.join(items)} ...]"
    items = [str(x) for x in data_list[:3]]
    return f"[{', '.join(items)} ...]"

# ------------------------------------------------------------
# 📦 打印函數 (格式化輸出)
# ------------------------------------------------------------
def print_event_summary(topic, data, transport_latency_ms, internal_latency_ms, msg_count):
    """
    根據 topic 類型打印格式化的 Log。
    - transport_latency: 網路/Queue 延遲
    - internal_latency: Python 處理延遲
    """
    
    total_latency = transport_latency_ms + internal_latency_ms
    
    if topic == TICK_TOPIC:
        event_dt = datetime.fromtimestamp(data.timestamp_ms / 1000.0).strftime('%H:%M:%S.%f')[:-3]
        type_str = {1: "外盤", 2: "內盤"}.get(data.tick_type, "未知")

        print(f"\n⚡ [TICK] {data.code} @ {event_dt} (總延遲 {total_latency:.1f}ms | 批次: {msg_count})")
        print(f"   ├─ 外部傳輸: {transport_latency_ms:.1f}ms (Queue Lag)")
        print(f"   ├─ 內部處理: {internal_latency_ms:.3f}ms (解析開銷)") 
        print(f"   ├─ 成交價: {to_decimal(data.close):.0f} | 單量: {data.volume}")
        print(f"   └─ 內外盤: {data.tick_type} ({type_str})")

    elif topic == BIDASK_TOPIC:
        event_dt = datetime.fromtimestamp(data.timestamp_ms / 1000.0).strftime('%H:%M:%S.%f')[:-3]
        bid1 = to_decimal(data.bid_price[0]) if data.bid_price else 0
        ask1 = to_decimal(data.ask_price[0]) if data.ask_price else 0
        
        print(f"\n📊 [BID/ASK] {data.code} @ {event_dt} (總延遲 {total_latency:.1f}ms | 批次: {msg_count})")
        print(f"   ├─ 外部傳輸: {transport_latency_ms:.1f}ms (Queue Lag)")
        print(f"   ├─ 內部處理: {internal_latency_ms:.3f}ms (解析開銷)") 
        print(f"   └─ 買一: {bid1:.0f} | 賣一: {ask1:.0f}")

# ==========================================
# Main Execution
# ==========================================

def main():    
    # --- 1. 初始化 Consumer ---
    print("🔧 初始化 Kafka Consumer (時間延遲分析模式)...")
    
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([TICK_TOPIC, BIDASK_TOPIC])
        
        print(f"🚀 監控啟動 | Group: {GROUP_ID}")
        print(f"📡 監聽 Topics: {TICK_TOPIC}, {BIDASK_TOPIC}")
        print( "⏳ 等待資料中... (按 Ctrl+C 停止)")
        print( "-" * 60)
        
    except Exception as e:
        print(f"❌ Consumer 初始化失敗: {e}")
        sys.exit(1)

    # --- 2. 進入監控迴圈 ---
    try:
        # [計時變數]
        deadline = time.time() + 1.0 # 控制每秒輸出一次 (限流)

        while True:
            # (A) 重置狀態 (每輪開始時，假設沒有 Tick/BA)
            latest_tick_msg = None
            latest_ba_msg = None

            # (B) 批次抓取 (0.1s timeout，確保 Consumer 不會空轉)
            msgs = consumer.consume(num_messages=500, timeout=0.1)
            
            # 【關鍵點 1: T_Arrival】 Python 拿到這批資料的時間點
            # 這是延遲計算的第二個基準點 (資料抵達應用程式的時間)
            arrival_time_ns = time.time_ns()
            
            if not msgs: 
                continue
            
            # --- 3. 遍歷批次 (In-Batch Tracking) ---
            valid_msgs = [m for m in msgs if not m.error()]
            valid_msgs_count = len(valid_msgs)
            if valid_msgs_count == 0: continue
            
            for msg in valid_msgs:
                topic = msg.topic()
                # 追蹤每個 Topic 最後收到的那條訊息 (最新)
                if topic == TICK_TOPIC:
                    latest_tick_msg = msg
                elif topic == BIDASK_TOPIC:
                    latest_ba_msg = msg

            # --- 4. 定時打印邏輯 (每秒一次) ---
            if time.time() >= deadline:
                
                # (!! 關鍵：解析與計算必須在 if 裡面，避免無意義的 CPU 運算 !!)
                
                # 處理 TICK
                if latest_tick_msg:
                    tick_data = txf_data_pb2.Tick()
                    
                    # [T_Ready Start] 計算解析耗時的起點
                    start_parse_ns = time.time_ns()
                    tick_data.ParseFromString(latest_tick_msg.value())
                    
                    # [T_Ready End] 計算解析耗時的終點
                    parse_done_ns = time.time_ns()
                    
                    # 計算延遲 (單位換算為 ms)
                    # 外部傳輸 = (T_Arrival) - (T_Event)
                    transport_latency = (arrival_time_ns / 1_000_000) - tick_data.timestamp_ms
                    
                    # 內部處理 = (T_Ready) - (T_Arrival)
                    internal_latency = (parse_done_ns - start_parse_ns) / 1_000_000
                    
                    print_event_summary(TICK_TOPIC, tick_data, transport_latency, internal_latency, valid_msgs_count)

                # 處理 BIDASK
                if latest_ba_msg:
                    ba_data = txf_data_pb2.BidAsk()
                    
                    # [T_Ready Start]
                    start_parse_ns = time.time_ns()
                    ba_data.ParseFromString(latest_ba_msg.value())
                    
                    # [T_Ready End]
                    parse_done_ns = time.time_ns()
                    
                    transport_latency = (arrival_time_ns / 1_000_000) - ba_data.timestamp_ms
                    internal_latency = (parse_done_ns - start_parse_ns) / 1_000_000
                    
                    print_event_summary(BIDASK_TOPIC, ba_data, transport_latency, internal_latency, valid_msgs_count)
                
                print("-" * 40)
                deadline = time.time() + 1.0 # (重設下一次的打印時間)

    except KeyboardInterrupt:
        print("\n👋 收到停止訊號，監控結束。")
        
    finally:
        print("⏳ 正在關閉 Consumer...")
        consumer.close()
        print("✅ Consumer 已關閉。")

if __name__ == '__main__':
    main()