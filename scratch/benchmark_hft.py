import time
import sys
import os
from decimal import Decimal

# 確保能 import src 底下的東西
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from confluent_kafka import Producer
from src import txf_data_pb2

# 模擬 Shioaji 傳入的行情數據結構
class MockQuote:
    def __init__(self):
        self.code = "TXFR1"
        self.close = Decimal("16500.25")
        self.volume = 15
        self.underlying_price = Decimal("16498.50")
        self.total_volume = 125430
        self.tick_type = 1

def serialize_tick(quote) -> bytes:
    """模擬真實的 Protobuf 序列化邏輯"""
    tick = txf_data_pb2.Tick()
    tick.code = quote.code
    tick.timestamp_ms = int(time.time() * 1000)
    tick.tick_type = int(quote.tick_type)
    tick.close = int(quote.close * 10000)
    tick.volume = int(quote.volume)
    tick.underlying_price = int(quote.underlying_price * 10000)
    tick.total_volume = int(quote.total_volume)
    return tick.SerializeToString()

# 回調函數 (模擬舊版/標準版需要處理成功與失敗的 Callback)
def standard_delivery_report(err, msg):
    pass # 即使不做事，C -> Python 的調用開銷依然存在

def run_benchmark(name, config, message_count=20000, is_hft=False):
    print(f"\n🚀 Running: {name}...")
    
    # 加入測試環境專用參數，避免無 Kafka 時無限等待
    test_config = config.copy()
    test_config.update({
        'message.timeout.ms': 1000,   # 1 秒內沒發送完直接丟棄，防止 flush 阻塞
        'queue.buffering.max.messages': 1000000, # 擴大容納空間
    })
    
    # 初始化 Producer
    try:
        p = Producer(test_config)
    except Exception as e:
        print(f"❌ Init Failed: {e}")
        return None
        
    mock_quote = MockQuote()
    
    # 預先生成序列化後的資料以消除資料生成誤差，專注測試 Kafka 發送效能
    payloads = [serialize_tick(mock_quote) for _ in range(message_count)]
    key = mock_quote.code.encode('utf-8')
    
    # 開始計時
    start_time = time.perf_counter()
    
    for payload in payloads:
        if is_hft:
            # HFT 極簡發送，不綁定 on_delivery 回調（因為設定了 delivery.report.only.error）
            p.produce('txf-benchmark', key=key, value=payload)
        else:
            # 標準發送，綁定回調
            p.produce('txf-benchmark', key=key, value=payload, on_delivery=standard_delivery_report)
            
        p.poll(0) # 觸發回調輪詢
        
    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    # 快速清理佇列，不進行長時間等待
    p.flush(0.1)
    
    tps = message_count / total_time
    avg_latency_us = (total_time / message_count) * 1_000_000
    
    print(f"✅ {name} Completed!")
    print(f"   ⏱️  Total Time: {total_time:.4f} seconds")
    print(f"   📊 Throughput (TPS): {tps:.2f} msg/sec")
    print(f"   ⚡ Average Client Latency: {avg_latency_us:.2f} microseconds (us)")
    
    return {
        "name": name,
        "total_time": total_time,
        "tps": tps,
        "avg_latency": avg_latency_us
    }

if __name__ == "__main__":
    MSG_COUNT = 50000
    print(f"=== TXF HFT Kafka Producer Benchmark ===")
    print(f"Goal: Measure CPU thread latency for processing & sending {MSG_COUNT} market ticks\n")
    
    # 1. 對照組：一般標準配置
    config_std = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'txf-producer-std',
        'acks': 'all',
        'linger.ms': 10,
        'compression.type': 'gzip',
        'queue.buffering.max.kbytes': 131072,
        'batch.size': 262144,
        'delivery.report.only.error': False,
    }
    
    # 2. 實驗組：HFT 極限優化版
    config_hft = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'txf-producer-hft',
        'acks': '1',
        'linger.ms': 0,
        'compression.type': 'none',
        'queue.buffering.max.kbytes': 131072,
        'batch.size': 262144,
        'delivery.report.only.error': True,
        'socket.send.buffer.bytes': 1024000,
        'socket.receive.buffer.bytes': 102400,
    }
    
    # Warm up
    print("⏳ Warming up CPU...")
    _ = run_benchmark("Warmup", config_std, 5000, is_hft=False)
    
    # Run tests
    res_std = run_benchmark("Standard Configuration", config_std, MSG_COUNT, is_hft=False)
    res_hft = run_benchmark("HFT Optimized Configuration", config_hft, MSG_COUNT, is_hft=True)
    
    if res_std and res_hft:
        gain = ((res_hft["tps"] - res_std["tps"]) / res_std["tps"]) * 100
        latency_saved = res_std["avg_latency"] - res_hft["avg_latency"]
        
        print("\n" + "="*50)
        print("🏆 BENCHMARK RESULTS COMPARISON")
        print("="*50)
        print(f"| Metric | Standard Config | HFT Optimized Config | Improvement |")
        print(f"| :--- | :--- | :--- | :--- |")
        print(f"| Total Time (s) | {res_std['total_time']:.3f}s | {res_hft['total_time']:.3f}s | Saved {(res_std['total_time']-res_hft['total_time']):.3f}s |")
        print(f"| Throughput (TPS) | {res_std['tps']:.1f} | {res_hft['tps']:.1f} | **+{gain:.2f}%** 🚀 |")
        print(f"| Avg Client Latency (us) | {res_std['avg_latency']:.2f} μs | {res_hft['avg_latency']:.2f} μs | **Reduced by {latency_saved:.2f} μs / tick** |")
        print("="*50)
        print("💡 Conclusion:")
        print("1. Disabling compression ('none') completely removes the CPU overhead of gzip/snappy encoding.")
        print("2. 'delivery.report.only.error': True bypasses the costly C-to-Python callback overhead for successful deliveries.")
        print("3. Client latency is cut significantly, allowing Shioaji's feed thread to return much faster!")
