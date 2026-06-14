import socket
import time
import statistics

SERVER_IP = '192.168.1.50'
PORT = 9999
TEST_ROUNDS = 10000  # 連續發射一萬發測極限
PAYLOAD = b'X' * 64  # 模擬一個小型 Tick 封包的大小 (64 Bytes)

def run_client():
    latencies_us = []
    
    print(f"連線至 {SERVER_IP}:{PORT} ...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) # HFT 標配：關閉 Nagle
        s.connect((SERVER_IP, PORT))
        print("✅ 連線成功，開始進行 1 萬次微秒級桌球壓測 (Ping-Pong Test)...")
        
        # 暖機 (讓作業系統與網卡進入高頻運作狀態)
        for _ in range(100):
            s.sendall(PAYLOAD)
            s.recv(64)
            
        # 正式測量
        for _ in range(TEST_ROUNDS):
            t1 = time.perf_counter_ns()
            s.sendall(PAYLOAD)
            s.recv(64)
            t2 = time.perf_counter_ns()
            
            # 換算成微秒 (Microseconds)
            rtt_us = (t2 - t1) / 1000.0
            latencies_us.append(rtt_us)

    # 統計報告
    avg_rtt = statistics.mean(latencies_us)
    min_rtt = min(latencies_us)
    max_rtt = max(latencies_us)
    
    # 算 P99 需要排序
    latencies_us.sort()
    p99_idx = int(len(latencies_us) * 0.99)
    p99_rtt = latencies_us[p99_idx]

    print("\n" + "="*50)
    print("🏆 實體網路 RTT (Round-Trip Time) 極限報告")
    print("="*50)
    print(f" 🎯 測試次數 : {TEST_ROUNDS:,} 次")
    print(f" ⚡ 單向網路延遲推估 (RTT/2) : {(avg_rtt/2):.2f} μs")
    print("-" * 50)
    print(f" 📊 平均 RTT (Avg) : {avg_rtt:.2f} μs")
    print(f" 📉 最佳 RTT (Min) : {min_rtt:.2f} μs")
    print(f" 📈 最差 RTT (Max) : {max_rtt:.2f} μs (抖動峰值)")
    print(f" 🛡️ 99% 信心水準 (P99) : {p99_rtt:.2f} μs")
    print("="*50)

if __name__ == "__main__":
    run_client()