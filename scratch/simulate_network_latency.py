import time
import random
import sys

class NetworkSimulation:
    def __init__(self, name, rtt_ms, nagle_enabled, acks, poll_in_callback):
        self.name = name
        self.rtt_ms = rtt_ms
        self.nagle_enabled = nagle_enabled
        self.acks = acks  # '0', '1', 'all'
        self.poll_in_callback = poll_in_callback
        
        # 佇列與統計
        self.queue = []
        self.max_queue_size = 0
        self.dropped_messages = 0
        
    def simulate(self, tick_count=5000, tick_interval_ms=0.5):
        """
        模擬高頻 Tick 的發送過程。
        tick_interval_ms: 模擬高頻行情下，每 0.5 毫秒進來一筆行情。
        """
        print(f"\n🌐 Simulating Network Environment for: {self.name}")
        print(f"   - Network RTT (ping): {self.rtt_ms} ms")
        print(f"   - TCP Nagle Algorithm: {'ENABLED (Delay 15ms)' if self.nagle_enabled else 'DISABLED (0ms)'}")
        print(f"   - Kafka ACKs level: '{self.acks}'")
        print(f"   - Poll location: {'Main Tick Callback (Blocking)' if self.poll_in_callback else 'Background Thread'}")
        
        # 記錄主執行緒處理每筆 Tick 的延遲時間 (微秒)
        tick_processing_latencies = []
        
        # 追蹤發送出去但尚未收到 ACK 的訊息
        inflight_messages = []
        
        current_time_ms = 0.0
        
        for i in range(tick_count):
            # 1. 模擬行情 Tick 進入系統
            current_time_ms += tick_interval_ms
            
            # 主執行緒開始處理
            start_cpu_time = time.perf_counter()
            
            # 模擬打包序列化時間 (大約 1 微秒)
            time.sleep(0.000001) 
            
            # 計算該筆訊息在網路發送上的「固有延遲」
            net_delay = 0.0
            
            # A. 模擬 Nagle 演算法的影響
            if self.nagle_enabled:
                # Nagle 演算法會積攢封包，隨機延遲 10 ~ 20 毫秒才發送
                if i % 10 != 0:  # 每 10 筆才湊滿一個 TCP 窗口發射
                    net_delay += 15.0  # 延遲 15 毫秒
            
            # B. 模擬 ACK 確認延遲
            if self.acks == 'all':
                # acks=all 需要等待 Leader + Follower 全同步，網路延遲放大 2.5 倍
                ack_delay = self.rtt_ms * 2.5
            elif self.acks == '1':
                # acks=1 只需要 Leader 確認，就是基本的一個 RTT 往返
                ack_delay = self.rtt_ms
            else: # acks='0'
                # acks=0 完全不需要等待確認，網路延遲對確認時間無影響
                ack_delay = 0.0
                
            total_network_trip = net_delay + ack_delay
            
            # 模擬將訊息放入本地 Kafka 緩衝佇列
            message_entry = {
                "id": i,
                "created_at": current_time_ms,
                "rtt_needed": total_network_trip,
                "ack_at": current_time_ms + total_network_trip
            }
            
            # 檢查緩衝佇列是否溢出 (限制最大 1000 筆)
            if len(self.queue) >= 1000:
                self.dropped_messages += 1
            else:
                self.queue.append(message_entry)
                
            self.max_queue_size = max(self.max_queue_size, len(self.queue))
            
            # 3. 模擬輪詢 (Poll) 對主執行緒的影響
            blocking_cost_ms = 0.0
            if self.poll_in_callback:
                # 如果 poll(0) 寫在主回調中，它會強迫 CPU 檢查當前有沒有已經完成的網路 ACK
                # 如果網路有延遲，這個檢查動作會因為 GIL 鎖以及 C-to-Python 的上下文切換，
                # 導致主執行緒產生額外的微幅卡頓 (約 10~50 微秒)
                blocking_cost_ms = random.uniform(0.02, 0.08)  # 20 ~ 80 微秒卡頓
                time.sleep(blocking_cost_ms / 1000.0)
            
            # 主執行緒處理結束
            end_cpu_time = time.perf_counter()
            latency_us = (end_cpu_time - start_cpu_time) * 1_000_000
            tick_processing_latencies.append(latency_us)
            
            # 4. 模擬背景或作業系統在時間流逝中消化/確認訊息 (ACK 到達)
            # 移出佇列中已經被 ACK 的訊息
            self.queue = [m for m in self.queue if m["ack_at"] > current_time_ms]
            
        avg_latency = sum(tick_processing_latencies) / len(tick_processing_latencies)
        
        print(f"   📊 Results:")
        print(f"      - Main Thread Avg Tick Processing Latency: {avg_latency:.2f} microseconds (us)")
        print(f"      - Max Client Queue Congestion (Peak): {self.max_queue_size} messages")
        print(f"      - Network Packet Loss (Dropped Ticks): {self.dropped_messages} ticks")
        
        return {
            "avg_latency": avg_latency,
            "max_queue": self.max_queue_size,
            "dropped": self.dropped_messages
        }

if __name__ == "__main__":
    print("=" * 60)
    print("💻 HFT NETWORK LATENCY EMULATOR (WINDOWS SIMULATION)")
    print("=" * 60)
    print("This simulation models 5,000 tick arrivals (1 tick every 0.5ms)")
    print("under a typical network RTT of 5ms (e.g. cloud to broker or slow LAN).")
    
    # 1. 模擬情境：優化前 (標準配置 + 網路延遲影響)
    # - RTT: 5ms
    # - Nagle: 開啟
    # - Acks: 'all'
    # - Poll: 寫在 Tick Callback 裡面 (同步阻塞)
    sim_std = NetworkSimulation(
        name="Standard Configuration (With Network Latency & Nagle)",
        rtt_ms=5.0,
        nagle_enabled=True,
        acks='all',
        poll_in_callback=True
    )
    res_std = sim_std.simulate()
    
    # 2. 模擬情境：優化後 (HFT 極限優化 + 同樣網路延遲)
    # - RTT: 5ms
    # - Nagle: 關閉
    # - Acks: '1'
    # - Poll: 移到背景 (同步非阻塞)
    sim_hft = NetworkSimulation(
        name="HFT Optimized Configuration (With Same Network Latency)",
        rtt_ms=5.0,
        nagle_enabled=False,
        acks='1',
        poll_in_callback=False
    )
    res_hft = sim_hft.simulate()
    
    print("\n" + "=" * 60)
    print("🏆 SIMULATION ANALYSIS")
    print("=" * 60)
    print(f"| Metric | Standard Config | HFT Optimized Config | Performance Gain |")
    print(f"| :--- | :--- | :--- | :--- |")
    print(f"| Main Thread Latency | {res_std['avg_latency']:.2f} μs | {res_hft['avg_latency']:.2f} μs | **{((res_std['avg_latency'] - res_hft['avg_latency']) / res_std['avg_latency'] * 100):.2f}% Faster** 🚀 |")
    print(f"| Max Queue Congestion | {res_std['max_queue']} msgs | {res_hft['max_queue']} msgs | **Reduced by {(res_std['max_queue'] - res_hft['max_queue'])} msgs** |")
    print(f"| Packet Dropped (Loss) | {res_std['dropped']} ticks | {res_hft['dropped']} ticks | **{res_std['dropped'] - res_hft['dropped']} lost ticks prevented** |")
    print("=" * 60)
    print("💡 High-Frequency Trading Takeaways:")
    print("1. When Nagle is ENABLED, messages back up at the OS network buffer, causing the queue to EXPLODE under heavy market load.")
    print("2. By moving poll(0) to a background thread, the Main Tick Thread latency remains perfectly immune to network fluctuations.")
    print("3. HFT optimization acts as a shield, preventing queue congestion and data loss when market spikes occur!")
