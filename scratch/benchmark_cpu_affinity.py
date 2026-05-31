import time
import os
import sys
import threading

# 嘗試載入 psutil 用於跨平台設定 CPU 親和性，若無則在 Linux 上採用原生 os.sched_setaffinity
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

def set_cpu_affinity(core_ids):
    """將當前進程鎖定在指定的 CPU 核心清單中"""
    if HAS_PSUTIL:
        p = psutil.Process()
        p.cpu_affinity(core_ids)
    elif hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, set(core_ids))
    else:
        raise NotImplementedError("此作業系統目前不支援設定 CPU 親和性 (請安裝 psutil 庫)")

def clear_cpu_affinity(total_cores):
    """還原進程親和性，允許在所有核心上運行"""
    if HAS_PSUTIL:
        p = psutil.Process()
        p.cpu_affinity([])  # 空清單代表還原為所有可用核心
    elif hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, set(range(total_cores)))

# 模擬高負載的 CPU 雜訊 (背景干擾執行緒)
def cpu_stress_worker(stop_event, stress_core_id):
    """在指定核心上持續進行運算，模擬繁忙的 CPU 負載"""
    try:
        set_cpu_affinity([stress_core_id])
    except Exception:
        pass  # 若綁定失敗則隨機運行
        
    while not stop_event.is_set():
        # 進行無意義的運算消耗該核心的 CPU 資源
        _ = 12345678.9 * 98765432.1

def run_latency_test(name, iterations=30000):
    """模擬 HFT 核心運算循環，並記錄延遲分布"""
    print(f"\n   ⏳ 啟動 [{name}] 測試...")
    latencies = []
    
    # 進行模擬行情 Tick 數據解析與處理
    for _ in range(iterations):
        t_start = time.perf_counter()
        
        # 模擬 HFT 行情解碼工作 (如 Protobuf 解析、Scale價格運算與矩陣乘法)
        total = 0.0
        for i in range(120):
            total += (i * 0.001) ** 2
            
        t_end = time.perf_counter()
        latencies.append((t_end - t_start) * 1_000_000) # 轉換為微秒 (μs)
        
    # 計算延遲百分位數 (HFT 最在意的指標)
    latencies.sort()
    avg_latency = sum(latencies) / len(latencies)
    p95_latency = latencies[int(len(latencies) * 0.95)]
    p99_latency = latencies[int(len(latencies) * 0.99)]  # 99% 的發送都在此時間內完成
    max_latency = latencies[-1]                         # 最慘的那一次延遲 (Max Jitter)
    
    print(f"      ✅ 完成！")
    print(f"      ⏱️  平均延遲 (Average): {avg_latency:.2f} μs")
    print(f"      ⚡ p95 延遲 (95th%): {p95_latency:.2f} μs")
    print(f"      🔥 p99 延遲 (Worst 1%): {p99_latency:.2f} μs")
    print(f"      💥 最大延遲抖動 (Max Jitter): {max_latency:.2f} μs")
    
    return {
        "avg": avg_latency,
        "p95": p95_latency,
        "p99": p99_latency,
        "max": max_latency
    }

if __name__ == "__main__":
    total_cores = os.cpu_count() or 4
    print("=" * 65)
    print("🖥️  HFT CPU AFFINITY BENCHMARK EMULATOR")
    print("=" * 65)
    print(f"系統偵測到核心數: {total_cores} 核")
    
    if not HAS_PSUTIL and not hasattr(os, "sched_setaffinity"):
        print("❌ 偵測不到 psutil 庫且非 Linux 系統，無法設定 CPU 親和性。")
        print("💡 請先在虛擬環境執行: .venv\\Scripts\\pip install psutil")
        sys.exit(1)
        
    if total_cores < 3:
        print("⚠️ 您的主機核心數少於 3 核，測試對比效果可能較不明顯，但仍會執行...")

    # 1. 啟動背景 CPU 干擾，模擬繁忙伺服器 (將噪聲鎖定在 核心 0 和 1)
    print("\n🔥 正在啟動背景 CPU 噪音執行緒...")
    print("   - 模擬系統其他常規工作 (如 Kafka 叢集、日誌寫入、核心網路中斷) 佔滿 Core 0 與 Core 1。")
    stop_event = threading.Event()
    stress_threads = []
    
    # 佔用核心 0 與 核心 1
    for core_to_stress in [0, 1]:
        t = threading.Thread(target=cpu_stress_worker, args=(stop_event, core_to_stress), daemon=True)
        t.start()
        stress_threads.append(t)
        
    time.sleep(1.0) # 等待 CPU 負載穩定
    
    try:
        # ==========================================
        # 測試 A: 未綁定 CPU (任由 OS 排程器動態分流)
        # ==========================================
        print("\n" + "-"*40)
        print("🧪 測試 A: 未綁定核心 (Floating Process)")
        print("   說明: 交易主執行緒未綁定，允許在 Core 0, 1, 2, 3 上隨機流動。")
        print("-"*40)
        clear_cpu_affinity(total_cores)
        res_float = run_latency_test("未綁定 (Floating)")
        
        # ==========================================
        # 測試 B: 綁定單核心 (獨佔 Core 2 - 完全安靜的隔離核心)
        # ==========================================
        print("\n" + "-"*40)
        print("🧪 測試 B: 綁定單一核心 (Pinned to Core 2)")
        print("   說明: 將交易主執行緒強制鎖定在 Core 2，避開繁忙的 Core 0 與 1。")
        print("-"*40)
        try:
            set_cpu_affinity([2])
            res_pinned = run_latency_test("鎖定 Core 2 (Pinned)")
        except Exception as e:
            print(f"❌ 鎖定 Core 2 失敗 (可能核心不足或權限限制): {e}")
            res_pinned = None

        # ==========================================
        # 統計分析結果
        # ==========================================
        if res_pinned:
            print("\n" + "=" * 65)
            print("🏆 CPU 鎖定核心效能分析報告 (A/B Test)")
            print("=" * 65)
            print("| 監控指標 (Metric) | 未綁定核心 (Floating) | 鎖定獨佔核心 2 (Pinned) | 效能優化幅度 |")
            print("| :--- | :--- | :--- | :--- |")
            print(f"| 平均延遲 (Avg) | {res_float['avg']:.2f} μs | {res_pinned['avg']:.2f} μs | **{((res_float['avg'] - res_pinned['avg'])/res_float['avg']*100):.2f}% Faster** |")
            print(f"| p95 延遲 | {res_float['p95']:.2f} μs | {res_pinned['p95']:.2f} μs | **{((res_float['p95'] - res_pinned['p95'])/res_float['p95']*100):.2f}% Faster** |")
            print(f"| **p99 延遲 (極限抖動)** | {res_float['p99']:.2f} μs | {res_pinned['p99']:.2f} μs | **{((res_float['p99'] - res_pinned['p99'])/res_float['p99']*100):.2f}% Less Jitter** 🚀 |")
            print(f"| 最大隨機延遲 (Max) | {res_float['max']:.2f} μs | {res_pinned['max']:.2f} μs | 減少 {res_float['max'] - res_pinned['max']:.1f} μs 的長尾卡頓 |")
            print("=" * 65)
            print("💡 HFT 核心觀點:")
            print("1. 平均延遲 (Avg) 兩者差距看似不大 (可能只差幾微秒)。")
            print("2. 但在 p99 (Worst 1%) 與 Max Jitter 上，未綁定核心會隨機被分配到 Busy Core 0/1 上，導致延遲暴增！")
            print("3. CPU Affinity (綁定核心) 就像是一個『保險護盾』，確保您的交易系統在最快市、伺服器負擔最高時，」")
            print("   依然能維持穩如泰山的極限低延遲！這就是 HFT 消滅長尾延遲 (Long-tail Latency) 的秘密武器。")

    finally:
        # 清理並關閉背景噪音
        stop_event.set()
        clear_cpu_affinity(total_cores)
        print("\n🧹 背景噪音執行緒已安全關閉，CPU 已還原。")
