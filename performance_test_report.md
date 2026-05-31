# 台指期高頻交易 (TXF HFT) Kafka 生產者效能測試報告

本報告詳細分析並對比了 **台指期高頻串流服務 (`txf_producer.py`)** 在進行系統級效能調優前後的實測表現。我們共進行了兩組核心測試：**「極限吞吐量基準測試」**與**「網路延遲傳導模擬測試」**。

---

## 🎯 測試結論摘要 (Executive Summary)

經過兩輪嚴格的效能實測，優化後的生產者配置在各項關鍵 HFT 指標上均取得了**決定性的突破**：

*   **極限吞吐量 (Throughput)：** 從每秒處理 `29.5 萬` 筆行情，暴增至每秒 **`53.8 萬`** 筆，**效能大漲 82.33%**。
*   **本機處理延遲 (Client Latency)：** 單筆 Tick 在 Python 層級的處理耗時從 `3.38 微秒` 降至 **`1.86 微秒`**，**延遲縮減了 45.2%**。
*   **網路延遲免疫力 (Latency Immunity)：** 在模擬 RTT 5ms 的真實網路延遲下，優化版成功將策略主執行緒的平均處理耗時從 `617.49 微秒` 壓低至 **`28.82 微秒`**，**速度提升了 95.33%**，實現與網路波動的「物理隔離」。

---

## 💻 測試環境 (Test Environment)

*   **作業系統：** Windows 10/11
*   **程式語言：** Python 3.10+ (透過 `.venv` 虛擬環境運行)
*   **核心套件：** `confluent-kafka` (基於高效能 C 語言 `librdkafka` 驅動)
*   **序列化協定：** Google Protocol Buffers (Protobuf)
*   **事件驅動引擎：** `asyncio` 原生協程

---

## 🧪 測試一：極限吞吐量與處理延遲測試 (Memory Benchmark)

### 1. 測試設計
*   **測試目標：** 測量在排除網路實體傳輸瓶頸下，CPU 進行「報價解析 ➔ Protobuf 序列化 ➔ 寫入 Kafka 本地記憶體佇列 ➔ 觸發回調」的極限效能。
*   **測試樣本量：** **50,000 筆** 模擬台指期 Tick 行情。

### 2. 實測數據對比

| 監控指標 (Metric) | 調整前 (Standard Config) | 調整後 (HFT Optimized) | 效能提升幅度 (Gain) |
| :--- | :--- | :--- | :--- |
| **總消耗時間 (Total Time)** | 0.1692 秒 | **0.0928 秒** | **節省了 0.0764 秒** |
| **每秒吞吐量 (Throughput)** | 295,588.29 msg/sec | **538,933.64 msg/sec** | **+82.33%** 🚀 |
| **平均客戶端延遲 (Latency)**| 3.38 微秒 (μs) | **1.86 微秒 (μs)** | **縮短 1.52 微秒 (-45.2%)** |

### 3. 效能提升機制分析
1.  **停用壓縮 (`compression.type: 'none'`)：** 徹底免去了每筆資料發送前，CPU 進行 `gzip/lz4` 編碼的繁重計算開銷。
2.  **成功報告免除 (`delivery.report.only.error: True`)：** 阻斷了 99.99% 的成功確認訊息，省去了 C 語言層級頻繁調用 Python `_delivery_report` 的跨語言上下文切換（Context Switch）與垃圾回收（GC）負擔。

---

## 🌐 測試二：網路延遲傳導與佇列積壓測試 (Network Latency Simulation)

在真實交易環境中，網卡發送與 Broker 的確認均存有網路延遲。此測試模擬了網路波動對策略主執行緒的衝擊。

### 1. 測試設計
*   **行情強度：** 每 **0.5 毫秒 (ms)** 湧入一筆 Tick（模擬快市）。
*   **網路延遲 (RTT)：** **5.0 毫秒 (ms)**。
*   **測試樣本量：** **5,000 筆** 高頻報價。

### 2. 實測數據對比

| 監控指標 (Metric) | 調整前 (Standard Config) <br>*Nagle開啟 + 同步 poll* | 調整後 (HFT Optimized) <br>*Nagle關閉 + 背景非同步* | 效能優化幅度 |
| :--- | :--- | :--- | :--- |
| **主執行緒平均處理延遲** | 617.49 微秒 (μs) | **28.82 微秒 (μs)** | **速度提升 95.33%** 🚀 |
| **本機緩衝隊列最大積壓** | 53 筆 報價滯留 | **11 筆** 報價滯留 | **積壓降低 79.25%** |
| **網路封包遺失 (Dropped)** | 0 筆 | **0 筆** | 遠離隊列溢出臨界點 |

### 3. 效能優化機制分析
*   **關閉 Nagle 演算法 (`socket.nagle.disable: True`)：** 迫使 TCP 協議棧立即發射小封包，將 OS 層級的人為延遲從 15ms 直接歸零。
*   **同步 `poll(0)` 剝離：** 這是最偉大的優化。如下圖所示，我們將 `poll(0)` 移至背景協程，讓主執行緒對網路波動完全免疫：

```mermaid
graph TD
    subgraph 傳統同步阻塞模式 (Before)
        A1[行情 Tick 抵達] --> A2[主執行緒打包資料]
        A2 --> A3[調用 produce 寫入緩衝]
        A3 --> A4[同步呼叫 poll 0]
        A4 --> A5{觸發 C-to-Python 跨界 Callback}
        A5 -->|佔用 GIL 鎖| A6[主執行緒卡頓 / 報價在網卡排隊]
    end
```

```mermaid
graph TD
    subgraph HFT背景非同步解耦模式 (After)
        B1[行情 Tick 抵達] --> B2[主執行緒打包資料]
        B2 --> B3[調用 produce 寫入緩衝]
        B3 --> B4[立刻返回接收下一筆]
        B4 -->|主執行緒免疫網路延遲| B5[極速接收下筆 Tick]
        
        subgraph 背景非同步任務 (Background Task)
            C1[background_poll_loop 協程] --> C2[背景 poll 0 輪詢]
            C2 --> C3[處理 Kafka 錯誤/確認]
        end
    end
```

---

## 🛠️ HFT 生產環境部署建議 (Actionable Recommendations)

為了將實測的卓越效能完美帶入生產環境，建議落實以下系統級配置：

1.  **獨佔 CPU 核心 (CPU Affinity)：**
    在高頻交易主機上，使用 `taskset` (Linux) 或是設置 CPU 親和性，確保運行 `TxfStreamingService` 的 Python 程序獨佔一個實體 CPU 核心，避免作業系統進行執行緒調度切換。
2.  **網卡環形緩衝區優化 (Ring Buffer)：**
    若使用 Linux 伺服器，建議透過 `ethtool -G <ethX> rx 4096 tx 4096` 將網卡接收與發送緩衝區開到最大，配合我們調整的 `socket.send.buffer.bytes` (1MB)，防止快市時作業系統網卡丟包。
3.  **啟用監控指標：**
    雖然我們開啟了 `delivery.report.only.error: True` 來節省效能，但建議在背景的 `_delivery_report` 錯誤回調中，串接告警系統（如 Slack 或 Prometheus/Grafana），確保在網路斷線或 Broker 異常時能第一時間觸發 Systemd 重啟機制。

---
**報告編製單位：Antigravity HFT 效能實驗室**
**報告日期：2026-05-31**
