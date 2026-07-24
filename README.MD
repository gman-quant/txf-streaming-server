# TXF Streaming Server

台指期 (TXF) 即時行情 producer:**Shioaji → Protobuf → Kafka**。
以 Asyncio 連永豐 Shioaji API,將近月 (TXFR1) / 次月 (TXFR2) 台指期的 **Tick** 與 **BidAsk**
經 Google Protobuf 序列化(價格採 ×10000 scaled int64)後即時推送到 Apache Kafka,
供下游回測 / 看盤 / 戰情室消費。

- **執行**:`python -m src.txf_producer`
- **Shioaji**:1.7(`requirements.txt` 釘 `shioaji[speed]>=1.7.0`)
- **部署**:Ubuntu + systemd,見 [部署](#部署-deployment)

---

## 資料格式 (Wire Schema)

Protobuf 3;價格欄位一律 **Scaled Integer(實際價 × 10000)**,還原時 ÷10000。
`protos/txf_data.proto` 是**跨 repo 共用的 wire 契約**,改動會波及所有下游消費者。

### Tick (`txf.Tick`)
| Field | Type | 說明 |
| :--- | :--- | :--- |
| `code` | string | 商品代碼(如 `TXFH6`) |
| `timestamp_ms` | int64 | Unix Epoch (ms) |
| `close` | int64 | 成交價 ×10000 |
| `volume` | int32 | 單筆量 |
| `tick_type` | int32 | 1:外盤 / 2:內盤 |
| `total_volume` | int32 | 當日總量(用於偵測封包遺漏) |
| `underlying_price` | int64 | 標的價 ×10000 |

### BidAsk (`txf.BidAsk`)
| Field | Type | 說明 |
| :--- | :--- | :--- |
| `bid/ask_total_vol` | int32 | 委託總量 |
| `bid/ask_price` | repeated int64 | 五檔價 ×10000 |
| `bid/ask_volume` | repeated int32 | 五檔量 |
| `diff_..._vol` | repeated int32 | 五檔掛單變化量(策略訊號用) |

**Topic 路由**:TXFR1 tick → `txf-tick`、TXFR2 tick → `txfr2-tick`(R2 無 BidAsk)、TXFR1 BidAsk → `txf-bidask`。

---

## 快速開始 (Quick Start)

```bash
git clone https://github.com/gman-quant/txf-streaming-server.git
cd txf-streaming-server

uv venv
. .venv/Scripts/activate            # Windows(mac/Linux:. .venv/bin/activate)
uv pip install -r requirements.txt  # 精確重現既有環境用 requirements.lock

cp .env.example .env                # 填入 Shioaji 金鑰與 Kafka 位址(見下)

python -m src.txf_producer
```

### 設定 `.env`

完整範本見 `.env.example`;關鍵欄位:

```bash
SHIOAJI_API_KEY=...
SHIOAJI_SECRET_KEY=...
KAFKA_BOOTSTRAP_SERVERS=broker_ip:9092
TICK_TOPIC=txf-tick          # 三個 topic 都必填 ——
TICK_R2_TOPIC=txfr2-tick     #   缺 TICK_R2_TOPIC 會讓每筆 R2 tick 發送失敗
BIDASK_TOPIC=txf-bidask
```

### 重新編譯 Protobuf(僅在改了 `.proto` 時)

```bash
python -m grpc_tools.protoc -I protos --python_out=src protos/txf_data.proto
```

### 除錯工具:Kafka Console Inspector

即時 consume 並解碼行情、拆解傳輸/處理延遲,用來檢查串流是否正常:

```bash
python -m src.tools.txf_consumer
```

---

## 部署 (Deployment)

Ubuntu systemd 背景服務,配合 crontab 隨 TAIFEX 時段自動啟停。
unit 檔已含實測調校(CPU 綁核、OOM 保護、`SIGINT` graceful stop),見 `deploy/txf-producer.service`。

```bash
# 新主機重建(git clone → 可運作)
git clone https://github.com/gman-quant/txf-streaming-server.git
cd txf-streaming-server
cp .env.example .env && vi .env
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

sudo useradd -r -m -d /home/shioaji_svc shioaji_svc
sudo cp deploy/txf-producer.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now txf-producer
journalctl -u txf-producer -f
```

> ⚠️ 換機器要檢查 `deploy/txf-producer.service` 兩處:
> - **`CPUAffinity`** —— 要綁「同一實體核的 HT 兄弟」那兩個邏輯核(用
>   `cat /sys/devices/system/cpu/cpu<N>/topology/thread_siblings_list` 查);unit 已設 `2 6`(本機 i7-4770,無需再改)。只綁一個核 = 另一個 HT 兄弟仍會來搶。
> - **`WorkingDirectory` / `ExecStart`** 路徑。
>
> Kafka 從零建置見 `deploy/kafka/SETUP.md`;若目標機**已有 Kafka**(如既有生產機),略過該文件。

### 交易時段排程 (crontab)

```cron
# Day    08:40 start / 13:46 stop
40 08 * * 1-5 /bin/systemctl start txf-producer.service
46 13 * * 1-5 /bin/systemctl stop  txf-producer.service
# Night  14:55 start / (次日) 05:01 stop
55 14 * * 1-5 /bin/systemctl start txf-producer.service
01 05 * * 2-6 /bin/systemctl stop  txf-producer.service
```

### 健康檢查

systemd 只知道行程在不在,測不到「連得上但不再送 tick」(斷線重連失敗、訂閱掉了)。
`deploy/check_feed_alive.sh` 檢查 topic offset 是否在交易時段內前進(掛法見腳本尾端)。

---

## 效能

**真實負載**(近 25 個交易日實測):單日約 **73,000 筆**、單秒峰值 **479 筆/秒**(2026-07-09)。

**本地處理成本**(dev 鏡像實測;protobuf 訊息 Tick 33 B / BidAsk 118 B):

| 路徑 | 每則成本 |
| :--- | :--- |
| producer build + serialize(Tick) | ~3.8 µs |
| producer build + serialize(BidAsk・五檔) | ~12.7 µs |
| consumer decode(Tick / BidAsk) | ~1.2 / 1.8 µs |

即使最壞情況(峰值全為較重的 BidAsk),producer 序列化在 479 筆/秒下僅佔
**約 0.6 % 的一顆核心** —— **本地 CPU 不是瓶頸**。

**真正的約束是上游網路延遲**:Shioaji → 伺服器實測平均 **5.7 ms**
(於生產機量測、30 次取樣、0 % 封包遺漏),比本地處理大三個數量級,且本程式無法改變它;
producer 能做的是「不成為額外瓶頸」,上表已確認。

> 本段**刻意不宣稱吞吐上限**。早期一組「53.8 萬 msg/s」的 benchmark 已移除 ——
> 在一個有數百倍餘裕的維度上,再漂亮的數字都不改變任何決策。取捨理由詳見 `CLAUDE.md`。

![server status](pic/1.png)

---

## Disclaimer

本專案供程式交易研究與教育用途。高頻交易風險極高,API 連線不穩、報價延遲或系統故障
皆可能造成損失,使用者需自行承擔。
