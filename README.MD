# txf-streaming-server — 台指期即時行情 producer

**Shioaji → Protobuf → Kafka**。以 Asyncio 連永豐 Shioaji API,把近月(TXFR1)/ 次月(TXFR2)
台指期的 **Tick** 與 **BidAsk** 序列化後即時推進 Kafka,供下游看盤 / 戰情室消費。

```
永豐 Shioaji API
   ├─（即時）→ 【本專案】txf-streaming-server ─Protobuf→ Kafka ──┐
   └─（歷史）→ txf-data-lake ─────Polars─────→ Parquet ─────────┤
                                          (D:\txf-data)        ├─→ txf-quant-platform（看盤/回測）
                                                               └─→ txf-gale-engine（戰情室看板）
```

**Kafka 的消費者是 quant-platform 與 gale-engine**;`txf-data-lake` 走 Shioaji 歷史 API,不碰 Kafka。

| | |
|---|---|
| **執行** | `python -m src.txf_producer`(必須是模組形式) |
| **需求** | Python 3.13(`.python-version` 鎖定)、shioaji ≥ 1.7、一個 Kafka broker |
| **部署** | Ubuntu + systemd + crontab,見 [第 3 節](#3-部署) |

---

## 1. 要部署一整套?照這個順序

| 順序 | 做什麼 | 看哪份文件 | 需要 Kafka? |
|---|---|---|---|
| 1️⃣ | **架 Kafka broker**(空白機器從零) | [`deploy/kafka/SETUP.md`](deploy/kafka/SETUP.md) | — |
| 2️⃣ | **部署本專案**(producer,行情進 Kafka) | 本文件第 2、3 節 | ✅ |
| 3️⃣ | 建歷史資料湖(**可與 1️⃣2️⃣ 平行做,不需 Kafka**) | `txf-data-lake` README | ❌ |
| 4️⃣ | 消費端:看盤 / 回測 / 策略 | `txf-quant-platform` README | 只有即時看盤要 |
| 5️⃣ | 消費端:當沖戰情室看板 | `txf-gale-engine` README | 只有 live 模式要 |

- 目標機器**已經有 Kafka** 就跳過 1️⃣。
- 4️⃣5️⃣ **兩個消費端都需要 3️⃣ 的 Parquet 資料湖**(回測、日線、prev-close 都靠它),
  只有「即時跳動」那部分才走 Kafka。**先把資料湖建起來,兩個看盤器就能離線用了。**
- 四個 repo 都用 **Python 3.13 + uv**,依賴各自獨立(每個 repo 自己的 `.venv`)。

---

## 2. 安裝與設定

```bash
git clone https://github.com/gman-quant/txf-streaming-server.git
cd txf-streaming-server

uv python install 3.13              # 取得 .python-version 鎖定的 Python
uv venv                             # 建 .venv
uv pip install -r requirements.txt

cp .env.example .env && vi .env     # 填金鑰與 Kafka 位址(見下)
python -m src.txf_producer
```

### `.env`

```bash
SHIOAJI_API_KEY=...
SHIOAJI_SECRET_KEY=...
KAFKA_BOOTSTRAP_SERVERS=broker_ip:9092
TICK_TOPIC=txf-tick          # 三個 topic 都必填 ——
TICK_R2_TOPIC=txfr2-tick     #   缺 TICK_R2_TOPIC 會讓每筆 R2 tick 發送失敗
BIDASK_TOPIC=txf-bidask
```

> `.env` 含金鑰,已被 gitignore。**broker 位址是 per-machine 的,不要 commit。**

### 驗證裝好了沒(不必等開盤)

```bash
python -m src.tools.verify_pipeline    # 合成報價跑完整條鏈,只寫測試 topic
python -m src.tools.txf_consumer       # 即時解碼行情、量測延遲(需有行情)
```

`verify_pipeline` 會驗:shioaji 模型欄位、protobuf 序列化、Kafka 投遞、R2 路由、
simtrade 過濾、位元組解得回原值。它**斷言測試 topic 與生產 topic 無交集**,有交集直接拒跑。

---

## 3. 部署

Ubuntu systemd 背景服務 + crontab 隨 TAIFEX 時段自動啟停。
unit 檔已含實測調校(CPU 綁核、OOM 保護、`SIGINT` graceful stop)。

```bash
sudo useradd -r -m -d /home/shioaji_svc shioaji_svc
sudo cp deploy/txf-producer.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl start txf-producer
journalctl -u txf-producer -f
```

要看到:`Login OK` → `Found Near/Next Month` → `Subscribed R1/R2` → `Service is running`。

> ⚠️ **換機器必改 `deploy/txf-producer.service` 兩處:**
> - **`CPUAffinity`** —— 要綁「同一顆實體核的兩個 HT 兄弟」,用
>   `cat /sys/devices/system/cpu/cpu<N>/topology/thread_siblings_list` 查。
>   只綁一個 = 另一個兄弟核的工作仍會來搶。(現值 `2 6` 是 i7-4770 的答案,別照抄到別台機器。)
> - **`WorkingDirectory` / `ExecStart`** 路徑。

### 交易時段排程(crontab)

`systemctl enable` 是**不需要的** —— 啟停由 crontab 控制,不靠開機自啟。

```cron
40 08 * * 1-5 /bin/systemctl start txf-producer.service   # 日盤 08:45
46 13 * * 1-5 /bin/systemctl stop  txf-producer.service   # 日盤收 13:45
55 14 * * 1-5 /bin/systemctl start txf-producer.service   # 夜盤 15:00
01 05 * * 2-6 /bin/systemctl stop  txf-producer.service   # 夜盤收 05:00
```

### 🔴 重啟的代價不對稱

- `daemon-reload` → **零影響**,隨時可做
- `restart` → 斷線幾秒,而且:
  - **tick** 隔天可從 Shioaji 歷史 API 補回
  - **bidask 只存在 Kafka,Shioaji 沒有歷史 API —— 斷幾秒就永久少幾秒**

**→ 改設定隨時可做,重啟一律等收盤**(週六 05:00 後,或平日 13:45–15:00 盤間)。

### 健康檢查

systemd 只知道行程在不在,測不到「連得上但不再送 tick」(斷線重連失敗、訂閱掉了、
回調在吞例外)。`deploy/check_feed_alive.sh` 檢查 topic offset 在交易時段內是否前進
(掛法見腳本尾端)。**建議先只告警、觀察數日再開自動重啟** —— 誤判重啟會製造 bidask 破洞。

---

## 4. 資料格式(wire schema)

Protobuf 3;價格一律 **Scaled Integer(實際價 × 10000)**,還原時 ÷10000。
`protos/txf_data.proto` 是**跨 repo 共用的契約**,改動會波及所有下游消費者。

### Tick (`txf.Tick`)
| Field | Type | 說明 |
| :--- | :--- | :--- |
| `code` | string | 商品代碼(如 `TXFH6`) |
| `timestamp_ms` | int64 | Unix Epoch (ms) |
| `close` | int64 | 成交價 ×10000 |
| `volume` | int32 | 單筆量 |
| `tick_type` | int32 | 1:外盤 / 2:內盤 |
| `total_volume` | int32 | 當日總量(可用來偵測封包遺漏) |
| `underlying_price` | int64 | 標的價 ×10000 |

### BidAsk (`txf.BidAsk`)
| Field | Type | 說明 |
| :--- | :--- | :--- |
| `bid/ask_total_vol` | int32 | 委託總量 |
| `bid/ask_price` | repeated int64 | 五檔價 ×10000 |
| `bid/ask_volume` | repeated int32 | 五檔量 |
| `diff_..._vol` | repeated int32 | 五檔掛單變化量 |

**Topic 路由**:TXFR1 tick → `txf-tick`、TXFR2 tick → `txfr2-tick`(R2 無 BidAsk)、
TXFR1 BidAsk → `txf-bidask`。

> R2 的 `quote.code` 是**真實月份碼**(如 `TXFI6`)不是 `"TXFR2"`,路由要同時比對
> `target_code` —— 這是修過的 bug,別退回。

### 改了 `.proto` 之後

```bash
python -m grpc_tools.protoc -I protos --python_out=src protos/txf_data.proto
```

---

## 5. 這台機器夠不夠?怎麼自己量

以下數字**不是 benchmark**,是從生產環境的 Kafka 與 systemd 直接量出來的
(2026-07-25,涵蓋 2025-12 起 190 個交易日、7,860 萬則訊息)。
**換機器後照下面的指令自己重量一次**,別直接引用這裡的數字。

### 實際負載

| | |
|---|---|
| 日均 | **413,787 則** |
| 最忙一天 | **787,302 則**(2026-03-03) |
| **峰值(全 topic 合計,滑動 1 秒)** | **353 則/秒**(p99 僅 50/秒) |
| 成長 | 2025-12 日均 28.7 萬 → 2026-06 日均 49.3 萬(**8 個月 +72%**) |

組成(最忙那天):`txf-bidask` **50 萬則(86%)**、`txf-tick` 16.5 萬、`txfr2-tick` <1%。

> ⚠️ **BidAsk 才是主要負載,不是 tick。** 只算 tick 會低估八倍 ——
> 早期文件寫「單日 73,000 筆」就是只算了 tick。評估硬碟、頻寬、保留期時務必用合計值。

**峰值怎麼算的**(這個數字很容易被算錯,寫清楚):

- **滑動 1 秒視窗**,不是固定桶。對齊整秒的固定桶會把跨邊界的爆量切成兩半而低估 ——
  同一天固定桶只報 316/秒,滑動才是真實觀測的 353/秒(2026-07-09 差更多:261 vs 340)。
- **全 topic 合計**,因為 producer 是單一行程同時扛三條流。
- 依 **produce 時間**分桶 = producer 實際要扛的速率。改依**交易所時間**分桶是「市場爆量速率」,
  同日略高(tick 187、bidask 195 則/秒),但同一數量級。
- ⚠️ 更早的文件曾寫「單秒峰值 479 筆/秒」,**兩種時間軸都重現不出來**,量法不明,已不採用。

### CPU 餘裕(systemd 自己在量,免費)

每次收工 journal 都有一行 `Consumed X CPU time`,那是**真實流量下的實測**,
不需要跑任何 benchmark:

```bash
journalctl -u txf-producer --since "7 days ago" | grep Consumed
```

實測結果:**0.6 ~ 1.0% 的一顆核心**(日盤 5 小時消耗 2~3 分鐘 CPU;夜盤 14 小時 5~6 分鐘)。
**本地 CPU 不是瓶頸**,而這是量出來的、不是推算的。

> 這是整段時段的**平均**。要抓峰值得在開盤時以 1 秒間隔採樣 `/proc/<pid>/stat` 取 max。

### 延遲(每一則訊息都自帶,不必 ping)

topic 設定是 `message.timestamp.type=CreateTime`,所以 **Kafka 訊息時間戳 = produce 當下的
本機時鐘**,而 payload 的 `timestamp_ms` = **交易所時間**。兩者相減就是端到端延遲。

```bash
python -m src.tools.measure_load profile --date 2026-07-24
```

| 日期 | tick p50 | tick p99 | max |
|---|---|---|---|
| 平靜日(2026-07-09) | 74 ms | 143 ms | 171 ms |
| 最忙日(2026-03-03) | 178 ms | 773 ms | 4,337 ms |

- **BidAsk 的 p50 只有 5~7 ms,tick 卻是 74~178 ms** —— 這不是傳輸差異,是**時間戳語意不同**
  (tick 帶撮合時間、bidask 帶快照時間),別拿兩者互相比較。
- ⚠️ **前提是機器有 NTP 校時**(`timedatectl` 看 `System clock synchronized: yes`)。
  沒校時的話量到的是時鐘偏差。工具偵測到負延遲會出警告。

### 完整性:兩條獨立路徑對帳

比對 Kafka(即時 producer)與資料湖(Shioaji 歷史 API)—— 兩者取得路徑完全獨立,
對得上就是真的沒漏:

```bash
python -m src.tools.measure_load verify --date 2026-07-24
```

實測 2026-03-03 與 2026-07-24:**筆數與口數完全一致,零遺漏**。

> 🔴 別用 `total_volume` 的跳號推遺漏 —— 那個假設不成立,會產生大量假警報
> (誤報 659 次不連續,而同日跨來源對帳其實完全一致)。

### 找供料中斷

```bash
python -m src.tools.measure_load stalls --start-date 2026-07-01 --end-date 2026-07-25
```

依「空窗前 60 秒的實際速率」推算該有幾則訊息,所以冷清時段不會誤報。
2026-07 實測只有兩次:一次是市場端(資料湖獨立佐證)、一次是券商斷線
(當時因回調 bug 導致自癒失效,已於 `1ed0764` 修復)。

### 全史負載掃描(零消費,幾十秒)

```bash
python -m src.tools.measure_load scan
```

只查 Kafka 位移、不搬任何訊息,產出每日總量、月趨勢、最尖峰的時段。
**部署新機器規劃硬碟與保留期時先跑這個。**

![server status](pic/1.png)

---

## Disclaimer

本專案供程式交易研究與教育用途。API 連線不穩、報價延遲或系統故障皆可能造成損失,
使用者需自行承擔。
