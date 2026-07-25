# txf-streaming-server — AI Agent 指南

Live 行情 producer:Shioaji → Protobuf → Kafka。**生產環境在 Ubuntu 192.168.1.50**(systemd `txf-producer.service` + crontab 隨 TAIFEX 時段啟停);這個 Windows checkout 是開發鏡像。下游四個消費者:txf-quant-platform/stable(txf-tick、txfr2-tick)、txf-gale-engine(txf-tick、txf-bidask)、txf-data-lake(kafka_reader 回補)。

## 紅線

1. **絕不隨便啟動 producer**(`python -m src.txf_producer`):它做真實 Shioaji 登入,**同一人併發連線有配額** —— 本地跑一個可能把生產 session 踢下線(shioaji.log 記錄過兩次 451 Too Many Connections)。
2. `protos/txf_data.proto` 是**四個 repo 共用的 wire 契約**(價格 ×10000 scaled int64):改 schema/topic 名的爆炸半徑是全 workspace,動之前列出所有下游。
3. `.env` 含 API 金鑰(gitignored、未進 git 史,僅存在本機磁碟);其 broker=localhost:9092 是**伺服器本機值**,在這台 PC 測試要指 192.168.1.50:9092,但**別 commit broker 變更**(.env 本來就 per-machine)。
4. `src/txf_producer.py` 以 `simulation=True` 登入(committed 狀態如此)——生產伺服器實際跑什麼無法從本 repo 驗證。**別擅自「修正」這個 flag**,問用戶。

## 事實

- 執行必須是模組形式:`python -m src.txf_producer`(相對 import;直接跑檔案會 ImportError)。venv Python 3.13(uv 管理,`.python-version` 鎖定)。
- Topic 路由:TXFR1 tick→txf-tick、TXFR2 tick→txfr2-tick(**R2 無 BidAsk**)、R1 BidAsk→txf-bidask。R2 的 quote.code 是真實合約碼(如 TXFG6)非 "TXFR2",路由要同時比對 target_code(commit 6441f48 修的 bug,別退回)。
- simtrade==1(試撮)事件被過濾,不進 Kafka。
- producer.poll(0) 刻意移到 100ms 背景任務 —— **別加回熱路徑**(GIL 阻塞,commit 0ad6332 的刻意移除)。
- 下游假設 topic **單一 partition**(data-lake kafka_reader 硬編 TopicPartition(topic, 0))。
- protobuf 重生成:`python -m grpc_tools.protoc -I protos --python_out=src protos/txf_data.proto`(生成檔是 committed 的,重生成會出現 tracked diff)。
- README 的 .env 範本已於 2026-07-04 補上 `TICK_R2_TOPIC`(src/config.py 讀取、無預設值,缺了會讓每筆 R2 tick 發送失敗)。
- 部署程序(程式碼如何上 192.168.1.50、伺服器目前在哪個 commit)**無文件、無法從本機驗證** —— 涉及伺服器的問題直接問用戶。

## 生產機實況(2026-07-25 升級後實況)

**主機**:`home-lab` / Ubuntu 24.04.4 LTS / i7-4770(4 實體核 + HT = 8 邏輯核)/ 7.7 GB RAM。
**服務**:`txf-producer.service`,`User=shioaji_svc`。**`systemctl is-enabled` = disabled 是正確的**
—— 啟停由 root crontab 控制,不靠開機自啟。
**版本(2026-07-25 升級)**:Python **3.13.14** / shioaji **1.7.0** / protobuf 7.35.1 /
confluent-kafka 2.15.0 / grpcio 1.83.0 / uvloop 0.22.1。依賴由 **uv** 管理。
protobuf 6→7 跨版本解碼相容已實測(本 repo 編碼→platform 用 6.x 解碼,欄位全對)。

### crontab 啟停排程(root crontab,2026-07-25 實查)

| 時間 | 動作 | 對應 |
|---|---|---|
| 08:40 一–五 | start | 日盤 08:45 |
| 13:46 一–五 | stop | 日盤收 13:45 |
| 14:55 一–五 | start | 夜盤 15:00 |
| 05:01 二–六 | stop | 夜盤收 05:00 |

→ 週末整段不跑。**要動生產機,週六是最安全的窗口**(服務本來就 inactive,零 bidask 破洞風險)。

### ⚠️ 部署方式:2026-07-25 起改為 `git clone`,舊的「手動複製」已終結

2026-07-25 升級時**整個目錄重新 clone**(舊目錄備份成 `txf-streaming-server.bak.2026-07-25`),
所以生產機的 `git log` **從此可信** —— 要知道它在跑哪版就直接查,別在本檔記 hash(會腐爛):
`sudo -u shioaji_svc bash -lc 'cd /home/shioaji_svc/txf-streaming-server && git log --oneline -1'`
2026-07-21 之前的舊狀態(`.git` 停在 `a5dfdec` 但檔案是手動複製的較新版)已不再適用。
`.env.example` 與 `deploy/txf-producer.service` 都在 repo 內,新主機 clone 就重建得出來。

### uv 的 PATH 陷阱(runbook 實戰補充)

`uv` 裝在 `/home/shioaji_svc/.local/bin`,而 **`sudo -u shioaji_svc uv …` 不會載入該使用者的登入環境
→ 找不到 uv**。一律用 `sudo -u shioaji_svc bash -lc '…'` 包起來(或用絕對路徑)。

### Kafka 保留設定(topic 層級覆寫,`server.properties` 的 168h 不作數)

用 `kafka-configs.sh --describe --all` 查**生效值**(不加 `--all` 看不到繼承值):

| topic | retention.ms | retention.bytes | 目前用量 | 實際資料起點 |
|---|---|---|---|---|
| `txf-tick` | 365 天 | 10 GB | 1.2 GB | 2025-12-01 |
| `txf-bidask` | 365 天 | **20 GB** | **12 GB** | 2025-12-01 |
| `txfr2-tick` | 365 天 | 1 GB | 14 MB | 2026-06-15(雙 topic 上線日) |

**兩道閘先到先砍。** `txf-bidask` 約 52 MB/天 → 一年約 19 GB,**逼近 20 GB 上限**;
成交量一放大,**大小閘會先砍掉還沒滿一年的資料,而且是靜默的**。`txf-tick` 則很寬鬆。

### ⚠️ 重啟的不對稱代價

`systemctl daemon-reload` 零影響;**`systemctl restart` 才會斷幾秒**,而代價不對稱:
- `tick` → 隔天 ETL 從 Shioaji 歷史 API 補得回,湖不會缺
- `bidask` → **只存在 Kafka,Shioaji 無歷史 API,斷幾秒就永久少幾秒**

→ **改設定隨時可做,重啟一律等收盤**(週六 05:00 後,或平日 13:45–15:00 盤間)。

### unit 檔的三個缺口(2026-07-21 發現,**2026-07-25 已套用到生產機**)

1. `Restart=on-failure` → `always`:多接住「非 systemctl 的 SIGTERM 導致 exit 0」。
   ⚠️ 07-21 原註解宣稱「Shioaji session 正常結束 / 某條路徑走到 sys.exit(0)」——
   **查碼後證實不成立**(所有錯誤路徑都是 `sys.exit(1)`,碼裡沒有 `sys.exit(0)`;
   exit 0 只來自訊號觸發的乾淨關閉)。unit 內註解已於 07-25 改成事實正確版本。
   對 crontab 無影響:`systemctl stop` 是明確停止,`Restart=` 不生效。
2. `StartLimitIntervalSec=60s` → `600s`:原設定配 `RestartSec=25s`,60 秒最多重啟 2 次,
   **5 次門檻永遠碰不到 → 崩潰迴圈保護等於失效**。
   📌 副作用:保護生效後變成「**約 100 秒內失敗 5 次就放棄**」,若開盤時券商端故障超過 2 分鐘,
   日盤整段沒資料(等 14:55 crontab 再啟)。想讓短暫故障熬得過去可把 `RestartSec` 拉到 60s;
   目前**刻意維持 25s**(優先避免猛敲登入觸發 451 鎖帳號)。
3. `CPUAffinity=2` → `2 6`:邏輯核 2 與 6 是**同一顆實體核的 HT 兄弟**
   (`/sys/devices/system/cpu/cpu2/topology/thread_siblings_list` = `2,6`),只綁 2 仍會被核 6 的工作搶。

### 健康檢查:`deploy/check_feed_alive.sh`

systemd 只知道行程在不在。producer 可能**連得上但不再送 tick**(斷線重連失敗、迴圈卡死、
訂閱掉了)——行程活著、status 顯示 active,**systemd 永遠不會發現**。
本腳本檢查 topic offset 在 45 秒內是否前進,並**只在交易時段檢查**(非交易時段零 tick 是正常的,
天天誤報會讓人忽略真警報)。時段判定含「週五夜盤延到週六凌晨」。
掛法(systemd timer 範例)寫在腳本尾端。**建議先只告警、觀察數日再開自動重啟** ——
誤判重啟會製造 bidask 破洞。

## 效能:實測需求 vs 已移除的 benchmark(2026-07-21)

**真實需求**(近 25 個交易日的 tick 實測):單日約 **73,000 筆**、
**單秒峰值 479 筆/秒**(2026-07-09)。

**保留的有用數據 —— 全鏈路延遲**(30 次取樣,0% 封包遺失;這是唯一量到真瓶頸的表):

| 鏈路 | 平均 | 抖動 | 最差 |
|---|---|---|---|
| Server → 券商 API | **5.7 ms** | 2.0 ms | 12.3 ms |
| Server → PC(LAN) | 0.4 ms | 0.0 ms | 0.5 ms |
| Server → iPad(LAN) | 1.6 ms | 0.1 ms | 1.8 ms |

### ⚠️ 已移除:兩組誤導性的效能數字

`README` 的「53.8 萬 msg/s、8 μs」與 `performance_test_report.md` 的
「404,989 TPS、2.47 μs」**已於 2026-07-21 移除**(報告整份刪除、README 章節改寫),
連同伺服器上的 `tests/performance_test.py`(該檔從未進 git —— `.gitignore` 擋 `tests/`)。

**為什麼移除(不是因為數字錯,是因為量錯維度):**
- 宣稱能力 40~54 萬 msg/s,實際峰值 479 筆/秒 → **餘裕 845~1,123 倍**。
  在這種餘裕下,「+23% 吞吐、延遲 3.04→2.47 μs」不具決策價值。
- 報告據此推論「**已完全具備處理快市海嘯的穩定運算能力**」——**該推論不成立**。
  快市的瓶頸是券商餵資料速度(同報告自己量到 **5.7 ms**,比 2.47 μs 大 2,300 倍)、
  Kafka 磁碟寫入、網路。本地 CPU 迴圈的微秒級優化對這三者毫無幫助。
- 兩組數字在 repo 內並存且未交叉引用(+82% vs +23%、-99% vs -19%),
  未來讀者無從判斷該信哪個。

**教訓(值得記住的通則):** benchmark 要先確認**測的維度是不是真的約束**。
量一個有三位數倍餘裕的東西,再漂亮的改善幅度都不會改變任何決策。
