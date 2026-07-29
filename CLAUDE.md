# txf-streaming-server — AI Agent 指南

Live 行情 producer:Shioaji → Protobuf(看盤線)+ JSON(研究線)→ Kafka。**生產環境在 Ubuntu 192.168.1.50**(systemd `txf-producer.service` + crontab 隨 TAIFEX 時段啟停);這個 Windows checkout 是開發鏡像。下游消費者:txf-quant-platform/stable(txf-tick、txfr2-tick)、txf-gale-engine(txf-tick、txf-bidask)。
⚠️ **txf-data-lake 已不是 Kafka 消費者**(`core/kafka_reader` 於 2026-07-21 隨舊看盤一併刪除,
自此純走 Shioaji 歷史 API)——舊文件說「四個消費者」是過期的。

## 紅線

1. **絕不隨便啟動 producer**(`python -m src.txf_producer`):它做真實 Shioaji 登入,**同一人併發連線有配額** —— 本地跑一個可能把生產 session 踢下線(shioaji.log 記錄過兩次 451 Too Many Connections)。
2. `protos/txf_data.proto` 是**四個 repo 共用的 wire 契約**(價格 ×10000 scaled int64):改 schema/topic 名的爆炸半徑是全 workspace,動之前列出所有下游。
3. `.env` 含 API 金鑰(gitignored、未進 git 史,僅存在本機磁碟);其 broker=localhost:9092 是**伺服器本機值**,在這台 PC 測試要指 192.168.1.50:9092,但**別 commit broker 變更**(.env 本來就 per-machine)。
4. `src/txf_producer.py` 以 `simulation=True` 登入(committed 狀態如此)——生產伺服器實際跑什麼無法從本 repo 驗證。**別擅自「修正」這個 flag**,問用戶。

## 事實

- 執行必須是模組形式:`python -m src.txf_producer`(相對 import;直接跑檔案會 ImportError)。venv Python 3.13(uv 管理,`.python-version` 鎖定)。
- Topic 路由(**2026-07-28 V-FLIP/V-FLIP2 改版**;訂閱端態 = **Quote×2 兩訂閱**):
  | 來源 | topic | 格式 | 消費者 |
  |---|---|---|---|
  | R1 tick(**由 Quote 合成**,`_synth_tick`) | `txf-tick` | protobuf | platform/stable viewer、gale |
  | R2 tick(**由 Quote 合成**) | `txfr2-tick` | protobuf | platform/stable viewer |
  | R1 BidAsk(**由 Quote 合成**,`_synth_bidask`) | `txf-bidask` | protobuf | gale(匯出 `{date}_TXF_bidask.parquet`) |
  | **R1/R2 Quote** | **`txf-md-raw`** | **JSON(orjson)** | gale `tools/export_md_raw.py`(手動,待掛排程) |

  **V-FLIP(2026-07-28)**:Tick×2 與 BidAsk R2 退訂。tick 改由 Quote 以 `synth_tick_dv`
  (running-max of total_volume)合成 —— 依據:原生 Tick 實測掉單(崩盤夜 144 處/188 口),
  合成總量 == 權威累計量;同成交延遲 med −1.6ms;代價 = 掃檔為事件級,5s K 棒 H/L
  崩盤爆量段可差 ≤13 點/58 根(無仲裁者)。驗收:`src/tools/verify_tick_synth.py`
  (離線重放 md-raw,用生產同一顆函數)。
  **V-FLIP2(同日)**:BidAsk R1 也退訂,txf-bidask 由 `_synth_bidask` 合成 —— 依據:
  R1 逐則同簿 **100.00% 雙向**(178,219/178,219,含 diff 欄;archive vs quote 全欄 join)、
  totals==Σ五檔零違例。成交時刻「~34% 不同拍」經查為**配對抖動**(同一批簿況狀態、
  到達序不同),內容序列 100% 同 —— 不構成保留原生的理由。
  **回退 = git revert 整個 V-FLIP/V-FLIP2 commit;勿只加回訂閱(原生+合成雙寫 = 訊息重複)。**
  ⚠️ **R2 的 BidAsk 絕對不可進 `txf-bidask`** —— gale 匯出時不依 code 過濾,混進去會污染 R1 的檔。分流在 `process_bidask()` 開頭 early-return(R2 BidAsk 已退訂,守衛保留防復訂污染)。
  ⚠️ R2 的 quote.code 是真實合約碼(如 TXFI6)非 "TXFR2",判定要同時比對 target_code(commit 6441f48 修的 bug,別退回)—— 已抽成 `_is_r2()`。
  ⚠️ 訂閱順序 Quote 在前(慣例保留)。「共訂會不會互踢」已於 7/27–7/28 實測**不互踢**(六訂閱期間 txf-tick 與 md-raw tick 流逐則同數);V-FLIP 後僅剩兩型別,此顧慮不再適用。
- simtrade==1(試撮)**只在 protobuf 路徑被過濾**;`txf-md-raw` **刻意保留試撮**(欄位在,要濾隨時能濾)。試撮是歷史 API 完全沒有的資料:日盤 08:30–08:45、夜盤 14:50–15:00,每 5 秒一則(量體極小,約 180 則/商品/盤)。
- **⚠ `to_dict()` 本身就不完整(2026-07-28 生產實證)**:`QuoteFOPv1` 有 46 個屬性,
  `to_dict()` 只吐 **34** 個 —— SDK 的 schema 沒宣告那 12 個
  (`amount_sum` / `diff_price` / `diff_rate` / `diff_type` / `first_derived_{bid,ask}_volume` /
  `target_kind_price` / `trade_{bid,ask}_cnt` / `trade_{bid,ask}_vol_sum` / `vol_sum`),
  **所以任何走 to_dict 的序列化天生就漏** —— 已由 `_extra_field_names` 以 `getattr`
  補進 raw payload(名單每 (kind, role) 只算一次,之後每則 12 次 getattr;**全程不印 log**)。
  名單用 `dir()` 動態算而非寫死 —— SDK 日後新增欄位自動收進來,不必有人記得改碼。
  2026-07-27 的一次性 `FIELD-AUDIT` 傾印(每次啟動 16 行)**已移除**:診斷任務完成即除役
  (使用者指示,2026-07-28)。離線驗證:scratchpad `test_extra_fields.py`(假 Quote,不登入不連 Kafka)。
  ⚠️ **更正(2026-07-29 實測)**:舊版此處寫「V-FLIP2 退訂 BidAsk 後 `first_derived_*`
  一度沒有任何來源在收」—— **不成立**。`to_dict()` 早就有 `first_derived_{bid,ask}_vol`
  與 `_price`,與補收的 `_volume` 版**逐列完全相同**(md_raw 5 萬列比對,零筆不同)。
  ⇒ 那 12 欄裡 **2 欄是既有欄位的別名**,衍生一檔從未斷過來源;**消費端認 `_vol` 版為主**。
  仍保留補收(不剔除):剔除需 producer 與 gale `EXPECTED` 名單同步部署,不同步時
  exporter 判「缺欄位」→ `sys.exit(1)`,風險大於重複 2 個小整數的成本。
- `txf-md-raw` 的設計原則:**`to_dict()` 全欄位、不挑、不改精度**。代價已經付過 —— protobuf 的 BidAsk 漏了 `first_derived_*`(衍生一檔=組合簿的唯一入口)整整八個月,而 bidask 無歷史 API、補不回來。用 **orjson** 是因為它回傳 bytes 且**原生保留 datetime 微秒**(protobuf 路徑的 `int(ts*1000)` 把微秒截掉了;交易所 `INFORMATION-TIME` 其實給到微秒)。附加欄位 `_type` / `_role`(R1/R2,因為 code 會隨換月變)/ `_recv_ns` / `_seq`。
  ⚠️ `raw_json_default` **刻意嚴格**(只認 `.value` 枚舉與 Decimal,其餘 raise)—— 別改成 `default=str`,那會讓 Shioaji 改型別時靜默產生怪資料。
  ⚠️ `_emit_raw` **絕不 re-raise**:例外冒回 Shioaji 回調執行緒可能讓該回調永久靜默停止。錯誤只計數 + 每 500 次節流 log。
- **`enable.idempotence=True`(2026-07-26 新增)**:原本 `acks=1` + 未設 idempotence + `max.in.flight` 預設 1000000 + retries 極大 → 暫時性錯誤重試時**分區內可能亂序**(librdkafka 明確警告)。本 broker **RF=1**,故 idempotence 強制的 `acks=all` 等同 `acks=1`,**延遲代價為零**。
  ⚠️ 但 Kafka offset 的順序只保證「`produce()` 被呼叫的順序」,**跨訂閱流(R1 vs R2、tick vs bidask vs quote)並不可靠** —— 三個回調分開派發、Shioaji 無排序文件。交易所的 `PROD-MSG-SEQ`(跨 I024/I081/I083 連續的商品序號)Shioaji 沒轉出來,**拿不到**。這是 order-flow 研究的天花板,價差研究(秒~分尺度)不受影響。
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
**protobuf 版本歪斜已於本次升級消失**:生產機從 6.33.1 上到 7.35.1 後,
四個 repo(streaming-server / quant-platform / gale-engine / data-lake)**全部統一 7.35.1**
(2026-07-25 實查)。過去記錄的「6→7 跨版本解碼相容」是升級前的狀態,現已不適用 ——
**若日後只升其中一邊,歪斜會回來,屆時要重驗**。

### crontab 啟停排程(root crontab,2026-07-25 實查)

| 時間 | 動作 | 對應 |
|---|---|---|
| **08:28** 一–五 | start | 日盤 08:45,**提早吃 08:30–08:45 試撮** |
| 13:46 一–五 | stop | 日盤收 13:45 |
| **14:48** 一–五 | start | 夜盤 15:00,**提早吃 14:50–15:00 試撮** |
| 05:01 二–六 | stop | 夜盤收 05:00 |

→ 週末整段不跑。**要動生產機,週六/週日是最安全的窗口**(服務本來就 inactive,零 bidask 破洞風險)。

⚠️ **啟動時間於 2026-07-26 提早**(原 08:40 / 14:55):試撮只進 `txf-md-raw`
(protobuf 路徑仍過濾 `simtrade`,viewer 完全不受影響)。
已確認 `check_feed_alive.sh` 的 `in_session()` 排除 `05:00–08:45` 與 `13:45–15:00`,
**提早啟動不會造成誤報**(副作用:試撮期的斷線健康檢查也看不到,要自己看資料)。

### ⚠️ 部署方式:2026-07-25 起改為 `git clone`,舊的「手動複製」已終結

2026-07-25 升級時**整個目錄重新 clone**(舊目錄備份成 `txf-streaming-server.bak.2026-07-25`),
所以生產機的 `git log` **從此可信** —— 要知道它在跑哪版就直接查,別在本檔記 hash(會腐爛):
`sudo -u shioaji_svc bash -lc 'cd /home/shioaji_svc/txf-streaming-server && git log --oneline -1'`
2026-07-21 之前的舊狀態(`.git` 停在 `a5dfdec` 但檔案是手動複製的較新版)已不再適用。
`.env.example` 與 `deploy/txf-producer.service` 都在 repo 內,新主機 clone 就重建得出來。

### ⚠️ 2026-07-26:`orjson` 是新的直接依賴,舊環境要手動裝

`txf-md-raw` 用 orjson 序列化,而 **生產機的 venv 原本沒有它**(部署時實際撞到
`ModuleNotFoundError: No module named 'orjson'`,producer 崩在 import、systemd 重啟 4 次
才被 StartLimit 擋住 —— 好在崩在登入之前,零 Shioaji 連線、無 451 風險)。

```bash
sudo -u shioaji_svc bash -lc 'cd ~/txf-streaming-server && uv pip install --python .venv/bin/python orjson==3.11.9'
```

**教訓**:`requirements.lock` 裡有某個套件 **不等於** 目標機器裝了它 ——
lock 是在 Windows dev 端解析出來的,orjson 在那邊是 `pysolace` 的間接相依,
Linux 端的解析結果不同。**間接相依永遠不可當成「一定在」**,要嘛明列、要嘛實際驗證。

⚠️ **不要用 `uv pip sync requirements.lock` 修這種問題** —— `sync` 會**移除**不在 lock 裡的套件,
爆炸半徑遠大於裝單一套件。

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
| `txf-md-raw` | **30 天** | **-1(無閘)** | — | 2026-07-27(預計) |

**兩道閘先到先砍。** `txf-bidask` 約 52 MB/天 → 大小閘約 5 個月後會先觸發,
**先砍掉還沒滿一年的資料,而且是靜默的** —— 所以「保留 365 天」其實做不到。
⚠️ 但 2026-07-26 查證:**parquet 匯出 163/163 天零缺口**(`*_bidask.parquet` 合計 891 MB
vs Kafka 12 GB → **zstd 壓縮比 13.5×**)。既然 parquet 才是檔案庫、Kafka 只是緩衝,
`txf-bidask` 存 365 天/20 GB 是**過度配置**;正確方向是**縮短**(待新匯出器上線後改 60 天),
不是放寬。現況雖然設定與意圖不符,但**不會掉資料**。

**磁碟位置(2026-07-26 實查)**:`log.dirs=/opt/kafka/data/kraft-combined-logs` → 掛在 `/`
→ `sda`(**SanDisk SSD**,232 G,可用 **198 G**)。那顆 1 TB 的 `sdb` 是
**ST1000DM003 = 7200rpm 機械碟**,掛在 `/mnt/data`(給 `txf-backup` 用)。
**別把 Kafka 搬到 sdb** —— 那是 SSD → HDD 的降級,而且 SSD 的 198 G 已是十年份。

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

## 效能:量測方法的演進(2026-07-21 → 07-25)

**現行數字與量法一律以 `README` §5 與 `src/tools/measure_load.py` 為準。**
本節只保留「為什麼是現在這個量法」的決策史 —— 部署者不需要讀,改量測方式前要讀。

### 2026-07-25 全面改用「從生產資料量」,棄用合成 benchmark

`perf_hotpath.py` 只量 protobuf build+serialize(**不含 Kafka produce、不含回調開銷、
不含 GIL 競爭,且用假資料**),據此推算的「0.6% 一顆核心」是估計不是量測。
改用 `measure_load.py` 從 Kafka 與 systemd 直接量之後,發現三件事:

1. **舊的「單日 73,000 筆」少算八倍** —— 那只算了 tick,而 **BidAsk 佔全部訊息 86%**
   (最忙日:bidask 50 萬 vs tick 16.5 萬)。用它評估硬碟/保留期會嚴重低估。
   **舊的「單秒峰值 479 筆/秒」也重現不出來**:實測 8 個月最忙日全 topic 合計
   **353 則/秒**(滑動 1 秒視窗),改用交易所時間軸分桶亦僅略高、同一數量級。
   舊數字量法不明,已不採用。

   ⚠️ **峰值這個數字我在 2026-07-25 錯了兩次,兩個坑都值得記住**:
   - 先誤判「479 只算了 tick」→ 錯,它本來就是合計值等級的數字;
   - 再用一個有 bug 的合計(469/秒)去「證實」它 —— bug 是 `per_sec` 定義在
     `if buckets:` 內,**0 則訊息的 topic 不會重新賦值而沿用上一個 topic 的資料**,
     導致 txf-tick 被算兩次(469 = 153+153+163)。
   **教訓**:合計值必須做健全性檢查 —— 合計不可能超過各分項最大值之和。
   這個矛盾一眼就能看出來,但我當時沒查就寫進文件了。
2. **舊的「5.7 ms」量的是 ping RTT,不是行情延遲。** 真正的端到端延遲每一則訊息都自帶
   (`CreateTime` 的 Kafka 時間戳 − payload 的交易所時間),實測 tick p50 是 74~178 ms、
   最忙日 p99 達 773 ms —— 與 ping 差兩個數量級,ping 永遠看不到這個。
3. **ping 也看不見「供料中斷」。** 改量之後才發現 2026-07-20 20:57 有 75 秒行情靜默,
   追下去挖出 `_handle_session_down` 的 arity bug(自癒機制形同不存在,見上)。

### 已移除:兩組誤導性的吞吐數字(2026-07-21)

`README` 的「53.8 萬 msg/s」與 `performance_test_report.md` 的「404,989 TPS、2.47 μs」
已移除(報告整份刪除),連同伺服器上的 `tests/performance_test.py`。

**為什麼移除(不是因為數字錯,是因為量錯維度):** 宣稱能力 40~54 萬 msg/s,
實際峰值 469 筆/秒 → 餘裕近千倍。在這種餘裕下,「+23% 吞吐、延遲 3.04→2.47 μs」
不具決策價值。報告據此推論「已完全具備處理快市海嘯的能力」——**該推論不成立**:
快市的瓶頸是券商餵資料速度、Kafka 磁碟寫入、網路,本地 CPU 迴圈的微秒優化幫不上忙。

**教訓(通則):** benchmark 要先確認**測的維度是不是真的約束**。量一個有三位數倍餘裕的
東西,再漂亮的改善幅度都不會改變任何決策 —— 而真正的問題(供料中斷、自癒失效)
會因為沒人在量而長期潛伏。

### 網路 RTT 參考值(2026-07-21 量,仍有效但別當成行情延遲)

| 鏈路 | 平均 | 抖動 | 最差 |
|---|---|---|---|
| Server → 券商 API | 5.7 ms | 2.0 ms | 12.3 ms |
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
