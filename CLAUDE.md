# txf-streaming-server — AI Agent 指南

Live 行情 producer:Shioaji → Protobuf → Kafka。**生產環境在 Ubuntu 192.168.1.50**(systemd `txf-producer.service` + crontab 隨 TAIFEX 時段啟停);這個 Windows checkout 是開發鏡像。下游四個消費者:txf-quant-platform/stable(txf-tick、txfr2-tick)、txf-gale-engine(txf-tick、txf-bidask)、txf-data-lake(kafka_reader 回補)。

## 紅線

1. **絕不隨便啟動 producer**(`python -m src.txf_producer`):它做真實 Shioaji 登入,**同一人併發連線有配額** —— 本地跑一個可能把生產 session 踢下線(shioaji.log 記錄過兩次 451 Too Many Connections)。
2. `protos/txf_data.proto` 是**四個 repo 共用的 wire 契約**(價格 ×10000 scaled int64):改 schema/topic 名的爆炸半徑是全 workspace,動之前列出所有下游。
3. `.env` 含 API 金鑰(gitignored、未進 git 史,僅存在本機磁碟);其 broker=localhost:9092 是**伺服器本機值**,在這台 PC 測試要指 192.168.1.50:9092,但**別 commit broker 變更**(.env 本來就 per-machine)。
4. `src/txf_producer.py` 以 `simulation=True` 登入(committed 狀態如此)——生產伺服器實際跑什麼無法從本 repo 驗證。**別擅自「修正」這個 flag**,問用戶。

## 事實

- 執行必須是模組形式:`python -m src.txf_producer`(相對 import;直接跑檔案會 ImportError)。venv Python 3.12。
- Topic 路由:TXFR1 tick→txf-tick、TXFR2 tick→txfr2-tick(**R2 無 BidAsk**)、R1 BidAsk→txf-bidask。R2 的 quote.code 是真實合約碼(如 TXFG6)非 "TXFR2",路由要同時比對 target_code(commit 6441f48 修的 bug,別退回)。
- simtrade==1(試撮)事件被過濾,不進 Kafka。
- producer.poll(0) 刻意移到 100ms 背景任務 —— **別加回熱路徑**(GIL 阻塞,commit 0ad6332 的刻意移除)。
- 下游假設 topic **單一 partition**(data-lake kafka_reader 硬編 TopicPartition(topic, 0))。
- protobuf 重生成:`python -m grpc_tools.protoc -I protos --python_out=src protos/txf_data.proto`(生成檔是 committed 的,重生成會出現 tracked diff)。
- README 的 .env 範本已於 2026-07-04 補上 `TICK_R2_TOPIC`(src/config.py 讀取、無預設值,缺了會讓每筆 R2 tick 發送失敗);效能數字(8μs/53萬 msg/s)是合成基準非 live 規格。
- 部署程序(程式碼如何上 192.168.1.50、伺服器目前在哪個 commit)**無文件、無法從本機驗證** —— 涉及伺服器的問題直接問用戶。
