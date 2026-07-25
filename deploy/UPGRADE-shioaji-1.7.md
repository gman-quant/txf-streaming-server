# 升級 runbook:shioaji 1.3.2 → 1.7 + Python 3.13 + systemd unit 三修

一次收盤窗把 producer 換成新版。

> 🔴 **只在收盤窗做**(週六 05:00 後,或平日 13:45–15:00 盤間)。
> 盤中重啟 = **bidask 永久破洞**(Shioaji 無歷史 API,斷幾秒補不回)。

本次一起帶入的變更(目標組合 **Python 3.13 + shioaji 1.7** 已在 dev 端到端驗證過:
真登入 → 合約 → 訂閱 → protobuf → Kafka → 解碼 全部正確):

- shioaji **1.3.2 → 1.7**(login 拿掉 contracts_timeout、合約改 `api.contracts.futures()`、回調 1-arg…,程式碼已改好)
- Python **3.12 → 3.13**(`.python-version` 鎖定;由 uv 管理,**不動系統 python**)
- systemd unit 三修:`CPUAffinity=2 6`、`Restart=always`、`StartLimitIntervalSec=600s`

> 前提:`shioaji_svc` 要有 `uv`。沒有先裝:
> ```bash
> sudo -u shioaji_svc bash -lc 'curl -LsSf https://astral.sh/uv/install.sh | sh'
> ```
>
> ⚠️ **PATH 陷阱(2026-07-25 實戰踩到)**:uv 裝在 `/home/shioaji_svc/.local/bin`,
> 而 **`sudo -u shioaji_svc uv …` 不會載入該使用者的登入環境 → `uv: command not found`**。
> 下面所有 uv 指令都用 **`sudo -u shioaji_svc bash -lc '…'`** 包起來(或改用絕對路徑
> `/home/shioaji_svc/.local/bin/uv`)。

> ✅ **本 runbook 已於 2026-07-25(週六 08:0x,收盤窗)在 `home-lab` 實際執行完畢**:
> Python 3.13.14 + shioaji 1.7.0,登入 / 合約 / 訂閱三關全過、**零 DeprecationWarning**,
> unit 三修已生效(`Restart=always` / `StartLimitIntervalUSec=10min` / `CPUAffinity=2 6`)。
> 舊版備份於 `/home/shioaji_svc/txf-streaming-server.bak.2026-07-25`(回滾點)。

---

## Part 1 — producer(主要)

```bash
D=/home/shioaji_svc/txf-streaming-server                 #(下面都用絕對路徑,免 cwd 陷阱)
sudo systemctl stop txf-producer                        # 0. 確認已收盤再停

# 1. 備份舊目錄 = 回滾點(舊 code + 舊 venv + 舊 .env 原封不動),再重新 clone
sudo mv $D $D.bak.$(date +%F)
sudo -u shioaji_svc bash -lc "cd /home/shioaji_svc && git clone https://github.com/gman-quant/txf-streaming-server.git"
sudo -u shioaji_svc bash -lc "cp $D.bak.$(date +%F)/.env $D/.env"    # 沿用舊 .env(金鑰 + topic)
# 驗 .env 有搬到(**只印鍵名不印值**,別把金鑰吐到畫面/日誌上):
sudo -u shioaji_svc bash -lc "grep -v '^#' $D/.env | grep '=' | cut -d= -f1"
#   要看到 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY / KAFKA_BOOTSTRAP_SERVERS /
#   TICK_TOPIC / TICK_R2_TOPIC / BIDASK_TOPIC ——
#   ⚠ TICK_R2_TOPIC 缺了會讓每筆 R2 tick 發送失敗(config.py 讀它且無預設值)

# 2. 建 venv + 裝依賴(注意 bash -lc,見上面的 PATH 陷阱)
sudo -u shioaji_svc bash -lc "cd $D && uv python install 3.13"
sudo -u shioaji_svc bash -lc "cd $D && uv venv"                      # 讀 .python-version → 3.13
sudo -u shioaji_svc bash -lc "cd $D && uv pip install -r requirements.txt"
sudo -u shioaji_svc bash -lc "cd $D && .venv/bin/python -c 'import sys, shioaji; print(sys.version.split()[0], shioaji.__version__)'"
#   → 3.13.x 1.7.x

# 3. 套修好的 unit(三修都在裡面)。**先 diff 再蓋** —— 確認生產機沒有 repo 未收錄的在地調整
sudo cp /etc/systemd/system/txf-producer.service /etc/systemd/system/txf-producer.service.bak.$(date +%F)
sudo diff /etc/systemd/system/txf-producer.service $D/deploy/txf-producer.service
#   預期只差三修 + 註解。若 ExecStart/WorkingDirectory/User/Requires/ExecStartPre 有差 → 停下來人工確認
sudo cp $D/deploy/txf-producer.service /etc/systemd/system/
sudo systemctl daemon-reload
systemctl show txf-producer -p Restart -p StartLimitIntervalUSec -p CPUAffinity
#   → Restart=always / StartLimitIntervalUSec=10min / CPUAffinity=2 6

# 4. Smoke 測試:手動啟動,驗「登入 → 合約 → 訂閱」三關
sudo systemctl start txf-producer
journalctl -u txf-producer -n 40 --no-pager
#    要看到:Kafka Producer Initialized / Login & Contracts Loaded /
#            Found Near Month (TXFR1) / Found Next Month (TXFR2) /
#            Subscribed R1 (Tick + BidAsk) / Subscribed R2 (Tick only) / Service is running
#    ⚠ 同時確認**零 DeprecationWarning** —— 有的話代表還有 1.3.x 舊 API 沒遷移乾淨

# 5. 收尾:停掉,交還給 crontab(收盤窗做的話沒行情,留著跑只是掛個閒置 broker session)
sudo systemctl stop txf-producer
systemctl is-active txf-producer                        # → inactive
```

> 📌 收盤窗做這個升級時,**smoke 測試只證明得了前三關**(登入/合約/訂閱)——
> tick → protobuf → Kafka 那段沒有行情可驗。

**收盤也能驗後兩關**(合成報價,**只寫測試 topic**,不必等開盤):

```bash
sudo -u shioaji_svc bash -lc 'cd /home/shioaji_svc/txf-streaming-server && .venv/bin/python -m src.tools.verify_pipeline'
```

驗:shioaji 模型欄位是否還在(升版改名會被抓到)、protobuf 序列化、Kafka 投遞、
**R2 路由**(`quote.code` 是真實月份碼非 `TXFR2`)、simtrade 過濾、位元組解得回原值。
⚠️ 唯一驗不到的是「shioaji 是否真的以 1-arg 呼叫回調」——那需要真實行情。
腳本會斷言測試 topic 與生產 topic 無交集,有交集直接拒跑。

**驗收(下一個開盤日)**

```bash
journalctl -u txf-producer -n 20 --no-pager    # crontab 08:40 自動啟動後,確認正常
/home/shioaji_svc/txf-streaming-server/deploy/check_feed_alive.sh; echo "exit=$?"
#    → exit=0 表示 Kafka offset 有前進 = 行情真的流進去了(收盤時零 tick 是正常,腳本會自己跳過)
```

順便確認下游(platform viewer / gale / data-lake)都有吃到資料。

**回滾**(新版有問題 → 換回舊 code + 舊 venv 1.3.2,原封不動)

```bash
sudo systemctl stop txf-producer
cd /home/shioaji_svc && sudo rm -rf txf-streaming-server
sudo mv txf-streaming-server.bak.* txf-streaming-server
sudo systemctl start txf-producer              # 新 unit 的 CPUAffinity/Restart 與舊 code 相容,可留
```

---

## Part 2 —(選配)刪 Kafka EXTERNAL 監聽器

安全收尾:`EXTERNAL://…:19092` 是 **PLAINTEXT、零認證**,目前只靠 ufw 擋著。
producer 與所有下游都只用 **INTERNAL 9092**,刪掉零影響。需 `restart kafka`(同一收盤窗)。

```bash
sudo cp /opt/kafka/config/server.properties{,.bak}
sudo vi /opt/kafka/config/server.properties
#   從這三行各刪掉 EXTERNAL 那一項:
#     listeners=                     …刪 EXTERNAL://0.0.0.0:19092,
#     advertised.listeners=          …刪 EXTERNAL://<你的公網IP>:19092,
#     listener.security.protocol.map=…刪 EXTERNAL:PLAINTEXT,
sudo systemctl restart kafka && sudo systemctl status kafka
```

---

## 完成後

下個交易時段觀察 `check_feed_alive.sh` 與下游(platform / gale / data-lake)是否正常收到行情。
穩定數日後刪備份:`sudo rm -rf /home/shioaji_svc/txf-streaming-server.bak.*`
