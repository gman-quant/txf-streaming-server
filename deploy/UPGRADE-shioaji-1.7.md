# 升級 runbook:shioaji 1.3.2 → 1.7 + Python 3.13 + systemd unit 三修

一次收盤窗把 producer 換成新版。

> 🔴 **只在收盤窗做**(週六 05:00 後,或平日 13:45–15:00 盤間)。
> 盤中重啟 = **bidask 永久破洞**(Shioaji 無歷史 API,斷幾秒補不回)。

本次一起帶入的變更(目標組合 **Python 3.13 + shioaji 1.7** 已在 dev 端到端驗證過:
真登入 → 合約 → 訂閱 → protobuf → Kafka → 解碼 全部正確):

- shioaji **1.3.2 → 1.7**(login 拿掉 contracts_timeout、合約改 `api.contracts.futures()`、回調 1-arg…,程式碼已改好)
- Python **3.12 → 3.13**(`.python-version` 鎖定;由 uv 管理,**不動系統 python**)
- systemd unit 三修:`CPUAffinity=2 6`、`Restart=always`、`StartLimitIntervalSec=600s`

> 前提:`shioaji_svc` 要有 `uv`。沒有先裝:`curl -LsSf https://astral.sh/uv/install.sh | sh`

---

## Part 1 — producer(主要)

```bash
sudo systemctl stop txf-producer                        # 0. 確認已收盤再停

cd /home/shioaji_svc                                    # 1. 重新 clone(避開舊 stale .git)
sudo mv txf-streaming-server txf-streaming-server.bak.$(date +%F)    # 備份 = 回滾點
sudo -u shioaji_svc git clone https://github.com/gman-quant/txf-streaming-server.git
cd txf-streaming-server
sudo -u shioaji_svc cp ../txf-streaming-server.bak.*/.env .env       # 沿用舊 .env(金鑰 + topic)

sudo -u shioaji_svc uv python install 3.13              # 2. 取得 3.13(.python-version 已鎖)
sudo -u shioaji_svc uv venv                             #    建 .venv(Python 3.13)
sudo -u shioaji_svc uv pip install -r requirements.txt  #    裝依賴(含 shioaji 1.7)
.venv/bin/python --version                              #    → 3.13.x
.venv/bin/python -c "import shioaji; print(shioaji.__version__)"     #    → 1.7.x

sudo cp deploy/txf-producer.service /etc/systemd/system/    # 3. 套修好的 unit(三修都在裡面)
sudo systemctl daemon-reload

sudo systemctl start txf-producer                       # 4. 啟動
journalctl -u txf-producer -f
#    要看到:Login OK / Found Near Month (TXFR1) / Found Next Month (TXFR2) / Subscribed R1,R2
```

**驗收**

```bash
systemctl show txf-producer -p CPUAffinity     # 確認 2 6
deploy/check_feed_alive.sh                     # 開盤後回 0(收盤時零 tick 是正常)
```

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
