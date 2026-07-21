# 從空白機器建置 Kafka + TXF producer

依生產機(`home-lab`,Ubuntu 24.04 / i7-4770 / 8 GB RAM)2026-07-21 實況整理。
目標:新機器照著跑,就能複製出同一套行情管線。

---

## 0. 前提

- Ubuntu 22.04+ / 24.04
- 一顆能連外的網路(Shioaji API 需要)
- 磁碟:Kafka 資料以現況估約 **13 GB / 232 天**,建議預留 **50 GB 以上**

---

## 1. 安裝 Java 與 Kafka

```bash
sudo apt update && sudo apt install -y openjdk-21-jdk
java -version          # 應為 21.x

KAFKA_VER=2.13-4.0.0   # 生產機實測版本(Kafka 4.0.0,Scala 2.13)
wget https://downloads.apache.org/kafka/4.0.0/kafka_${KAFKA_VER}.tgz
sudo tar xzf kafka_${KAFKA_VER}.tgz -C /opt
sudo mv /opt/kafka_${KAFKA_VER} /opt/kafka
sudo useradd -r -d /opt/kafka -s /sbin/nologin kafka
sudo mkdir -p /opt/kafka/data/kraft-combined-logs /opt/kafka/data/kraft-metadata-logs
sudo chown -R kafka:kafka /opt/kafka
```

## 2. 設定 broker

```bash
sudo cp deploy/kafka/server.properties.template /opt/kafka/config/server.properties
sudo vi /opt/kafka/config/server.properties
```

**一定要改的三處:**

| 設定 | 說明 |
|---|---|
| `node.id` | 單機保持 `1` |
| `controller.quorum.bootstrap.servers` / `listeners` / `advertised.listeners` | 把 `192.168.1.50` 換成新機器的內網 IP |
| `EXTERNAL` 監聽器 | **建議整組刪掉**(見安全提醒) |

### 🔴 安全提醒:EXTERNAL 監聽器無認證

範本裡的 `EXTERNAL://0.0.0.0:19092` 是 **PLAINTEXT、零認證**
(`sasl` / `authorizer` 設定行數 = 0)。若該埠對網際網路開放,任何人都能:

- 讀取你全部的行情資料
- **寫入偽造 tick**(viewer 會顯示假價格,且你不會知道)
- 刪除 topic 與資料

**只在內網用的話,把 `EXTERNAL` 那三處(`listeners` / `advertised.listeners` /
`listener.security.protocol.map`)整組刪掉就好。** 真的需要對外,請加 SASL/SSL +
防火牆白名單,別靠「沒人知道這個埠」。

## 3. 初始化 KRaft 並啟動

```bash
KAFKA_CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
  -t "$KAFKA_CLUSTER_ID" -c /opt/kafka/config/server.properties

sudo cp deploy/kafka/kafka.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now kafka
systemctl status kafka
```

> ⚠️ `kafka.service` 裡的 `Environment="JAVA_HOME=..."` 路徑要對應實際安裝的 JDK
> (生產機是 `/usr/lib/jvm/temurin-21-jdk-amd64`,用 apt 裝 openjdk 的話通常是
> `/usr/lib/jvm/java-21-openjdk-amd64`)。
> unit 裡的 `Wants=shioaji_kafka_bridge.service` 是**已退役的舊服務**(生產機上為
> inactive),新機器可刪掉那行。

## 4. 建立 topic —— **含保留設定覆寫,這步不能漏**

`server.properties` 的 `log.retention.hours=168`(7 天)會被下面的 **topic 層級設定覆寫**。
生產機實際生效的是**一年**——漏掉這步,資料會在 7 天後開始被砍。

```bash
K=/opt/kafka/bin
B=localhost:9092
YEAR_MS=31536000000     # 365 天

# 近月逐筆(TXFR1)—— 一年 / 10 GB
$K/kafka-topics.sh --bootstrap-server $B --create --topic txf-tick --partitions 1 --replication-factor 1
$K/kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name txf-tick --alter \
  --add-config retention.ms=$YEAR_MS,retention.bytes=10737418240,segment.bytes=1073741824

# 次月逐筆(TXFR2)—— 一年 / 1 GB;viewer 的 basis 與 smart-rollover 依賴它
$K/kafka-topics.sh --bootstrap-server $B --create --topic txfr2-tick --partitions 1 --replication-factor 1
$K/kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name txfr2-tick --alter \
  --add-config retention.ms=$YEAR_MS,retention.bytes=1073741824,segment.ms=2592000000

# 委託簿 —— 一年 / 20 GB
$K/kafka-topics.sh --bootstrap-server $B --create --topic txf-bidask --partitions 1 --replication-factor 1
$K/kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name txf-bidask --alter \
  --add-config retention.ms=$YEAR_MS,retention.bytes=21474836480
```

**驗證(務必用 `--all`,不加看不到繼承後的生效值):**
```bash
for t in txf-tick txfr2-tick txf-bidask; do
  echo "== $t"; $K/kafka-configs.sh --bootstrap-server $B --entity-type topics \
    --entity-name $t --describe --all | tr ' ' '\n' | grep -E "^retention\.(ms|bytes)="
done
```

### ⚠️ 兩道閘先到先砍

保留期與大小上限**任一到達就刪**。生產機實測 `txf-bidask` 約 **52 MB/天**
→ 一年約 19 GB,**逼近 20 GB 上限**。成交量放大時,**大小閘會先於時間閘觸發,
把還沒滿一年的資料靜默砍掉**。要留滿一年就把 `retention.bytes` 調大。

## 5. 部署 producer

見 repo 根 `README.MD` 的「在新主機重建」章節。摘要:

```bash
git clone https://github.com/gman-quant/txf-streaming-server.git
cd txf-streaming-server
cp .env.example .env && vi .env       # 填 Shioaji 金鑰與 Kafka 位址
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
sudo useradd -r -m -d /home/shioaji_svc shioaji_svc
sudo cp deploy/txf-producer.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now txf-producer
```

⚠️ `txf-producer.service` 的 `CPUAffinity` 要依新機器的 CPU 拓撲調整:
```bash
cat /sys/devices/system/cpu/cpu2/topology/thread_siblings_list   # 找 HT 兄弟,兩個都綁
```

## 6. 驗收

```bash
deploy/check_feed_alive.sh            # 交易時段內應回傳 0
journalctl -u txf-producer -f
```
