# 從空白機器建置 Kafka + TXF producer

依生產機(`home-lab`,Ubuntu 24.04)實況整理;照著跑即可複製同一套行情管線。
Kafka **4.0.0(KRaft,無 ZooKeeper)**;磁碟預留 **≥ 50 GB**(現況 ~13 GB / 232 天)。

---

## 1. 安裝 Java + Kafka

```bash
sudo apt update && sudo apt install -y openjdk-21-jdk
KAFKA_VER=2.13-4.0.0
wget https://downloads.apache.org/kafka/4.0.0/kafka_${KAFKA_VER}.tgz
sudo tar xzf kafka_${KAFKA_VER}.tgz -C /opt && sudo mv /opt/kafka_${KAFKA_VER} /opt/kafka
sudo useradd -r -d /opt/kafka -s /sbin/nologin kafka
sudo mkdir -p /opt/kafka/data/kraft-combined-logs && sudo chown -R kafka:kafka /opt/kafka
```

## 2. 設定 broker

```bash
sudo cp deploy/kafka/server.properties.template /opt/kafka/config/server.properties
sudo vi /opt/kafka/config/server.properties
```

**核心只有兩個要填**(把範本的 `192.168.1.50` 換成本機內網 IP):

| 欄位 | 作用 |
| :--- | :--- |
| `listeners` | broker 綁哪些位址收連線 |
| `advertised.listeners` | broker **告訴客戶端**要連的位址 —— 填錯遠端就連不進來 |

`node.id=1`、`controller.quorum.bootstrap.servers`、`log.dirs` 保持範本值即可。

> 🔴 **內網用請把 `EXTERNAL` 監聽器整組刪掉**(`listeners` / `advertised.listeners` /
> `listener.security.protocol.map` 三處的 `EXTERNAL` 項)。範本的 `EXTERNAL://…:19092`
> 是 **PLAINTEXT、零認證** —— 留著就是「哪天防火牆或路由改壞,就變成對全世界開放的
> 無密碼 broker(可被讀取行情、**寫入偽造 tick**、刪 topic)」。真要對外請加 SASL/SSL +
> 防火牆白名單,別靠「沒人知道這個埠」。

## 3. 初始化 KRaft 並啟動

```bash
CID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format -t "$CID" -c /opt/kafka/config/server.properties
sudo cp deploy/kafka/kafka.service /etc/systemd/system/     # 內含 JAVA_HOME,路徑要對應實際 JDK
sudo systemctl daemon-reload && sudo systemctl enable --now kafka
systemctl status kafka
```

## 4. 防火牆 —— 不能省(Kafka 本身零認證,安全全靠這層)

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from <消費端IP> to any port 9092 proto tcp   # 只放行要連的機器,逐台加
sudo ufw enable && sudo ufw status verbose                  # 確認 Default: deny (incoming)
```

> 照本文件裝一台新機、卻略過這步 = 一台無密碼、對全世界開放的 broker。

## 5. 建立 topic + 保留設定(不能漏)

`server.properties` 的 `log.retention.hours=168`(7 天)會被下面的 **topic 層級設定覆寫**成一年;
漏掉這步,資料會在 7 天後開始被砍。

```bash
K=/opt/kafka/bin; B=localhost:9092; YEAR=31536000000        # 365 天(ms)
for t in txf-tick txfr2-tick txf-bidask; do
  $K/kafka-topics.sh --bootstrap-server $B --create --topic $t --partitions 1 --replication-factor 1
done
# 逐 topic 覆寫保留(bytes 為大小上限)
$K/kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name txf-tick   --alter --add-config retention.ms=$YEAR,retention.bytes=10737418240
$K/kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name txfr2-tick --alter --add-config retention.ms=$YEAR,retention.bytes=1073741824
$K/kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name txf-bidask --alter --add-config retention.ms=$YEAR,retention.bytes=21474836480

# 驗證(務必 --all,不加看不到繼承後的生效值)
for t in txf-tick txfr2-tick txf-bidask; do
  echo "== $t"; $K/kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name $t --describe --all \
    | tr ' ' '\n' | grep -E "^retention\.(ms|bytes)="
done
```

> ⚠️ **保留期與大小上限任一到達就砍。** `txf-bidask` 約 **52 MB/天** → 一年 ~19 GB,逼近 20 GB 上限;
> 成交量放大時,**大小閘會先於時間閘、把還沒滿一年的資料靜默砍掉**。要留滿一年就把 `retention.bytes` 調大。

## 6. 部署 producer + 驗收

producer 部署見 repo 根 `README.md` 的「部署」章節(含 systemd unit 與 crontab)。驗收:

```bash
deploy/check_feed_alive.sh          # 交易時段內應回 0
journalctl -u txf-producer -f
```
