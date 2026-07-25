# Wire Schema — 跨 repo 共用的資料契約

`txf_data.proto` 定義的訊息格式,由 `txf-streaming-server` 編碼、
`txf-quant-platform` 與 `txf-gale-engine` 解碼。**改動的爆炸半徑是整個 workspace**,
動之前先列出所有下游消費者。

寫消費端程式時看這份;只是要**部署**的話不必讀。

---

## 通則

- Protobuf 3。
- **價格一律 Scaled Integer(實際價 × 10000)**,還原時 ÷10000。
  用整數而非浮點是為了金融資料的絕對精度。
- 時間一律 `timestamp_ms` = Unix Epoch 毫秒,**值是交易所時間**(非收到時間)。

## Tick (`txf.Tick`)

| Field | Type | 說明 |
| :--- | :--- | :--- |
| `code` | string | 商品代碼(如 `TXFH6`) |
| `timestamp_ms` | int64 | 交易所時間(ms) |
| `close` | int64 | 成交價 ×10000 |
| `volume` | int32 | 單筆量 |
| `tick_type` | int32 | 1 = 外盤 / 2 = 內盤 |
| `total_volume` | int32 | 當盤累計量 |
| `underlying_price` | int64 | 標的(現貨)價 ×10000 |

> 🔴 **`total_volume` 不能拿來偵測遺漏。** 曾假設
> `total_volume[i] − total_volume[i-1] == volume[i]`,**該假設不成立**,會產生大量假警報
> (2026-03-03 誤報 659 次不連續,而同日與資料湖對帳其實是零遺漏)。
> 要驗完整性請用 `src/tools/measure_load.py verify` 跟資料湖對帳。

## BidAsk (`txf.BidAsk`)

| Field | Type | 說明 |
| :--- | :--- | :--- |
| `code` | string | 商品代碼 |
| `timestamp_ms` | int64 | 交易所時間(ms) |
| `bid_total_vol` / `ask_total_vol` | int32 | 委託總量 |
| `bid_price` / `ask_price` | repeated int64 | 五檔價 ×10000 |
| `bid_volume` / `ask_volume` | repeated int32 | 五檔量 |
| `diff_bid_vol` / `diff_ask_vol` | repeated int32 | 五檔掛單變化量(策略訊號用) |

## Topic 路由

| 來源 | Topic |
|---|---|
| TXFR1 tick | `txf-tick` |
| TXFR2 tick | `txfr2-tick` |
| TXFR1 BidAsk | `txf-bidask` |

- **TXFR2 沒有 BidAsk**(只訂 tick)。
- 🔴 **R2 的 `quote.code` 是真實月份碼**(如 `TXFI6`),**不是 `"TXFR2"`** ——
  路由必須同時比對 `target_code`。這是修過的 bug(commit `6441f48`),別退回。
- 下游假設每個 topic **單一 partition**(data-lake 的舊 reader 曾硬編 `TopicPartition(topic, 0)`)。
- `simtrade == 1`(試撮)事件會被過濾,不進 Kafka。

## 訊息大小(實測)

Tick 約 33 B、BidAsk 約 118 B。**BidAsk 佔全部訊息量的 86%** —— 規劃磁碟與保留期時
務必用合計值,只算 tick 會低估八倍。

## 改了 `.proto` 之後

```bash
python -m grpc_tools.protoc -I protos --python_out=src protos/txf_data.proto
```

生成的 `*_pb2.py` 是 **committed** 的,重新生成會出現 tracked diff。
下游三個 repo 各自有自己的一份,改 schema 要同步更新。
