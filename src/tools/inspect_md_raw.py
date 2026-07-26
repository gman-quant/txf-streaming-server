"""
txf-md-raw 檢查器(唯讀)
------------------------------------------------------------------
用途:2026-07-26 上線 `txf-md-raw` 之後,一次回答五個未知數 ——

  1. 資料到底有沒有進去(順帶確認 idempotence PID 的 warning 只是一次性)
  2. **Quote 的觸發節奏**:委託簿變動就發(≈ bidask 量級)還是成交才發(≈ tick 量級)?
     → 決定要不要繼續保留 R1/R2 的 BidAsk 訂閱
  3. **Shioaji 到底給不給微秒**:交易所 INFORMATION-TIME 給到微秒,
     protobuf 路徑被我們自己 int(ts*1000) 截掉;raw 路徑已確認不截斷,所以看到的就是真相
  4. `first_derived_*`(衍生一檔 = 期交所組合簿的唯一入口)有沒有值、夜盤有沒有值
     ⚠ TAIFEX 規格:委託刪除時揭示價量 **0** → `price == 0` 是「無衍生報價」不是「價格零」
  5. 試撮(simtrade)有沒有錄到 —— crontab 已提早到 08:28 / 14:48 就是為了這個

另外兩個附帶檢查:
  · **`_seq` 斷號 = raw 路徑靜默失敗的次數**。`_emit_raw` 先取號再 produce,
    所以序號被消耗但沒送出 = 斷號。這是目前唯一能偵測到那個「刻意不 re-raise」
    失敗模式的方法(systemd active、viewer 正常、check_feed_alive 也綠燈)。
  · 現有三個 protobuf topic 的 offset 有沒有繼續前進 ——
    驗證新增的 Quote 訂閱**沒有踢掉 Tick 訂閱**(Shioaji 對同合約多型別訂閱無文件)。

用法(Windows dev 端跑,broker 要指伺服器):
    python -m src.tools.inspect_md_raw --broker 192.168.1.50:9092
    python -m src.tools.inspect_md_raw --broker 192.168.1.50:9092 --max 200000

唯讀:只 assign + seek,不 commit offset、不寫任何東西。
"""

import argparse
import sys
from collections import Counter, defaultdict

import orjson
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING

from ..config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TICK_TOPIC, TICK_R2_TOPIC, BIDASK_TOPIC, MD_RAW_TOPIC,
)

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")     # cp950 會炸中文/emoji


def _session_of(hhmm: int) -> str:
    """08:45–13:45 日盤;15:00–05:00 夜盤;其餘是盤前(含試撮)。"""
    if 845 <= hhmm < 1345:
        return "Day"
    if hhmm >= 1500 or hhmm < 500:
        return "Night"
    return "PreOpen"


def _drain(consumer, topic, max_msgs):
    """把單一 partition 從頭讀到 high watermark。回傳 (訊息 list, lo, hi)。"""
    tp = TopicPartition(topic, 0)
    lo, hi = consumer.get_watermark_offsets(tp, timeout=10.0, cached=False)
    if hi <= lo:
        return [], lo, hi
    tp.offset = OFFSET_BEGINNING
    consumer.assign([tp])
    out, empty = [], 0
    while len(out) < max_msgs:
        batch = consumer.consume(num_messages=5000, timeout=2.0)
        if not batch:
            empty += 1
            if empty >= 3:
                break
            continue
        empty = 0
        for m in batch:
            if m.error():
                continue
            out.append(m)
        if out and out[-1].offset() >= hi - 1:
            break
    return out, lo, hi


def main():
    ap = argparse.ArgumentParser(description="txf-md-raw 唯讀檢查器")
    ap.add_argument("--broker", default=KAFKA_BOOTSTRAP_SERVERS,
                    help="Kafka broker(Windows dev 端要指 192.168.1.50:9092)")
    ap.add_argument("--max", type=int, default=1_000_000, help="最多讀幾則")
    ap.add_argument("--topic", default=MD_RAW_TOPIC)
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": "md-raw-inspector-readonly",
        "enable.auto.commit": False,          # 唯讀:不動任何 offset
        "auto.offset.reset": "earliest",
    })

    # ---------- 附帶檢查:現有 protobuf topic 還活著嗎 ----------
    print("=" * 78)
    print("0) 現有 protobuf topic 的水位(驗證 Quote 訂閱沒有踢掉 Tick 訂閱)")
    print("=" * 78)
    for t in (TICK_TOPIC, TICK_R2_TOPIC, BIDASK_TOPIC):
        try:
            lo, hi = consumer.get_watermark_offsets(TopicPartition(t, 0), timeout=10.0, cached=False)
            print(f"  {t:<14} offset {lo:>10,} → {hi:>10,}   ({hi - lo:,} 則)")
        except Exception as e:
            print(f"  {t:<14} ❌ {e}")
    print("  ⚠ 這裡只看得到累計水位。要確認『今天還在前進』,盤中隔幾分鐘再跑一次比對 hi。")

    # ---------- 主體 ----------
    print()
    print("=" * 78)
    print(f"1) {args.topic} 內容")
    print("=" * 78)
    msgs, lo, hi = _drain(consumer, args.topic, args.max)
    print(f"  offset {lo:,} → {hi:,}(共 {hi - lo:,} 則),實際讀入 {len(msgs):,} 則")
    if not msgs:
        print("\n  ❌ 沒有任何訊息。可能原因:")
        print("     · producer 還沒在交易時段跑過")
        print("     · _emit_raw 每則都失敗(去看 journalctl 的 'Raw emit error')")
        print("     · idempotence PID 始終沒拿到(啟動時的 GETPID warning 沒恢復)")
        consumer.close()
        return

    rows = []
    bad_json = 0
    for m in msgs:
        try:
            rows.append(orjson.loads(m.value()))
        except Exception:
            bad_json += 1
    if bad_json:
        print(f"  ⚠ {bad_json} 則無法解析為 JSON")

    # ---------- 2) type × role → Quote 節奏 ----------
    print()
    print("=" * 78)
    print("2) 型別 × 商品 分佈 —— **Quote 的節奏**")
    print("=" * 78)
    cnt = Counter((r.get("_type"), r.get("_role")) for r in rows)
    for (t, role), n in sorted(cnt.items(), key=lambda kv: -kv[1]):
        print(f"  {str(t):<8} {str(role):<4} {n:>10,}")
    q1 = cnt.get(("quote", "R1"), 0)
    ba1 = None
    try:
        b_lo, b_hi = consumer.get_watermark_offsets(TopicPartition(BIDASK_TOPIC, 0), timeout=10.0)
        ba1 = b_hi - b_lo
    except Exception:
        pass
    print()
    print(f"  R1 quote = {q1:,} 則。判讀:")
    print("    · 若與 R1 的 bidask 同數量級(數十萬/日)→ **book 節奏**,Quote 可取代 BidAsk")
    print("    · 若與 R1 的 tick 同數量級(數萬/日)  → **trade 節奏**,Quote 取代不了 BidAsk")
    if ba1:
        print(f"    (參考:{BIDASK_TOPIC} 累計 {ba1:,} 則,但那是多日累計,要用同一天比)")

    # ---------- 3) 微秒 ----------
    print()
    print("=" * 78)
    print("3) 時間精度 —— Shioaji 到底給不給微秒")
    print("=" * 78)
    sub_ms = Counter()
    frac_len = Counter()
    for r in rows:
        dtv = r.get("datetime")
        if not isinstance(dtv, str) or "." not in dtv:
            frac_len["(無小數)"] += 1
            continue
        frac = dtv.split(".")[-1]
        frac = "".join(ch for ch in frac if ch.isdigit())
        frac_len[len(frac)] += 1
        if len(frac) >= 6:
            sub_ms[frac[3:6] != "000"] += 1
    print(f"  小數位長度分佈:{dict(frac_len)}")
    tot = sum(sub_ms.values())
    if tot:
        nz = sub_ms.get(True, 0)
        print(f"  次毫秒(第 4–6 位)非零的比例:{nz:,}/{tot:,} = {100*nz/tot:.2f}%")
        print("  → 接近 0% 代表 **Shioaji 只給到毫秒**(交易所其實給到微秒,是 SDK 或上游截的)")
        print("  → 明顯 > 0% 代表 **真的有微秒**,值得在 B2 匯出器完整保留")
    else:
        print("  (沒有可判讀的小數位)")

    # ---------- 4) 衍生一檔 ----------
    print()
    print("=" * 78)
    print("4) 衍生一檔 first_derived_*(組合簿的唯一入口)")
    print("=" * 78)
    for role in ("R1", "R2"):
        for typ in ("bidask", "quote"):
            sub = [r for r in rows if r.get("_role") == role and r.get("_type") == typ]
            if not sub:
                continue
            by_sess = defaultdict(lambda: [0, 0])   # [有值, 總數]
            for r in sub:
                dtv = r.get("datetime") or ""
                try:
                    hhmm = int(dtv[11:13]) * 100 + int(dtv[14:16])
                except Exception:
                    hhmm = -1
                s = _session_of(hhmm)
                bidp = r.get("first_derived_bid_price")
                askp = r.get("first_derived_ask_price")
                has = False
                for v in (bidp, askp):
                    try:
                        if v is not None and float(v) != 0.0:
                            has = True
                    except (TypeError, ValueError):
                        pass
                by_sess[s][1] += 1
                if has:
                    by_sess[s][0] += 1
            print(f"  {role} {typ}:")
            for s in ("PreOpen", "Day", "Night"):
                if s in by_sess:
                    h, n = by_sess[s]
                    print(f"      {s:<8} 有衍生報價 {h:>8,} / {n:>8,}  ({100*h/n if n else 0:5.1f}%)")
    print("  ⚠ price==0 是『無衍生報價』(TAIFEX:委託刪除時揭示 0),不是價格為零。")

    # ---------- 5) 試撮 ----------
    print()
    print("=" * 78)
    print("5) 試撮 simtrade(crontab 提早到 08:28 / 14:48 就是為了它)")
    print("=" * 78)
    sim = [r for r in rows if r.get("simtrade") in (True, 1)]
    print(f"  simtrade 訊息 {len(sim):,} 則 / 全部 {len(rows):,} 則")
    if sim:
        times = sorted(str(r.get("datetime", ""))[11:19] for r in sim if r.get("datetime"))
        print(f"  時間範圍:{times[0]} ~ {times[-1]}")
        print(f"  ✅ 試撮有錄到(每 5 秒一則,日盤 08:30–08:45、夜盤 14:50–15:00)")
    else:
        print("  ⚠ 沒有試撮訊息 —— 若 producer 已在 08:28/14:48 啟動過,要查為什麼")

    # ---------- 6) _seq 斷號 = 靜默失敗 ----------
    print()
    print("=" * 78)
    print("6) _seq 連續性 —— 偵測 raw 路徑的靜默失敗")
    print("=" * 78)
    seqs = [r.get("_seq") for r in rows if isinstance(r.get("_seq"), int)]
    gaps, resets = 0, 0
    for a, b in zip(seqs, seqs[1:]):
        if b == a + 1:
            continue
        if b < a:
            resets += 1          # producer 重啟,計數器歸零 —— 正常
        else:
            gaps += b - a - 1
    print(f"  序號筆數 {len(seqs):,},計數器歸零 {resets} 次(= producer 重啟次數,正常)")
    if gaps:
        print(f"  ❌ 斷號 {gaps:,} 個 = **_emit_raw 失敗了這麼多次**(取號後 produce 沒成功)")
        print("     去 journalctl 找 'Raw emit error'(每 500 次節流印一次)")
    else:
        print("  ✅ 無斷號 —— raw 路徑沒有靜默失敗")

    # ---------- 7) 欄位集合(B2 匯出器的 schema 依據) ----------
    print()
    print("=" * 78)
    print("7) 各型別實際出現的欄位(B2 匯出器要據此定 schema 與斷言)")
    print("=" * 78)
    keys_by_type = defaultdict(Counter)
    for r in rows:
        for k in r:
            keys_by_type[r.get("_type")][k] += 1
    for t, kc in keys_by_type.items():
        n = sum(1 for r in rows if r.get("_type") == t)
        always = sorted(k for k, c in kc.items() if c == n)
        sometimes = sorted(k for k, c in kc.items() if c != n)
        print(f"  [{t}] {n:,} 則")
        print(f"      恆存在({len(always)}):{always}")
        if sometimes:
            print(f"      ⚠ 非恆存在:{[(k, kc[k]) for k in sometimes]}")

    consumer.close()
    print()
    print("=" * 78)
    print("完成(唯讀,未 commit 任何 offset)")
    print("=" * 78)


if __name__ == "__main__":
    main()
