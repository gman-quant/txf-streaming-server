"""負載 / 延遲 / 資料完整性實測 —— 從 Kafka 既有資料量,不跑合成 benchmark。

為什麼不用 microbenchmark:`perf_hotpath.py` 只量 protobuf build+serialize,
不含 Kafka produce、不含回調開銷、不含 GIL 競爭,而且用的是假資料。
真實負載、真實延遲、真實遺漏率**全都已經寫在 Kafka 裡了**,直接量它。

兩層設計(成本差三個數量級,所以分開):

  Tier 1  `scan`    —— **零消費**。只用 offsets_for_times 查位移,以「小時」為粒度
                       掃過全部歷史,得到每日總量、成長趨勢、以及**最尖峰的那幾個小時**。
                       8 個月 × 3 topic 幾十秒跑完,不搬任何訊息。

  Tier 2  `profile` —— 真的消費指定某一天,得到:
                       • 100ms 桶的**瞬時峰值**(1 秒平均會藏住突發)
                       • **端到端延遲分布**(見下)
                       • **實際遺漏率**(用 Tick.total_volume 跳號偵測)

延遲怎麼算得出來(關鍵前提,換機器要重新確認):
  topic 的 `message.timestamp.type=CreateTime` → Kafka 訊息時間戳 = **producer 端
  produce 當下的本機時鐘**;而 payload 裡的 `timestamp_ms` = **交易所時間**。
  兩者相減 = 「交易所發生 → 我們送進 Kafka」的真實延遲,每一則訊息都自帶。
  ⚠️ 前提:producer 主機要有 NTP 校時(`timedatectl` 看 System clock synchronized)。
     沒校時的話量到的是時鐘偏差,不是延遲。

完整性怎麼驗(`verify` 子命令):
  **跨來源對帳** —— 把 Kafka 的內容跟 `txf-data-lake` 的 parquet 比。兩者是**完全獨立的
  取得路徑**(即時 producer vs Shioaji 歷史 API),對得上就是真的沒漏。
  比對視窗必須用**交易日**(前一日 15:00 夜盤 → 當日 13:45 日盤收),不是日曆日 ——
  湖的檔案就是這樣切的,用日曆日比會多算一段而得到假差異。

  🔴 **已判死的錯誤方法(別再實作一次)**:曾經用
     `total_volume[i] - total_volume[i-1] == volume[i]` 當遺漏偵測 ——
     **這個假設不成立**,會產生大量假警報(2026-03-03 誤報 659 次不連續、1080 口,
     而同日跨來源對帳是 130,548 筆 / 204,052 口**完全一致、零遺漏**)。
     `volume` 不是 `total_volume` 的增量,別再靠它推遺漏。

用法:
    python -m src.tools.measure_load scan --broker 192.168.1.50:9092
    python -m src.tools.measure_load scan --days 90
    python -m src.tools.measure_load profile --date 2026-07-24
    python -m src.tools.measure_load profile --date 2026-07-24 --topic txf-bidask

🔒 唯讀:只建 Consumer、不 produce、不 commit offset(`enable.auto.commit=False`
   且用獨立 group.id),不會干擾任何生產消費者。
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, TopicPartition

from .. import txf_data_pb2
from ..config import KAFKA_BOOTSTRAP_SERVERS

TZ = timezone(timedelta(hours=8))            # Asia/Taipei
DEFAULT_TOPICS = ["txf-tick", "txfr2-tick", "txf-bidask"]


def _consumer(broker: str, group_suffix: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": broker,
        "group.id": f"measure-load-{group_suffix}",
        "enable.auto.commit": False,           # 唯讀:絕不 commit
        "auto.offset.reset": "earliest",
    })


def _fmt(n: int) -> str:
    return f"{n:,}"


# ══════════════════════════════════════════════════════════════════════════
# Tier 1 —— scan(零消費)
# ══════════════════════════════════════════════════════════════════════════
def cmd_scan(args) -> int:
    c = _consumer(args.broker, "scan")
    try:
        print(f"broker = {args.broker}")

        # 先找每個 topic 的可用區間(earliest 的實際時間)
        ranges = {}
        for t in args.topics:
            tp = TopicPartition(t, 0)
            low, high = c.get_watermark_offsets(tp, timeout=15)
            ranges[t] = (low, high)

        now = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
        start = now - timedelta(days=args.days) if args.days else None

        print("\n" + "=" * 74)
        print("Tier 1:每小時粒度掃描(只查位移,不消費任何訊息)")
        print("=" * 74)

        # 對每個 topic 逐小時查位移
        per_topic_hourly: dict[str, list[tuple[datetime, int]]] = {}
        for t in args.topics:
            low, high = ranges[t]
            if high <= low:
                print(f"  {t}: 空 topic,略過")
                continue

            # 找出最早訊息的時間(用 earliest offset 讀一則的時間戳)
            first_ts = _offset_time(c, t, low)
            begin = datetime.fromtimestamp(first_ts / 1000, TZ).replace(
                minute=0, second=0, microsecond=0)
            if start and start > begin:
                begin = start

            hours = []
            cur = begin
            while cur <= now:
                hours.append(cur)
                cur += timedelta(hours=1)

            t0 = time.time()
            offsets = _offsets_for_times(c, t, hours)
            # 每小時訊息數 = 下一小時位移 − 本小時位移
            hourly = []
            for i, h in enumerate(hours):
                o_now = offsets[i]
                o_next = offsets[i + 1] if i + 1 < len(offsets) else high
                if o_now is None:
                    continue
                if o_next is None:
                    o_next = high
                hourly.append((h, max(0, o_next - o_now)))
            per_topic_hourly[t] = hourly
            print(f"  {t}: 查了 {len(hours)} 個小時、{time.time()-t0:.1f}s、"
                  f"共 {_fmt(high - low)} 則")

        # ── 每日總量 ────────────────────────────────────────────────
        daily: dict[str, dict[str, int]] = defaultdict(dict)
        for t, hourly in per_topic_hourly.items():
            per_day: dict[str, int] = defaultdict(int)
            for h, n in hourly:
                per_day[h.strftime("%Y-%m-%d")] += n
            for d, n in per_day.items():
                daily[d][t] = n

        days_sorted = sorted(daily)
        active = [d for d in days_sorted if sum(daily[d].values()) > 0]
        print(f"\n有資料的日子:{len(active)} 天"
              f"({active[0]} ~ {active[-1]})" if active else "\n(無資料)")

        if active:
            totals = {d: sum(daily[d].values()) for d in active}
            avg = sum(totals.values()) / len(totals)
            print(f"日均訊息量:{_fmt(int(avg))} 則")

            print(f"\n── 最忙的 {args.top} 天(依總訊息數)" + "─" * 30)
            print(f"{'日期':<12}{'總計':>12}" +
                  "".join(f"{t:>16}" for t in args.topics))
            for d in sorted(totals, key=totals.get, reverse=True)[:args.top]:
                row = "".join(f"{_fmt(daily[d].get(t,0)):>16}" for t in args.topics)
                print(f"{d:<12}{_fmt(totals[d]):>12}{row}")

            # 月趨勢
            month: dict[str, list[int]] = defaultdict(list)
            for d in active:
                month[d[:7]].append(totals[d])
            print(f"\n── 月趨勢(日均)" + "─" * 44)
            for m in sorted(month):
                v = month[m]
                print(f"  {m}: 日均 {_fmt(int(sum(v)/len(v))):>10}  "
                      f"(最高 {_fmt(max(v))}, {len(v)} 天)")

        # ── 尖峰小時(全 topic 合計)──────────────────────────────
        combined: dict[datetime, int] = defaultdict(int)
        for hourly in per_topic_hourly.values():
            for h, n in hourly:
                combined[h] += n
        if combined:
            print(f"\n── 最尖峰的 {args.top} 個小時(全 topic 合計)" + "─" * 22)
            print(f"{'時間':<20}{'該小時訊息數':>14}{'平均/秒':>12}")
            for h in sorted(combined, key=combined.get, reverse=True)[:args.top]:
                n = combined[h]
                print(f"{h.strftime('%Y-%m-%d %H:00'):<20}{_fmt(n):>14}{n/3600:>12.0f}")
            print("\n💡 上面這幾天/小時就是 Tier 2 該深挖的對象:")
            top_days = sorted({h.strftime('%Y-%m-%d')
                               for h in sorted(combined, key=combined.get,
                                               reverse=True)[:args.top]})
            for d in top_days[:5]:
                print(f"     python -m src.tools.measure_load profile --date {d}")
    finally:
        c.close()
    return 0


def _offsets_for_times(c: Consumer, topic: str,
                       times: list[datetime]) -> list[int | None]:
    """查「某時間點之後的第一個位移」,**一次只問一個時間戳**。

    🔴 絕對不要把多個時間戳塞進同一次 offsets_for_times 呼叫。
       Kafka 的 ListOffsets 請求以 (topic, partition) 為鍵,同一個 partition 重複出現
       會被摺疊 —— 客戶端**把最後一個的答案複製給全部**,不報錯、直接回錯的數字
       (2026-07-25 實測踩到:全史掃描算出日均 460 萬,實際 59 萬,錯 8 倍)。
       逐一查詢每次僅約 1.3 ms,全史一萬七千次也只要 ~25 秒,沒有批次的必要。
    """
    out: list[int | None] = []
    for t in times:
        tp = TopicPartition(topic, 0, int(t.timestamp() * 1000))
        r = c.offsets_for_times([tp], timeout=30)[0]
        out.append(r.offset if r.offset >= 0 else None)
    return out


def _offset_time(c: Consumer, topic: str, offset: int) -> int:
    """讀某個位移上那一則的 Kafka 時間戳。"""
    c.assign([TopicPartition(topic, 0, offset)])
    try:
        for _ in range(40):
            m = c.poll(0.5)
            if m is not None and not m.error():
                return m.timestamp()[1]
    finally:
        c.assign([])
    return int(time.time() * 1000)


# ══════════════════════════════════════════════════════════════════════════
# Tier 2 —— profile(消費指定一天)
# ══════════════════════════════════════════════════════════════════════════
def _sliding_max(stamps: list[int], window_ms: int) -> int:
    """任意 window_ms 視窗內的**最大訊息數**(滑動,非固定桶)。

    為什麼不用固定桶:對齊整秒的桶會把跨邊界的爆量切成兩半而低估峰值
    (例如某秒後半 300 則 + 下一秒前半 300 則,固定桶只會看到兩個 300,
    滑動視窗才會抓到真正的 600)。固定桶的值是**下界**,滑動才是真實觀測最大。
    """
    if not stamps:
        return 0
    st = sorted(stamps)
    best = 0
    i = 0
    for j in range(len(st)):
        while st[j] - st[i] >= window_ms:
            i += 1
        best = max(best, j - i + 1)
    return best


def _fixed_buckets(stamps: list[int], width_ms: int) -> dict[int, int]:
    """對齊的固定桶計數(供對照用;真實峰值請看 _sliding_max)。"""
    out: dict[int, int] = defaultdict(int)
    for t in stamps:
        out[t // width_ms] += 1
    return out


def _pct(sorted_vals: list[int], p: float) -> int:
    if not sorted_vals:
        return 0
    k = min(len(sorted_vals) - 1, int(len(sorted_vals) * p))
    return sorted_vals[k]


def cmd_profile(args) -> int:
    day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=TZ)
    start_ms = int(day.timestamp() * 1000)
    end_ms = int((day + timedelta(days=1)).timestamp() * 1000)
    topics = [args.topic] if args.topic else args.topics
    all_sec: dict[str, dict[int, int]] = {}

    print("=" * 74)
    print(f"Tier 2:{args.date} 深度剖析(消費訊息)")
    print("=" * 74)
    print(f"broker = {args.broker}\n")

    for topic in topics:
        c = _consumer(args.broker, f"profile-{topic}")
        try:
            res = c.offsets_for_times([TopicPartition(topic, 0, start_ms)], timeout=30)
            begin = res[0].offset
            res = c.offsets_for_times([TopicPartition(topic, 0, end_ms)], timeout=30)
            end = res[0].offset
            if begin is None or begin < 0:
                print(f"── {topic}:該日無資料\n")
                continue
            _low, high = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=15)
            if end is None or end < 0:
                end = high
            total = end - begin
            print(f"── {topic}:{_fmt(total)} 則,消費中…")

            c.assign([TopicPartition(topic, 0, begin)])
            is_tick = topic != "txf-bidask"

            stamps: list[int] = []                         # produce 時間戳(算 producer 承受速率)
            ex_stamps: list[int] = []                      # 交易所時間戳(算市場爆量速率)
            lat: list[int] = []                            # 延遲(ms)
            n = 0
            t0 = time.time()

            while n < total:
                m = c.poll(2.0)
                if m is None:
                    break
                if m.error():
                    continue
                kts = m.timestamp()[1]
                if kts >= end_ms:
                    break
                n += 1
                stamps.append(kts)

                if is_tick:
                    t = txf_data_pb2.Tick()
                    t.ParseFromString(m.value())
                    payload_ms = t.timestamp_ms
                else:
                    b = txf_data_pb2.BidAsk()
                    b.ParseFromString(m.value())
                    payload_ms = b.timestamp_ms

                if payload_ms > 0:
                    lat.append(kts - payload_ms)
                    ex_stamps.append(payload_ms)

            elapsed = time.time() - t0
            print(f"   讀完 {_fmt(n)} 則 / {elapsed:.1f}s")

            # 瞬時峰值
            if stamps:
                slide = _sliding_max(stamps, 1000)
                fixed = sorted(_fixed_buckets(stamps, 1000).values(), reverse=True)
                print(f"   峰值(滑動 1 秒視窗,真實觀測最大):{slide:,} 則/秒")
                print(f"        固定 1 秒桶最大 {fixed[0]:,}/秒 "
                      f"(對齊整秒,爆量跨邊界會被切半 → 這是下界)")
                print(f"        滑動 100ms 最大 {_sliding_max(stamps, 100):,} 則")
                if ex_stamps:
                    # 同一批訊息改用**交易所時間**分桶 = 市場真正的爆量速率。
                    # produce 時間會被鏈路延遲抖動抹平(p50 就有上百 ms),
                    # 兩個數字問的是不同問題:producer 要扛多少 vs 市場多快。
                    print(f"        [依交易所時間] 滑動 1 秒最大 "
                          f"{_sliding_max(ex_stamps, 1000):,} 則/秒 = 市場爆量速率")

            # 延遲
            if lat:
                s = sorted(lat)
                print(f"   延遲(交易所→produce,ms):"
                      f"p50 {_pct(s,0.50)}  p95 {_pct(s,0.95)}  "
                      f"p99 {_pct(s,0.99)}  p99.9 {_pct(s,0.999)}  max {s[-1]}")
                neg = sum(1 for v in s if v < 0)
                if neg:
                    print(f"   ⚠️ 有 {neg} 則延遲為負 → 時鐘不同步或交易所時間戳有偏移,"
                          f"延遲數字不可信")

            # 🔴 只在真的有資料時寫入。原本 per_sec 定義在 `if buckets:` 內,
            #    0 則訊息的 topic 不會重新賦值 → 沿用**上一個 topic** 的資料,
            #    合計時被重複計算(2026-07-25 實錯:合計報 469/秒 = 153+153+163,
            #    而 tick+bidask 各自最大僅 153 與 163,合計不可能超過 316)。
            if stamps:
                all_sec[topic] = stamps
            print()
        finally:
            c.close()

    # 跨 topic 合計的瞬時峰值 —— producer 是**單一行程**同時扛三條流,
    # 各 topic 分開看會低估它真正要吞的速率。
    if len(all_sec) > 1:
        merged_stamps = sorted(x for v in all_sec.values() for x in v)
        per_sec_fixed = sorted(_fixed_buckets(merged_stamps, 1000).values())
        print("── 全 topic 合計(producer 實際承受的速率)" + "─" * 24)
        print(f"   峰值(滑動 1 秒):{_sliding_max(merged_stamps, 1000):,} 則/秒")
        print(f"   固定 1 秒桶:最大 {per_sec_fixed[-1]:,}  "
              f"p99 {_pct(per_sec_fixed,0.99):,}  p50 {_pct(per_sec_fixed,0.50):,} /秒")
        print(f"   滑動 100ms 最大:{_sliding_max(merged_stamps, 100):,} 則")
        print()

    print("💡 完整性驗證請跑:")
    print(f"   python -m src.tools.measure_load verify --date {args.date}")
    return 0


def cmd_verify(args) -> int:
    """跨來源對帳:Kafka vs txf-data-lake 的 parquet(兩條獨立取得路徑)。

    **視窗用交易日**(前一日 15:00 → 當日 13:45),與湖的檔案切法一致。
    """
    import glob

    import pyarrow.compute as pc          # pyarrow 已是本 repo 依賴,不必額外裝 polars
    import pyarrow.parquet as pq

    pat = f"{args.lake}/raw_ticks/TXF/**/{args.date}_TXF_ticks.parquet"
    files = glob.glob(pat, recursive=True)
    if not files:
        print(f"❌ 找不到湖檔:{pat}")
        print("   (湖檔以**交易日**命名;非交易日或湖還沒補到該日都會找不到)")
        return 2
    tbl = pq.read_table(files[0], columns=["ts", "volume"])
    lake_n = tbl.num_rows
    lake_vol = int(pc.sum(tbl["volume"]).as_py())
    t_min = pc.min(tbl["ts"]).as_py()
    t_max = pc.max(tbl["ts"]).as_py()

    print("=" * 74)
    print(f"完整性對帳:{args.date}(交易日)")
    print("=" * 74)
    print(f"湖檔視窗:{t_min} ~ {t_max}(前一日夜盤 → 當日日盤收)")

    s_ms = int(t_min.replace(tzinfo=TZ).timestamp() * 1000)
    e_ms = int(t_max.replace(tzinfo=TZ).timestamp() * 1000) + 1000

    c = _consumer(args.broker, "verify")
    try:
        b = c.offsets_for_times([TopicPartition("txf-tick", 0, s_ms)], timeout=30)[0].offset
        if b is None or b < 0:
            print("❌ Kafka 該視窗無資料(可能已被 retention 砍掉)")
            return 1
        c.assign([TopicPartition("txf-tick", 0, b)])
        n = vol = 0
        while True:
            m = c.poll(3.0)
            if m is None or m.error():
                if m is None:
                    break
                continue
            if m.timestamp()[1] >= e_ms:
                break
            t = txf_data_pb2.Tick()
            t.ParseFromString(m.value())
            n += 1
            vol += t.volume
    finally:
        c.close()

    print(f"  Kafka(即時 producer)  : {_fmt(n):>10} 筆  {_fmt(vol):>10} 口")
    print(f"  資料湖(Shioaji 歷史 API): {_fmt(lake_n):>10} 筆  {_fmt(lake_vol):>10} 口")
    print(f"  差異                    : {n-lake_n:+,} 筆  {vol-lake_vol:+,} 口")
    ok = (n == lake_n and vol == lake_vol)
    print("\n" + ("✅ 兩條獨立路徑完全一致 —— 零遺漏" if ok else
                  "⚠️ 有差異 —— 檢查該日是否有 producer 重啟 / 斷線"))
    return 0 if ok else 1


def cmd_stalls(args) -> int:
    """找 produce 時間軸上的靜默窗 —— 只讀 Kafka 時間戳,不解 protobuf(快)。

    用 `txf-bidask` 當探針:它在交易時段幾乎連續有更新,一旦靜默數秒就是異常
    (tick 本來就會有自然空檔,不適合當探針)。

    ⚠️ 盤間休息會被排除:日盤收 13:45→夜盤開 15:00、夜盤收 05:00→日盤開 08:45。
       那些不是停頓。
    """
    d0 = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=TZ)
    d1 = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=TZ)
    thr_ms = int(args.min_gap * 1000)

    print("=" * 74)
    print(f"停頓掃描:{args.start_date} ~ {args.end_date},門檻 {args.min_gap}s")
    print("=" * 74)
    print("(探針 = txf-bidask;盤間休息已排除)\n")

    c = _consumer(args.broker, "stalls")
    found: list[tuple[datetime, float]] = []
    try:
        cur = d0
        while cur <= d1:
            nxt = cur + timedelta(days=1)
            s_ms, e_ms = int(cur.timestamp() * 1000), int(nxt.timestamp() * 1000)
            r = c.offsets_for_times([TopicPartition("txf-bidask", 0, s_ms)], timeout=30)[0]
            if r.offset is None or r.offset < 0:
                cur = nxt
                continue
            c.assign([TopicPartition("txf-bidask", 0, r.offset)])
            prev = None
            window: deque[int] = deque()      # 前 60 秒的訊息時間戳
            day_hits = []
            while True:
                m = c.poll(2.0)
                if m is None:
                    break
                k = m.timestamp()[1]
                if k >= e_ms:
                    break
                if m.error():
                    continue
                if prev is not None and k - prev > thr_ms:
                    # 用「空窗前 60 秒的實際速率」推算這段本來該有幾則訊息。
                    # 冷清時段速率低 → 期望值低 → 自然不會被誤報;
                    # 活躍時段突然全無訊息才會被抓出來。
                    rate = len(window) / 60.0
                    gap_s = (k - prev) / 1000
                    expected = rate * gap_s
                    trad = _trading_seconds(prev, k)
                    if expected >= args.min_expected and trad >= args.min_gap:
                        day_hits.append((datetime.fromtimestamp(prev / 1000, TZ),
                                         gap_s, rate, expected))
                prev = k
                window.append(k)
                while window and k - window[0] > 60_000:
                    window.popleft()
            c.assign([])
            for at, g, rate, exp in day_hits:
                print(f"  {at:%Y-%m-%d %H:%M:%S}  靜默 {g:6.2f}s  "
                      f"(前 60s 速率 {rate:5.1f}/s → 本該有 ~{exp:.0f} 則,實得 0)")
            found.extend(day_hits)
            cur = nxt
    finally:
        c.close()

    print(f"\n共 {len(found)} 次 ≥{args.min_gap}s 的異常靜默")
    if found:
        worst = max(found, key=lambda x: x[1])
        print(f"最久:{worst[0]:%Y-%m-%d %H:%M:%S} 靜默 {worst[1]:.2f}s"
          f"(當時速率 {worst[2]:.1f}/s,本該有 ~{worst[3]:.0f} 則)")
        print("\n💡 拿時間去對照 producer 日誌(判斷是我方停頓還是上游斷線):")
        print(f"   journalctl -u txf-producer --since '{worst[0]:%Y-%m-%d %H:%M:%S}' "
              f"--until '{(worst[0]+timedelta(seconds=30)):%Y-%m-%d %H:%M:%S}'")
    return 0


def _trading_seconds(start_ms: int, end_ms: int) -> float:
    """算這段空窗裡**真正屬於交易時段**的秒數。

    收盤、週末、連假的空窗,交易秒數趨近 0 → 自然不會被當成停頓,
    不需要維護假日表。

    ⚠️ 別改用「空窗起點是否落在盤中」那種判斷:收盤前最後一則常落在
       `05:00:00.123`(比收盤時刻晚一點點),起點看起來還在盤內,
       整段其實是收盤 —— 2026-07-25 就是這樣誤報了 7,953 次。
    """
    total = 0.0
    # 1 秒粒度:60 秒步進會在收盤邊界誤判 —— 收盤前最後一則落在 04:59:59 時,
    # 第一個 60 秒桶被整段算成交易時間,13500 秒的收盤空窗就被當成停頓
    # (2026-07-03 誤報實錄)。候選空窗本來就少,逐秒算的成本可以忽略。
    step = 1.0
    t = start_ms / 1000
    end = end_ms / 1000
    while t < end:
        d = datetime.fromtimestamp(t, TZ)
        hm = d.hour * 60 + d.minute
        day_sess = (8 * 60 + 45) <= hm < (13 * 60 + 45)      # 日盤
        night_sess = hm >= 15 * 60 or hm < 5 * 60             # 夜盤(跨午夜)
        weekday = d.weekday()                                 # 0=一 … 6=日
        # 夜盤週一~週五開盤;週六凌晨是週五夜盤的延續
        open_now = (weekday < 5 and day_sess) or                    (night_sess and (weekday < 5 or (weekday == 5 and hm < 5 * 60)))
        if open_now:
            total += min(step, end - t)
        t += step
    return total


def main() -> int:
    # 共用參數放 parent,讓 `--broker` 放在子命令前後都能用
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--broker", default=KAFKA_BOOTSTRAP_SERVERS)
    common.add_argument("--topics", nargs="+", default=DEFAULT_TOPICS)

    ap = argparse.ArgumentParser(description="從 Kafka 實測負載 / 延遲 / 遺漏",
                                 parents=[common])
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scan", parents=[common],
                       help="Tier 1:零消費,每小時粒度掃全史")
    s.add_argument("--days", type=int, default=0, help="只看最近 N 天(0=全史)")
    s.add_argument("--top", type=int, default=10, help="列出前 N 名")
    s.set_defaults(func=cmd_scan)

    p = sub.add_parser("profile", parents=[common],
                       help="Tier 2:消費指定日,算瞬時峰值與延遲分布")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--topic", help="只看單一 topic(預設三個都跑)")
    p.set_defaults(func=cmd_profile)

    st = sub.add_parser("stalls", parents=[common],
                        help="找 produce 時間軸上的異常靜默(停頓)")
    st.add_argument("--start-date", required=True)
    st.add_argument("--end-date", required=True)
    st.add_argument("--min-gap", type=float, default=2.0, help="最小空窗秒數")
    st.add_argument("--min-expected", type=float, default=40.0,
                    help="依前 60s 速率推算,這段本該有幾則才算異常")
    st.set_defaults(func=cmd_stalls)

    v = sub.add_parser("verify", parents=[common],
                       help="完整性對帳:Kafka vs 資料湖(兩條獨立路徑)")
    v.add_argument("--date", required=True, help="YYYY-MM-DD(交易日)")
    v.add_argument("--lake", default="D:/txf-data", help="資料湖根目錄")
    v.set_defaults(func=cmd_verify)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
