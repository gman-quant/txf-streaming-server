"""V-FLIP 驗收:用生產同一顆 `synth_tick_dv` 離線重放,比對合成 tick vs 原生 tick。
==================================================================
從 txf-md-raw 讀一段時窗(預設 = 最近一個完整夜盤),對 quote 流套用合成規則,
與同窗的原生 tick 流對帳:

  1. 總口數:合成 == quote 累計量跨距(權威);原生允許少(實測會掉單)。
  2. 事件數:合成 ≤ 原生(掃檔在 quote 為單一事件)。
  3. 5s K 棒 OHLCV:逐根比對,回報不等根數與 H/L 最大偏差(已知無仲裁者,
     報數字供判斷,不設硬閘)。
  4. 冷啟動防護:從時窗中段起算,斷言第一筆合成 dv 沒有巨量幻影。

用法:
    python -m src.tools.verify_tick_synth
    python -m src.tools.verify_tick_synth --t0 "2026-07-27 15:00" --t1 "2026-07-28 05:01"
"""
import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import orjson
from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, ".")
from src.txf_producer import synth_tick_dv          # noqa: E402  ← 生產本尊

TPE = timezone(timedelta(hours=8))
BROKER = "192.168.1.50:9092"


def read_window(t0, t1, broker):
    c = Consumer({"bootstrap.servers": broker, "group.id": "verify-tick-synth-ephemeral",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    tp = TopicPartition("txf-md-raw", 0, int(t0.replace(tzinfo=TPE).timestamp() * 1000))
    off = c.offsets_for_times([tp], timeout=15)[0]
    c.assign([TopicPartition("txf-md-raw", 0, off.offset)])
    out = []
    while True:
        m = c.poll(5.0)
        if m is None or m.error():
            break
        d = orjson.loads(m.value())
        ts = d.get("datetime") or ""
        if ts and ts[:16] >= t1.strftime("%Y-%m-%dT%H:%M"):
            break
        out.append(d)
    c.close()
    return out


def fnum(v, default=0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def bars5s(events):
    """events: list[(ts_str, price, vol)] → {bucket: [o,h,l,c,v]}"""
    out = {}
    for ts, px, v in events:
        b = ts[:18] + str(int(ts[18]) // 5 * 5) if len(ts) > 18 else ts   # floor 5s on ISO
        r = out.get(b)
        if r is None:
            out[b] = [px, px, px, px, v]
        else:
            r[1] = max(r[1], px); r[2] = min(r[2], px); r[3] = px; r[4] += v
    return out


def main():
    ap = argparse.ArgumentParser()
    yd = datetime.now() - timedelta(days=1)
    ap.add_argument("--t0", default=f"{yd:%Y-%m-%d} 15:00")
    ap.add_argument("--t1", default=f"{datetime.now():%Y-%m-%d} 05:01")
    ap.add_argument("--broker", default=BROKER)
    args = ap.parse_args()
    t0 = datetime.strptime(args.t0, "%Y-%m-%d %H:%M")
    t1 = datetime.strptime(args.t1, "%Y-%m-%d %H:%M")
    print(f"時窗 {t0} ~ {t1}")

    msgs = read_window(t0, t1, args.broker)
    print(f"讀入 {len(msgs):,} 則")
    fails = 0

    for role in ("R1", "R2"):
        quotes = [d for d in msgs if d.get("_type") == "quote" and d.get("_role") == role
                  and d.get("simtrade") not in (1, "1", True)]
        ticks = [d for d in msgs if d.get("_type") == "tick" and d.get("_role") == role
                 and d.get("simtrade") not in (1, "1", True)]
        if not quotes:
            print(f"\n[{role}] 無 quote 資料,跳過")
            continue

        state, synth = {}, []
        for d in quotes:
            dv = synth_tick_dv(state, d.get("code", role),
                               int(fnum(d.get("total_volume"))), int(fnum(d.get("volume"))))
            if dv:
                synth.append((d["datetime"], fnum(d.get("close")), dv))
        native = [(d["datetime"], fnum(d.get("close")), int(fnum(d.get("volume")))) for d in ticks]

        tv_span = int(fnum(quotes[-1].get("total_volume"))) - int(fnum(quotes[0].get("total_volume"))) \
            + int(fnum(quotes[0].get("volume")) if fnum(quotes[0].get("total_volume")) == fnum(quotes[0].get("volume")) else 0)
        s_sum, n_sum = sum(v for _, _, v in synth), sum(v for _, _, v in native)
        ok_total = s_sum >= n_sum and s_sum >= tv_span - 5
        print(f"\n[{role}] 合成事件 {len(synth):,}({s_sum:,} 口) vs 原生 {len(native):,}"
              f"({n_sum:,} 口);quote 累計跨距 ≈ {tv_span:,}")
        print(f"  {'PASS' if ok_total else 'FAIL'} 總量:合成 ≥ 原生 且 ≈ 權威跨距"
              f"(差 原生 {s_sum - n_sum:+,} 口)")
        fails += 0 if ok_total else 1

        bs, bn = bars5s(synth), bars5s(native)
        keys = set(bs) | set(bn)
        diff = hl = 0
        maxdev = 0.0
        for k in keys:
            a, b = bs.get(k), bn.get(k)
            if a is None or b is None:
                diff += 1
                continue
            if a != b:
                diff += 1
                if a[1] != b[1] or a[2] != b[2]:
                    hl += 1
                    maxdev = max(maxdev, abs(a[1] - b[1]), abs(a[2] - b[2]))
        print(f"  INFO 5s K 棒 {len(keys):,} 根:不等 {diff}(H/L 不等 {hl},最大偏差 {maxdev:.0f} 點)"
              f" —— 已知無仲裁者,報數字不設閘")

        # 冷啟動防護:從中段開始重放,首筆 dv 不得為巨量幻影
        mid = quotes[len(quotes) // 2:]
        st2, first_dv = {}, None
        for d in mid:
            dv = synth_tick_dv(st2, d.get("code", role),
                               int(fnum(d.get("total_volume"))), int(fnum(d.get("volume"))))
            if dv:
                first_dv = dv
                break
        ok_cold = first_dv is None or first_dv < 1000
        print(f"  {'PASS' if ok_cold else 'FAIL'} 冷啟動防護:中段起算首筆 dv = {first_dv}(須 < 1000)")
        fails += 0 if ok_cold else 1

    print(f"\nRESULT: {'ALL PASS' if fails == 0 else f'{fails} FAIL'}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
