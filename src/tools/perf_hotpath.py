"""Hot-path microbenchmark —— 執行:python -m src.tools.perf_hotpath

誠實地只量「跨機器有代表性、且真的是約束」的維度:
producer 每則 build+serialize 成本、consumer 每則 decode 成本,再換算成對真實峰值的餘裕。
**刻意不宣稱吞吐上限**(那個維度有數百倍餘裕、非約束;見 CLAUDE.md 的教訓),
也不在此量端到端延遲(dev 機非同機,會誤導;真延遲 5.7ms 是在生產機量的)。
"""
import time
import platform

from .. import txf_data_pb2

SCALE = 10000
def _sc(v):
    return int(v * SCALE)

def build_tick():
    t = txf_data_pb2.Tick()
    t.code = "TXFH6"; t.timestamp_ms = 1784000000000; t.tick_type = 1
    t.close = _sc(44047.0); t.volume = 1
    t.underlying_price = _sc(43654.84); t.total_volume = 5127
    return t

def build_bidask():
    b = txf_data_pb2.BidAsk()
    b.code = "TXFH6"; b.timestamp_ms = 1784000000000
    b.bid_total_vol = 11; b.ask_total_vol = 12
    b.bid_price.extend([_sc(x) for x in (44045.0, 44044, 44043, 44042, 44041)])
    b.ask_price.extend([_sc(x) for x in (44049.0, 44050, 44051, 44052, 44053)])
    b.bid_volume.extend((3, 5, 2, 1, 4)); b.ask_volume.extend((2, 4, 1, 3, 2))
    b.diff_bid_vol.extend((0, 1, -1, 0, 0)); b.diff_ask_vol.extend((1, 0, 0, -1, 0))
    return b

def _bench(fn, n=500000, warmup=50000):
    for _ in range(warmup):
        fn()
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    return (time.perf_counter() - t0) / n * 1e6  # us/op

def main():
    tick_bytes = build_tick().SerializeToString()
    ba_bytes = build_bidask().SerializeToString()
    print(f"env: Python {platform.python_version()} / {platform.machine()} / "
          f"msg sizes Tick={len(tick_bytes)}B BidAsk={len(ba_bytes)}B")

    N = 500000
    res = {
        "producer build+serialize Tick":   _bench(lambda: build_tick().SerializeToString(), N),
        "producer build+serialize BidAsk": _bench(lambda: build_bidask().SerializeToString(), N),
        "consumer decode Tick":            _bench(lambda: txf_data_pb2.Tick().ParseFromString(tick_bytes), N),
        "consumer decode BidAsk":          _bench(lambda: txf_data_pb2.BidAsk().ParseFromString(ba_bytes), N),
    }
    print(f"\n--- per-message cost (mean over {N:,} iters) ---")
    for k, v in res.items():
        print(f"  {k:32s}: {v:.3f} us/msg")

    PEAK = 479  # 真實觀測峰值 msg/s(2026-07-09)
    heavy = max(res["producer build+serialize Tick"], res["producer build+serialize BidAsk"])
    load_us = PEAK * heavy
    print(f"\n--- headroom @ real peak {PEAK} msg/s (worst case: all heavier msg) ---")
    print(f"  producer serialize load = {load_us:.1f} us/s = {load_us/1e4:.4f}% of one core "
          f"-> local CPU is NOT the constraint (real bottleneck = ~5.7ms upstream network)")

if __name__ == "__main__":
    main()
