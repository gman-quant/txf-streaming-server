"""
合成報價管線驗證 —— 不登入 Shioaji、**只寫測試 topic**。

用途:升級 shioaji / protobuf / confluent-kafka 之後,在**沒有行情的時段**(收盤、週末)
就能驗證「回調 → protobuf 序列化 → Kafka 投遞 → 解碼」這條鏈,不必等開盤才知道有沒有壞。

為什麼需要:`process_tick` / `process_bidask` 內部是 `except Exception: logger.error(...)`
—— **例外被吞掉**。回調若壞掉,producer 不會崩、systemd 顯示 active,只是安靜地什麼都不送。
光看 `systemctl status` 完全看不出來。

執行(在 repo 根目錄):
    .venv/bin/python -m src.tools.verify_pipeline

驗得到:
  - shioaji 1.7 的 TickFOPv1 / BidAskFOPv1 **是否還有回調用到的每一個欄位**(靜態檢查)
  - protobuf 序列化 + Kafka produce 在**這台機器的套件版本**下可用
  - **R2 路由**(quote.code 是真實月份碼如 TXFI6、不是 "TXFR2",要比對 target_code)
  - simtrade 過濾
  - 投遞出去的位元組**解得回原值**

驗不到(要誠實):
  - shioaji 是否真的以 1-arg 呼叫我們的回調 —— 這裡是我們自己呼叫的。
    該項需真實行情,已於 2026-07-24 在 dev 端用真登入 + 真 tick 驗過。
  - 真實資料的邊界情況(某欄位偶爾是 None 等)。

🔒 安全:測試 topic 寫死在下面,且啟動時**斷言它們與生產 topic 無交集**,
   任一相同就拒跑。本腳本永遠不會寫入 txf-tick / txfr2-tick / txf-bidask。
"""
from __future__ import annotations

import sys
import time
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from confluent_kafka import Consumer, TopicPartition

from .. import txf_data_pb2
from .. import txf_producer as P
from ..config import KAFKA_BOOTSTRAP_SERVERS

# ── 測試 topic(寫死;生產 topic 由 config 提供,兩者必須無交集)──────────────
TEST_TICK_TOPIC = "txf-tick-test"
TEST_R2_TOPIC = "test"
TEST_BIDASK_TOPIC = "txf-bidask-test"

SCALE = P.SCALE


def _guard_topics() -> None:
    """絕不寫生產 topic —— 有任何交集就中止。"""
    prod = {P.TICK_TOPIC, P.TICK_R2_TOPIC, P.BIDASK_TOPIC}
    test = {TEST_TICK_TOPIC, TEST_R2_TOPIC, TEST_BIDASK_TOPIC}
    clash = prod & test
    if clash:
        print(f"❌ 中止:測試 topic 與生產 topic 相同 {sorted(clash)}")
        sys.exit(2)
    print(f"🔒 生產 topic {sorted(prod)} 不會被寫入")
    print(f"🎯 只寫測試 topic {sorted(test)}")


def _check_shioaji_fields() -> bool:
    """靜態檢查:回調用到的欄位在 shioaji 當前版本的模型類別上是否都還在。"""
    from shioaji import BidAskFOPv1, TickFOPv1

    need = {
        "TickFOPv1": (TickFOPv1, ["simtrade", "code", "datetime", "tick_type",
                                  "close", "volume", "underlying_price", "total_volume"]),
        "BidAskFOPv1": (BidAskFOPv1, ["simtrade", "code", "datetime",
                                      "bid_total_vol", "ask_total_vol",
                                      "bid_price", "ask_price", "bid_volume",
                                      "ask_volume", "diff_bid_vol", "diff_ask_vol"]),
    }
    ok = True
    for name, (cls, fields) in need.items():
        have = set(dir(cls))
        missing = [f for f in fields if f not in have]
        if missing:
            ok = False
            print(f"  ❌ {name} 缺少欄位 {missing} —— 回調會在真實行情進來時炸")
        else:
            print(f"  ✅ {name}:{len(fields)} 個欄位全部存在")
    return ok


# ── 合成報價(鴨子型別;欄位名已由上面的靜態檢查對真實類別驗過)────────────────
def _fake_tick(code: str, simtrade: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        code=code,
        datetime=datetime(2026, 7, 25, 13, 0, 0),
        tick_type=1,
        close=Decimal("23456.0"),
        volume=3,
        underlying_price=Decimal("23400.5"),
        total_volume=12345,
        simtrade=simtrade,
    )


def _fake_bidask(code: str, simtrade: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        code=code,
        datetime=datetime(2026, 7, 25, 13, 0, 1),
        bid_total_vol=100,
        ask_total_vol=120,
        bid_price=[Decimal(str(23456 - i)) for i in range(5)],
        ask_price=[Decimal(str(23457 + i)) for i in range(5)],
        bid_volume=[5, 4, 3, 2, 1],
        ask_volume=[6, 5, 4, 3, 2],
        diff_bid_vol=[1, 0, -1, 0, 0],
        diff_ask_vol=[0, 1, 0, -1, 0],
        simtrade=simtrade,
    )


def _high_watermarks(topics: list[str]) -> dict[str, int]:
    """記錄產出前各 topic 的結尾位移,之後只讀我們自己送的訊息。"""
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                  "group.id": "verify-pipeline-probe", "enable.auto.commit": False})
    marks = {}
    try:
        for t in topics:
            _low, high = c.get_watermark_offsets(TopicPartition(t, 0), timeout=10)
            marks[t] = high
    finally:
        c.close()
    return marks


def _read_since(topic: str, offset: int, want: int, timeout: float = 10.0) -> list[bytes]:
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                  "group.id": "verify-pipeline-read", "enable.auto.commit": False})
    out: list[bytes] = []
    try:
        c.assign([TopicPartition(topic, 0, offset)])
        deadline = time.time() + timeout
        while len(out) < want and time.time() < deadline:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            out.append(msg.value())
    finally:
        c.close()
    return out


def main() -> int:
    print("=" * 68)
    print("合成報價管線驗證(不登入 Shioaji)")
    print("=" * 68)

    _guard_topics()

    print("\n[1] shioaji 模型欄位靜態檢查")
    fields_ok = _check_shioaji_fields()

    # 把 topic 常數改指到測試 topic(回調讀的是模組全域)
    P.TICK_TOPIC = TEST_TICK_TOPIC
    P.TICK_R2_TOPIC = TEST_R2_TOPIC
    P.BIDASK_TOPIC = TEST_BIDASK_TOPIC

    topics = [TEST_TICK_TOPIC, TEST_R2_TOPIC, TEST_BIDASK_TOPIC]
    print(f"\n[2] 記錄產出前位移 @ {KAFKA_BOOTSTRAP_SERVERS}")
    before = _high_watermarks(topics)
    for t, o in before.items():
        print(f"  {t}: offset={o}")

    print("\n[3] 建 Kafka producer(不登入 Shioaji)")
    svc = P.TxfStreamingService()
    svc._init_kafka()
    # 次月合約:code 是 "TXFR2",但實際 tick 帶的是真實月份碼(target_code)
    svc.contract_r2 = SimpleNamespace(code="TXFR2", target_code="TXFI6")

    print("\n[4] 餵合成報價進真實回調")
    svc.process_tick(_fake_tick("TXFH6"))                 # → 近月 topic
    svc.process_tick(_fake_tick("TXFI6"))                 # → 次月 topic(靠 target_code 比對)
    svc.process_tick(_fake_tick("TXFH6", simtrade=1))     # → 應被過濾,不送
    svc.process_bidask(_fake_bidask("TXFH6"))             # → bidask topic
    svc.producer.flush(10)
    print("  已送出並 flush")

    print("\n[5] 讀回來解碼比對")
    results: list[tuple[str, bool, str]] = []

    # --- 近月 tick ---
    raw = _read_since(TEST_TICK_TOPIC, before[TEST_TICK_TOPIC], want=2)
    if len(raw) != 1:
        results.append(("近月 tick 路由", False,
                        f"預期 1 筆(simtrade 那筆該被濾掉),實得 {len(raw)}"))
    else:
        t = txf_data_pb2.Tick(); t.ParseFromString(raw[0])
        exp_close = int(Decimal("23456.0") * SCALE)
        exp_und = int(Decimal("23400.5") * SCALE)
        ok = (t.code == "TXFH6" and t.close == exp_close and t.volume == 3
              and t.total_volume == 12345 and t.tick_type == 1
              and t.underlying_price == exp_und)
        results.append(("近月 tick 路由 + 欄位", ok,
                        f"code={t.code} close={t.close}(期望 {exp_close}) "
                        f"vol={t.volume} total={t.total_volume} und={t.underlying_price}"))
        results.append(("simtrade 過濾", True, "只收到 1 筆 = 試撮那筆確實沒送出"))

    # --- 次月 tick(R2 路由)---
    raw = _read_since(TEST_R2_TOPIC, before[TEST_R2_TOPIC], want=1)
    if len(raw) != 1:
        results.append(("次月 R2 路由", False, f"預期 1 筆,實得 {len(raw)}"))
    else:
        t = txf_data_pb2.Tick(); t.ParseFromString(raw[0])
        ok = t.code == "TXFI6"
        results.append(("次月 R2 路由(target_code 比對)", ok, f"code={t.code} → {TEST_R2_TOPIC}"))

    # --- bidask ---
    raw = _read_since(TEST_BIDASK_TOPIC, before[TEST_BIDASK_TOPIC], want=1)
    if len(raw) != 1:
        results.append(("bidask", False, f"預期 1 筆,實得 {len(raw)}"))
    else:
        b = txf_data_pb2.BidAsk(); b.ParseFromString(raw[0])
        exp_bid0 = int(Decimal("23456") * SCALE)
        ok = (b.code == "TXFH6" and len(b.bid_price) == 5 and len(b.ask_price) == 5
              and len(b.bid_volume) == 5 and len(b.diff_ask_vol) == 5
              and b.bid_price[0] == exp_bid0 and b.bid_total_vol == 100)
        results.append(("bidask 五檔 + 欄位", ok,
                        f"code={b.code} 檔數={len(b.bid_price)}/{len(b.ask_price)} "
                        f"bid0={b.bid_price[0]}(期望 {exp_bid0}) bid_total={b.bid_total_vol}"))

    print("\n" + "=" * 68)
    all_ok = fields_ok and all(ok for _, ok, _ in results)
    for name, ok, detail in results:
        print(f"  {'✅' if ok else '❌'} {name}\n      {detail}")
    print("=" * 68)
    if all_ok:
        print("✅ 全部通過 —— 回調 / protobuf / Kafka / 路由 這條鏈在本機可用")
        print("   (仍需開盤驗真實行情:shioaji 是否以 1-arg 呼叫回調)")
    else:
        print("❌ 有項目失敗 —— 見上方細節")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
