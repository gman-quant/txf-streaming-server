"""
==========================================
TXF Streaming Producer (Class-Based Optimized)
==========================================
Architecture:
  - Pattern: Event-Driven Producer
  - Core: Shioaji (Source) -> Protobuf (Serialize) -> Kafka (Sink)
  - Routing: Dual-Topic Dispatch (TXFR1 -> txf-tick, TXFR2 -> txfr2-tick)
  - Optimization: Class encapsulation, lazy loading, error isolation
  - Asyncio Engine: uvloop (High-Performance Event Loop)

Requires: shioaji >= 1.7, Python 3.13(兩者由 requirements.txt / .python-version 鎖定)

shioaji 1.7 遷移點(2026-07-24,**不可退回 1.3.x 寫法**):
  - login() 拿掉 `contracts_timeout`(1.7 已移除該參數,傳了會 TypeError)
  - 合約改 `api.contracts.futures("TXF")` 取完整清單再以 `c.code` 建索引。
    **不要用 `api.contracts.get("TXFR1")`** —— 它回傳的是精簡合約,
    缺 name / delivery_month / delivery_date,日誌與交割月判斷會壞。
  - 行情回調改 **1-arg**(`process_tick(self, quote)`);2-arg 舊簽章在 1.7 只剩
    DeprecationWarning,將來會直接失效。
  - `sj.constant.QuoteType` -> `sj.QuoteType`;`api.quote.subscribe` -> `api.subscribe`。

退出碼語意(systemd `Restart=` 依此判斷):所有錯誤路徑一律 `sys.exit(1)`;
exit 0 只發生在收到 SIGTERM/SIGINT 後的乾淨關閉(見 main() 的 stop_event)。

Author: Garrett & Gemini
Last Updated: 2026-07-25(shioaji 1.7 + Python 3.13:07-24 改碼、07-25 上生產機驗證通過)
"""

import sys
import signal
import time
import asyncio
import logging
import itertools
from decimal import Decimal
from typing import Optional

# --- Third-party Imports ---
import shioaji as sj
import orjson
from shioaji import TickFOPv1, BidAskFOPv1, QuoteFOPv1
from confluent_kafka import Producer

# --- Local Imports ---
from . import txf_data_pb2
from .config import (
    SHIOAJI_API_KEY, SHIOAJI_SECRET_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    TICK_TOPIC, TICK_R2_TOPIC, BIDASK_TOPIC, MD_RAW_TOPIC
)

# ==========================================
# 1. Setup & Utilities
# ==========================================

def setup_logging():
    """設定 Logging (區分 Dev 與 Systemd 模式)"""
    # 判斷是否為互動模式 (TTY)
    is_interactive = sys.stdout.isatty()
    
    log_fmt = '%(asctime)s [%(levelname)s] %(message)s' if is_interactive else '[%(levelname)s] %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    # 降低第三方套件的噪音
    logging.getLogger("shioaji").setLevel(logging.WARNING) 
    return logging.getLogger("TXF_Producer")

logger = setup_logging()

# 常數定義
SCALE = 10000
FATAL_CODES = {1, 2, 8}

# raw topic 的錯誤 log 節流:每 N 次才印一次。
# 為什麼要節流:若某型別的 quote 每則都序列化失敗,逐則 log 會刷爆 journalctl 與磁碟。
RAW_ERR_LOG_EVERY = 500


def raw_json_default(o):
    """orjson 的 fallback —— **刻意嚴格**。

    只認「有 .value 的枚舉」(shioaji 的 Exchange 等走自家 EnumMixin,不是 enum.Enum,
    所以用 duck-typing 而非 isinstance)與 Decimal;其餘一律 raise。

    ⚠ 絕不寫成 `default=str`:那會把任何沒預期到的型別悄悄變成字串,
      Shioaji 改型別時我們不會知道。寧可大聲失敗(且失敗被 _emit_raw 接住並計數)。
    """
    v = getattr(o, "value", None)
    if isinstance(v, (str, int, float)):
        return v
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"unserializable type in raw payload: {type(o).__name__}")

# ==========================================
# 2. Core Service Class
# ==========================================

class TxfStreamingService:
    """
    台指期行情串流服務
    封裝了 API 連線、Kafka 發送與錯誤處理邏輯
    
    Methods:
        start()          - 登入 API, 訂閱資料
        shutdown()       - 優雅關閉
        process_tick()   - Tick 處理
        process_bidask() - BidAsk 處理
    """
    def __init__(self):
        self.api: Optional[sj.Shioaji] = None
        self.producer: Optional[Producer] = None
        self.contract_r1 = None
        self.contract_r2 = None
        self.running = False
        self._loop = None
        # raw topic 用:producer 自己的單調序號 + 錯誤計數。
        # _raw_seq 記錄「**producer 觀察到的順序**」——不是交易所的順序。
        # 交易所的 PROD-MSG-SEQ(跨 I024/I081/I083 連續)Shioaji 沒有轉出來,拿不到;
        # 這個序號的價值是「比 Kafka offset 更早、不受 librdkafka 重試影響」,
        # 兩者都記下來,日後若不一致查得出是哪一層造成的。
        # next() 對 itertools.count 是 C 層操作,GIL 下原子,不需要鎖。
        self._raw_seq = itertools.count()
        self._raw_err = 0
        # 已做過欄位稽核的 (type, role);每種只在啟動後的第一則印一次(見 _field_audit)
        self._audited = set()

    def _init_kafka(self):
        """初始化 Kafka Producer"""
        if not MD_RAW_TOPIC:
            logger.critical("❌ MD_RAW_TOPIC 未設定(.env 或 config 預設值都沒給)")
            sys.exit(1)
        kafka_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'txf-producer-hft',

            # --- HFT 速度核心參數（極限優化版） ---
            # 2026-07-26:啟用 idempotence 修正「分區內可能亂序」的既有缺陷。
            #   原設定 acks=1 + 未設 enable.idempotence + max.in.flight 預設 1000000 +
            #   retries 預設極大 → 暫時性錯誤觸發重試時,**早的批次可能落在晚的批次之後**
            #   (librdkafka 明確警告的情況)。txf-tick / txf-bidask 同樣受影響。
            # 代價:idempotence 會強制 acks=all。但本 broker **ReplicationFactor=1**
            #   (2026-07-26 實查)→ 只有 leader、沒有 follower,acks=all 等於 acks=1,
            #   延遲代價實質為零。
            'enable.idempotence': True,
            'acks': 'all',                         # idempotence 要求;RF=1 下等同 acks=1
            'linger.ms': 0,                        # 零延遲，有數據立刻發送
            'compression.type': 'none',            # [修改] 停用壓縮！區網頻寬富裕，拒絕消耗 CPU 計算壓縮
            'socket.nagle.disable': True,          # [修改] 關閉 Nagle 演算法，網卡層零等待立即發射

            # --- 記憶體與佇列優化 ---
            'queue.buffering.max.kbytes': 131072,  # 128MB Buffer
            'batch.size': 262144,                  # 256KB Batch 限制
            'delivery.report.only.error': True,    # [新增] 只回報錯誤，正常投遞不觸發 callback，省下 CPU 週期

            # --- 針對區域網路高吞吐優化 ---
            'socket.send.buffer.bytes': 1024000,   # [修改] 稍微加大發送緩衝（1MB），防止極端快市時網卡瞬間塞車
            'socket.receive.buffer.bytes': 102400,
        }
        try:
            self.producer = Producer(kafka_conf)
            logger.info("✅ Kafka Producer Initialized")
        except Exception as e:
            logger.critical(f"❌ Kafka Init Failed: {e}")
            sys.exit(1)

    def _delivery_report(self, err, msg):
        """Kafka Error Callback"""
        if err:
            logger.error(f'❌ Kafka Delivery Failed: {err}')

    def _to_scaled_int(self, val: Optional[Decimal]) -> int:
        """
        快速轉換 Decimal 為 int64 (x10000)。
        說明：這是為了確保金融數據精度，並將其轉換為 Protobuf 效率最高的 int64 格式。
        (註: Decimal 運算比 float 慢，但換取了絕對精度。)
        """
        return int(val * SCALE) if val is not None else 0

    # --- Raw(完整行情)輔助 ---

    def _is_r2(self, code: str) -> bool:
        """判定 quote 是否為次月。

        ⚠ quote.code 是**真實合約碼**(如 TXFI6)不是 'TXFR2',所以必須同時比對
          target_code —— 這是 commit 6441f48 修過的 bug,別退回成單一比對。
        """
        r2 = self.contract_r2
        if not r2:
            return False
        return code in (r2.code, getattr(r2, "target_code", ""))

    def _field_audit(self, kind: str, role: str, quote) -> None:
        """一次性欄位稽核:比對三種取值法,找出**我們正在漏掉的欄位**。

        為什麼需要 —— `to_dict()` 已證實不完整(2026-07-27 實查):
          · `BidAskFOPv1.to_dict()` 少了 `exchange`(型別宣告有,to_dict 不吐)
          · `QuoteFOPv1` 有 **46** 個屬性,`to_dict()` 只給 **34** 個
            (少了 target_kind_price / vol_sum / amount_sum / trade_bid_cnt /
             trade_ask_cnt / trade_*_vol_sum / diff_price / diff_rate / diff_type /
             first_derived_*_volume)
          · 三個型別都有 `to_dict(raw=False)` 簽章,`raw=True` 從沒試過
        而擷取層漏掉的東西是**永久的**(bidask/quote 沒有歷史 API,補不回來)。

        每個 (kind, role) 只跑一次。整段吞例外 —— **稽核絕不能影響擷取**。
        """
        key = (kind, role)
        if key in self._audited:
            return
        self._audited.add(key)          # 先設旗標:就算下面炸了也不要每則重試
        try:
            k_dict = set(quote.to_dict())

            k_raw, raw_err = None, ""
            try:
                k_raw = set(quote.to_dict(raw=True))
            except Exception as e:
                raw_err = repr(e)

            # dir() + getattr:**逐個**包 try —— PyO3 的 property 可能拋錯
            attrs, attr_err = {}, {}
            for name in dir(quote):
                if name.startswith("_"):
                    continue
                try:
                    v = getattr(quote, name)
                except Exception as e:
                    attr_err[name] = repr(e)[:120]
                    continue
                if callable(v):
                    continue
                attrs[name] = v
            k_attr = set(attrs)

            out = [f"🔍 FIELD-AUDIT {kind}/{role} code={getattr(quote, 'code', '?')}",
                   f"   to_dict()      {len(k_dict):>3} keys",
                   f"   to_dict(raw)   " + (f"{len(k_raw):>3} keys" if k_raw is not None
                                            else f"取不到 → {raw_err}"),
                   f"   dir()+getattr  {len(k_attr):>3} keys"
                   + (f"   ⚠ 取值失敗 {len(attr_err)}: {attr_err}" if attr_err else "")]

            missing = sorted(k_attr - k_dict)
            out.append(f"   ▶ dir − to_dict = {len(missing)} 個【我們正在漏的】")
            for n in missing:
                out.append(f"       {n} = {repr(attrs[n])[:200]}")
            if k_raw is not None:
                out.append(f"   ▶ raw − to_dict = {sorted(k_raw - k_dict)}")
                out.append(f"   ▶ to_dict − raw = {sorted(k_dict - k_raw)}")
            extra = sorted(k_dict - k_attr)
            out.append(f"   ▶ to_dict − dir = {extra}"
                       + ("   ⚠ 非空 = dir() 拿不到某些 to_dict 有的東西" if extra else ""))
            logger.info("\n".join(out))
        except Exception as e:
            logger.error(f"❌ FIELD-AUDIT {kind}/{role} 失敗(不影響擷取): {e}")

    def _emit_raw(self, kind: str, quote, role: str) -> None:
        """把 Shioaji 原始行情物件**完整**送進 raw topic(JSON)。

        設計原則(2026-07-26):**不挑欄位、不改精度、不過濾 simtrade**。
          · 挑欄位的代價已經付過 —— proto 的 BidAsk 漏了 first_derived_*(衍生一檔,
            組合簿的唯一入口)整整八個月,而 bidask 沒有歷史 API,補不回來。
            唯一結構上的解法是 to_dict() 全收。
          · orjson 而非 json:回傳 bytes(Kafka 直接吃)且**原生把 datetime 序列化成
            RFC 3339 含微秒** —— 舊的 proto 路徑 int(ts*1000) 把微秒截掉了,
            交易所的 INFORMATION-TIME 其實給到微秒。
          · 不過濾 simtrade:試撮(日盤 08:30–08:45、夜盤 14:50–15:00,每 5 秒一則)
            是歷史 API 完全沒有的資料。欄位保留著,要濾隨時能濾。

        附加欄位一律 `_` 前綴,避免與 Shioaji 欄位名相撞。
        """
        # 一次性稽核(自帶 try/except,印完設旗標就不再進來)
        self._field_audit(kind, role, quote)
        try:
            payload = quote.to_dict()
            payload["_type"] = kind          # tick / bidask / quote
            payload["_role"] = role          # R1 / R2 —— code 會隨換月變,role 不會
            payload["_recv_ns"] = time.time_ns()
            payload["_seq"] = next(self._raw_seq)
            self.producer.produce(
                MD_RAW_TOPIC,
                key=str(payload.get("code") or role).encode("utf-8"),
                value=orjson.dumps(payload, default=raw_json_default),
                on_delivery=self._delivery_report,
            )
        except Exception as e:
            # ⚠ **絕不 re-raise**:例外若冒回 Shioaji 的回調執行緒,可能讓該回調
            #   永久停止(SDK 未必重新註冊),而且是靜默的。這裡只計數 + 節流 log。
            self._raw_err += 1
            if self._raw_err % RAW_ERR_LOG_EVERY == 1:
                logger.error(f"❌ Raw emit error #{self._raw_err} ({kind}/{role}): {e}")

    # --- Data Processing Callbacks ---

    def process_quote(self, quote: QuoteFOPv1):
        """Quote(QUO/v2,tick+bidask 合併)→ **只**進 raw topic,不碰任何 proto topic。

        QuoteFOPv1 在資訊上是 TickFOPv1 ∪ BidAskFOPv1 的超集:
        含 first_derived_* ×4、bid/ask_side_total_cnt(另兩者都沒有),
        缺的 bid_total_vol/ask_total_vol 實測 100% 等於五檔加總(544,067 列)= 純冗餘。
        觸發節奏已實測判定(2026-07-27/28):**book 節奏**,與 BidAsk 事件 1:1、
        逐則同簿 100.00%;成交以 total_volume 前進(running-max)辨識。
        7/28 崩盤夜另證:原生 Tick 流掉單 144 處/188 口(0.35%),Quote 的累計量
        與 tick 流自身累計欄一致 = 權威 —— 未來若以 Quote 萃取取代 tick 流,依據在此。
        """
        self._emit_raw("quote", quote, "R2" if self._is_r2(quote.code) else "R1")

    def process_tick(self, quote: TickFOPv1):
        """處理 Tick 並推送到 Kafka"""
        # 2026-07-27:tick 也進 raw topic。兩個位置上的講究:
        #  ⚠ 必須在 `simtrade` 過濾**之前** —— raw 要保留試撮(日盤 08:30–08:45、
        #    夜盤 14:50–15:00,歷史 API 一則都沒有);proto 路徑照舊過濾。
        #  ⚠ 必須在原有 try **之外** —— _emit_raw 自帶 try/except 且絕不 re-raise,
        #    放外面才不會跟既有的錯誤處理糾纏。
        # 為什麼要做:Quote 是 book 節奏,**19% 的成交會被併進同一次書更新**
        #    (實測 11,625 次變化 vs 11,409 筆成交,最多一次併 36 筆)。
        #    tick 不進 raw,逐筆粒度與「成交 vs 書更新的相對順序」就永遠補不回來 ——
        #    交易所的 PROD-MSG-SEQ 拿不到,同一個 partition 的順序是唯一的替代品。
        self._emit_raw("tick", quote, "R2" if self._is_r2(quote.code) else "R1")

        try:
            if quote.simtrade == 1: return

            # 直接在此建立 Protobuf 物件，減少函數調用開銷
            tick = txf_data_pb2.Tick()
            tick.code = quote.code
            tick.timestamp_ms = int(quote.datetime.timestamp() * 1000)
            tick.tick_type = int(quote.tick_type)
            tick.close = self._to_scaled_int(quote.close)
            tick.volume = int(quote.volume)
            tick.underlying_price = self._to_scaled_int(quote.underlying_price)
            tick.total_volume = int(quote.total_volume)

            target_topic = TICK_TOPIC
            # 檢查 quote.code 是否為次月合約 (比對 code 或是 target_code，因為連續合約的代碼是 TXFR2 但收到的 tick 是 TXFG6)
            if self.contract_r2 and quote.code in (self.contract_r2.code, getattr(self.contract_r2, "target_code", "")):
                target_topic = TICK_R2_TOPIC

            self.producer.produce(
                target_topic,
                key=tick.code.encode('utf-8'),
                value=tick.SerializeToString(),
                on_delivery=self._delivery_report
            )
            # 修正：移除這行 self.producer.poll(0) 避免 GIL 阻塞，將其移至背景非同步執行緒
            
        except Exception as e:
            logger.error(f"❌ Tick Process Error: {e}")

    def process_bidask(self, quote: BidAskFOPv1):
        """處理 BidAsk 並推送到 Kafka"""
        # 2026-07-26:新增 R2 BidAsk 訂閱後,R2 的訊息也會走進這個回調。
        # ⚠⚠ R2 **絕對不可以**進 BIDASK_TOPIC(txf-bidask)——
        #     那個 topic 是 gale-engine 匯出 `{date}_TXF_bidask.parquet` 的來源,
        #     且匯出時**不依 code 過濾**,混進 R2 會直接污染 R1 的檔。
        #     R2 只進 raw topic,然後 return。以下 R1 的原有邏輯一行未改。
        if self._is_r2(quote.code):
            self._emit_raw("bidask", quote, "R2")
            return

        try:
            if quote.simtrade == 1: return

            ba = txf_data_pb2.BidAsk()
            ba.code = quote.code
            ba.timestamp_ms = int(quote.datetime.timestamp() * 1000)
            ba.bid_total_vol = int(quote.bid_total_vol)
            ba.ask_total_vol = int(quote.ask_total_vol)
            
            # 使用 extend 稍微比迴圈 append 快
            ba.bid_price.extend([self._to_scaled_int(x) for x in quote.bid_price])
            ba.ask_price.extend([self._to_scaled_int(x) for x in quote.ask_price])
            ba.bid_volume.extend(quote.bid_volume)
            ba.ask_volume.extend(quote.ask_volume)
            ba.diff_bid_vol.extend(quote.diff_bid_vol)
            ba.diff_ask_vol.extend(quote.diff_ask_vol)

            self.producer.produce(
                BIDASK_TOPIC,
                key=ba.code.encode('utf-8'),
                value=ba.SerializeToString(),
                on_delivery=self._delivery_report
            )
            # 修正：移除這行 self.producer.poll(0)

        except Exception as e:
            logger.error(f"❌ BidAsk Process Error: {e}")

    # --- System Events ---

    def _handle_session_down(self, reason: str = "SDK 未提供原因", *_extra):
        """券商連線中斷 → 乾淨關閉 + exit(1),交給 systemd 重啟(自癒的本體)。

        🔴 `reason` **必須有預設值,別改回必填** —— 這個函式有兩條呼叫路徑,arity 不同:
             1. `api.on_session_down(self._handle_session_down)` → pysolace **不帶參數**呼叫
             2. `_handle_solace_event()` 內部呼叫 → 帶 reason 字串

           2026-07-20 20:57 事故:簽章當時是 `(self, reason)` 必填,真正斷線時
           pysolace 零參數呼叫 → `TypeError: missing 1 required positional argument`
           → **下面的 shutdown/exit(1) 從來沒執行過,自癒機制等於不存在**。
           那次能恢復純屬僥倖(Solace SDK 自己的重連剛好成功),行情靜默了 75 秒;
           若重連失敗,producer 會停在「行程活著、systemd 顯示 active、卻完全不送資料」
           —— systemd 永遠發現不了,只有 deploy/check_feed_alive.sh 抓得到。

           `*_extra` 是防呆:未來 SDK 若改成帶參數呼叫,也不會再因為 arity 不合而炸掉。
           這個回調的失敗模式是「靜默停止供料」,值得多這一層保險。
        """
        logger.critical(f"🚨 Session Down: {reason}. Triggering Systemd Restart.")
        self.shutdown()
        sys.exit(1)

    def _handle_solace_event(self, resp_code, event_code, info, event):
        if event_code in {0, 6, 10, 13, 15, 16, 18}:
            if event_code == 13: logger.info("✅ Solace Reconnected")
            return
        if event_code == 12: 
            return # Retrying...
        if event_code in FATAL_CODES:
            self._handle_session_down(f"Fatal Code {event_code}: {info}")
        logger.warning(f"⚠️ Solace Event {event_code}: {info}")

    # --- Lifecycle Methods ---

    def start(self):
        """啟動服務：登入、綁定回調、訂閱"""
        self._init_kafka()
        
        logger.info("🔑 Logging into Shioaji...")
        self.api = sj.Shioaji(simulation=True)
        try:
            # shioaji 1.7 內部自管合約:login 回來時合約即就緒(實測 ~0.3s),無需 contracts_timeout。
            # ⚠ 該參數 1.7 已從 login 簽章移除,傳入會 TypeError(舊版靠它同步等待、避開
            #   fetch_contracts race;1.7 由 SDK 內部處理,不再需要)。
            self.api.login(
                api_key=SHIOAJI_API_KEY,
                secret_key=SHIOAJI_SECRET_KEY,
            )
            logger.info("✅ Login & Contracts Loaded")
        except Exception as e:
            logger.critical(f"❌ Login Failed: {e}")
            # Prevent "Too Many Connections" by adding a safety delay
            logger.debug("⏳ Waiting 5s for system stability...")
            time.sleep(5)
            # Ensure we send logout if possible to clean up "Ghost Connections"
            self.shutdown()
            sys.exit(1)

        # 在你的 start() 方法中，Login 成功後加入：
        try:
            usage = self.api.usage()
            logger.info(f"📊 API Usage: Connections={usage.connections}, Traffic={usage.bytes/1024/1024:.2f}MB")
        except:
            pass

        # 綁定事件
        self.api.on_session_down(self._handle_session_down)
        self.api.on_event(self._handle_solace_event)
        
        # 綁定數據回調 (直接綁定方法，不需額外裝飾器)
        self.api.set_on_tick_fop_v1_callback(self.process_tick)
        self.api.set_on_bidask_fop_v1_callback(self.process_bidask)
        self.api.set_on_quote_fop_v1_callback(self.process_quote)

        # 訂閱
        logger.info("⏳ Looking for TXF contracts...")
        
        # 合約解析(shioaji 1.7 v2 contracts API):抓一份完整合約 list、以 code 索引,
        # 取連續合約 TXFR1/TXFR2。舊版「dict 直取 → except → 手動迭代」是在繞 v1 的
        # StreamMultiContract 怪脾氣;v2 的 futures() 直接給完整 list(name / delivery_* /
        # target_code 實測齊全),主路徑與 fallback 共用同一份、短很多。
        # ⚠ 不可改用 api.contracts.get("TXFR1"):它回閹割版(缺 name/delivery_month/delivery_date)。
        try:
            txf = self.api.contracts.futures("TXF")
            by_code = {c.code: c for c in txf}
            self.contract_r1 = by_code.get("TXFR1")
            self.contract_r2 = by_code.get("TXFR2")

            if self.contract_r1 is None:  # fallback:同一份 list 取最近兩個月合約
                months = sorted(
                    (c for c in txf if len(c.code) == 5 and c.code[:3] == "TXF"
                     and not c.code.startswith("TXFR")),
                    key=lambda c: c.delivery_date,
                )
                if months:
                    self.contract_r1 = months[0]
                    if self.contract_r2 is None and len(months) > 1:
                        self.contract_r2 = months[1]
                    logger.warning(f"⚠️ 'TXFR1' 不在 list,改用最近月合約: {self.contract_r1.code}")
        except Exception as e:
            logger.critical(f"❌ Contract Lookup Failed: {e}")
            sys.exit(1)

        if not self.contract_r1:
            logger.critical("❌ Failed to identify TXF contract.")
            sys.exit(1)
        logger.info(f"✅ Found Near Month: {self.contract_r1.name} ({self.contract_r1.code})")

        if self.contract_r2:
            logger.info(f"✅ Found Next Month: {self.contract_r2.name} ({self.contract_r2.code})")
        else:
            logger.warning("⚠️ Could not find TXFR2 contract. Only R1 will be streamed.")

        logger.info("⏳ Subscribing to TXF contracts...")
        
        # 顯示更詳細的合約資訊 (包含月份與實際代碼)
        r1_desc = f"{self.contract_r1.name} ({self.contract_r1.code})"
        if hasattr(self.contract_r1, 'delivery_month'):
            r1_desc += f" [Delivery: {self.contract_r1.delivery_month}]"
            
        # ⚠ 訂閱順序刻意讓 **Quote 排在 Tick/BidAsk 之前**。
        #   原因:同一合約同時訂 Tick + BidAsk + Quote,Shioaji **沒有任何文件**說明
        #   後訂的會不會覆蓋前面的。若真的會覆蓋,「Quote 最後訂」的下場是
        #   Tick 被踢掉 → txf-tick 斷 → viewer 直接沒行情(真錢工具)。
        #   反過來把 Quote 放最前面,最壞情況只是拿不到 Quote(研究線少一份資料,無害)。
        #   ⇒ 失敗模式從「看盤瞎掉」降級為「研究資料缺一項」。實測後可再調整。
        self.api.subscribe(self.contract_r1, quote_type=sj.QuoteType.Quote)
        self.api.subscribe(self.contract_r1, quote_type=sj.QuoteType.Tick)
        self.api.subscribe(self.contract_r1, quote_type=sj.QuoteType.BidAsk)
        logger.info(f"✅ Subscribed R1 (Quote + Tick + BidAsk): {r1_desc}")

        if self.contract_r2:
            r2_desc = f"{self.contract_r2.name} ({self.contract_r2.code})"
            if hasattr(self.contract_r2, 'delivery_month'):
                r2_desc += f" [Delivery: {self.contract_r2.delivery_month}]"

            self.api.subscribe(self.contract_r2, quote_type=sj.QuoteType.Quote)
            self.api.subscribe(self.contract_r2, quote_type=sj.QuoteType.Tick)
            # R2 BidAsk:2026-07-26 訂、2026-07-28 退訂。實測(7/27 日盤 + 7/28 崩盤夜):
            # Quote 對 BidAsk 事件 1:1(117,896:117,895 / 203,516:203,514)、逐則同簿
            # 100.00%、簿況鏈式一致 → R2 BidAsk 純冗餘。它唯一下游是 raw topic,
            # 衍生一檔在 Quote 的 first_derived_* 完整保留。process_bidask 的 R2 分流
            # 守衛**保留不動**(誤訂/復訂時仍擋 R2 污染 txf-bidask)。
            logger.info(f"✅ Subscribed R2 (Quote + Tick): {r2_desc}")

        logger.info(f"📡 Raw topic: {MD_RAW_TOPIC} (quote×2 + tick×2, 完整 JSON)")
        self.running = True

    def shutdown(self):
        """優雅關閉資源"""
        logger.info("⏳ Shutting down services...")
        if self.api:
            try:
                logger.info("Logout API...")
                self.api.logout()
            except: pass
        
        if self.producer:
            logger.info("Flushing Kafka...")
            self.producer.flush()
        logger.info("👋 Bye")

# ==========================================
# 3. Main Entry Point
# ==========================================

async def main():
    service = TxfStreamingService()
    service.start()
    
    # 建立 Async Event 來等待停止訊號
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def signal_handler(*args):
        logger.info("🛑 Signal received, stopping...")
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("🟢 Service is running (Ctrl+C to stop)")
    
    # 建立背景輪詢 Task (專門處理 Kafka 回調，釋放主執行緒)
    async def background_poll_loop():
        while not stop_event.is_set():
            if service.producer:
                service.producer.poll(0)
            await asyncio.sleep(0.1)  # 每 100 毫秒巡視一次即可
            
    asyncio.create_task(background_poll_loop())
    logger.info("✅ Background Poll Task started.")
    
    await stop_event.wait()
    service.shutdown()


if __name__ == "__main__":
    # 跨平台相容性處理
    try:
        import uvloop
        # --- 覆蓋標準 asyncio loop ---
        # uvloop.install() 將 Python 內建的 asyncio 事件迴圈替換為 libuv 實現。
        # 這是提升 I/O 調度效率的最高效益優化 (CPU cycles -> C code)。
        uvloop.install()
        logger.debug("✅ Linux detected: uvloop installed.")
    except (ImportError, AttributeError):
        # 在 Windows 上會抓到 ImportError
        logger.debug("ℹ️ Windows/Other detected: using native asyncio loop.")
    
    try:
        # 啟動 Asyncio 執行環境，運行主協程 (Coroutine) main()
        asyncio.run(main())
    except KeyboardInterrupt:
        # 處理 Ctrl+C
        pass
    except Exception as e:
        logger.critical(f"❌ Main execution failed: {e}")
        sys.exit(1)