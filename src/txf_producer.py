"""
==========================================
TXF Streaming Producer (Class-Based Optimized)
==========================================
Architecture:
  - Pattern: Event-Driven Producer
  - Core: Shioaji (Source) -> Protobuf (Serialize) -> Kafka (Sink)
  - Optimization: Class encapsulation, lazy loading, error isolation
  - Asyncio Engine: uvloop (High-Performance Event Loop)
Author: Garrett & Gemini
Last Updated: 2026-01-19
"""

import sys
import signal
import time
import asyncio
import logging
from decimal import Decimal
from typing import Optional

# --- Third-party Imports ---
import shioaji as sj
from shioaji import TickFOPv1, BidAskFOPv1
from confluent_kafka import Producer

# --- Local Imports ---
from . import txf_data_pb2
from .config import (
    SHIOAJI_API_KEY, SHIOAJI_SECRET_KEY, 
    KAFKA_BOOTSTRAP_SERVERS, 
    TICK_TOPIC, BIDASK_TOPIC
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
        self.running = False
        self._loop = None

    def _init_kafka(self):
        """初始化 Kafka Producer"""
        kafka_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'txf-producer-hft',

            # --- HFT 速度核心參數（極限優化版） ---
            'acks': '1',                           # 等待 Leader 確認，兼顧低延遲與防漏資料
            'linger.ms': 0,                        # 零延遲，有數據立刻發送
            'compression.type': 'none',            # [修改] 停用壓縮！區網頻寬富裕，拒絕消耗 CPU 計算壓縮
            # 備註：librdkafka 預設已自動啟用 TCP_NODELAY (socket.nodelay)，無需且不可手動設定以防報錯

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

    # --- Data Processing Callbacks ---

    def process_tick(self, exchange, quote: TickFOPv1):
        """處理 Tick 並推送到 Kafka"""
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

            self.producer.produce(
                TICK_TOPIC,
                key=tick.code.encode('utf-8'),
                value=tick.SerializeToString(),
                on_delivery=self._delivery_report
            )
            # 關鍵點：立即觸發 librdkafka 的回調函數檢查 (Delivery/Error)。
            # 雖然頻繁 poll(0) 增加 CPU 負載，但確保極致延遲和 queue 不阻塞。
            self.producer.poll(0)

        except Exception as e:
            logger.error(f"❌ Tick Process Error: {e}")

    def process_bidask(self, exchange, quote: BidAskFOPv1):
        """處理 BidAsk 並推送到 Kafka"""
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
            self.producer.poll(0)

        except Exception as e:
            logger.error(f"❌ BidAsk Process Error: {e}")

    # --- System Events ---

    def _handle_session_down(self, reason):
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
            # contracts_timeout=10000: 登入時同步等待合約下載 (最多 10秒)
            # 這能避免手動呼叫 fetch_contracts() 造成的 race condition (IndexError)
            self.api.login(
                api_key=SHIOAJI_API_KEY, 
                secret_key=SHIOAJI_SECRET_KEY, 
                contracts_timeout=10000
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
        self.api.quote.on_event(self._handle_solace_event)
        
        # 綁定數據回調 (直接綁定方法，不需額外裝飾器)
        self.api.quote.set_on_tick_fop_v1_callback(self.process_tick)
        self.api.quote.set_on_bidask_fop_v1_callback(self.process_bidask)

        # 訂閱
        logger.info("⏳ Looking for TXF contract...")
        
        # Robust Contract Lookup (Fix for AttributeError: 'StreamMultiContract' object has no attribute 'TXFR1')
        contract = None
        try:
            # 修正存取路徑：Futures -> TXF -> TXFR1 (配合 fetch_contracts 使用)
            contract = self.api.Contracts.Futures.TXF["TXFR1"] 
            logger.info(f"✅ Found Near Month via direct lookup: {contract.name} ({contract.code})")
        except (AttributeError, KeyError):
            logger.warning("⚠️ 'TXFR1' direct lookup failed, attempting manual iteration...")
            try:
                # Fallback: Iterate and find the nearest monthly contract
                # Filter for TXF contracts (excluding spreads which usually have different patterns or are handled separately)
                txf_contracts = [
                    c for c in self.api.Contracts.Futures.TXF 
                    if c.code.startswith('TXF') and len(c.code) == 9  # Standard format e.g., TXF202602 (TXF + YYYYMM)
                ]
                # Sort by delivery date
                txf_contracts.sort(key=lambda x: x.delivery_date)
                
                if txf_contracts:
                    contract = txf_contracts[0]
                    logger.info(f"✅ Found Near Month via fallback: {contract.name} ({contract.code})")
                else:
                    raise ValueError("No TXF contracts found in lookup.")
            except Exception as e:
                logger.critical(f"❌ Contract Lookup Failed: {e}")
                sys.exit(1)

        if not contract:
            logger.critical("❌ Failed to identify TXF contract.")
            sys.exit(1)

        logger.info("⏳ Subscribing to TXF...")
        self.api.quote.subscribe(contract, quote_type=sj.constant.QuoteType.Tick)
        self.api.quote.subscribe(contract, quote_type=sj.constant.QuoteType.BidAsk)
        logger.info(f"✅ Subscribed: {contract.name} ({contract.code})")
        
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
    
    # 在這裡可以加入 Watchdog 邏輯 (如需要)
    # asyncio.create_task(watchdog(service, stop_event))
    
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