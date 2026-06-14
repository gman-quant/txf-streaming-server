import pyarrow.parquet as pq
from datetime import datetime, time

TICK_FILE = '2026-05-28_TXF_ticks.parquet'
BIDASK_FILE = '2026-05-28_TXF_bidask.parquet'

def filter_day_session():
    # 讀取並轉為 list
    ticks = pq.read_table(TICK_FILE).to_pylist()
    bas = pq.read_table(BIDASK_FILE).to_pylist()
    
    # 嚴格篩選時間：2026-05-28 08:45:00 以後
    day_start = datetime(2026, 5, 28, 8, 45, 0)
    
    valid_ticks = [
        r for r in ticks 
        if (r['ts'] if isinstance(r['ts'], datetime) else datetime.fromtimestamp(r['ts']/1000)) >= day_start
    ]
    
    valid_bas = [
        r for r in bas 
        if datetime.fromtimestamp(r['timestamp_ms']/1000) >= day_start
    ]
    
    print(f"=== 2026-05-28 日盤 (08:45+) 嚴格篩選結果 ===")
    print(f"TICK_COUNT : {len(valid_ticks):,}")
    print(f"BA_COUNT   : {len(valid_bas):,}")
    print(f"TOTAL_DATA : {len(valid_ticks) + len(valid_bas):,}")
    print("============================================")

if __name__ == "__main__":
    filter_day_session()