from concurrent.futures import ThreadPoolExecutor, as_completed# analyzer.py
from collections import defaultdict
from tqdm import tqdm
import json
from datetime import datetime,timedelta
from collections import defaultdict
from db_manager import DBManager

class StationLog:
    def __init__(self, timestamp: datetime, available_spaces: int):
        self.timestamp = timestamp
        self.available_spaces = available_spaces

class Analyzer:
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
        
        # 三種快取
        self.snapshot_cache = {}        # {timestamp_unix: 已格式化快照列表}
        self.range_cache = {}           # {(station_no, start_ts, end_ts): [StationLog]}
        self.hourly_cache = {}          # {station_no: hourly_avg_list}
        self.delta_cache = {}           # {station_no: hourly_delta_dict}

    # -------------------------------------------------
    # 1. 取得單一快照（/data/<ts>）
    # -------------------------------------------------
    def get_snapshot_by_timestamp(self, dt: datetime):
        ts_unix = int(dt.timestamp())
        if ts_unix in self.snapshot_cache:
            return self.snapshot_cache[ts_unix]
        
        result = self.db_manager.get_snapshot_by_timestamp(dt)
        formatted = []
        for item in result:
            ts = item.pop('timestamp')
            item['timestamp'] = ts.isoformat().split('.')[0] if isinstance(ts, datetime) else ts
            formatted.append(item)
        
        self.snapshot_cache[ts_unix] = formatted
        return formatted

    # -------------------------------------------------
    # 2. 範圍查詢（/range）
    # -------------------------------------------------
    def get_logs_in_range(self, station_no: str, start: datetime, end: datetime):
        key = (station_no, int(start.timestamp()), int(end.timestamp()))
        if key in self.range_cache:
            return self.range_cache[key]
        
        db_records = self.db_manager.get_range_logs(station_no, start, end)
        logs = []
        for r in db_records:
            ts = r['timestamp']
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            logs.append(StationLog(ts, r['available_spaces']))
        
        self.range_cache[key] = logs
        return logs

    # -------------------------------------------------
    # 3. 每小時平均（使用快取中的過去七天快照）
    # -------------------------------------------------
    def get_hourly_avg(self, station_no: str):
        if station_no in self.hourly_cache:
            return self.hourly_cache[station_no]

        hourly_data = defaultdict(list)

        for ts, snapshot in self.snapshot_cache.items():
            hour = datetime.fromtimestamp(ts).hour
            for item in snapshot:
                if item["station_no"] == station_no:
                    hourly_data[hour].append(item["available_spaces"])

        result = [
            round(sum(hourly_data[h]) / len(hourly_data[h]), 2) if hourly_data[h] else 0.0
            for h in range(24)
        ]

        self.hourly_cache[station_no] = result
        return result

    # -------------------------------------------------
    # 4. 每小時變化量（使用快取中的過去七天快照）
     -------------------------------------------------
    def get_hourly_avg_delta(self, station_no: str):
        if station_no in self.delta_cache:
            return self.delta_cache[station_no]

        hourly_flow = defaultdict(float)
        prev_spaces = None
        prev_hour = None

        for ts in sorted(self.snapshot_cache.keys()):
            hour = datetime.fromtimestamp(ts).hour
            item = next(
                (it for it in self.snapshot_cache[ts] if it["station_no"] == station_no),
                None,
            )
            if item:
                curr = item["available_spaces"]
                if prev_spaces is not None and prev_hour == hour:
                    hourly_flow[hour] += abs(curr - prev_spaces)
                prev_spaces = curr
                prev_hour = hour

        result = {h: round(hourly_flow[h], 2) for h in range(24)}
        self.delta_cache[station_no] = result
        return result
    # -------------------------------------------------
    # 5. 背景更新：每30分鐘清空並重新載入所有快取
    # -------------------------------------------------
    def refresh_all_cache(self):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 開始更新快取...")
        #self.snapshot_cache.clear()
        self.range_cache.clear()
        self.hourly_cache.clear()
        self.delta_cache.clear()
        print("所有快取已更新完成")

    # -------------------------------------------------
    # 6. 上傳新資料後立即更新快取（讓最新一筆馬上可用）
    # -------------------------------------------------

    def load_previous_week_snapshots(self):
    
        # 1. 確定時間範圍
        # 如果快取中有資料，以快取中最新的時間為終點，否則以現在時間為終點
        #end_ts = max(self.snapshot_cache.keys()) if self.snapshot_cache else int(datetime.now().timestamp())
        # 計算 7 天前的時間點
        #start_ts = end_ts - 7 * 24 * 3600

        start_ts = int(datetime(2025, 12, 1, 0, 0, 0).timestamp())
        end_ts = int(datetime(2025, 12, 8, 0, 0, 0).timestamp())
        # 2. 取得所有在範圍內的唯一 timestamp
        ts_query = """
        SELECT DISTINCT timestamp_unix
        FROM station_records
        WHERE timestamp_unix >= %s AND timestamp_unix <= %s
        ORDER BY timestamp_unix
        """
        timestamps = [row['timestamp_unix'] for row in 
                    self.db_manager._execute_query(ts_query, (start_ts, end_ts), fetch_all=True)]

        if not timestamps:
            print("前一週無快照資料")
            return

        # 3. 定義單個 timestamp 的載入函數 (作為內部 helper 函式)
        def load_single_snapshot(ts):
            # 注意：這裡使用 fromisoformat().split('.')[0] 是為了確保格式化輸出，
            # 其實也可以直接從 data_snapshots 表中獲取 timestamp_iso，但如果只存 timestamp_unix 則維持此邏輯。
            iso_str = datetime.fromtimestamp(ts).isoformat().split('.')[0]
        
            # 查詢該時間點的所有站點記錄
            query = """
            SELECT station_no, parking_spaces, available_spaces, empty_spaces,
                yb2, eyb, forbidden_spaces, available_level
            FROM station_records
            WHERE timestamp_unix = %s
            """
            rows = self.db_manager._execute_query(query, (ts,), fetch_all=True)
        
            # 格式化資料
            formatted_data = [{
                "station_no": row['station_no'],
                "parking_spaces": row['parking_spaces'],
                "available_spaces": row['available_spaces'],
                "empty_spaces": row['empty_spaces'],
                "yb2": row['yb2'],
                "eyb": row['eyb'],
                "forbidden_spaces": row['forbidden_spaces'],
                "available_level": row['available_level'],
                "timestamp": iso_str,
            } for row in rows]
        
            return ts, formatted_data

        # 使用 tqdm 顯示進度條
        for ts in tqdm(timestamps, desc="載入前一週快照"):
            ts_key, formatted_data = load_single_snapshot(ts)
            self.snapshot_cache[ts_key] = formatted_data

        print(f"前一週快照（{start_ts} ~ {end_ts}）已載入，共 {len(timestamps)} 個時間點") 


    def format_logs_as_json(self, logs):
        # 保持不變
        return [
            {
                # 這裡假設 log.timestamp 仍然是 datetime 物件
                "timestamp": log.timestamp.isoformat(), 
                "available_spaces": log.available_spaces
            } for log in logs
        ]
