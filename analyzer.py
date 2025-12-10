import json
from datetime import datetime
from collections import defaultdict

class StationLog:
    def __init__(self, timestamp: datetime, available_spaces: int):
        self.timestamp = timestamp
        self.available_spaces = available_spaces

class Analyzer:
    def __init__(self):
        self._imported_data = defaultdict(list)

    def load_from_file(self, file):
        text=file.read().strip()
        data=json.loads(text)
        try:
            # print(data)
            timestamp = datetime.fromisoformat(data['timestamp'])
            # print(timestamp)
            stations = data.get('stations', [])

            for station in stations:
                station_no = station.get('station_no')
                available = station.get('available_spaces')

                if station_no is not None and available is not None:
                    log = StationLog(timestamp, available)
                    self._imported_data[station_no].append(log)

        except Exception as e:
            print(f"解析失敗：{e}")

    def get_snapshot_by_timestamp(self, dt: datetime):
        result = []
        for station_no, logs in self._imported_data.items():
            for log in logs:
                if log.timestamp.replace(microsecond=0) == dt.replace(microsecond=0):
                    result.append({
                        "station_no": station_no,
                        "available_spaces": log.available_spaces,
                        "timestamp": log.timestamp.isoformat()
                    })
        return result

    def get_logs_in_range(self, station_no: str, start: datetime, end: datetime):
        logs = self._imported_data.get(station_no, [])
        return [
            log for log in logs
            if start <= log.timestamp <= end
        ]

    def format_logs_as_json(self, logs):
        return [
            {
                "timestamp": log.timestamp.isoformat(),
                "available_spaces": log.available_spaces
            } for log in logs
        ]

    def get_hourly_avg(self, station_no: str):
        logs = self._imported_data.get(station_no)
        if not logs:
            return {}

        hourly_values = defaultdict(list)
        for log in logs:
            hour = log.timestamp.hour
            hourly_values[hour].append(log.available_spaces)

        return {
            hour: round(sum(values) / len(values), 2)
            for hour, values in hourly_values.items()
        }
    
    def get_hourly_avg_delta(self, station_no: str):
        logs = self._imported_data.get(station_no)
        if not logs or len(logs) < 2:
            return {}

        # 確保按時間排序
        logs.sort(key=lambda l: l.timestamp)

        # 群組：每小時一組 (用 tuple(date, hour) 當 key)
        hourly_groups = defaultdict(list)

        for log in logs:
            key = (log.timestamp.date(), log.timestamp.hour)
            hourly_groups[key].append(log.available_spaces)

        # 每組只取頭尾差值
        hourly_flow = defaultdict(float)
        for (date, hour), values in hourly_groups.items():
            if len(values) >= 2:
                flow = abs(values[-1] - values[0])
                hourly_flow[hour] += flow  # 仍以 hour 為主 key，跨天會加總到同一小時

        return {hour: round(flow, 2) for hour, flow in hourly_flow.items()}
