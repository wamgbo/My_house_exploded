from concurrent.futures import ThreadPoolExecutor, as_completed
from mysql.connector import pooling
from datetime import datetime
import mysql.connector
import time
import json
import jwt
from werkzeug.security import generate_password_hash, check_password_hash

"""sql操作"""

class DBManager:
    """管理所有 MySQL 資料庫連線和操作。"""
    
    def __init__(self, db_config):
        print("DBManager: 初始化連線池...")
        self.db_config = db_config
        self.SECRET_KEY = "youbike_app_secret_key_fixed_later"
        try:
            # 使用連線池來管理資料庫連線
            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name="youbike_pool",
                pool_size=32,  # 設置連線池大小
                **self.db_config
            )
            print("DBManager: 連線池創建成功。")
        except mysql.connector.Error as err:
            print(f"DBManager: 連線池創建失敗: {err}")
            raise
    # --- 產生 JWT Token ---
    def generate_token(self, user_id):
        """產生一個有效期限為 7 天的 JWT"""
        from datetime import datetime, timedelta # 在函式內匯入確保安全，或修正頂部匯入
        try:
            # 因為你頂部 import datetime 是類別，所以這裡直接用 datetime.utcnow()
            payload = {
                'exp': datetime.utcnow() + timedelta(days=7),
                'iat': datetime.utcnow(),
                'sub': str(user_id)
            }
            # 確保 SECRET_KEY 存在
            token = jwt.encode(payload, self.SECRET_KEY, algorithm='HS256')
            
            # PyJWT 2.0+ encode 直接回傳字串，若舊版則需 .decode('utf-8')
    #        return token
            return jwt.encode(
            payload,
            self.SECRET_KEY,
            algorithm='HS256'
        )
        except Exception as e:
            print(f"JWT Error: {e}")
            return None # 失敗時回傳 None，避免回傳錯誤字串導致後端 KeyError

    # --- 登入驗證 (回傳 Token) ---
    def login_user(self, username, password):
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT id, username, password_hash FROM users WHERE username = %s", (username,))
            user = cursor.fetchone()
            
            if user and check_password_hash(user['password_hash'], password):
                # 驗證成功，產生 Token
                token = self.generate_token(user['id'])
                return {
                    "id": user['id'],
                    "username": user['username'],
                    "token": token
                }
            return None
        finally:
            conn.close()
    
    def get_connection(self):
        """從連線池獲取一個連線。"""
        return self.connection_pool.get_connection()

    def _execute_query(self, query, params=None, fetch_one=False, fetch_all=False):
        """執行資料庫查詢的通用函式。"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True) # 使用 dictionary=True 獲取字典格式結果
            cursor.execute(query, params)
            
            if fetch_all:
                result = cursor.fetchall()
            elif fetch_one:
                result = cursor.fetchone()
            else:
                conn.commit()
                result = cursor.lastrowid if 'INSERT' in query.upper() else None

            cursor.close()
            return result

        except mysql.connector.Error as err:
            print(f"資料庫錯誤: {err}")
            # 發生錯誤時回滾
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

    def save_snapshot(self, converted_data: dict) -> int:
        """
        將轉換後的單次 YouBike 快照資料寫入 data_snapshots 和 station_records 表。
        :param converted_data: 格式為 {"timestamp": "...", "stations": [...]}
        :return: 寫入的 Unix 時間戳。
        """
        if not converted_data or not converted_data.get("stations"):
            return 0
        
        # 1. 處理時間戳
        iso_timestamp = converted_data["timestamp"]
        dt = datetime.fromisoformat(iso_timestamp)
        unix_timestamp = int(dt.timestamp())
        stations_data = converted_data["stations"]
        record_count = len(stations_data)
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 2. 寫入 data_snapshots 表
            snapshot_query = """
            INSERT INTO data_snapshots (timestamp_unix, timestamp_iso, record_count) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE record_count = VALUES(record_count)
            """
            cursor.execute(snapshot_query, (unix_timestamp, iso_timestamp, record_count))
            
            # 3. 準備批量寫入 station_records 表
            records_query = """
            INSERT INTO station_records (
                timestamp_unix, station_no, parking_spaces, available_spaces, empty_spaces, 
                yb2, eyb, forbidden_spaces, available_level
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            records_to_insert = []
            for station in stations_data:
                # 提取 yb2 和 eyb
                detail = station.get("available_spaces_detail", {})
                yb2 = detail.get("yb2", 0)
                eyb = detail.get("eyb", 0)
                
                # 準備插入資料
                records_to_insert.append((
                    unix_timestamp,
                    station.get("station_no"),
                    station.get("parking_spaces"),
                    station.get("available_spaces"),
                    station.get("empty_spaces"),
                    yb2,
                    eyb,
                    station.get("forbidden_spaces"),
                    station.get("available_spaces_level")
                ))

            # 批量執行插入
            if records_to_insert:
                cursor.executemany(records_query, records_to_insert)

            # 4. 提交事務
            conn.commit()
            print(f"DBManager: 快照 {unix_timestamp} (共 {record_count} 筆) 寫入資料庫成功。")
            return unix_timestamp

        except mysql.connector.Error as err:
            conn.rollback()
            print(f"DBManager: 寫入資料庫失敗: {err}")
            raise
        finally:
            cursor.close()
            conn.close()

    # --------------------
    # DBManager 內的擴充方法 (供 Analyzer 呼叫)
    # --------------------
    def load_all_records(self):
        """從資料庫讀取所有站點記錄。"""
        query = """
        SELECT 
            sr.station_no,
            s.timestamp_iso,
            sr.available_spaces
        FROM station_records sr
        JOIN data_snapshots s ON sr.timestamp_unix = s.timestamp_unix
        ORDER BY s.timestamp_unix ASC
        """
        # 使用通用查詢方法執行
        return self._execute_query(query, fetch_all=True)


    def get_snapshot_by_timestamp(self, dt: datetime):
        """
        根據精確時間戳 (DATETIME) 從 DB 獲取單次快照的所有站點數據。
        用於 /data/<ts> 路由，必須與記憶體查詢結果結構相似。
        """
        # 轉換 datetime 為 ISO 格式字串，忽略微秒
        iso_str = dt.isoformat().split('.')[0] 
        
        query = """
        SELECT 
            sr.station_no,
            sr.available_spaces,
            s.timestamp_iso AS timestamp
        FROM station_records sr
        JOIN data_snapshots s ON sr.timestamp_unix = s.timestamp_unix
        WHERE s.timestamp_iso = %s 
        """
        
        # 使用通用查詢方法執行
        return self._execute_query(query, params=(iso_str,), fetch_all=True)


    def get_range_logs(self, station_no: str, start: datetime, end: datetime, batch_size=5000, max_workers=5):
        """
        根據站點編號和時間範圍從 station_records 獲取記錄（多線程分批按 timestamp 抓取）。
        """
        start_ts = int(start.timestamp())
        end_ts = int(end.timestamp())
        all_rows = []

        # 先取得第一批的起始 timestamp
        batch_starts = [start_ts]

        def fetch_batch(batch_start):
            query = """
            SELECT timestamp_unix, available_spaces
            FROM station_records
            WHERE station_no = %s
            AND timestamp_unix BETWEEN %s AND %s
            AND timestamp_unix > %s
            ORDER BY timestamp_unix ASC
            LIMIT %s
            """
            rows = self._execute_query(query, params=(station_no, start_ts, end_ts, batch_start, batch_size), fetch_all=True)
            for row in rows:
                row['timestamp'] = datetime.fromtimestamp(row.pop('timestamp_unix'))
            return rows

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            current_start = start_ts
            while True:
                future = executor.submit(fetch_batch, current_start)
                rows = future.result()
                if not rows:
                    break
                all_rows.extend(rows)
                # 下一批從最後一筆 timestamp 往後抓
                current_start = int(rows[-1]['timestamp'].timestamp())

        return all_rows

    def get_all_station_nos(self):
        query = "SELECT DISTINCT station_no FROM station_records"
        return self._execute_query(query, fetch_all=True)

    def load_snapshots_batch(self, limit=500, offset=0):
        """分批讀取 data_snapshots"""
        query = f"""
        SELECT timestamp_unix, timestamp_iso, record_count
        FROM data_snapshots
        ORDER BY timestamp_unix ASC
        LIMIT {limit} OFFSET {offset};
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return rows
        except mysql.connector.Error as e:
            print(f"DBManager: 讀取快照失敗: {e}")
            return []
    
    def record_station_click(self, user_id, station_id):
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            # 1. 處理最近使用
            cursor.execute("SELECT recent_stations_json FROM users WHERE id = %s", (user_id,))
            user = cursor.fetchone()
            recent = json.loads(user['recent_stations_json']) if user['recent_stations_json'] else []
            
            if station_id in recent: recent.remove(station_id)
            recent.insert(0, station_id)
            recent = recent[:5]
            
            cursor.execute("UPDATE users SET recent_stations_json = %s WHERE id = %s", 
                           (json.dumps(recent), user_id))

            # 2. 處理最愛站點點擊時間 (如果有在清單內才更新)
            cursor.execute("""
                UPDATE user_favorite_stations 
                SET last_clicked_at = NOW() 
                WHERE user_id = %s AND station_no = %s
            """, (user_id, station_id))
            
            conn.commit()
        finally:
            conn.close()
    
    def register_user(self, username, password):
        """註冊新使用者"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # 產生密碼雜湊
            hashed_pw = generate_password_hash(password)
            query = "INSERT INTO users (username, password_hash, recent_stations_json) VALUES (%s, %s, %s)"
            # 初始化最近站點為空陣列的字串
            cursor.execute(query, (username, hashed_pw, json.dumps([])))
            conn.commit()
            return cursor.lastrowid
        except mysql.connector.Error as err:
            if err.errno == 1062: # Duplicate entry (username 已存在)
                raise ValueError("該帳號名稱已被使用")
            raise err
        finally:
            conn.close()

    # -------------------------
    # 站點互動操作 (最愛與最近)
    # -------------------------
    def toggle_favorite(self, user_id, station_no, action):
        """添加或刪除最愛站點"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if action == 'add':
                # 使用 IGNORE 避免重複添加報錯
                query = "INSERT IGNORE INTO user_favorite_stations (user_id, station_no) VALUES (%s, %s)"
                cursor.execute(query, (user_id, station_no))
            elif action == 'remove':
                query = "DELETE FROM user_favorite_stations WHERE user_id = %s AND station_no = %s"
                cursor.execute(query, (user_id, station_no))
            conn.commit()
        finally:
            conn.close()
    
    def get_user_activity(self, user_id):
        """
        獲取使用者的最近使用與最愛站點 (包含即時狀態)
        """
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            # 1. 取得最近使用站點 ID 列表
            cursor.execute("SELECT recent_stations_json FROM users WHERE id = %s", (user_id,))
            res = cursor.fetchone()
            recent_ids = json.loads(res['recent_stations_json']) if res and res['recent_stations_json'] else []

            # 2. 取得最愛站點與其最後點擊時間
            cursor.execute("""
                SELECT station_no, last_clicked_at 
                FROM user_favorite_stations 
                WHERE user_id = %s
                ORDER BY last_clicked_at DESC
            """, (user_id,))
            favorites_data = cursor.fetchall()
            fav_ids = [f['station_no'] for f in favorites_data]

            # 3. 統一獲取這些站點的「最新即時狀態」
            # 我們從 station_records 抓取最新的快照資訊
            all_target_ids = list(set(recent_ids + fav_ids))
            stations_info = {}
            
            if all_target_ids:
                # 這裡使用一個子查詢來確保抓到的是每個站點最新的那一筆資料
                format_strings = ','.join(['%s'] * len(all_target_ids))
                query = f"""
                    SELECT r.* FROM station_records r
                    INNER JOIN (
                        SELECT station_no, MAX(id) as max_id 
                        FROM station_records 
                        WHERE station_no IN ({format_strings})
                        GROUP BY station_no
                    ) as latest ON r.id = latest.max_id
                """
                cursor.execute(query, tuple(all_target_ids))
                for row in cursor.fetchall():
                    stations_info[row['station_no']] = row

            # 4. 組合回傳結果，保持最近使用的順序
            recent_list = [stations_info[sid] for sid in recent_ids if sid in stations_info]
            
            # 組合最愛清單，並附上最後點擊時間
            fav_list = []
            for f in favorites_data:
                sid = f['station_no']
                if sid in stations_info:
                    info = stations_info[sid].copy()
                    info['user_last_clicked_at'] = f['last_clicked_at'].isoformat()
                    fav_list.append(info)

            return {
                "recent_stations": recent_list,
                "favorite_stations": fav_list
            }
        finally:
            conn.close()
# 為了讓 app.py 的 load_dataset 邏輯可以直接使用 DBManager 載入所有資料
DBManager.load_all_snapshots = DBManager.load_all_records
