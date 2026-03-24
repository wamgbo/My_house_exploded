from flask import Flask, request, jsonify
from flask_cors import CORS
import json # 保持 json 引入，因為 API 回傳格式仍是 JSON
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
# 假設我們將資料庫相關操作集中到這兩個模組中
from analyzer import Analyzer 
from api import Youbike_API 
from db_manager import DBManager # <-- 新增：資料庫操作管理器
from threading import Thread
from functools import wraps
import jwt

# -------------------------
# 資料庫連線配置 (假設放在 db_config.py)
# 為了單一檔案展示，我將其暫時放在這裡，實際應用中建議獨立檔案

DB_CONFIG = {
    "user": "root",
    "password": "csie",
    "host": "192.168.0.19",
    "database": "ubyone",
    "port": "8787",
}
# -------------------------
#           log
# -------------------------
LOG_DIR = "./logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

logging.basicConfig(level=logging.INFO)

# main app logger
app_logger = logging.getLogger("app")
app_logger.setLevel(logging.INFO)

app_handler = RotatingFileHandler(
    f"{LOG_DIR}/app.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5
)
app_handler.setFormatter(logging.Formatter(LOG_FORMAT))
app_logger.addHandler(app_handler)

# error logger
error_handler = RotatingFileHandler(
    f"{LOG_DIR}/error.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5
)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(LOG_FORMAT))
app_logger.addHandler(error_handler)

# upload logger
upload_logger = logging.getLogger("upload")
upload_logger.setLevel(logging.INFO)

upload_handler = RotatingFileHandler(
    f"{LOG_DIR}/upload.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5
)
upload_handler.setFormatter(logging.Formatter(LOG_FORMAT))
upload_logger.addHandler(upload_handler)

# -------------------------
#           log
# -------------------------

app = Flask(__name__)
CORS(app)

YOUBIKE_API = "https://api.kcg.gov.tw:443/api/service/Get/b4dd9c40-9027-4125-8666-06bef1756092"

# 移除 DATASET_FOLDER 和 os.makedirs

# 創建資料庫管理器實例
db_manager = DBManager(DB_CONFIG)

# 將 DBManager 實例傳遞給 Analyzer 和 Youbike_API
analyzer = Analyzer(db_manager=db_manager) 
API = Youbike_API(YOUBIKE_API=YOUBIKE_API, db_manager=db_manager)

# --- Token 驗證裝飾器 ---
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        print("--- Incoming Request Debug ---")
        print(f"Path: {request.path}")
        print(f"All Headers: {dict(request.headers)}")
        token = None
        # 檢查 Header 是否有 Authorization (通常格式是 "Bearer <token>")
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            try:
                token = auth_header.split(" ")[1] # 取得 Bearer 後面的字串
            except IndexError:
                return jsonify({'message': 'Token 格式錯誤 (應為 Bearer <token>)'}), 401

        if not token:
            return jsonify({'message': '缺少 Token，請先登入'}), 401

        try:
            # 解碼 Token
            data = jwt.decode(token, db_manager.SECRET_KEY, algorithms=["HS256"])
            # 將 user_id 從 Token 中提取出來
            current_user_id = data['sub']
        except jwt.ExpiredSignatureError as e: 
            print("DEBUG: Token Expired") # <--- 這裡加 print
            return jsonify({'message': 'Token 已過期，請重新登入'}), 401
        except jwt.InvalidTokenError as e:
            print(f"DEBUG: Invalid Token Error: {str(e)}") # <--- 這裡加 print，看看到底為什麼無效
            return jsonify({'message': '無效的 Token'}), 401

        # 將驗證後的 user_id 傳給後面的 function 使用
        return f(current_user_id, *args, **kwargs)
    
    return decorated

def preload_all_hourly_data():
    print("開始背景預載所有站點 hourly_avg 和 hourly_delta...")
    stations = db_manager.get_all_station_nos()  # DBManager 需有此方法
    
    for station in stations:
        station_no = station['station_no']
        # 觸發計算並快取（不等結果）
        analyzer.get_hourly_avg(station_no)
        analyzer.get_hourly_avg_delta(station_no)
    
    print(f"預載完成，共 {len(stations)} 個站點")
with app.app_context():
    analyzer.refresh_all_cache()
    analyzer.load_previous_week_snapshots()
    
# -------------------------
# API
# -------------------------
@app.route('/upload', methods=['POST'])
def upload():
    try:
        raw_body = request.get_data(as_text=True)
        if not raw_body:
            return jsonify({"error": "Empty request body"}), 400
        raw_json = json.loads(raw_body)
        processed_data = API.process_raw_data(raw_json)
        if not processed_data:
            return jsonify({"error": "No valid stations"}), 400
        
        timestamp_unix = db_manager.save_snapshot(processed_data)
        
        # 立即更新快取，讓下一次呼叫馬上看到最新資料
        analyzer.update_cache_after_upload(processed_data, timestamp_unix)
        
        return jsonify({
            "status": "ok",
            "saved_timestamp": timestamp_unix,
            "record_count": len(processed_data)
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 以下路由由於調用的是 Analyzer 內的方法，且回傳格式不變，故不需要修改
@app.route('/data/<ts>', methods=['GET'])
def get_data(ts):
    try:
        dt = datetime.fromisoformat(ts)
        # Analyzer 內部會改為從 DB 查詢
        result = analyzer.get_snapshot_by_timestamp(dt) 
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/range', methods=['GET'])
def range_query():
    try:
        start = datetime.fromisoformat(request.args['start'])
        end = datetime.fromisoformat(request.args['end'])
        station_no = request.args.get('station_id')
        
        # Analyzer 內部會改為從 DB 查詢
        logs = analyzer.get_logs_in_range(station_no, start, end)
        return jsonify(analyzer.format_logs_as_json(logs))
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/hourly_avg/<station_id>', methods=['GET'])
def hourly_avg(station_id):
    try:
        # Analyzer 內部會改為從 DB 查詢
        avg = analyzer.get_hourly_avg(station_id) 
        return jsonify(avg)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/hourly_delta/<station_id>', methods=['GET'])
def hourly_delta(station_id):
    try:
        # Analyzer 內部會改為從 DB 查詢
        delta = analyzer.get_hourly_avg_delta(station_id) 
        return jsonify(delta)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/user/click', methods=['POST'])
@token_required # 套用裝飾器，此 API 會被保護
def record_click(current_user_id):
    """
    因為有 @token_required，
    1. 前端呼叫時不必傳 user_id，改從 Header 傳 Token
    2. 裝飾器會自動把解析出的 current_user_id 丟進來
    """
    data = request.json
    station_id = data.get('station_id')
    
    if not station_id:
        return jsonify({"message": "缺少 station_id"}), 400
        
    db_manager.record_station_click(current_user_id, station_id)
    return jsonify({"status": "ok"})

@app.route('/api/user/favorite', methods=['POST'])
@token_required
def toggle_favorite(current_user_id):
    data = request.json
    station_id = data.get('station_id')
    action = data.get('action') # 'add' 或 'remove'
    
    db_manager.toggle_favorite(current_user_id, station_id, action)
    return jsonify({"status": "ok"})

@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.json
    try:
        user_id = db_manager.register_user(data['username'], data['password'])
        return jsonify({"status": "ok", "user_id": user_id}), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": "註冊失敗"}), 500

@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.json
    result = db_manager.login_user(data.get('username'), data.get('password'))
    
    if result:
        return jsonify({
            "status": "success",
            "token": result['token'],
            "user": {
                "id": result['id'],
                "username": result['username']
            }
        })
    return jsonify({"status": "error", "message": "帳號或密碼錯誤"}), 401

@app.route('/api/user/profile', methods=['GET'])
@token_required
def get_user_profile(current_user_id):
    """
    獲取使用者的個人化資訊：最近使用與最愛站點
    """
    try:
        data = db_manager.get_user_activity(current_user_id)
        return jsonify({
            "status": "success",
            "data": data
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
@app.route('/empty-route')
def empty():
    return "", 200  # 即使是空字串，也會帶上頭
# 每分鐘執行一次
scheduler = BackgroundScheduler()
# # API.get_YouBike2_API 內部會執行資料擷取和寫入 DB
scheduler.add_job(API.get_YouBike2_API, 'interval', minutes=1, coalesce=True, misfire_grace_time=30,)
# scheduler.start()

# Thread(target=preload_all_hourly_data, daemon=True).start()
scheduler.add_job(
    func=analyzer.refresh_all_cache,
    trigger="interval",
    minutes=60,
    coalesce=True,
    misfire_grace_time=60
)
scheduler.start()

if __name__ == '__main__':
    # 啟動時先執行一次 API 抓取並寫入 DB
    # API.get_YouBike2_API() 
    app.run(host='0.0.0.0', port=5000, use_reloader=False)
