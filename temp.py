# //
# //                       _oo0oo_
# //                      o8888888o
# //                      88" . "88
# //                      (| -_- |)
# //                      0\  =  /0
# //                    ___/`---'\___
# //                  .' \\|     |// '.
# //                 / \\|||  :  |||// \
# //                / _||||| -:- |||||- \
# //               |   | \\\  -  /// |   |
# //               | \_|  ''\---/''  |_/ |
# //               \  .-\__  '-'  ___/-. /
# //             ___'. .'  /--.--\  `. .'___
# //          ."" '<  `.___\_<|>_/___.' >' "".
# //         | | :  `- \`.;`\ _ /`;.`/ - ` : | |
# //         \  \ `_.   \_ __\ /__ _/   .-` /  /
# //     =====`-.____`.___ \_____/___.-`___.-'=====
# //                       `=---='
# //
# //
# //     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# //
# //               佛祖保佑         永無BUG
# //
# //
# //
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import json
import os
from datetime import datetime
import api
app = Flask(__name__)

DATASET_FOLDER = 'dataset'
TEMP_FOLDER = 'temp'
os.makedirs(DATASET_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

YOBIKE_API = "https://api.kcg.gov.tw:443/api/service/Get/b4dd9c40-9027-4125-8666-06bef1756092"

def convert_youbike_full(raw_json):
    """將原始 JSON 轉換成 stations 陣列格式，帶 timestamp"""
    try:
        retVal = raw_json["data"]["data"]["retVal"]
    except KeyError:
        raise ValueError("輸入 JSON 結構不符合預期，找不到 retVal")
    
    stations = []
    for item in retVal:
        parking_spaces = int(item.get("tot", 0))
        available_spaces = int(item.get("sbi", 0))
        empty_spaces = int(item.get("bemp", 0))
        yb2 = int(item.get("sbi_detail", {}).get("yb2", 0))
        eyb = int(item.get("sbi_detail", {}).get("eyb", 0))
        
        station = {
            "station_no": item.get("sno", ""),
            "parking_spaces": parking_spaces,
            "available_spaces": available_spaces,
            "available_spaces_detail": {
                "yb2": yb2,
                "eyb": eyb
            },
            "empty_spaces": empty_spaces,
            "forbidden_spaces": 0,
            "available_spaces_level": round(available_spaces / parking_spaces * 100) if parking_spaces else 0
        }
        stations.append(station)
    
    result = {
        "timestamp": datetime.now().isoformat(),
        "stations": stations
    }
    return result

def get_YouBike2_API():
    """抓取原始 JSON 並產生轉換後 JSON"""
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.youbike.com.tw/",
        "Origin": "https://www.youbike.com.tw",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(YOBIKE_API, headers=HEADERS, timeout=10)
        response.raise_for_status()
        raw_json = response.json()

        # 存原始 JSON
        date_str = raw_json["data"]["data"]["updated_at"].replace(" ", "_")
        converted = convert_youbike_full(raw_json)
        dataset_path = os.path.join(DATASET_FOLDER, f"{date_str}.json")
        with open(dataset_path, "w", encoding="utf-8") as f:
            json.dump(converted, f, ensure_ascii=False, indent=2)
        print("已寫入原始 JSON:", dataset_path)
    except Exception as e:
        print("抓取或轉換失敗:", e)

# official_stations, official_file = api.load_official_youbike()
# 每分鐘執行一次
scheduler = BackgroundScheduler()
# scheduler.add_job(get_YouBike2_API, 'interval', minutes=1, coalesce=True, misfire_grace_time=30)
# scheduler.add_job(api.run, 'interval', minutes=1, coalesce=True, misfire_grace_time=30,args=[official_stations])
scheduler.add_job(api.run2, 'interval', minutes=1, coalesce=True, misfire_grace_time=30)
scheduler.add_job(api.run, 'interval', minutes=17, coalesce=True, misfire_grace_time=30)
scheduler.start()

# Flask 路由
@app.route("/")
def index():
    return "OK"

if __name__ == "__main__":
    # 啟動時先抓一次
    # get_YouBike2_API()
    # official_stations, official_file = api.load_official_youbike()
    api.run()
    app.run(host="0.0.0.0", port=5001)
