from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import requests
import json
from tqdm import tqdm  # CLI 進度條
from datetime import datetime
from analyzer import Analyzer
from api import Youbike_API
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app)

YOUBIKE_API = "https://api.kcg.gov.tw:443/api/service/Get/b4dd9c40-9027-4125-8666-06bef1756092"
DATASET_FOLDER = 'dataset'
os.makedirs(DATASET_FOLDER, exist_ok=True)
analyzer = Analyzer()
API=Youbike_API(YOUBIKE_API=YOUBIKE_API,DATASET_FOLDER=DATASET_FOLDER)

with app.app_context():#告訴python在flask環境下運行，這樣可以使用本py的function
    def load_dataset():
        files = [f for f in os.listdir(DATASET_FOLDER) if f.endswith('.json')]
        for fname in tqdm(files,desc="讀取資料"):
            if fname.endswith('.json'):
                try:
                    with open(os.path.join(DATASET_FOLDER, fname), "r", encoding="utf-8") as f:
                        analyzer.load_from_file(f)
                except Exception as e:
                    print(f"Failed to parse {fname}: {e}")
    load_dataset()
# -------------------------
# API
# -------------------------
@app.route('/upload', methods=['POST'])
def upload():
    try:
        raw_body = request.get_data(as_text=True)

        timestamp = datetime.now().isoformat()
        filename = f'data_{timestamp.replace(":", "-")}.json'
        path = os.path.join(DATASET_FOLDER, filename)

        with open(path, 'w', encoding='utf-8') as f:
            f.write(raw_body)

        with open(path, 'r', encoding='utf-8') as f:
            analyzer.load_from_file(f)

        return jsonify({"status": "ok", "saved": filename}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/data/<ts>', methods=['GET'])
def get_data(ts):
    try:
        dt = datetime.fromisoformat(ts)
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

        logs = analyzer.get_logs_in_range(station_no, start, end)
        return jsonify(analyzer.format_logs_as_json(logs))
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/hourly_avg/<station_id>', methods=['GET'])
def hourly_avg(station_id):
    try:
        avg = analyzer.get_hourly_avg(station_id)
        return jsonify(avg)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/hourly_delta/<station_id>', methods=['GET'])
def hourly_delta(station_id):
    try:
        delta = analyzer.get_hourly_avg_delta(station_id)
        return jsonify(delta)
    except Exception as e:
        return jsonify({"error": str(e)}), 400



# 每分鐘執行一次
scheduler = BackgroundScheduler()
scheduler.add_job(API.get_YouBike2_API, 'interval', minutes=1, coalesce=True, misfire_grace_time=30,)
scheduler.start()

if __name__ == '__main__':
    API.get_YouBike2_API()
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
