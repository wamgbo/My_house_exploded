import requests
import json
import time
import random
from math import radians, cos, sin, asin, sqrt
import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


# -------------------------
# 擷取資料內retVal中的sno,data
# -------------------------
def _fetch_station(sno):
    url = f"https://apis.youbike.com.tw/api/front/bike/lists?station_no={sno}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data.get("retVal"), list):
            return sno, data["retVal"]
        else:
            return sno, data
    except Exception as e:
        return sno, {"error": str(e)}


# -------------------------
# 解取官方文件中每一站的車輛資訊 by thread
# -------------------------
def get_all_bike_threaded(official_stations, max_workers=10):
    start_time = time.time()  # 計時一次抓完時間用
    station_nos = [item.get("sno") for item in official_stations]
    all_results = {}

    # 使用 ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_station, sno): sno for sno in station_nos}

        for future in as_completed(futures):
            sno, result = future.result()
            all_results[sno] = result
            print(
                f"完成站點 {sno}, 車輛數: {len(result) if isinstance(result, list) else 'error'}"
            )
            time.sleep(random.uniform(0.3, 1.0))  # 控制速率，避免被封鎖(顯然沒什麼用)

    # 存成單一 JSON
    output_dir = "./Bike_dataset"
    os.makedirs(output_dir, exist_ok=True)
    unix = int(time.time())  # 設定成unix時間作為filename
    output_file = os.path.join(output_dir, f"Bike_{unix}.json")
    # 整合眉筆資料成一筆json
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"執行時間: {elapsed:.4f} 秒")
    print(f"\n完成！所有站點已存成：{output_file}")
    return output_file


# -------------------------
# 計算兩點距離 (公尺)
# -------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # 地球半徑 (公尺)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )
    c = 2 * asin(sqrt(a))
    return R * c


# -------------------------
# 抓官方高雄 YouBike JSON
# -------------------------
def save_official_youbike():
    url = "https://api.kcg.gov.tw/Api/Service/Get/b4dd9c40-9027-4125-8666-06bef1756092"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        official_data_raw = resp.json()
        official_data = official_data_raw["data"]["data"]["retVal"]

        timestamp = int(time.time())
        filename = f"kaohsiung_official_{timestamp}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(official_data, f, ensure_ascii=False, indent=2)

        print(f"官方 JSON 已存檔: {filename}, 站點數量: {len(official_data)}")
        print(official_data)
        return official_data, filename
    except Exception as e:
        print("抓取官方資料失敗:", e)
        return [], None
def load_official_youbike():
    latest_file="kaohsiung_official_1764749435.json"
    try:
        with open(latest_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"已載入最新官方資料: {latest_file}, 站點數量: {len(data)}")
        return data, latest_file
    except Exception as e:
        print("載入官方資料失敗:", e)
        return [], None
# -------------------------
# 抓自訂圓心範圍站點
# -------------------------
def get_youbike_stations():
    current_unix = int(time.time())
    centers = [
        {"name": "A", "lat": 23.1800, "lng": 120.2000},
        {"name": "B", "lat": 22.7700, "lng": 120.2900},
        {"name": "C", "lat": 22.7700, "lng": 120.3800},
        {"name": "D", "lat": 22.7000, "lng": 120.2500},
        {"name": "E", "lat": 22.6800, "lng": 120.3400},
        {"name": "F", "lat": 22.6800, "lng": 120.4100},
        {"name": "G", "lat": 22.6200, "lng": 120.2700},
        {"name": "H", "lat": 22.6000, "lng": 120.3600},
        {"name": "I", "lat": 22.5500, "lng": 120.3800},
        {"name": "J", "lat": 22.4800, "lng": 120.3000},
        {"name": "K", "lat": 22.8000, "lng": 120.3500},
        {"name": "L", "lat": 22.6500, "lng": 120.4500},
        {"name": "M", "lat": 22.7300, "lng": 120.2000},
        {"name": "N", "lat": 22.9450, "lng": 120.1000},
        {"name": "O", "lat": 22.8700, "lng": 120.2700},
        {"name": "P", "lat": 22.8300, "lng": 120.3500},
        {"name": "Q", "lat": 22.8900, "lng": 120.4500},
        {"name": "R", "lat": 22.9500, "lng": 120.3700},
        {"name": "S", "lat": 22.8000, "lng": 120.5500},
        {"name": "美濃湖", "lat": 22.90761, "lng": 120.55159},
        {"name": "甲仙親水公園", "lat": 23.08244, "lng": 120.58698},
    ]
    radius = 10000  # 半徑 10 km
    url = "https://apis.youbike.com.tw/tw2/parkingInfo"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

    all_stations = {}
    for center in centers:
        payload = {"lat": center["lat"], "lng": center["lng"], "maxDistance": radius}
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for station in data.get("retVal", []):
                    station_no = station.get("station_no")
                    #不擷取重複站點
                    if station_no not in all_stations:
                        all_stations[station_no] = station
            else:
                print(f"圓心 {center['name']} HTTP {resp.status_code}")
        except Exception as e:
            print(f"圓心 {center['name']} 抓取錯誤: {e}")

    # 移除不必要欄位
    for station in all_stations.values():
        for key in ["lat", "lng", "status"]:
            station.pop(key, None)

    print(f"自抓站點完成，總共 {len(all_stations)} 個站點")
    return list(all_stations.values())




def get_all_bike(official_stations):
    # 1. 取得全部站點代號 sno
    station_nos = [item.get("sno") for item in official_stations]

    all_results = {}

    for sno in station_nos:
        url = f"https://apis.youbike.com.tw/api/front/bike/lists?station_no={sno}"
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()

            data = resp.json()

            if isinstance(data.get("retVal"), list):
                all_results[sno] = data["retVal"]
            else:
                all_results[sno] = data

            print(f"成功抓到站點 {sno}, 車輛數={len(all_results[sno])}")

        except Exception as e:
            print(f"站點 {sno} 錯誤：{e}")
            all_results[sno] = {"error": str(e)}

    # 4. 存成單一 JSON
    output_dir = "./Bike_dataset"
    os.makedirs(output_dir, exist_ok=True)

    unix = int(time.time())
    output_file = os.path.join(output_dir, f"Bike_{unix}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\n完成！所有站點已存成：{output_file}")
    return output_file


# -------------------------
# 篩選自抓站點存在官方的站點
# -------------------------
def filter_against_official(local_stations, official_stations):
    #官方站點s
    official_station_nos = {item.get("sno") for item in official_stations}
    #只擷取與官方站點相同的
    filtered = [
        s for s in local_stations if s.get("station_no") in official_station_nos
    ]
    #檔名存成unix時間
    timestamp = int(time.time())

    output_dir = "./dataset"
    os.makedirs(output_dir, exist_ok=True)  
    output_file = os.path.join(output_dir, f"{timestamp}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False, indent=2)
    print(f"篩選完成，總站點數: {len(filtered)}, 已存檔: {output_file}")
    return output_file


# def run(official_stations):
def run2():
    print(datetime.datetime.now())
    #下載官方 JSON（存成本地端）
    # official_stations, official_file = save_official_youbike()
    #娶本地端
    official_stations, official_file = load_official_youbike()
    #取得自抓站點（不存檔）
    local_stations = get_youbike_stations()
    #篩選自抓站點存在官方的站點，最後才存成 JSON
    filter_against_official(local_stations, official_stations)
    #抓取每一站點詳細資料
    # get_all_bike_threaded(official_stations)
def run():
    print(datetime.datetime.now())
    #下載官方 JSON（存成本地端）
    # official_stations, official_file = save_official_youbike()
    #娶本地端
    official_stations, official_file = load_official_youbike()
    #取得自抓站點（不存檔）
    # local_stations = get_youbike_stations()
    #篩選自抓站點存在官方的站點，最後才存成 JSON
    # filter_against_official(local_stations, official_stations)
    #抓取每一站點詳細資料
    get_all_bike_threaded(official_stations)


# -------------------------
# 主程式
# -------------------------
if __name__ == "__main__":
    run()