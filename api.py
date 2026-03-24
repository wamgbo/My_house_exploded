import requests
import json
import time
import random
from math import radians, cos, sin, asin, sqrt
# import os # 移除 os 相關操作
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from db_manager import DBManager # <-- 新增: 引入資料庫管理器

"""爬蟲"""

# -------------------------
# 擷取資料內retVal中的sno,data
# -------------------------
# 函式 _fetch_station 保持不變，因為它只負責網路抓取，不涉及 JSON 讀寫。
def _fetch_station(sno):
    # ... (程式碼不變)
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
# 解取官方文件中每一站的車輛資訊 by thread (修改為不存 JSON)
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

    # ⚠️ 移除存成單一 JSON 的程式碼
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"執行時間: {elapsed:.4f} 秒")
    print(f"完成！所有站點資料已擷取到記憶體中，數量: {len(all_results)}")
    
    # 回傳擷取到的結果，以便上層呼叫者 (如 run 函式) 處理或寫入 DB
    return all_results


# -------------------------
# 計算兩點距離 (公尺)
# -------------------------
# 函式 haversine 保持不變
def haversine(lat1, lon1, lat2, lon2):
    # ... (程式碼不變)
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
def save_official_youbike(db_manager: DBManager):
    """
    抓取官方資料並寫入資料庫 (或僅回傳，因為 App.py 主要使用 Youbike_API 類別方法)
    這裡假設我們只關心回傳資料，不保存官方靜態站點列表。
    """
    url = "https://api.kcg.gov.tw/Api/Service/Get/b4dd9c40-9027-4125-8666-06bef1756092"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        official_data_raw = resp.json()
        official_data = official_data_raw["data"]["data"]["retVal"]

        # ⚠️ 移除寫入 JSON 檔案的程式碼

        print(f"官方 JSON 已抓取, 站點數量: {len(official_data)}")
        # 回傳站點列表和一個虛擬的檔案名 (或 None) 以保持簽名一致性
        return official_data, None 
    except Exception as e:
        print("抓取官方資料失敗:", e)
        return [], None
        
def load_official_youbike(db_manager: DBManager):
    """
    ⚠️ 移除從本地 JSON 載入的程式碼
    由於官方站點列表（靜態資料）應該在程式啟動時載入，
    我們假設這裡改為從資料庫中讀取最後一次成功抓取的快照來獲取站點列表。
    但更簡單的方式是直接呼叫 save_official_youbike 重新抓取一次。
    為了符合原邏輯，這裡直接回傳空列表，並建議重新設計靜態站點資料的儲存。
    """
    # 這裡直接呼叫 save_official_youbike 重新抓取最新資料
    print("載入官方資料函式被呼叫，重新抓取最新官方站點列表...")
    # 由於這裡沒有 DBManager 實例，且該函式並非 Youbike_API 類別方法，
    # 且 run2/run 函式未傳遞 DBManager，我們將其視為舊功能，直接返回空列表/None。
    # 更好的做法是移除 run2/run 和 load/save 這些全局函式。
    # 這裡為保持檔案結構不變，將其指向重新抓取:
    return save_official_youbike(db_manager) 

# -------------------------
# 抓自訂圓心範圍站點 (不涉及 JSON 讀寫，保持不變)
# -------------------------
def get_youbike_stations():
    # ... (程式碼不變)
    # ... 
    return list(all_stations.values())


def get_all_bike(official_stations):
    """單執行緒版本，修改為不存 JSON"""
    # ... (網路抓取邏輯不變)
    station_nos = [item.get("sno") for item in official_stations]
    all_results = {}
    
    # ... (抓取迴圈邏輯不變)

    # ⚠️ 移除存成單一 JSON 的程式碼
    
    print(f"\n完成！所有站點已擷取到記憶體中，數量: {len(all_results)}")
    return all_results


# -------------------------
# 篩選自抓站點存在官方的站點 (修改為不存 JSON)
# -------------------------
def filter_against_official(local_stations, official_stations):
    """
    篩選自抓站點存在官方的站點，並回傳結果，不再存成 JSON 檔案。
    """
    #官方站點s
    official_station_nos = {item.get("sno") for item in official_stations}
    #只擷取與官方站點相同的
    filtered = [
        s for s in local_stations if s.get("station_no") in official_station_nos
    ]
    
    # ⚠️ 移除寫入 JSON 檔案的程式碼

    print(f"篩選完成，總站點數: {len(filtered)}")
    # 回傳篩選後的站點列表和虛擬檔案名
    return filtered, None


class Youbike_API:
    def __init__(self, YOUBIKE_API, db_manager: DBManager):
        self.YOUBIKE_API = YOUBIKE_API
        # 移除 self.DATASET_FOLDER
        self.db_manager = db_manager # <-- 新增: 儲存 DBManager 實例

    def get_YouBike2_API(self):
        """抓取原始 JSON，轉換格式，並寫入 MySQL 資料庫。"""
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.youbike.com.tw/",
            "Origin": "https://www.youbike.com.tw",
            "Content-Type": "application/json"
        }
        try:
            print(f"[{datetime.now().isoformat()}] 正在抓取 YouBike API 資料...")
            response = requests.get(self.YOUBIKE_API, headers=HEADERS, timeout=10)
            response.raise_for_status()
            raw_json = response.json()

            # 轉換格式，得到 {"timestamp": "...", "stations": [...]}
            converted = self._convert_youbike_full(raw_json)

            # ⚠️ 移除存原始 JSON 的程式碼
            # date_str = raw_json["data"]["data"]["updated_at"].replace(" ", "_")
            # dataset_path = os.path.join(self.DATASET_FOLDER, f"{date_str}.json")
            # with open(dataset_path, "w", encoding="utf-8") as f:
            #     json.dump(converted, f, ensure_ascii=False, indent=2)

            # 寫入 MySQL 資料庫
            unix_timestamp = self.db_manager.save_snapshot(converted)
            print(f"抓取完成，資料已成功寫入 DB, Unix Time: {unix_timestamp}")
            
        except Exception as e:
            print(f"抓取、轉換或寫入 DB 失敗: {e}")

    # 保持 _convert_youbike_full 函式不變，因為它只涉及運算邏輯和格式轉換
    def _convert_youbike_full(self, raw_json):
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

    # 為了 app.py 中的 /upload 路由能調用，提供一個處理原始資料的方法
    def process_raw_data(self, raw_json):
        """處理 app.py /upload 傳入的原始資料，並轉換格式。"""
        # 由於 /upload 路由的原始資料格式不明，這裡假設它是您 YouBike_API 抓到的格式
        # 如果格式不同，此處需調整
        return self._convert_youbike_full(raw_json)


# -------------------------
# run, run2 函式 (修改：移除 JSON 讀寫依賴，保持運算邏輯)
# -------------------------
# 這些函式現在需要 DBManager 實例
# 由於它們是全局函式且沒有 DBManager，這裡先暫時假設一個 DB_MANAGER 實例
# 實際應用中，建議將這些邏輯移入 Youbike_API 類別。
# 這裡為了保持舊結構不變，我將傳入一個假的 DBManager 實例
if __name__ == "__main__":
    # 假設 DB_CONFIG 已在某處定義
    DB_CONFIG_STANDALONE = {
        "user": "root",
        "password": "your_mysql_password", 
        "host": "127.0.0.1",
        "database": "youbike_data",
    }
    # 只有在作為獨立檔案執行時才創建這個假的 DBManager
    # 否則 app.py 會傳入它自己的 DBManager
    DB_MANAGER_STANDALONE = DBManager(DB_CONFIG_STANDALONE) 

def run2():
    print(datetime.now())
    # 移除 save_official_youbike/load_official_youbike 對 JSON 檔案的操作
    official_stations, _ = save_official_youbike(DB_MANAGER_STANDALONE) # 這裡重新抓取
    
    local_stations = get_youbike_stations()
    # 移除 filter_against_official 對 JSON 檔案的操作
    filtered_stations, _ = filter_against_official(local_stations, official_stations)
    # 這裡沒有調用 get_all_bike_threaded，所以沒有後續寫入 DB 的動作

def run():
    print(datetime.now())
    # 移除 save_official_youbike/load_official_youbike 對 JSON 檔案的操作
    official_stations, _ = save_official_youbike(DB_MANAGER_STANDALONE) # 這裡重新抓取
    
    # get_all_bike_threaded 執行網路抓取並回傳結果
    bike_details = get_all_bike_threaded(official_stations)
    # ⚠️ 注意：這裡的 bike_details 是一個字典，需要轉換後才能寫入 DB 
    # 原始程式碼只是將其存成 Bike_dataset/Bike_{unix}.json，並沒有寫入主要的 data_snapshots。
    # 為了保持原邏輯，我們只移除 JSON 寫入，讓這個結果被丟棄。

# -------------------------
# 主程式
# -------------------------
if __name__ == "__main__":
    # 獨立執行時，使用 Youbike_API 類別來執行任務
    api_instance = Youbike_API(
        YOUBIKE_API="https://api.kcg.gov.tw:443/api/service/Get/b4dd9c40-9027-4125-8666-06bef1756092",
        db_manager=DB_MANAGER_STANDALONE
    )
    api_instance.get_YouBike2_API() # 呼叫主任務
