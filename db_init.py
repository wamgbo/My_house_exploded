import mysql.connector
from mysql.connector import errorcode
import getpass

"""初始化資料庫"""

# ⚠️ **請根據您的環境修改這些連線設定** 
DB_CONFIG = {
    "user": "USER",
    "password": "PASSWORD", 
    "host": "IPADDRESS",
    "database": "DATABASE",
}

# 資料表定義
TABLES = {}

# 1. 資料快照/批次記錄表
TABLES['data_snapshots'] = (
    """
    CREATE TABLE data_snapshots (
        timestamp_unix INT UNSIGNED NOT NULL PRIMARY KEY,
        timestamp_iso DATETIME,
        record_count INT UNSIGNED
    ) ENGINE=InnoDB
    """
)

# 2. 站點記錄表
TABLES['station_records'] = (
    """
    CREATE TABLE station_records (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        timestamp_unix INT UNSIGNED NOT NULL,
        station_no VARCHAR(10) NOT NULL,
        parking_spaces SMALLINT UNSIGNED,
        available_spaces SMALLINT UNSIGNED,
        empty_spaces SMALLINT UNSIGNED,
        yb2 SMALLINT UNSIGNED,
        eyb SMALLINT UNSIGNED,
        forbidden_spaces SMALLINT UNSIGNED,
        available_level TINYINT UNSIGNED,
        
        -- 複合索引，用於加快時間點和站點編號的查詢
        INDEX idx_timestamp_station (timestamp_unix, station_no),
        
        -- 外鍵約束：確保每筆記錄都對應一個有效的快照時間
        FOREIGN KEY (timestamp_unix)
            REFERENCES data_snapshots(timestamp_unix)
            ON DELETE CASCADE 
            ON UPDATE CASCADE
    ) ENGINE=InnoDB
    """
)

TABLES['users'] = (
    """
    CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    recent_stations_json TEXT, -- 存儲 JSON 格式的最近 5 個站點 ID
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
)

TABLES['user_favorite_stations'] = (
    """
    CREATE TABLE user_favorite_stations (
    user_id INT NOT NULL,
    station_no VARCHAR(10) NOT NULL,
    last_clicked_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, station_no),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
)
def create_database(cursor, db_name):
    """嘗試創建資料庫，如果已存在則忽略。"""
    try:
        cursor.execute(f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET 'utf8mb4'")
        print(f"成功創建資料庫: {db_name}")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DB_CREATE_EXISTS:
            print(f"資料庫 {db_name} 已存在，跳過創建。")
        else:
            print(f"創建資料庫失敗: {err}")
            exit(1)


def create_tables(cursor):
    """創建所有定義的資料表。"""
    for name, ddl in TABLES.items():
        try:
            print(f"創建資料表 {name}: ", end='')
            cursor.execute(ddl)
            print("OK")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("已存在，跳過。")
            else:
                print(err.msg)
                print("創建資料表失敗，請檢查 DDL 語法。")


def initialize_db():
    """主初始化函式"""
    
    # 第一次連線：不指定資料庫，以便先創建資料庫
    try:
        # 如果密碼為空，提示使用者輸入
        if not DB_CONFIG['password']:
            print("注意：您的密碼設定為空，請輸入密碼 (如果您沒有密碼，直接按 Enter)")
            DB_CONFIG['password'] = getpass.getpass("MySQL 密碼: ")
            
        cnx = mysql.connector.connect(
            user=DB_CONFIG['user'], 
            password=DB_CONFIG['password'], 
            host=DB_CONFIG['host']
        )
        cursor = cnx.cursor()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("連線失敗：使用者名稱或密碼錯誤。")
        elif err.errno == errorcode.CR_CONN_ERROR:
            print(f"連線失敗：無法連線到 MySQL 主機 {DB_CONFIG['host']}。請確保服務已啟動。")
        else:
            print(f"連線失敗：{err}")
        return

    # 1. 創建資料庫
    create_database(cursor, DB_CONFIG['database'])
    
    # 2. 切換到新創建的資料庫
    try:
        cnx.database = DB_CONFIG['database']
    except mysql.connector.Error as err:
        print(f"切換資料庫失敗: {err}")
        return
        
    # 3. 創建資料表
    create_tables(cursor)

    cursor.close()
    cnx.close()
    print("\n資料庫初始化完成。")


if __name__ == '__main__':
    initialize_db()
