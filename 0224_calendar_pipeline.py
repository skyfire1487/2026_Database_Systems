# ===============================
# 匯入標準函式庫：日期處理
# ===============================
from datetime import date
import calendar as pycal

# ===============================
# 其他工具
# ===============================
import time                  # 用來暫停，讓你看到瀏覽器畫面
import requests              # 呼叫 TWSE 官方 API
import pymssql               # 連線 SQL Server
from bs4 import BeautifulSoup  # 解析股票清單 HTML

# ===============================
# selenium：只用來「顯示流程」
# ===============================
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ===============================
# 建立資料庫連線
# ===============================
def get_db_conn():
    return pymssql.connect(
        server="127.0.0.1",
        user="skyfire",
        password="1487",
        database="ncu_db"
    )


# ===============================
# 安全解析 TWSE 日期字串
# ===============================
def parse_twse_date(s):
    # 不是字串就直接放棄
    if not isinstance(s, str):
        return None

    # 去除前後空白
    s = s.strip()

    # 沒有 / 的一定不是日期
    if "/" not in s:
        return None

    # 必須剛好是 YYYY/MM/DD
    parts = s.split("/")
    if len(parts) != 3:
        return None

    try:
        # 嘗試轉成 date 物件
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except:
        # 任何轉型錯誤都視為非法
        return None


# ===============================
# 開啟瀏覽器（只為了讓你看到）
# ===============================
def open_browser(title, url, wait=3):
    print(f"\n[BROWSER] {title}")

    # 設定 Chrome 視窗大小
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")

    # 自動下載並啟動 ChromeDriver
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    # 開啟指定網址
    driver.get(url)

    # 停留幾秒讓你看清楚
    time.sleep(wait)

    return driver


# ===============================
# STEP 1：建立 calendar / year_calendar
# ===============================
def crawl_calendar(target_year):
    print(f"\n[STEP 1] 建立 calendar / year_calendar（{target_year}）")

    # 開瀏覽器顯示來源頁
    driver = open_browser(
        "顯示 TWSE 行事曆來源頁",
        "https://www.twse.com.tw/holidaySchedule/holidaySchedule",
        wait=5
    )

    # 呼叫 TWSE 官方行事曆 API
    print("[CALENDAR] 呼叫官方 API")
    res = requests.get(
        "https://www.twse.com.tw/holidaySchedule/holidaySchedule",
        params={"response": "json"},
        timeout=30
    )
    data = res.json()

    # 收集指定年份的假日
    holiday_dict = {}

    for r in data.get("data", []):
        dt = parse_twse_date(r[0])
        if dt and dt.year == target_year:
            holiday_dict[dt] = r[1] if len(r) > 1 else ""

    print(f"[CALENDAR] 假日 {len(holiday_dict)} 筆")

    # 寫入資料庫
    conn = get_db_conn()
    cursor = conn.cursor()

    # 先清掉舊資料，避免重複
    cursor.execute("DELETE FROM calendar WHERE YEAR(date)=%s", (target_year,))
    cursor.execute("DELETE FROM year_calendar WHERE year=%s", (target_year,))
    conn.commit()

    work_day = 0

    # 跑完整年每一天
    for m in range(1, 13):
        for d in range(1, pycal.monthrange(target_year, m)[1] + 1):
            dt = date(target_year, m, d)
            weekday = pycal.weekday(target_year, m, d)

            # 預設為非交易日
            day_of_stock = -1
            other = ""

            # 假日
            if dt in holiday_dict:
                other = holiday_dict[dt]
            # 週末
            elif weekday in (5, 6):
                pass
            # 平日交易日
            else:
                work_day += 1
                day_of_stock = work_day

            # 寫入 calendar
            cursor.execute(
                "INSERT INTO calendar (date, day_of_stock, other) VALUES (%s,%s,%s)",
                (dt, day_of_stock, other)
            )

    # 寫入 year_calendar
    cursor.execute(
        "INSERT INTO year_calendar (year, total_day) VALUES (%s,%s)",
        (target_year, work_day)
    )

    conn.commit()
    conn.close()

    print(f"[CALENDAR] 完成，交易日={work_day}")

    # 保留瀏覽器畫面
    time.sleep(5)
    driver.quit()


# ===============================
# STEP 2：建立 stock_list（上市）
# ===============================
def crawl_stock_list():
    print("\n[STEP 2] 建立 stock_list")

    # 顯示資料來源頁
    driver = open_browser(
        "顯示 ISIN 股票清單來源",
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
        wait=5
    )

    # 下載 HTML
    res = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2")
    soup = BeautifulSoup(res.text, "html.parser")

    conn = get_db_conn()
    cursor = conn.cursor()

    count = 0

    # 逐列解析股票資料
    for tr in soup.select("table tr"):
        tds = tr.find_all("td")
        if len(tds) >= 5 and "　" in tds[0].text:
            code, name = tds[0].text.strip().split("　", 1)
            category = tds[4].text.strip()

            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM stock_list WHERE stock_code=%s)
                INSERT INTO stock_list
                (stock_code, name, type, category, isTaiwan50)
                VALUES (%s,%s,%s,%s,0)
            """, (code, code, name, "上市", category))

            count += cursor.rowcount

    conn.commit()
    conn.close()

    print(f"[STOCK_LIST] 新增 {count} 筆")

    time.sleep(5)
    driver.quit()


# ===============================
# STEP 3：建立 stock_data（2330 日資料）
# ===============================
def crawl_stock_data():
    print("\n[STEP 3] 建立 stock_data（2330 日資料）")

    # 顯示股價來源頁
    driver = open_browser(
        "顯示 TWSE 股價查詢頁",
        "https://www.twse.com.tw/zh/trading/historical/stock-day.html",
        wait=5
    )

    # 呼叫股價 API
    print("[STOCK_DATA] 呼叫 TWSE API")
    res = requests.get(
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY",
        params={
            "response": "json",
            "date": "20240101",
            "stockNo": "2330"
        },
        timeout=30
    ).json()

    conn = get_db_conn()
    cursor = conn.cursor()

    # 寫入每日股價
    for r in res.get("data", []):
        y, m, d = r[0].split("/")
        trade_date = date(int(y) + 1911, int(m), int(d))

        cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM stock_data
            WHERE stock_code='2330' AND date=%s AND time IS NULL
        )
        INSERT INTO stock_data
        (stock_code, date, time, tv, t, o, h, l, c, d, v)
        VALUES
        ('2330', %s, NULL, %s,%s,%s,%s,%s,%s,%s,%s)
        """, (
        trade_date,                              # WHERE date=%s
        trade_date,                              # VALUES %s (date)
        int(r[1].replace(",", "")),              # tv
        int(r[2].replace(",", "")),              # t
        float(r[3]),                             # o
        float(r[4]),                             # h
        float(r[5]),                             # l
        float(r[6]),                             # c
        float(r[7]),                             # d ← ★你之前少的就是這個
        int(r[8].replace(",", ""))               # v
        ))


    conn.commit()
    conn.close()

    print("[STOCK_DATA] 完成")

    time.sleep(5)
    driver.quit()


# ===============================
# 主程式（控制是否重跑）
# ===============================
if __name__ == "__main__":
    RUN_CALENDAR   = False
    RUN_STOCK_LIST = False
    RUN_STOCK_DATA = True

    if RUN_CALENDAR:
        crawl_calendar(2025)

    if RUN_STOCK_LIST:
        crawl_stock_list()

    if RUN_STOCK_DATA:
        crawl_stock_data()

    print("\n=== 全部流程完成，可直接截圖驗收 ===")
