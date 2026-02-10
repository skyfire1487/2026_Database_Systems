# ===============================
# 1) 匯入：正則/DB/HTTP/HTML解析/瀏覽器自動化
# ===============================
import re                          # 正則：用來從文字中「抓出4位數股票代碼」
import pymssql                     # 連線 SQL Server（把爬到的資料寫入資料庫）
import requests                    # 發HTTP請求：下載網頁HTML（ISIN股票清單頁）
from bs4 import BeautifulSoup      # 解析HTML：把HTML變成可用CSS selector找資料的物件

# selenium：用「真的開瀏覽器」方式抓動態內容（CMoney是動態渲染，requests可能抓不到表格）
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# 等待機制：避免網頁還沒載完就抓元素（常見爬蟲失敗原因）
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.common.exceptions import TimeoutException  # 網頁載入超時例外
from selenium.webdriver.chrome.service import Service

# webdriver_manager：自動下載對應版本的ChromeDriver（不用手動放exe）
from webdriver_manager.chrome import ChromeDriverManager


# ===============================
# 2) SQL Server連線設定（對應你本機SQL帳密）
# ===============================
db_settings = {
    "host": "127.0.0.1",      # 本機
    "user": "skyfire",        # SQL帳號
    "password": "1487",       # SQL密碼
    "database": "ncu_db",     # 使用的資料庫
    "charset": "utf8"
}

# ===============================
# 3) taiwan50：存「0050前10大成分股」的股票代碼
#    用set的理由：查詢 O(1)、不會重複
# ===============================
taiwan50 = set()


def extract_4digit_code(text: str) -> str:
    """
    功能：從一段文字中抓出「4位數股票代碼」

    Why（為什麼要做這件事）：
    - CMoney表格第一欄「代號」理論上應該是2330、2317這種純數字
    - 但實務上常遇到：
      1) 代號 + 名稱黏在一起（例如：'2330 台積電'）
      2) 有換行/空白（例如：' 2330\\n台積電 '）
    - 如果你直接用字串比對：'2330 台積電' != '2330'
      → 會造成你之前的問題：台灣50只標到6筆

    How（怎麼做）：
    - 用正則找出「剛好四位數」的片段
      \\b\\d{4}\\b
      \\b：單字邊界
      \\d{4}：四位數字
    """
    if not text:              # text是空或None就直接回傳空字串
        return ""

    m = re.search(r"\b\d{4}\b", text)  # 在text中找四位數
    return m.group(0) if m else ""     # 找到就回傳那四位數，找不到回空字串


def find_Taiwan50():
    """
    STEP 1：從 CMoney 的 0050 持股明細頁，抓出前10大成分股代碼
    """
    print("\n[STEP 1] 開始爬取0050前10大成分股(股票代碼)...")

    # ---------- Selenium瀏覽器設定 ----------
    options = Options()
    options.add_argument("--headless")                 # headless：不顯示視窗（跑得快/適合自動化）
    options.add_argument("--disable-notifications")    # 關通知彈窗（避免擋住DOM）
    options.add_argument("start-maximized")            # 視窗最大化（有些網站會因尺寸不同改版面）

    # ---------- 啟動ChromeDriver ----------
    print("[DEBUG] 啟動 Chrome Driver...")
    service = Service(ChromeDriverManager().install())  # 自動下載/定位chromedriver
    driver = webdriver.Chrome(service=service, options=options)

    # ---------- 打開CMoney頁面 ----------
    print("[DEBUG] 正在訪問 CMoney 網站...")
    driver.get("https://www.cmoney.tw/etf/tw/0050/fundholding")

    try:
        print("[DEBUG] 等待頁面載入...")

        # Why：CMoney是動態渲染，資料不是一開始HTML就有
        # How：等到 table 裡真的出現 tr（列）再開始抓
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//table//tbody//tr"))
        )

        # 抓所有列（tr）：每列通常是一檔股票
        rows = driver.find_elements(By.XPATH, "//table//tbody//tr")
        print(f"[DEBUG] 抓到 rows 數量: {len(rows)}")

        # 只取前10個「有抓到四位數代號的」列
        count = 0
        for tr in rows:
            tds = tr.find_elements(By.XPATH, ".//td")  # 每列的欄位（td）
            if not tds:
                continue

            # CMoney表格的第一欄是「代號」
            raw = tds[0].text.strip()

            # 把raw清洗成純4位數（2330）
            code = extract_4digit_code(raw)

            # 如果這列不是股票代碼（例如是空列/小標題），就跳過
            if not code:
                continue

            taiwan50.add(code)     # 存入set
            count += 1
            print(f"  [{count}] {code} (raw: {raw})")

            if count >= 10:        # 取滿10筆就停止
                break

        print(f"[台灣50] 成功取得 {len(taiwan50)} 支(應該=10) -> {sorted(taiwan50)}")

        # 如果不是10：代表網站可能改版/你抓到的tbody不對/表格結構變了
        if len(taiwan50) != 10:
            print("[WARNING] 抓到的前10代碼不是10筆，請檢查CMoney頁面是否改版或XPATH是否需要調整。")

    except TimeoutException as e:
        print(f"[錯誤] 頁面載入超時：{e}")
    except Exception as e:
        print(f"[錯誤] {e}")
    finally:
        driver.quit()
        print("[DEBUG] 關閉瀏覽器")


def find_stock(url, start, end, stock_type):
    """
    STEP 2：抓「上市/上櫃」股票清單，寫入 dbo.stock_list
    這裡用 requests+BeautifulSoup（因為ISIN頁面是靜態HTML，不需要selenium）
    """
    print(f"\n[STEP 2] 開始爬取{stock_type}股票清單...")
    print(f"[DEBUG] URL: {url}")

    try:
        # ---------- 連線資料庫 ----------
        conn = pymssql.connect(**db_settings)
        print("[DEBUG] 資料庫連線成功")

        with conn.cursor() as cursor:
            # 先查是否存在（避免重複、也方便更新標記）
            check_command = "SELECT COUNT(*) FROM dbo.stock_list WHERE stock_code = %s"

            # 不存在：新增
            insert_command = """
            INSERT INTO dbo.stock_list (stock_code, name, type, category, isTaiwan50)
            VALUES (%s, %s, %s, %s, %s)
            """

            # 已存在：更新（關鍵！避免你之前“存在就跳過”導致台灣50只剩6）
            update_command = """
            UPDATE dbo.stock_list
            SET name = %s,
                type = %s,
                category = %s,
                isTaiwan50 = %s
            WHERE stock_code = %s
            """

            # ---------- 下載ISIN網頁 ----------
            print("[DEBUG] 正在下載股票清單網頁...")
            response = requests.get(url, timeout=30)
            soup = BeautifulSoup(response.text, "html.parser")

            # ---------- 找出“起點/終點”所在的<tr> ----------
            # Why：這張表很長，裡面有「股票」「特別股」「權證」等段落
            # How：先找到b標籤中，文字等於start與end的那兩列<tr>
            print("[DEBUG] 正在解析 HTML...")
            result = soup.select("table td b")
            start_td = None
            end_td = None

            for b in result:
                if b.text.strip() == start:
                    start_td = b.find_parent("tr")  # 起點那一列<tr>
                    print(f"[DEBUG] 找到起點：{start}")
                elif b.text.strip() == end:
                    end_td = b.find_parent("tr")    # 終點那一列<tr>
                    print(f"[DEBUG] 找到終點：{end}")

            if not start_td or not end_td:
                print("[錯誤] 找不到起點或終點標籤(網站可能改版)")
                return

            # 從起點的下一列開始走，直到終點之前
            row = start_td.find_next("tr")

            total_count = 0
            inserted_count = 0
            updated_count = 0
            skipped_count = 0

            while row and row != end_td:
                tds = row.find_all("td")

                # 這裡的判斷：
                # - 至少要有5欄（代號名稱、ISIN等欄位）
                # - 第一欄通常是 "2330　台積電" 這種格式，中間是全形空白"　"
                if len(tds) >= 5 and "　" in tds[0].text:
                    stock_id, stock_name = tds[0].text.strip().split("　", 1)

                    # stock_type_value：上市/上櫃頁面中會有類型欄（有時是上市、上櫃、ETF等）
                    stock_type_value = tds[3].text.strip()

                    # category：產業分類（如半導體業、金融保險業…）
                    category = tds[4].text.strip()

                    # isTaiwan50：如果這支股票在 taiwan50 set 裡，就標記1
                    is_taiwan50 = 1 if stock_id in taiwan50 else 0

                    # DB存在檢查
                    cursor.execute(check_command, (stock_id,))
                    exists = cursor.fetchone()[0] > 0

                    if exists:
                        # 已存在：更新（包含isTaiwan50）
                        cursor.execute(
                            update_command,
                            (stock_name, stock_type_value, category, is_taiwan50, stock_id)
                        )
                        updated_count += 1
                    else:
                        # 不存在：新增
                        cursor.execute(
                            insert_command,
                            (stock_id, stock_name, stock_type_value, category, is_taiwan50)
                        )
                        inserted_count += 1

                    total_count += 1
                else:
                    skipped_count += 1

                row = row.find_next("tr")

            conn.commit()
            print(f"\n[{stock_type}完成] 總處理 {total_count} 筆 | 新增 {inserted_count} | 更新 {updated_count} | 跳過 {skipped_count}")

    except Exception as e:
        print(f"[錯誤] {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            conn.close()
            print("[DEBUG] 資料庫連線已關閉")
        except:
            pass


# ===============================
# 主流程：先抓台灣50前10，再抓上櫃/上市股票清單寫入DB
# ===============================
print("=" * 60)
print("開始執行股票清單爬蟲程式")
print("=" * 60)

find_Taiwan50()
find_stock("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4", "股票", "特別股", "上櫃")
find_stock("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", "股票", "上市認購(售)權證", "上市")

print("\n" + "=" * 60)
print("全部流程完成！")
print("=" * 60)

"""
-- 如果你下一步要「驗收用」：我建議你跑完後只看這三條SQL（你可直接拿去當作業驗收）：
-- 台灣50前10應該=10
SELECT COUNT(*) AS 台灣50前10個數
FROM dbo.stock_list
WHERE isTaiwan50 = 1;

-- 列出那10筆
SELECT stock_code, name, type, category
FROM dbo.stock_list
WHERE isTaiwan50 = 1
ORDER BY stock_code;

-- 不應該有重複
SELECT stock_code, COUNT(*) AS cnt
FROM dbo.stock_list
GROUP BY stock_code
HAVING COUNT(*) > 1;
"""


"""
輸出:
============================================================
開始執行股票清單爬蟲程式
============================================================

[STEP 1] 開始爬取0050前10大成分股(股票代碼)...
[DEBUG] 啟動 Chrome Driver...

DevTools listening on ws://127.0.0.1:1237/devtools/browser/4b2105bf-2c43-4e17-9f9f-e91fc49794f1
[DEBUG] 正在訪問 CMoney 網站...
[DEBUG] 等待頁面載入...
[DEBUG] 抓到 rows 數量: 56
  [1] 2330 (raw: 2330)
  [2] 2887 (raw: 2887)
  [3] 2891 (raw: 2891)
  [4] 2883 (raw: 2883)
  [5] 2884 (raw: 2884)
  [6] 2317 (raw: 2317)
  [7] 2890 (raw: 2890)
  [8] 2886 (raw: 2886)
  [9] 2303 (raw: 2303)
  [10] 2002 (raw: 2002)
[台灣50] 成功取得 10 支(應該=10) -> ['2002', '2303', '2317', '2330', '2883', '2884', '2886', '2887', '2890', '2891']
[DEBUG] 關閉瀏覽器

[STEP 2] 開始爬取上櫃股票清單...
[DEBUG] URL: https://isin.twse.com.tw/isin/C_public.jsp?strMode=4
[DEBUG] 資料庫連線成功
[DEBUG] 正在下載股票清單網頁...
[DEBUG] 正在解析 HTML...
[DEBUG] 找到起點：股票
[DEBUG] 找到終點：特別股

[上櫃完成] 總處理 879 筆 | 新增 0 | 更新 879 | 跳過 0
[DEBUG] 資料庫連線已關閉

[STEP 2] 開始爬取上市股票清單...
[DEBUG] URL: https://isin.twse.com.tw/isin/C_public.jsp?strMode=2
[DEBUG] 資料庫連線成功
[DEBUG] 正在下載股票清單網頁...
[DEBUG] 正在解析 HTML...
[DEBUG] 找到起點：股票
[DEBUG] 找到終點：上市認購(售)權證

[上市完成] 總處理 1045 筆 | 新增 0 | 更新 1045 | 跳過 0
[DEBUG] 資料庫連線已關閉

============================================================
全部流程完成！
============================================================
"""