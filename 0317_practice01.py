# 匯入 pymssql，用來連接 SQL Server 資料庫
import pymssql

# 匯入 pandas，用來處理資料表格
import pandas as pd

# 匯入 mplfinance，用來畫 K 線圖
import mplfinance as mpf


# =========================================================
# 1. 資料庫連線設定
#    這裡直接使用你之前提供過的連線資訊
# =========================================================
db_settings = {
    "host": "127.0.0.1",      # SQL Server 主機位置，127.0.0.1 代表本機
    "user": "skyfire",        # SQL Server 登入帳號
    "password": "1487",       # SQL Server 登入密碼
    "database": "ncu_db",     # 要連進去的資料庫名稱
    "charset": "utf8"         # 文字編碼格式
}


# =========================================================
# 2. 建立資料庫連線
#    使用 pymssql.connect() 連接到 SQL Server
# =========================================================
conn = pymssql.connect(
    host=db_settings["host"],         # 讀取主機位址
    user=db_settings["user"],         # 讀取帳號
    password=db_settings["password"], # 讀取密碼
    database=db_settings["database"], # 讀取資料庫名稱
    charset=db_settings["charset"]    # 讀取編碼設定
)


# =========================================================
# 3. 撰寫 SQL 查詢語法
#    目標：
#    - 查出股票代碼 2330
#    - 日期範圍 2024-01-01 到 2024-12-31
#    - 取出畫 K 線圖需要的欄位
#
#    欄位對應：
#    date      → 日期
#    o         → Open
#    h         → High
#    l         → Low
#    c         → Close
#    tv        → Volume
# =========================================================
query = """
SELECT
    [date],                 -- 日期欄位
    o  AS [Open],           -- 開盤價 o 對應成 mplfinance 需要的 Open
    h  AS [High],           -- 最高價 h 對應成 High
    l  AS [Low],            -- 最低價 l 對應成 Low
    c  AS [Close],          -- 收盤價 c 對應成 Close
    tv AS [Volume]          -- 成交股數 tv 對應成 Volume
FROM dbo.stock_data
WHERE stock_code = '2330'   -- 只抓台積電 2330
  AND [date] BETWEEN '2024-01-01' AND '2024-12-31'  -- 只抓 2024 全年
ORDER BY [date] ASC         -- 依照日期由舊到新排序，畫圖時才會正確
"""


# =========================================================
# 4. 使用 pandas 讀取 SQL 查詢結果
#    pd.read_sql() 會直接把 SQL 查詢結果讀成 DataFrame
# =========================================================
df = pd.read_sql(query, conn)


# =========================================================
# 5. 查詢完畢後關閉資料庫連線
#    避免連線長時間占用資源
# =========================================================
conn.close()


# =========================================================
# 6. 檢查與轉換數值型態
#    因為從資料庫讀出的欄位，有時候可能不是純數字型態
#    所以要明確把價格與成交量欄位轉成數值
#
#    errors='coerce' 的意思：
#    如果有轉不成功的值，就先變成 NaN
# =========================================================
numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']

for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')


# =========================================================
# 7. 將 date 欄位轉成 datetime 型態
#    因為畫 K 線圖時，日期欄位必須是 datetime 格式
# =========================================================
df['date'] = pd.to_datetime(df['date'])


# =========================================================
# 8. 將 date 設成 DataFrame 的索引
#    mplfinance 規定索引必須是日期索引，才能正確畫圖
# =========================================================
df.set_index('date', inplace=True)


# =========================================================
# 9. 移除可能存在的空值
#    如果某些列有缺失值，可能造成畫圖出錯
# =========================================================
df.dropna(inplace=True)


# =========================================================
# 10. 設定台股 K 線顏色
#     mplfinance 預設是：
#     - 上漲綠色
#     - 下跌紅色
#
#     但台股習慣剛好相反：
#     - 上漲紅色
#     - 下跌綠色
#
#     所以這裡要手動修改
# =========================================================
mc = mpf.make_marketcolors(
    up='r',             # 上漲用紅色
    down='g',           # 下跌用綠色
    edge='inherit',     # K 線實體邊框顏色跟著 up/down 顏色
    wick='inherit',     # 上下影線顏色跟著 up/down 顏色
    volume='inherit'    # 成交量顏色也跟著 up/down 顏色
)


# =========================================================
# 11. 設定整體圖表風格
#     marketcolors=mc 代表套用剛剛自訂的紅漲綠跌規則
# =========================================================
my_style = mpf.make_mpf_style(
    marketcolors=mc,    # 套用市場顏色設定
    gridstyle='--',     # 網格線使用虛線
    y_on_right=False    # 價格刻度顯示在左邊
)


# =========================================================
# 12. 畫 K 線圖
#     type='candle' 表示畫蠟燭圖，也就是 K 線圖
#     volume=True 表示下方同時畫成交量
# =========================================================
mpf.plot(
    df,                             # 要拿來畫圖的 DataFrame
    type='candle',                  # 圖表型態：K 線圖
    style=my_style,                 # 套用自訂風格
    volume=True,                    # 顯示成交量
    title='2330 K-Line Chart (2024)',  # 圖表標題
    ylabel='Price',                 # 上方主圖 y 軸名稱
    ylabel_lower='Volume',          # 下方成交量圖 y 軸名稱
    datetime_format='%Y-%m-%d',     # x 軸日期顯示格式
    figsize=(14, 8)                 # 圖表大小
)