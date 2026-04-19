# -*- coding: utf-8 -*-
"""
作業練習：用Python在VS Code呼叫SQL Server的Stored Procedure
目標：輸入股票代碼+日期區間，回傳dbo.stock_data資料並印出結果
"""

# ①pymssql：用來連SQL Server(使用SQL Server驗證帳號密碼)
import pymssql

# ②pandas：把查回來的資料轉成表格(DataFrame)，方便後續畫圖/回測
import pandas as pd

# ③你的資料庫連線設定(照你提供的)
db_settings = {
    "host": "127.0.0.1",      # 本機SQL Server
    "user": "skyfire",        # SQL帳號
    "password": "1487",       # SQL密碼
    "database": "ncu_db",     # 你的資料庫名稱
    "charset": "utf8"
}

def fetch_stock_data_by_date_range(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    呼叫dbo.GetStockDataByDateRange這個Stored Procedure，並回傳DataFrame

    stock_code:例如'2330'
    start_date/end_date:例如'2024-01-01'
    """

    # ①建立資料庫連線
    # as_dict=False表示fetch出來是tuple；我們會用cursor.description自己組欄位名
    conn = pymssql.connect(
        server=db_settings["host"],
        user=db_settings["user"],
        password=db_settings["password"],
        database=db_settings["database"],
        charset=db_settings["charset"],
        as_dict=False
    )

    try:
        # ②建立游標(cursor)：用來送SQL指令給資料庫
        cursor = conn.cursor()

        # ③重點：呼叫SP
        # 注意：pymssql使用%s當placeholder，不是?
        # 這裡的%s會被driver安全替換成你傳入的參數值
        sql = """
        EXEC dbo.GetStockDataByDateRange
            @StockCode = %s,
            @StartDate = %s,
            @EndDate   = %s
        """

        # ④execute的第二個參數是tuple，依序對應上面的%s
        cursor.execute(sql, (stock_code, start_date, end_date))

        # ⑤cursor.description裡有欄位資訊，我們用它來取得欄位名稱
        col_names = [desc[0] for desc in cursor.description]

        # ⑥把所有資料撈出來
        rows = cursor.fetchall()

        # ⑦組成DataFrame，後面畫K線、算指標都用它
        df = pd.DataFrame(rows, columns=col_names)

        return df

    finally:
        # ⑧不管成功或失敗都要關連線，避免連線一直占著
        conn.close()


if __name__ == "__main__":
    # ①作業測試輸入：你可以直接改這三個值
    stock_code = "2330"
    start_date = "2024-01-01"
    end_date = "2024-01-31"

    # ②呼叫函式取得資料
    df = fetch_stock_data_by_date_range(stock_code, start_date, end_date)

    # ③印出基本資訊確認是否成功
    print("===查詢結果筆數===")
    print(len(df))

    print("\n===前10筆資料===")
    print(df.head(10))

    # ④如果你想確認欄位有哪些(很常用)
    print("\n===欄位列表===")
    print(df.columns.tolist())
