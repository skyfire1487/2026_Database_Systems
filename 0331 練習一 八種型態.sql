/* 
==========================================================
作業：二日K線型態判斷（8種）
資料來源：dbo.stock_data
判斷方式：使用 LAG 抓前一交易日資料
==========================================================
*/

WITH two_day AS (
    SELECT
        stock_code,                         -- 股票代碼

        date AS second_day,                 -- 第二天日期
        o AS o2,                            -- 第二天開盤價
        c AS c2,                            -- 第二天收盤價

        /* 
        LAG 函數：
        依照 stock_code 分組
        再依 date 排序
        抓「前一筆資料」
        */
        LAG(date) OVER (
            PARTITION BY stock_code 
            ORDER BY date
        ) AS first_day,                     -- 第一日日期

        LAG(o) OVER (
            PARTITION BY stock_code 
            ORDER BY date
        ) AS o1,                            -- 第一日開盤價

        LAG(c) OVER (
            PARTITION BY stock_code 
            ORDER BY date
        ) AS c1                             -- 第一日收盤價

    FROM dbo.stock_data
    WHERE stock_code = '2382'               -- ← 改成你要的股票代碼
)

/* 
主查詢：開始分類
*/
SELECT
    stock_code,
    first_day,
    second_day,

    CASE

        /* ============================================
           1️⃣ 多頭吞噬線
           第一天黑K (c1 < o1)
           第二天紅K (c2 > o2)
           第二天實體完全包住第一天
        ============================================ */
        WHEN c1 < o1
             AND c2 > o2
             AND o2 <= c1
             AND c2 >= o1
        THEN '多頭吞噬線'


        /* ============================================
           2️⃣ 空頭吞噬線
        ============================================ */
        WHEN c1 > o1
             AND c2 < o2
             AND o2 >= c1
             AND c2 <= o1
        THEN '空頭吞噬線'


        /* ============================================
           3️⃣ 多頭插入線
           第一天黑K
           第二天紅K
           收盤價超過第一天實體中點
           但沒有完全吞噬
        ============================================ */
        WHEN c1 < o1
             AND c2 > o2
             AND c2 > (o1 + c1) / 2.0
             AND c2 < o1
        THEN '多頭插入線'


        /* ============================================
           4️⃣ 空頭插入線
        ============================================ */
        WHEN c1 > o1
             AND c2 < o2
             AND c2 < (o1 + c1) / 2.0
             AND c2 > o1
        THEN '空頭插入線'


        /* ============================================
           5️⃣ 多頭懷抱線
           第二天實體完全落在第一天實體內
           且方向為黑→紅
        ============================================ */
        WHEN c1 < o1
             AND c2 > o2
             AND o2 BETWEEN c1 AND o1
             AND c2 BETWEEN c1 AND o1
        THEN '多頭懷抱線'


        /* ============================================
           6️⃣ 空頭懷抱線
        ============================================ */
        WHEN c1 > o1
             AND c2 < o2
             AND o2 BETWEEN o1 AND c1
             AND c2 BETWEEN o1 AND c1
        THEN '空頭懷抱線'


        /* ============================================
           7️⃣ 多頭遭遇線
           兩天收盤相同
           黑→紅
        ============================================ */
        WHEN c1 < o1
             AND c2 > o2
             AND c1 = c2
        THEN '多頭遭遇線'


        /* ============================================
           8️⃣ 空頭遭遇線
        ============================================ */
        WHEN c1 > o1
             AND c2 < o2
             AND c1 = c2
        THEN '空頭遭遇線'


        ELSE NULL

    END AS type

FROM two_day
WHERE first_day IS NOT NULL         -- 過濾第一筆沒有前一天資料
AND (
        c1 <> o1 OR c2 <> o2        -- 避免十字線干擾
    )
AND (
        -- 只顯示有型態的資料
        CASE
            WHEN c1 < o1 AND c2 > o2 AND o2 <= c1 AND c2 >= o1 THEN 1
            WHEN c1 > o1 AND c2 < o2 AND o2 >= c1 AND c2 <= o1 THEN 1
            WHEN c1 < o1 AND c2 > o2 AND c2 > (o1 + c1)/2.0 AND c2 < o1 THEN 1
            WHEN c1 > o1 AND c2 < o2 AND c2 < (o1 + c1)/2.0 AND c2 > o1 THEN 1
            WHEN c1 < o1 AND c2 > o2 AND o2 BETWEEN c1 AND o1 AND c2 BETWEEN c1 AND o1 THEN 1
            WHEN c1 > o1 AND c2 < o2 AND o2 BETWEEN o1 AND c1 AND c2 BETWEEN o1 AND c1 THEN 1
            WHEN c1 < o1 AND c2 > o2 AND c1 = c2 THEN 1
            WHEN c1 > o1 AND c2 < o2 AND c1 = c2 THEN 1
            ELSE 0
        END = 1
    )
ORDER BY second_day DESC;