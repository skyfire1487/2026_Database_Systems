WITH two_day AS (
    SELECT
        stock_code,
        date AS second_day,
        o AS o2,
        c AS c2,
        LAG(date) OVER (PARTITION BY stock_code ORDER BY date) AS first_day,
        LAG(o) OVER (PARTITION BY stock_code ORDER BY date) AS o1,
        LAG(c) OVER (PARTITION BY stock_code ORDER BY date) AS c1
    FROM dbo.stock_data
    WHERE stock_code = '2382'
),

pattern_result AS (
    SELECT
        stock_code,
        first_day,
        second_day,
        CASE
            WHEN c1 < o1 AND c2 > o2 AND o2 <= c1 AND c2 >= o1 THEN '多頭吞噬線'
            WHEN c1 > o1 AND c2 < o2 AND o2 >= c1 AND c2 <= o1 THEN '空頭吞噬線'
            WHEN c1 < o1 AND c2 > o2 AND c2 > (o1 + c1)/2.0 AND c2 < o1 THEN '多頭插入線'
            WHEN c1 > o1 AND c2 < o2 AND c2 < (o1 + c1)/2.0 AND c2 > o1 THEN '空頭插入線'
            WHEN c1 < o1 AND c2 > o2 AND o2 BETWEEN c1 AND o1 AND c2 BETWEEN c1 AND o1 THEN '多頭懷抱線'
            WHEN c1 > o1 AND c2 < o2 AND o2 BETWEEN o1 AND c1 AND c2 BETWEEN o1 AND c1 THEN '空頭懷抱線'
            WHEN c1 < o1 AND c2 > o2 AND c1 = c2 THEN '多頭遭遇線'
            WHEN c1 > o1 AND c2 < o2 AND c1 = c2 THEN '空頭遭遇線'
            ELSE NULL
        END AS type
    FROM two_day
    WHERE first_day IS NOT NULL
),

numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY second_day DESC) AS rn
    FROM pattern_result
    WHERE type IS NOT NULL
)

SELECT *
FROM numbered
WHERE rn = 9;