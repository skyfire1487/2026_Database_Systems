use ncu_db
go

SELECT TOP 10 *
FROM dbo.stock_data
WHERE stock_code = '2330'
ORDER BY date;

SELECT 
    stock_code,
    date,
    LAG(date) OVER (PARTITION BY stock_code ORDER BY date) AS prev_date
FROM dbo.stock_data
WHERE stock_code = '2330'
ORDER BY date;

SELECT 
    stock_code,
    date AS second_day,
    o AS o2,
    c AS c2,
    LAG(o) OVER (PARTITION BY stock_code ORDER BY date) AS o1,
    LAG(c) OVER (PARTITION BY stock_code ORDER BY date) AS c1
FROM dbo.stock_data
WHERE stock_code = '2330'
ORDER BY date;