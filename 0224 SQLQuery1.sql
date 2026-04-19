USE ncu_db;
GO

-- 1) 行事曆：前10筆
SELECT TOP (10) *
FROM dbo.calendar
ORDER BY [date];

-- 2) 2026 全年總天數
SELECT COUNT(*) AS total_days
FROM dbo.calendar
WHERE YEAR([date]) = 2026;

-- 3) 2026 交易日天數
SELECT COUNT(*) AS trading_days
FROM dbo.calendar
WHERE YEAR([date]) = 2026
  AND day_of_stock > 0;

-- 4) 年行事曆：2026
SELECT *
FROM dbo.year_calendar
WHERE [year] = 2026;

-- 5) 股票清單：前10筆
SELECT TOP (10) *
FROM dbo.stock_list;

-- 6) 股票總數
SELECT COUNT(*) AS total_stocks
FROM dbo.stock_list;

-- 7) 台積電(2330)股價：前10筆（依日期）
SELECT TOP (10) *
FROM dbo.stock_data
WHERE stock_code = '2330'
ORDER BY [date];

-- 8) 台積電(2330)資料天數
SELECT COUNT(*) AS days
FROM dbo.stock_data
WHERE stock_code = '2330';
GO
