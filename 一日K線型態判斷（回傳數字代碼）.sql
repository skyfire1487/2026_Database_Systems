use ncu_db
GO
SELECT * 
FROM dbo.candlestick_type('2330', '2024-10-15');

SELECT *
FROM dbo.stock_data
WHERE stock_code = '2330'
  AND date = '2024-10-15';