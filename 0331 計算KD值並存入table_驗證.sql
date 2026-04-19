SELECT  
    stock_code,
    date,
    o, h, l, c,
    K_value,
    D_value
FROM dbo.stock_data
WHERE stock_code = '2330'        -- 台積電股票代碼
  AND YEAR(date) = 2024          -- 假設看2024年
ORDER BY date ASC;               -- 依日期排序即可