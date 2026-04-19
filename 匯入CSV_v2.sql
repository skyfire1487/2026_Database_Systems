-- Created by GitHub Copilot in SSMS - review carefully before executing

-- 步驟 0: 清理可能存在的暫存表
IF OBJECT_ID('tempdb..#stock_temp') IS NOT NULL
    DROP TABLE #stock_temp;

-- 步驟 1: 建立暫存表
CREATE TABLE #stock_temp (
    stock_code VARCHAR(50),
    [date] VARCHAR(50),
    [time] VARCHAR(50),
    tv VARCHAR(50),
    t VARCHAR(50),
    o VARCHAR(50),
    h VARCHAR(50),
    l VARCHAR(50),
    c VARCHAR(50),
    d VARCHAR(50),
    v VARCHAR(50),
    MA5 VARCHAR(50),
    MA10 VARCHAR(50),
    MA20 VARCHAR(50),
    MA60 VARCHAR(50),
    MA120 VARCHAR(50),
    MA240 VARCHAR(50)
);

-- 步驟 2: 匯入 CSV
BULK INSERT #stock_temp
FROM '.\Full_StockData.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001'
);

-- 步驟 3: 使用 TRY_CAST 插入（容錯處理）
INSERT INTO dbo.stock_data (
    stock_code, [date], [time], tv, t, o, h, l, c, d, v,
    MA5, MA10, MA20, MA60, MA120, MA240
)
SELECT 
    stock_code,
    TRY_CAST([date] AS DATE),
    TRY_CAST([time] AS TIME),
    TRY_CAST(NULLIF(tv, '') AS BIGINT),
    TRY_CAST(NULLIF(t, '') AS BIGINT),
    TRY_CAST(NULLIF(o, '') AS REAL),
    TRY_CAST(NULLIF(h, '') AS REAL),
    TRY_CAST(NULLIF(l, '') AS REAL),
    TRY_CAST(NULLIF(c, '') AS REAL),
    TRY_CAST(NULLIF(d, '') AS REAL),
    TRY_CAST(NULLIF(v, '') AS INT),
    TRY_CAST(NULLIF(MA5, '') AS REAL),
    TRY_CAST(NULLIF(MA10, '') AS REAL),
    TRY_CAST(NULLIF(MA20, '') AS REAL),
    TRY_CAST(NULLIF(MA60, '') AS REAL),
    TRY_CAST(NULLIF(MA120, '') AS REAL),
    TRY_CAST(NULLIF(MA240, '') AS REAL)
FROM #stock_temp;

-- 步驟 4: 清理
DROP TABLE #stock_temp;

-- 步驟 5: 驗證結果
PRINT '========== 匯入完成 ==========';
SELECT COUNT(*) AS 總列數 FROM dbo.stock_data;
SELECT TOP 10 * FROM dbo.stock_data ORDER BY [date] DESC;