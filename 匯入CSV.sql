-- Created by GitHub Copilot in SSMS - review carefully before executing

BULK INSERT dbo.stock_data
FROM 'I:\LearningTime\中央大學文件\資料庫系統專題實作\Full_Crawl\Full_StockData.csv'
WITH (
    FIRSTROW = 2,              -- 跳過標題列
    FIELDTERMINATOR = ',',     -- CSV 欄位分隔符號
    ROWTERMINATOR = '\n',      -- 行分隔符號
    TABLOCK,
    CODEPAGE = '65001'         -- UTF-8 編碼
);