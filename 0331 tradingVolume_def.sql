/*==========================================================
  tradingVolume_def：成交量分類規則表（依PPT）
  compare_with：比較天數（例如 20、60）
  veryHigh_volume：前幾% → 極大量
  high_volume：前幾% → 大量
  low_volume：後幾% → 小量
  veryLow_volume：後幾% → 極小量
==========================================================*/

IF OBJECT_ID('dbo.tradingVolume_def', 'U') IS NOT NULL
    DROP TABLE dbo.tradingVolume_def;
GO

CREATE TABLE dbo.tradingVolume_def
(
    compare_with     INT         NOT NULL,  -- 要比較的天數（例如 20、60）
    veryHigh_volume  FLOAT       NULL,      -- 前幾% → 極大量
    high_volume      FLOAT       NULL,      -- 前幾% → 大量
    low_volume       FLOAT       NULL,      -- 後幾% → 小量
    veryLow_volume   FLOAT       NULL       -- 後幾% → 極小量
);
GO

/* 依PPT的資料值參考（照表打） */
INSERT INTO dbo.tradingVolume_def(compare_with, veryHigh_volume, high_volume, low_volume, veryLow_volume)
VALUES
(20, 0.05, 0.10, 0.10, 0.05),
(60, 0.08, 0.20, 0.20, 0.08),
(30, 0.30, 0.50, 0.50, 0.30);
GO