USE ncu_db;
GO

/*==============================================================
  功能：
  重新計算 dbo.stock_data 中所有股票的
  MA5、MA10、MA20、MA60、MA120、MA240

  計算基礎：
  使用收盤價 c 作為移動平均線計算依據

  邏輯說明：
  1. 先依 stock_code 分組
  2. 再依 date、time 排序
  3. 使用視窗函數 AVG() OVER() 計算各 MA
  4. 最後回寫更新到原表 dbo.stock_data

  注意：
  - 若前幾天資料不足，例如第 3 筆資料要算 MA5，
    則會自動用「目前累積到的資料」去平均
    也就是前 1~3 筆的平均
  - 這個行為符合你 PPT 上的要求
==============================================================*/

;WITH stock_data_with_ma AS
(
    SELECT
        sd.stock_code,   -- 股票代碼
        sd.[date],       -- 日期
        sd.[time],       -- 時間（若同一天有多筆資料，可輔助排序）

        /*------------------------------------------------------
          MA5：
          取得「目前這一筆 + 前 4 筆」的收盤價平均
          共最多 5 筆
        ------------------------------------------------------*/
        AVG(CAST(sd.c AS FLOAT)) OVER
        (
            PARTITION BY sd.stock_code
            ORDER BY 
                sd.[date],
                ISNULL(sd.[time], '00:00:00')
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS calc_MA5,

        /*------------------------------------------------------
          MA10：
          取得「目前這一筆 + 前 9 筆」的收盤價平均
          共最多 10 筆
        ------------------------------------------------------*/
        AVG(CAST(sd.c AS FLOAT)) OVER
        (
            PARTITION BY sd.stock_code
            ORDER BY 
                sd.[date],
                ISNULL(sd.[time], '00:00:00')
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS calc_MA10,

        /*------------------------------------------------------
          MA20：
          取得「目前這一筆 + 前 19 筆」的收盤價平均
          共最多 20 筆
        ------------------------------------------------------*/
        AVG(CAST(sd.c AS FLOAT)) OVER
        (
            PARTITION BY sd.stock_code
            ORDER BY 
                sd.[date],
                ISNULL(sd.[time], '00:00:00')
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS calc_MA20,

        /*------------------------------------------------------
          MA60：
          取得「目前這一筆 + 前 59 筆」的收盤價平均
          共最多 60 筆
        ------------------------------------------------------*/
        AVG(CAST(sd.c AS FLOAT)) OVER
        (
            PARTITION BY sd.stock_code
            ORDER BY 
                sd.[date],
                ISNULL(sd.[time], '00:00:00')
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS calc_MA60,

        /*------------------------------------------------------
          MA120：
          取得「目前這一筆 + 前 119 筆」的收盤價平均
          共最多 120 筆
        ------------------------------------------------------*/
        AVG(CAST(sd.c AS FLOAT)) OVER
        (
            PARTITION BY sd.stock_code
            ORDER BY 
                sd.[date],
                ISNULL(sd.[time], '00:00:00')
            ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
        ) AS calc_MA120,

        /*------------------------------------------------------
          MA240：
          取得「目前這一筆 + 前 239 筆」的收盤價平均
          共最多 240 筆
        ------------------------------------------------------*/
        AVG(CAST(sd.c AS FLOAT)) OVER
        (
            PARTITION BY sd.stock_code
            ORDER BY 
                sd.[date],
                ISNULL(sd.[time], '00:00:00')
            ROWS BETWEEN 239 PRECEDING AND CURRENT ROW
        ) AS calc_MA240
    FROM dbo.stock_data AS sd
    WHERE sd.c IS NOT NULL   -- 收盤價不能是空值，否則無法計算平均
)

/*==============================================================
  將剛剛 CTE 算出的結果，更新回原本的 dbo.stock_data
==============================================================*/
UPDATE target_table
SET
    target_table.MA5   = source_table.calc_MA5,     -- 更新 MA5
    target_table.MA10  = source_table.calc_MA10,    -- 更新 MA10
    target_table.MA20  = source_table.calc_MA20,    -- 更新 MA20
    target_table.MA60  = source_table.calc_MA60,    -- 更新 MA60
    target_table.MA120 = source_table.calc_MA120,   -- 更新 MA120
    target_table.MA240 = source_table.calc_MA240    -- 更新 MA240
FROM dbo.stock_data AS target_table
INNER JOIN stock_data_with_ma AS source_table
    ON target_table.stock_code = source_table.stock_code
   AND target_table.[date]     = source_table.[date]
   AND ISNULL(target_table.[time], '00:00:00') = ISNULL(source_table.[time], '00:00:00');
GO