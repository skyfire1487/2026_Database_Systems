/*==========================================================
  Procedure 名稱：dbo.tradingPrice_type

  功能說明：
    判斷指定公司在指定日期的最高價(h)
    相對於往回 @setting_days 筆交易日資料的排名
    並輸出分類結果(type)

  分類定義：
    1   高檔
    0   普通
   -1   低檔
    99  非交易日
==========================================================*/

-- 若已存在此Procedure則先刪除
IF OBJECT_ID('dbo.tradingPrice_type', 'P') IS NOT NULL
    DROP PROCEDURE dbo.tradingPrice_type;
GO

CREATE PROCEDURE dbo.tradingPrice_type
    @company_input VARCHAR(10),   -- 股票代碼
    @setting_days  INT,           -- 比較的交易日筆數 (20 / 60 ...)
    @setting_date  DATE,          -- 指定日期
    @type          INT OUTPUT     -- 輸出分類
AS
BEGIN
    SET NOCOUNT ON;

    /*------------------------------------------------------
      (1) 檢查是否為交易日
    ------------------------------------------------------*/
    DECLARE @is_workingday INT;

    SELECT @is_workingday = day_of_stock
    FROM dbo.calendar
    WHERE date = @setting_date;

    IF (@is_workingday IS NULL OR @is_workingday <= 0)
    BEGIN
        PRINT '日期：' + CAST(@setting_date AS VARCHAR) + ' 不是交易日，返回 99';
        SET @type = 99;
        RETURN;
    END

    /*------------------------------------------------------
      (2) 取得比例設定（高檔比例 / 低檔比例）
    ------------------------------------------------------*/
    DECLARE @high_ratio FLOAT;
    DECLARE @low_ratio  FLOAT;

    SELECT
        @high_ratio = high_ratio,
        @low_ratio  = low_ratio
    FROM dbo.tradingPrice_def
    WHERE compare_with = @setting_days;

    /*------------------------------------------------------
      (3) 建立暫存表
    ------------------------------------------------------*/
    IF OBJECT_ID('tempdb..#price_temp') IS NOT NULL
        DROP TABLE #price_temp;

    CREATE TABLE #price_temp
    (
        company_temp VARCHAR(10),
        date_temp    DATE,
        high_temp    FLOAT
    );

    /*------------------------------------------------------
      (4) 取出往回 N 筆交易日資料
          （不是往回N天，是TOP N筆）
    ------------------------------------------------------*/
    INSERT INTO #price_temp
    SELECT TOP (@setting_days)
        stock_code,
        date,
        h
    FROM dbo.stock_data
    WHERE stock_code = @company_input
      AND date <= @setting_date
    ORDER BY date DESC;

    /*------------------------------------------------------
      (5) 依最高價排序
          最大 = 排名1
    ------------------------------------------------------*/
    DECLARE @rank INT;

    SELECT @rank = ROWID
    FROM
    (
        SELECT
            ROW_NUMBER() OVER (ORDER BY high_temp DESC) AS ROWID,
            *
        FROM #price_temp
    ) T
    WHERE T.date_temp = @setting_date;

    /*------------------------------------------------------
      (6) 分類
    ------------------------------------------------------*/
    IF @rank <= @setting_days * @high_ratio
        SET @type = 1;      -- 高檔
    ELSE IF @rank >= @setting_days - (@setting_days * @low_ratio)
        SET @type = -1;     -- 低檔
    ELSE
        SET @type = 0;      -- 普通
END
GO

DECLARE @type INT;

EXEC dbo.tradingPrice_type '2330', 20, '2024-03-14', @type OUTPUT;
PRINT @type;

EXEC dbo.tradingPrice_type '2330', 20, '2024-02-02', @type OUTPUT;
PRINT @type;

EXEC dbo.tradingPrice_type '2330', 20, '2023-09-26', @type OUTPUT;
PRINT @type;