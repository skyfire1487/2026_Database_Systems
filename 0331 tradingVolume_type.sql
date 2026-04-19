/*==========================================================
  Procedure 名稱：dbo.tradingVolume_type

  功能說明：
    判斷指定公司在指定日期的成交量(tv)
    相對於往回 @setting_days 天內的排名百分比
    並輸出分類結果(type)

  分類定義：
    2   極大量
    1   大量
    0   普通
   -1   小量
   -2   極小量
    99  非交易日
==========================================================*/

-- 若已存在此Procedure則先刪除
IF OBJECT_ID('dbo.tradingVolume_type', 'P') IS NOT NULL
    DROP PROCEDURE dbo.tradingVolume_type;
GO

-- 建立Procedure
CREATE PROCEDURE dbo.tradingVolume_type
    @company_input VARCHAR(10),   -- 輸入：股票代碼
    @setting_days  INT,           -- 輸入：回溯比較天數 (20 / 60 ...)
    @setting_date  DATE,          -- 輸入：指定日期
    @type          INT OUTPUT     -- 輸出：分類結果
AS
BEGIN
    SET NOCOUNT ON;  -- 不顯示影響筆數，避免干擾輸出結果

    /*------------------------------------------------------
      (1) 檢查是否為開市日
          若不是交易日，直接回傳 99
    ------------------------------------------------------*/

    DECLARE @is_workingday INT;  -- 儲存calendar表中的day_of_stock值

    SELECT @is_workingday = day_of_stock
    FROM dbo.calendar
    WHERE date = @setting_date;

    -- 若查不到日期 或 day_of_stock <= 0 代表不是交易日
    IF (@is_workingday IS NULL OR @is_workingday <= 0)
    BEGIN
        PRINT '日期：' + CAST(@setting_date AS VARCHAR) + ' 不是交易日，返回 99';
        SET @type = 99;  -- 非交易日
        RETURN;          -- 結束Procedure
    END

    /*------------------------------------------------------
      (2) 依照比較天數，從 tradingVolume_def 取得分類比例
          由此取得對應天數的成交量定義值
    ------------------------------------------------------*/

    DECLARE @veryHigh_def FLOAT;  -- 極大量比例
    DECLARE @high_def     FLOAT;  -- 大量比例
    DECLARE @low_def      FLOAT;  -- 小量比例
    DECLARE @veryLow_def  FLOAT;  -- 極小量比例

    SELECT
        @veryHigh_def = veryHigh_volume,
        @high_def     = high_volume,
        @low_def      = low_volume,
        @veryLow_def  = veryLow_volume
    FROM dbo.tradingVolume_def
    WHERE compare_with = @setting_days;

    /*------------------------------------------------------
      (3) 建立暫存表 #stock_temp
          用來存放近 N 天成交量資料
    ------------------------------------------------------*/

    IF OBJECT_ID('tempdb..#stock_temp') IS NOT NULL
        DROP TABLE #stock_temp;

    CREATE TABLE #stock_temp
    (
        company_temp VARCHAR(10),  -- 股票代碼
        date_temp    DATE,         -- 日期
        tv_temp      BIGINT        -- 成交量
    );

    /*------------------------------------------------------
      (4) 取出近 N 天資料放入 #stock_temp
          依日期由新到舊排序
    ------------------------------------------------------*/

    DECLARE @sqltext NVARCHAR(MAX);

    SET @sqltext = N'
        INSERT INTO #stock_temp (company_temp, date_temp, tv_temp)
        SELECT TOP (' + CAST(@setting_days AS VARCHAR) + N')
            stock_code, date, tv
        FROM dbo.stock_data
        WHERE stock_code = ''' + @company_input + N'''
          AND date <= ''' + CAST(@setting_date AS VARCHAR) + N'''
        ORDER BY date DESC;
    ';

    EXEC sp_executesql @sqltext;

    /*------------------------------------------------------
      (5) 依成交量大小排序
          給每一行一個 ROWID
          最大成交量 ROWID = 1
    ------------------------------------------------------*/

    DECLARE @temp INT;  -- 儲存指定日期的排名

    SELECT @temp = ROWID
    FROM
    (
        SELECT
            ROW_NUMBER() OVER (ORDER BY tv_temp DESC) AS ROWID,
            *
        FROM #stock_temp
    ) T1
    WHERE T1.date_temp = @setting_date;

    /*------------------------------------------------------
      (6) 依排名百分比分類
          若在前 y% → 極大量 / 大量
          若在後 y% → 極小量 / 小量
    ------------------------------------------------------*/

    IF @temp <= @setting_days * @veryHigh_def
        SET @type = 2;    -- 極大量
    ELSE IF @temp <= @setting_days * @high_def
        SET @type = 1;    -- 大量
    ELSE IF @temp >= @setting_days - (@setting_days * @veryLow_def)
        SET @type = -2;   -- 極小量
    ELSE IF @temp >= @setting_days - (@setting_days * @low_def)
        SET @type = -1;   -- 小量
    ELSE
        SET @type = 0;    -- 普通
END
GO