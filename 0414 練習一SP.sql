USE ncu_db;
GO

/*======================================================================
  步驟 0：如果 dbo.sp_CalculateTrend 已經存在，先刪除舊版本
  ----------------------------------------------------------------------
  為什麼要這樣做？
  因為 SQL Server 中，若同名的 Stored Procedure 已存在，
  直接再次 CREATE 會報錯。

  OBJECT_ID('dbo.sp_CalculateTrend', 'P')
  的意思是：
  - 檢查名為 dbo.sp_CalculateTrend 的物件是否存在
  - 'P' 代表這個物件類型是 Stored Procedure
======================================================================*/
IF OBJECT_ID('dbo.sp_CalculateTrend', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_CalculateTrend;
GO


/*======================================================================
  步驟 1：建立 Stored Procedure
  ----------------------------------------------------------------------
  預存程序名稱：
      dbo.sp_CalculateTrend

  功能：
      根據使用者輸入的：
      1. 股票代碼
      2. 均線欄位名稱
      3. 往回看天數
      4. 決定天數

      去計算該股票每一天的均線趨勢，
      並把結果更新到 dbo.stock_data 的 Trend 欄位。

  趨勢判斷規則：
      若最近 @lookback_days 天內，
      有 >= @decision_days 天滿足：
          今天 MA > 昨天 MA
      則判斷為：
          上漲趨勢

      若最近 @lookback_days 天內，
      有 >= @decision_days 天滿足：
          今天 MA < 昨天 MA
      則判斷為：
          下跌趨勢

      若兩者都沒有達到門檻，
      則判斷為：
          橫盤整理
======================================================================*/
CREATE PROCEDURE dbo.sp_CalculateTrend
    @stock_code_input VARCHAR(10),   -- 使用者輸入的股票代碼，例如 '2330'
    @ma_column_name   VARCHAR(10),   -- 使用者輸入的均線欄位名稱，例如 'MA20'
    @lookback_days    INT,           -- 使用者輸入：每一天往回看幾天（含當天）
    @decision_days    INT            -- 使用者輸入：至少幾天成立才算趨勢
AS
BEGIN

    /*==================================================================
      SET NOCOUNT ON;
      ------------------------------------------------------------------
      作用：
      不顯示「(X rows affected)」這類訊息。

      為什麼常加？
      - 讓結果視窗更乾淨
      - 某些情況下可減少不必要訊息輸出
      - 寫 Stored Procedure 時很常見
    ==================================================================*/
    SET NOCOUNT ON;


    /*==================================================================
      步驟 2：基本防呆檢查
      ------------------------------------------------------------------
      這一段是在檢查：
      使用者輸入的參數是否合理。
    ==================================================================*/

    /*------------------------------------------------------------------
      檢查 1：@lookback_days 必須大於 1

      原因：
      因為我們要比較：
          今天的 MA
          和
          昨天的 MA

      如果只看 1 天，就沒有前一天可以比較，
      這樣無法判斷「今天比昨天上升還是下降」。
    ------------------------------------------------------------------*/
    IF @lookback_days <= 1
    BEGIN
        RAISERROR(N'@lookback_days 必須大於 1。', 16, 1);
        RETURN;
    END

    /*------------------------------------------------------------------
      檢查 2：@decision_days 必須合理

      必須同時滿足：
      1. 大於 0
      2. 不可以大於 @lookback_days

      為什麼？
      假設 lookback_days = 8，
      代表最近只看 8 天，
      那 decision_days 就不可能設定成 9 或 10，
      因為最近 8 天裡不可能出現 9 天成立。
    ------------------------------------------------------------------*/
    IF @decision_days <= 0 OR @decision_days > @lookback_days
    BEGIN
        RAISERROR(N'@decision_days 必須大於 0，且不能大於 @lookback_days。', 16, 1);
        RETURN;
    END

    /*------------------------------------------------------------------
      檢查 3：@ma_column_name 只能是允許的均線欄位之一

      為什麼要限制？
      因為這支程式會使用動態 SQL，
      而動態 SQL 若不限制欄位名稱，
      可能造成：
      - 欄位名稱打錯
      - SQL 組字串錯誤
      - 安全風險

      所以這裡只允許下列欄位：
      - MA5
      - MA10
      - MA20
      - MA60
      - MA120
      - MA240
    ------------------------------------------------------------------*/
    IF @ma_column_name NOT IN ('MA5', 'MA10', 'MA20', 'MA60', 'MA120', 'MA240')
    BEGIN
        RAISERROR(N'@ma_column_name 只能是 MA5、MA10、MA20、MA60、MA120、MA240。', 16, 1);
        RETURN;
    END


    /*==================================================================
      步驟 3：宣告動態 SQL 字串變數
      ------------------------------------------------------------------
      為什麼這題要用動態 SQL？
      因為 @ma_column_name 是「欄位名稱」，
      例如使用者可能輸入：
          'MA20'
      或：
          'MA60'

      在一般 SQL 中，
      變數不能直接拿來當欄位名稱使用，
      所以必須把 SQL 組成字串，再執行。
    ==================================================================*/
    DECLARE @sql_command NVARCHAR(MAX);


    /*==================================================================
      步驟 4：開始組動態 SQL 內容
      ------------------------------------------------------------------
      這裡會把整段 SQL 查詢邏輯組成一個大字串，
      最後再透過 sp_executesql 執行。
    ==================================================================*/
    SET @sql_command = N'

    /*==============================================================
      CTE 1：base_data
      --------------------------------------------------------------
      功能：
      從 dbo.stock_data 中抓出指定股票的資料，
      並把這次指定要判斷的均線欄位，統一命名成 selected_ma。

      為什麼要改名成 selected_ma？
      因為使用者每次可能選不同均線：
      - MA5
      - MA10
      - MA20
      ...
      先統一改名成 selected_ma，後面比較好寫。
    ==============================================================*/
    ;WITH base_data AS
    (
        SELECT
            stock_code,   -- 股票代碼
            [date],       -- 交易日期

            /*------------------------------------------------------
              CAST(欄位 AS FLOAT) AS selected_ma
              ------------------------------------------------------
              作用：
              把這次指定的均線欄位值轉成 FLOAT，
              並且統一命名為 selected_ma。

              這裡的欄位名稱不是固定寫死，
              而是由外層的 @ma_column_name 動態代入。

              例如：
              若 @ma_column_name = ''MA20''
              則這段會變成：
                  CAST([MA20] AS FLOAT) AS selected_ma
            ------------------------------------------------------*/
            CAST(' + QUOTENAME(@ma_column_name) + N' AS FLOAT) AS selected_ma
        FROM dbo.stock_data

        /*----------------------------------------------------------
          只抓使用者指定的那一支股票
        ----------------------------------------------------------*/
        WHERE stock_code = @stock_code_input
    ),


    /*==============================================================
      CTE 2：ma_compare
      --------------------------------------------------------------
      功能：
      取得每一天對應的「前一天均線值」。

      核心語法：
          LAG(selected_ma)

      previous_ma 的意思：
          前一天的均線值
    ==============================================================*/
    ma_compare AS
    (
        SELECT
            stock_code,   -- 股票代碼
            [date],       -- 交易日期
            selected_ma,  -- 今天這一天的均線值

            /*------------------------------------------------------
              LAG(selected_ma) OVER (...)
              ------------------------------------------------------
              LAG 的作用：
              取得前一筆資料。

              這裡的意思是：
              依照同一支股票、依日期排序後，
              取出前一天的 selected_ma，
              命名為 previous_ma。

              PARTITION BY stock_code
                  -> 每支股票分開計算，不要不同股票混在一起

              ORDER BY [date]
                  -> 按日期排序，才能知道誰是前一天
            ------------------------------------------------------*/
            LAG(selected_ma) OVER (
                PARTITION BY stock_code
                ORDER BY [date]
            ) AS previous_ma
        FROM base_data
    ),


    /*==============================================================
      CTE 3：ma_direction
      --------------------------------------------------------------
      功能：
      比較：
          今天的 MA（selected_ma）
          和
          昨天的 MA（previous_ma）

      產生兩個旗標欄位：
      1. ma_up_flag
         - 若今天 MA > 昨天 MA，則為 1
         - 否則為 0

      2. ma_down_flag
         - 若今天 MA < 昨天 MA，則為 1
         - 否則為 0
    ==============================================================*/
    ma_direction AS
    (
        SELECT
            stock_code,   -- 股票代碼
            [date],       -- 交易日期
            selected_ma,  -- 今天的均線值
            previous_ma,  -- 前一天的均線值

            /*------------------------------------------------------
              ma_up_flag
              ------------------------------------------------------
              當：
                  previous_ma IS NOT NULL
                  AND
                  selected_ma > previous_ma
              表示：
                  今天 MA 比昨天 MA 大
                  => 均線上升
              所以設為 1

              否則設為 0
            ------------------------------------------------------*/
            CASE
                WHEN previous_ma IS NOT NULL AND selected_ma > previous_ma THEN 1
                ELSE 0
            END AS ma_up_flag,

            /*------------------------------------------------------
              ma_down_flag
              ------------------------------------------------------
              當：
                  previous_ma IS NOT NULL
                  AND
                  selected_ma < previous_ma
              表示：
                  今天 MA 比昨天 MA 小
                  => 均線下降
              所以設為 1

              否則設為 0
            ------------------------------------------------------*/
            CASE
                WHEN previous_ma IS NOT NULL AND selected_ma < previous_ma THEN 1
                ELSE 0
            END AS ma_down_flag

        FROM ma_compare
    ),


    /*==============================================================
      CTE 4：trend_count
      --------------------------------------------------------------
      功能：
      對每一天來說，往前看最近 N 天（含今天），
      統計：
      1. 上升幾次 -> up_count
      2. 下降幾次 -> down_count

      這裡 N 就是外面傳進來的 @lookback_days。
    ==============================================================*/
    trend_count AS
    (
        SELECT
            stock_code,    -- 股票代碼
            [date],        -- 交易日期
            selected_ma,   -- 今天的均線值
            previous_ma,   -- 昨天的均線值
            ma_up_flag,    -- 今天是否上升（1 / 0）
            ma_down_flag,  -- 今天是否下降（1 / 0）

            /*------------------------------------------------------
              up_count
              ------------------------------------------------------
              用視窗函數 SUM(ma_up_flag) OVER (...)，
              去計算：
                  以今天為結尾，
                  最近 @lookback_days 天內，
                  有幾天均線上升。

              ROWS BETWEEN X PRECEDING AND CURRENT ROW
              的意思是：
                  從目前這列往前數 X 列，
                  一直到目前這列自己

              注意：
              若 @lookback_days = 8
              則要往前取 7 列，再加目前這列，共 8 列
              所以這裡要用：
                  @lookback_days - 1
              而不是直接用 @lookback_days
            ------------------------------------------------------*/
            SUM(ma_up_flag) OVER (
                PARTITION BY stock_code
                ORDER BY [date]
                ROWS BETWEEN ' + CAST(@lookback_days - 1 AS NVARCHAR(10)) + N' PRECEDING AND CURRENT ROW
            ) AS up_count,

            /*------------------------------------------------------
              down_count
              ------------------------------------------------------
              與 up_count 同理，
              這裡是計算：
                  以今天為結尾，
                  最近 @lookback_days 天內，
                  有幾天均線下降。
            ------------------------------------------------------*/
            SUM(ma_down_flag) OVER (
                PARTITION BY stock_code
                ORDER BY [date]
                ROWS BETWEEN ' + CAST(@lookback_days - 1 AS NVARCHAR(10)) + N' PRECEDING AND CURRENT ROW
            ) AS down_count

        FROM ma_direction
    ),


    /*==============================================================
      CTE 5：final_trend
      --------------------------------------------------------------
      功能：
      根據：
          up_count
          down_count
      和：
          @decision_days
      去決定這一天最終的趨勢名稱。

      規則：
      1. 若 up_count >= @decision_days
         => 上漲趨勢

      2. 若 down_count >= @decision_days
         => 下跌趨勢

      3. 否則
         => 橫盤整理
    ==============================================================*/
    final_trend AS
    (
        SELECT
            stock_code,  -- 股票代碼
            [date],      -- 交易日期

            /*------------------------------------------------------
              calculated_trend
              ------------------------------------------------------
              這是程式中途算出來的趨勢結果文字，
              後面會再把它更新回 dbo.stock_data 的 Trend 欄位。
            ------------------------------------------------------*/
            CASE
                WHEN up_count >= @decision_days THEN N''上漲趨勢''
                WHEN down_count >= @decision_days THEN N''下跌趨勢''
                ELSE N''橫盤整理''
            END AS calculated_trend

        FROM trend_count
    )


    /*==============================================================
      最後一步：把 final_trend 算出的結果更新回原表
      --------------------------------------------------------------
      target_table 就是 dbo.stock_data 的別名。

      更新條件：
      - 股票代碼相同
      - 日期相同

      更新內容：
      - 把 final_trend.calculated_trend
        寫入 dbo.stock_data.Trend
    ==============================================================*/
    UPDATE target_table
    SET target_table.Trend = final_trend.calculated_trend
    FROM dbo.stock_data AS target_table
    INNER JOIN final_trend
        ON target_table.stock_code = final_trend.stock_code
       AND target_table.[date] = final_trend.[date]
    WHERE target_table.stock_code = @stock_code_input;
    ';


    /*==================================================================
      步驟 5：執行動態 SQL
      ------------------------------------------------------------------
      使用 sp_executesql 執行剛剛組好的 SQL 字串。

      第一個參數：
          @sql_command
          -> 要執行的 SQL 字串

      第二個參數：
          N'@stock_code_input VARCHAR(10), @decision_days INT'
          -> 宣告這段動態 SQL 內會用到哪些參數及其型別

      後面兩行：
          把外層 Stored Procedure 的參數值，
          傳進動態 SQL 內部使用。
    ==================================================================*/
    EXEC sp_executesql
        @sql_command,
        N'@stock_code_input VARCHAR(10), @decision_days INT',
        @stock_code_input = @stock_code_input,
        @decision_days = @decision_days;


    /*==================================================================
      步驟 6：更新完成後，直接查出結果給使用者檢查
      ------------------------------------------------------------------
      這樣你執行完 SP 後，
      可以立刻看到：
      - 股票代碼
      - 日期
      - 各條均線
      - Trend

      方便確認結果有沒有被正確更新。
    ==================================================================*/
    SELECT
        stock_code,  -- 股票代碼
        [date],      -- 日期
        MA5,         -- 5日均線
        MA10,        -- 10日均線
        MA20,        -- 20日均線
        MA60,        -- 60日均線
        MA120,       -- 120日均線
        MA240,       -- 240日均線
        Trend        -- 最終更新後的趨勢結果
    FROM dbo.stock_data
    WHERE stock_code = @stock_code_input
    ORDER BY [date];

END
GO