USE ncu_db;
GO

/*==============================================================
  若已存在則刪除
==============================================================*/
IF OBJECT_ID('dbo.sp_Rule1_Breakthrough', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Rule1_Breakthrough;
GO


/*==============================================================
  Stored Procedure：Rule1 突破買點偵測
==============================================================*/
CREATE PROCEDURE dbo.sp_Rule1_Breakthrough
    @stock_code_input VARCHAR(10)   -- 股票代碼
AS
BEGIN

    SET NOCOUNT ON;

    /*==========================================================
      CTE 1：基礎資料
      ----------------------------------------------------------
      取得：
      - 股價（收盤價 c）
      - MA20
    ==========================================================*/
    ;WITH base_data AS
    (
        SELECT
            stock_code,
            [date],
            c,          -- 收盤價
            MA20        -- 20日均線
        FROM dbo.stock_data
        WHERE stock_code = @stock_code_input
    ),

    /*==========================================================
      CTE 2：加入前一天 MA20（判斷趨勢用）
    ==========================================================*/
    ma_compare AS
    (
        SELECT
            *,
            LAG(MA20) OVER (
                PARTITION BY stock_code
                ORDER BY [date]
            ) AS prev_MA20
        FROM base_data
    ),

    /*==========================================================
      CTE 3：判斷每天是否在 MA20 下方
    ==========================================================*/
    price_position AS
    (
        SELECT
            *,
            
            /*----------------------------------------------
              若股價 < MA20 → 1
              表示弱勢
            ----------------------------------------------*/
            CASE 
                WHEN c < MA20 THEN 1
                ELSE 0
            END AS below_MA_flag

        FROM ma_compare
    ),

    /*==========================================================
      CTE 4：計算「前7天」弱勢天數（不含今天）
    ==========================================================*/
    lookback_calc AS
    (
        SELECT
            *,
            
            /*----------------------------------------------
              往前看7天（不含今天）
              
              ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
              
              重點：
              - 不含 CURRENT ROW（今天）
            ----------------------------------------------*/
            SUM(below_MA_flag) OVER (
                PARTITION BY stock_code
                ORDER BY [date]
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS below_count_7

        FROM price_position
    ),

    /*==========================================================
      CTE 5：最終判斷 Rule1
    ==========================================================*/
    final_result AS
    (
        SELECT
            *,
            
            /*----------------------------------------------
              Rule1 條件判斷
            ----------------------------------------------*/
            CASE
                WHEN 
                    /* 條件1：前7天至少6天在MA20下 */
                    below_count_7 >= 6
                    
                    /* 條件2：今天站上MA20 */
                    AND c > MA20
                    
                    /* 條件3：均線不再下降 */
                    AND (MA20 >= prev_MA20 OR prev_MA20 IS NULL)

                THEN 1
                ELSE 0
            END AS is_breakthrough

        FROM lookback_calc
    )


    /*==========================================================
      最終輸出
    ==========================================================*/
    SELECT
        stock_code,
        [date],
        c,
        MA20,
        below_count_7,
        is_breakthrough
    FROM final_result
    WHERE is_breakthrough = 1   -- 只顯示買點
    ORDER BY [date];

END
GO

EXEC dbo.sp_Rule1_Breakthrough '2330';