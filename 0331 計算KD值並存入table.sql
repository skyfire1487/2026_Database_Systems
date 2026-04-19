/*==========================================================
  宣告計算KD會使用到的變數
==========================================================*/

-- 今日計算結果
DECLARE @K_value FLOAT;      -- 今日K值
DECLARE @D_value FLOAT;      -- 今日D值

-- 今日RSV
DECLARE @RSV FLOAT;          -- 今日RSV(0~100)

-- 今日資料
DECLARE @today_c FLOAT;      -- 今日收盤價
DECLARE @today_date DATE;    -- 今日日期
DECLARE @StockCode VARCHAR(10);  -- 股票代碼

-- 控制是否換股票
DECLARE @row_number INT;     -- 每檔股票內排序用

-- 昨日KD（用於平滑計算）
DECLARE @yesterday_K FLOAT;  -- 前一日K
DECLARE @yesterday_D FLOAT;  -- 前一日D

-- 初始化第一筆資料
SET @yesterday_K = 50;  -- 第一筆資料的昨日K預設50
SET @yesterday_D = 50;  -- 第一筆資料的昨日D預設50



/*==========================================================
  開啟 cursor：逐筆讀取 dbo.stock_data
==========================================================*/
DECLARE cur CURSOR FOR   -- 宣告一個名稱為 cur 的 cursor

SELECT 
    /*------------------------------------------------------
      ROW_NUMBER()：
      為每一檔股票(stock_code)重新編號
      用來判斷是不是該股票的第一筆資料
    -------------------------------------------------------*/
    ROW_NUMBER() OVER (
        PARTITION BY stock_code   -- 依「stock_code」分組
                                  -- 每檔股票獨立計算排序編號
        ORDER BY date ASC         -- 在每檔股票內
                                  -- 依「date」由小到大排序
                                  -- 也就是從最早日期排到最新日期
    ) AS row_number,              -- 產生排序編號欄位
    /*------------------------------------------------------
      以下欄位是等一下FETCH時要讀進變數的資料
    -------------------------------------------------------*/
    stock_code,   -- 股票代碼（對應資料表 stock_code 欄位）
    date,         -- 交易日期（對應資料表 date 欄位）
    c             -- 收盤價（對應資料表 c 欄位）

FROM dbo.stock_data   -- 資料來源：你現在的主資料表

/*----------------------------------------------------------
  再做一次整體排序（讓不同股票也照順序排列）
-----------------------------------------------------------*/
ORDER BY 
    stock_code ASC,   -- 先依股票代碼排序
    date ASC;         -- 再依日期由舊到新排序

OPEN cur;   -- 真正啟動cursor，開始準備逐筆讀資料



/*==========================================================
  計算RSV（以9天為例）
  使用 dbo.stock_data 表
==========================================================*/
SET NOCOUNT ON;

-- 若換到新股票
IF (@row_number = 1)
BEGIN
    SET @yesterday_K = 50;
    SET @yesterday_D = 50;
END

-- 計算RSV（9天）
SELECT @RSV =
       (@today_c - MIN(l))
       / (MAX(h) - MIN(l)) * 100
FROM dbo.stock_data
WHERE stock_code = @StockCode
  AND date <= @today_date
GROUP BY stock_code;

-- 計算K值
SET @K_value =
    (2.0/3.0) * @yesterday_K
  + (1.0/3.0) * @RSV;

-- 計算D值
SET @D_value =
    (2.0/3.0) * @yesterday_D
  + (1.0/3.0) * @K_value;



/*==========================================================
  將計算完成的K/D值寫回 dbo.stock_data
==========================================================*/
UPDATE dbo.stock_data
SET K_value = @K_value,
    D_value = @D_value
WHERE stock_code = @StockCode   -- 指定股票
  AND date = @today_date;       -- 指定日期
/*----------------------------------------------------------
  把今天的K/D存起來
  讓下一筆資料當作昨日K/D使用
-----------------------------------------------------------*/
SET @yesterday_K = @K_value;
SET @yesterday_D = @D_value;
/*----------------------------------------------------------
  讀取下一筆資料
-----------------------------------------------------------*/
FETCH NEXT FROM cur
INTO @row_number, @StockCode, @today_date, @today_c;
/*==========================================================
  迴圈結束後，關閉cursor
==========================================================*/
CLOSE cur;       -- 關閉cursor
DEALLOCATE cur;  -- 釋放cursor佔用的記憶體