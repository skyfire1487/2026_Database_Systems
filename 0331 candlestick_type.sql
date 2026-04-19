CREATE OR ALTER FUNCTION dbo.candlestick_type
(
    @company VARCHAR(10),
    @date DATE
)
RETURNS @result_table TABLE
(
    company VARCHAR(10),
    date DATE,
    type INT
)
AS
BEGIN

    DECLARE @o REAL;          -- 開盤價
    DECLARE @c REAL;          -- 收盤價
    DECLARE @ratio FLOAT;     -- 漲跌幅
    DECLARE @type INT;        -- 回傳分類代碼

    -- 取得當天開盤與收盤
    SELECT 
        @o = o,
        @c = c
    FROM dbo.stock_data
    WHERE stock_code = @company
      AND date = @date;

    -- 若資料不存在，回傳NULL
    IF @o IS NULL OR @c IS NULL
    BEGIN
        INSERT INTO @result_table VALUES (@company, @date, NULL);
        RETURN;
    END

    -- 計算漲跌幅
    SET @ratio = (@c - @o) * 1.0 / @o;

    -- 判斷型態
    IF ABS(@ratio) <= 0.005
        SET @type = 0;

    ELSE IF @ratio > 0
    BEGIN
        IF @ratio > 0.036
            SET @type = 4;
        ELSE IF @ratio >= 0.016
            SET @type = 3;
        ELSE IF @ratio >= 0.006
            SET @type = 2;
        ELSE
            SET @type = 1;
    END
    ELSE
    BEGIN
        IF @ratio < -0.036
            SET @type = -4;
        ELSE IF @ratio <= -0.016
            SET @type = -3;
        ELSE IF @ratio <= -0.006
            SET @type = -2;
        ELSE
            SET @type = -1;
    END

    INSERT INTO @result_table
    VALUES (@company, @date, @type);

    RETURN;

END;

