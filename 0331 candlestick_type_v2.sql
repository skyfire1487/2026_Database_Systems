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

    DECLARE @o REAL;
    DECLARE @c REAL;
    DECLARE @ratio FLOAT;
    DECLARE @type INT;

    DECLARE @long FLOAT;
    DECLARE @medium FLOAT;
    DECLARE @short FLOAT;
    DECLARE @tiny FLOAT;

    -- 讀取門檻
    SELECT @long = value FROM dbo.Candlestick_chart_def WHERE state = 'long';
    SELECT @medium = value FROM dbo.Candlestick_chart_def WHERE state = 'medium';
    SELECT @short = value FROM dbo.Candlestick_chart_def WHERE state = 'short';
    SELECT @tiny = value FROM dbo.Candlestick_chart_def WHERE state = 'tiny';

    -- 取得價格
    SELECT 
        @o = o,
        @c = c
    FROM dbo.stock_data
    WHERE stock_code = @company
      AND date = @date;

    IF @o IS NULL OR @c IS NULL
    BEGIN
        INSERT INTO @result_table VALUES (@company, @date, NULL);
        RETURN;
    END

    SET @ratio = (@c - @o) * 1.0 / @o;

    IF ABS(@ratio) <= @tiny
        SET @type = 0;

    ELSE IF @ratio > 0
    BEGIN
        IF @ratio > @long
            SET @type = 4;
        ELSE IF @ratio >= @medium
            SET @type = 3;
        ELSE IF @ratio >= @short
            SET @type = 2;
        ELSE
            SET @type = 1;
    END
    ELSE
    BEGIN
        IF @ratio < -@long
            SET @type = -4;
        ELSE IF @ratio <= -@medium
            SET @type = -3;
        ELSE IF @ratio <= -@short
            SET @type = -2;
        ELSE
            SET @type = -1;
    END

    INSERT INTO @result_table
    VALUES (@company, @date, @type);

    RETURN;

END;