DECLARE @type INT;

EXEC dbo.tradingPrice_type '2330', 20, '2024-03-14', @type OUTPUT;
PRINT @type;

EXEC dbo.tradingPrice_type '2330', 20, '2024-02-02', @type OUTPUT;
PRINT @type;

EXEC dbo.tradingPrice_type '2330', 20, '2023-09-26', @type OUTPUT;
PRINT @type;