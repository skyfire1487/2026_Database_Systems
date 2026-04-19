DECLARE @type INT;

-- 範例1：非交易日
EXEC dbo.tradingVolume_type 
     '2330', 20, '2025-01-01', @type OUTPUT;
PRINT @type;

-- 範例2：極大量
EXEC dbo.tradingVolume_type 
     '2330', 20, '2025-01-06', @type OUTPUT;
PRINT @type;

-- 範例3：大量
EXEC dbo.tradingVolume_type 
     '2330', 20, '2025-01-13', @type OUTPUT;
PRINT @type;

-- 範例4：普通
EXEC dbo.tradingVolume_type 
     '2330', 20, '2025-01-20', @type OUTPUT;
PRINT @type;

-- 範例5：小量
EXEC dbo.tradingVolume_type 
     '2330', 20, '2025-01-21', @type OUTPUT;
PRINT @type;

-- 範例6：極小量
EXEC dbo.tradingVolume_type 
     '2330', 20, '2024-12-25', @type OUTPUT;
PRINT @type;