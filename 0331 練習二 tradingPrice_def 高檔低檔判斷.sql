CREATE TABLE dbo.tradingPrice_def
(
    compare_with INT,
    high_ratio   FLOAT,
    low_ratio    FLOAT
);

INSERT INTO dbo.tradingPrice_def VALUES (20, 0.2, 0.2);
INSERT INTO dbo.tradingPrice_def VALUES (60, 0.2, 0.2);