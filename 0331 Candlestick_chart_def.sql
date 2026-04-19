CREATE TABLE dbo.Candlestick_chart_def
(
    state CHAR(10) NOT NULL,
    value FLOAT NOT NULL
);

INSERT INTO dbo.Candlestick_chart_def VALUES
('long', 0.036),
('medium', 0.016),
('short', 0.006),
('tiny', 0.005);