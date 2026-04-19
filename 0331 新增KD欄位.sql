/*==========================================================
  在 stock_data 資料表新增 KD 欄位
==========================================================*/

ALTER TABLE dbo.stock_data
ADD K_value FLOAT NULL,   -- 新增K值欄位，允許NULL
    D_value FLOAT NULL;   -- 新增D值欄位，允許NULL