
DROP TABLE IF EXISTS ak_da_stock_price_hl;
CREATE TABLE ak_da_stock_price_hl (
    s_code VARCHAR(20) NOT NULL,        -- 股票代码
    td DATE NOT NULL,                   -- 交易日期
    interval INT NOT NULL,              -- 时间间隔
    historical_high FLOAT,              -- 历史高点
    historical_low FLOAT,               -- 历史低点
    distance_historical_high INT,       -- 距历史高点的天数
    distance_historical_low INT,        -- 距历史低点的天数
    change_from_historical FLOAT,       -- 从历史高点或历史低点的变化值
    pct_chg_from_historical FLOAT,      -- 从历史高点或历史低点的百分比变化
    target_high FLOAT,                  -- 目标高点
    target_low FLOAT,                   -- 目标低点
    distance_target_high INT,           -- 距目标高点的天数
    distance_target_low INT,            -- 距目标低点的天数
    change_to_target FLOAT,             -- 到目标高点或目标低点的变化值
    pct_chg_to_target FLOAT,            -- 到目标高点或目标低点的百分比变化
    PRIMARY KEY (s_code, td, interval), -- 组合主键：股票代码、交易日期和间隔
    FOREIGN KEY (s_code, td) REFERENCES ak_dg_stock_zh_a_hist_store_daily_hfq (s_code, td) ON DELETE CASCADE
);

