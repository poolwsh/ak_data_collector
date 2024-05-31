
DROP TABLE IF EXISTS ak_da_stock_price_hl;
CREATE TABLE ak_da_stock_price_hl (
    s_code VARCHAR(20) NOT NULL,        -- 股票代码
    td DATE NOT NULL,                   -- 交易日期
    interval INT NOT NULL,              -- 时间间隔
    hs_h FLOAT,                         -- 历史高点
    hs_l FLOAT,                         -- 历史低点
    d_hs_h INT,                         -- 距历史高点的天数
    d_hs_l INT,                         -- 距历史低点的天数
    chg_from_hs FLOAT,                  -- 从历史高点或历史低点的变化值
    pct_chg_from_hs FLOAT,              -- 从历史高点或历史低点的百分比变化
    tg_h FLOAT,                         -- 目标高点
    tg_l FLOAT,                         -- 目标低点
    d_tg_h INT,                         -- 距目标高点的天数
    d_tg_l INT,                         -- 距目标低点的天数
    chg_to_tg FLOAT,                    -- 到目标高点或目标低点的变化值
    pct_chg_to_tg FLOAT,                -- 到目标高点或目标低点的百分比变化
    PRIMARY KEY (s_code, td, interval), -- 组合主键：股票代码、交易日期和间隔
    FOREIGN KEY (s_code, td) REFERENCES ak_dg_stock_zh_a_hist_store_daily_hfq (s_code, td) ON DELETE CASCADE
);
SELECT create_hypertable('ak_da_stock_price_hl', 'td');

DROP TABLE IF EXISTS ak_da_stock_price_hl_store;
CREATE TABLE ak_da_stock_price_hl_store (
    s_code VARCHAR(20) NOT NULL,        -- 股票代码
    td DATE NOT NULL,                   -- 交易日期
    interval INT NOT NULL,              -- 时间间隔
    hs_h FLOAT,                         -- 历史高点
    hs_l FLOAT,                         -- 历史低点
    d_hs_h INT,                         -- 距历史高点的天数
    d_hs_l INT,                         -- 距历史低点的天数
    chg_from_hs FLOAT,                  -- 从历史高点或历史低点的变化值
    pct_chg_from_hs FLOAT,              -- 从历史高点或历史低点的百分比变化
    tg_h FLOAT,                         -- 目标高点
    tg_l FLOAT,                         -- 目标低点
    d_tg_h INT,                         -- 距目标高点的天数
    d_tg_l INT,                         -- 距目标低点的天数
    chg_to_tg FLOAT,                    -- 到目标高点或目标低点的变化值
    pct_chg_to_tg FLOAT,                -- 到目标高点或目标低点的百分比变化
    PRIMARY KEY (s_code, td, interval), -- 组合主键：股票代码、交易日期和间隔
    FOREIGN KEY (s_code, td) REFERENCES ak_dg_stock_zh_a_hist_store_daily_hfq (s_code, td) ON DELETE CASCADE
);
SELECT create_hypertable('ak_da_stock_price_hl_store', 'td');
