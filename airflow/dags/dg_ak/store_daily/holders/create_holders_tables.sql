

CREATE TABLE dg_ak_stock_hold_num_cninfo (
    td DATE NOT NULL,              -- 变动日期，作为主键的一部分
    s_code VARCHAR(20) NOT NULL,   -- 证券代码，作为主键的一部分
    s_name VARCHAR(100),           -- 证券简称
    cur_sh INTEGER,                -- 本期股东人数
    pre_sh INTEGER,                -- 上期股东人数
    num_of_sh_chg_pct FLOAT,       -- 股东人数增幅
    cur_avg_holdings FLOAT,        -- 本期人均持股数量
    pre_avg_holdings FLOAT,        -- 上期人均持股数量
    avg_holdings_chg_pct FLOAT,    -- 人均持股数量增幅
    PRIMARY KEY (td, s_code)       -- 将变动日期和证券代码组合作为主键
);
