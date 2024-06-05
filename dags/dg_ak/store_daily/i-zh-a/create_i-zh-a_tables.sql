
DROP TABLE IF EXISTS ak_dg_index_zh_a_hist_daily_temp;
CREATE TABLE ak_dg_index_zh_a_hist_daily_temp (
    s_code VARCHAR(20) NOT NULL,    -- 股票代码，作为主键的第一部分
    td DATE NOT NULL,             -- 交易日期，作为主键的第二部分
    o FLOAT,                        -- 开盘价
    c FLOAT,                        -- 收盘价
    h FLOAT,                        -- 最高价
    l FLOAT,                        -- 最低价
    v BIGINT,                       -- 成交量
    a NUMERIC(18, 2),               -- 成交额
    amplitude FLOAT,                -- 振幅
    pct_chg FLOAT,                  -- 涨跌幅
    change FLOAT,                   -- 涨跌额
    turnover_rate FLOAT,            -- 换手率
    PRIMARY KEY (s_code, td)      -- 组合主键：股票代码和交易日期
);


DROP TABLE IF EXISTS ak_dg_index_zh_a_hist_daily;
CREATE TABLE ak_dg_index_zh_a_hist_daily (
    s_code VARCHAR(20) NOT NULL,    -- 股票代码，作为主键的第一部分
    td DATE NOT NULL,             -- 交易日期，作为主键的第二部分
    o FLOAT,                        -- 开盘价
    c FLOAT,                        -- 收盘价
    h FLOAT,                        -- 最高价
    l FLOAT,                        -- 最低价
    v BIGINT,                       -- 成交量
    a NUMERIC(18, 2),               -- 成交额
    amplitude FLOAT,                -- 振幅
    pct_chg FLOAT,                  -- 涨跌幅
    change FLOAT,                   -- 涨跌额
    turnover_rate FLOAT,            -- 换手率
    PRIMARY KEY (s_code, td)      -- 组合主键：股票代码和交易日期
);
SELECT create_hypertable('ak_dg_index_zh_a_hist_daily', 'td');


DROP TABLE IF EXISTS ak_dg_index_zh_a_code_name;
CREATE TABLE ak_dg_index_zh_a_code_name (
    i_code VARCHAR(20) NOT NULL PRIMARY KEY, -- 股票代码，作为主键
    i_name VARCHAR(100) NOT NULL,            -- 股票名称
    symbol VARCHAR(100),
    create_time TIMESTAMP,                   -- 创建时间
    update_time TIMESTAMP                    -- 更新时间
);