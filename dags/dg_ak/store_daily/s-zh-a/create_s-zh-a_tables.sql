DROP TABLE IF EXISTS ak_dg_stock_zh_a_hist_daily_bfq;
CREATE TABLE ak_dg_stock_zh_a_hist_daily_bfq (
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

DROP TABLE IF EXISTS ak_dg_stock_zh_a_hist_store_daily_bfq;
CREATE TABLE ak_dg_stock_zh_a_hist_store_daily_bfq (
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

DROP TABLE IF EXISTS ak_dg_stock_zh_a_hist_daily_hfq;
CREATE TABLE ak_dg_stock_zh_a_hist_daily_hfq (
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

DROP TABLE IF EXISTS ak_dg_stock_zh_a_hist_store_daily_hfq;
CREATE TABLE ak_dg_stock_zh_a_hist_store_daily_hfq (
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

DROP TABLE IF EXISTS ak_dg_stock_zh_a_trade_date;
CREATE TABLE ak_dg_stock_zh_a_trade_date (
    trade_date DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    PRIMARY KEY (trade_date)
);

DROP TABLE IF EXISTS ak_dg_stock_zh_a_code_name;
CREATE TABLE ak_dg_stock_zh_a_code_name (
    s_code VARCHAR(20) NOT NULL PRIMARY KEY, -- 股票代码，作为主键
    s_name VARCHAR(100) NOT NULL,            -- 股票名称
    create_time TIMESTAMP,                   -- 创建时间
    update_time TIMESTAMP                    -- 更新时间
);
