
DROP TABLE IF EXISTS fy_dg_tracing_s_us;
CREATE TABLE fy_dg_tracing_s_us (
    symbol VARCHAR NOT NULL,
    last_td DATE,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (symbol)
);


DROP TABLE IF EXISTS fh_dg_s_us_symbol;
CREATE TABLE fh_dg_s_us_symbol (
    symbol VARCHAR(20) NOT NULL PRIMARY KEY,  -- 股票代码，作为主键
    currency VARCHAR(10),                     -- 股票交易的货币
    description TEXT,                         -- 股票的描述或公司名称
    displaySymbol VARCHAR(20),                -- 股票在交易所显示的符号
    figi VARCHAR(50),                         -- 金融工具全球标识符
    isin VARCHAR(50),                         -- 国际证券识别码
    mic VARCHAR(20),                          -- 市场识别码
    shareClassFIGI VARCHAR(50),               -- 股票类别的FIGI
    symbol2 VARCHAR(20),                      -- 备用符号
    type VARCHAR(50),                         -- 股票的类型
    create_time TIMESTAMP,                    -- 创建时间
    update_time TIMESTAMP                     -- 更新时间
);


DROP TABLE IF EXISTS fy_dg_stock_us_trade_date;
CREATE TABLE fy_dg_stock_us_trade_date (
    trade_date DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    PRIMARY KEY (trade_date)
);

DROP TABLE IF EXISTS fy_dg_stock_us_hist_daily_bfq;
CREATE TABLE fy_dg_stock_us_hist_daily_bfq (
    symbol VARCHAR(20) NOT NULL,    -- 股票代码，作为主键的第一部分
    td DATE NOT NULL,               -- 交易日期，作为主键的第二部分
    o FLOAT,                        -- 开盘价
    c FLOAT,                        -- 收盘价
    h FLOAT,                        -- 最高价
    l FLOAT,                        -- 最低价
    v BIGINT,                       -- 成交量
    adj_close FLOAT,                -- 调整后的收盘价
    dividends FLOAT,                -- 分红
    stock_splits FLOAT,             -- 股票拆分
    PRIMARY KEY (symbol, td)        -- 组合主键：股票代码和交易日期
);
SELECT create_hypertable('fy_dg_stock_us_hist_daily_bfq', 'td');
