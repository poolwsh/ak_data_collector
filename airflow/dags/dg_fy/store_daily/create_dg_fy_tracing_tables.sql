


CREATE TABLE dg_fy_tracing_s_us (
    symbol VARCHAR NOT NULL,
    last_td DATE,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (symbol)
);




CREATE TABLE dg_fh_s_us_symbol (
    symbol VARCHAR(20) NOT NULL PRIMARY KEY,
    currency VARCHAR(10),                   
    description TEXT,                       
    displaySymbol VARCHAR(20),               
    figi VARCHAR(50),                       
    isin VARCHAR(50),                        
    mic VARCHAR(20),                         
    shareClassFIGI VARCHAR(50),              
    symbol2 VARCHAR(20),                    
    type VARCHAR(50),                      
    create_time TIMESTAMP,                  
    update_time TIMESTAMP                  
);




CREATE TABLE dg_fy_stock_us_trade_date (
    trade_date DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    PRIMARY KEY (trade_date)
);



CREATE TABLE dg_fy_stock_us_hist_daily_bfq (
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
SELECT create_hypertable('dg_fy_stock_us_hist_daily_bfq', 'td');
