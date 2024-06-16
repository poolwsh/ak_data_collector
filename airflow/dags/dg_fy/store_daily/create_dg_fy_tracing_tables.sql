


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
    symbol VARCHAR(20) NOT NULL,    -- Stock code, first part of the primary key
    td DATE NOT NULL,               -- Trading date, second part of the primary key
    o FLOAT,                        -- Opening price
    c FLOAT,                        -- Closing price
    h FLOAT,                        -- Highest price
    l FLOAT,                        -- Lowest price
    v BIGINT,                       -- Trading volume
    adj_close FLOAT,                -- Adjusted closing price
    dividends FLOAT,                -- Dividends
    stock_splits FLOAT,             -- Stock splits
    PRIMARY KEY (symbol, td)        -- Composite primary key: stock code and trading date
);
SELECT create_hypertable('dg_fy_stock_us_hist_daily_bfq', 'td');

