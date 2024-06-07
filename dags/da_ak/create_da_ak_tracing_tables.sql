DROP TABLE IF EXISTS da_ak_tracing_stock_price_hl;
CREATE TABLE da_ak_tracing_stock_price_hl (
    s_code VARCHAR NOT NULL,          -- 股票代码
    min_td DATE NOT NULL,             -- 已计算最小交易日期
    max_td DATE NOT NULL,             -- 已计算最大交易日期
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 记录创建时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 记录更新时间
    host_name VARCHAR,                -- 主机名
    PRIMARY KEY (s_code)              -- 主键：股票代码
);
