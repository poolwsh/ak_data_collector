
DROP TABLE IF EXISTS ak_dg_stock_zt_pool_em;
CREATE TABLE ak_dg_stock_zt_pool_em (
    td DATE NOT NULL,               -- 交易日期，作为主键的第一部分
    s_code VARCHAR(20) NOT NULL,    -- 股票代码，为主键的第二部分
    s_name VARCHAR(100),
    pct_chg FLOAT,                  -- 涨跌幅
    c FLOAT,                        -- 最新价
    a FLOAT,                        -- 成交额
    circulation_mv NUMERIC(18, 2),  -- 流通市值
    total_mv NUMERIC(18, 2),        -- 总市值
    turnover_rate FLOAT,            -- 换手率
    lock_fund BIGINT,               -- 封板资金
    first_lock_time TIME,           -- 首次封板时间
    last_lock_time TIME,            -- 最后封板时间
    failed_account INT,             -- 炸板次数
    zt_stat VARCHAR(20),            -- 涨停统计
    zt_account INT,                 -- 连板数
    industry VARCHAR(100),          -- 所属行业
    PRIMARY KEY (td, s_code)        -- 将交易日期和股票代码组合作为主键
);


DROP TABLE IF EXISTS ak_dg_stock_zt_pool_em_store;
CREATE TABLE ak_dg_stock_zt_pool_em_store (
    td DATE NOT NULL,               -- 交易日期，作为主键的第一部分
    s_code VARCHAR(20) NOT NULL,    -- 股票代码，为主键的第二部分
    s_name VARCHAR(100),
    pct_chg FLOAT,                  -- 涨跌幅
    c FLOAT,                        -- 最新价
    a FLOAT,                        -- 成交额
    circulation_mv NUMERIC(18, 2),  -- 流通市值
    total_mv NUMERIC(18, 2),        -- 总市值
    turnover_rate FLOAT,            -- 换手率
    lock_fund BIGINT,               -- 封板资金
    first_lock_time TIME,           -- 首次封板时间
    last_lock_time TIME,            -- 最后封板时间
    failed_account INT,             -- 炸板次数
    zt_stat VARCHAR(20),            -- 涨停统计
    zt_account INT,                 -- 连板数
    industry VARCHAR(100),          -- 所属行业
    PRIMARY KEY (td, s_code)        -- 将交易日期和股票代码组合作为主键
);



