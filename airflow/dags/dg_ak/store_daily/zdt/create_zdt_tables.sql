

CREATE TABLE dg_ak_stock_zt_pool_dtgc_em (
    td DATE NOT NULL,
    s_code VARCHAR(20) NOT NULL,
    s_name VARCHAR(100),
    pct_chg FLOAT,
    c FLOAT,
    a FLOAT,
    circulation_mv NUMERIC(18, 2),
    total_mv NUMERIC(18, 2),
    dynamic_pe FLOAT,
    turnover_rate FLOAT,
    lock_fund BIGINT,
    last_lock_time TIME,
    board_amount BIGINT,
    continuous_dt INT,
    open_account INT,
    industry VARCHAR(100),
    PRIMARY KEY (td, s_code)
);




CREATE TABLE dg_ak_stock_zt_pool_em (
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




CREATE TABLE dg_ak_stock_zt_pool_previous_em (
    td DATE NOT NULL,
    s_code VARCHAR(20) NOT NULL,
    s_name VARCHAR(100),
    pct_chg FLOAT,
    c FLOAT,
    zt_price FLOAT,
    a FLOAT,
    circulation_mv NUMERIC(18,2),
    total_mv NUMERIC(18,2),
    turnover_rate FLOAT,
    increase_speed FLOAT,
    amplitude FLOAT,
    yesterday_lock_time TIME,
    yesterday_zt_account INT,
    zt_stat VARCHAR(20),
    industry VARCHAR(100),
    PRIMARY KEY (td, s_code)
);





CREATE TABLE dg_ak_stock_zt_pool_strong_em (
    td DATE NOT NULL,
    s_code VARCHAR(20) NOT NULL,
    s_name VARCHAR(100),
    pct_chg FLOAT,
    c FLOAT,
    zt_price FLOAT,
    a FLOAT,
    circulation_mv NUMERIC(18,2),
    total_mv NUMERIC(18,2),
    turnover_rate FLOAT,
    increase_speed FLOAT,
    is_new_high BOOLEAN,
    quantity_ratio FLOAT,
    zt_stat VARCHAR(20),
    selection_reason VARCHAR(255),
    industry VARCHAR(100),
    PRIMARY KEY (td, s_code)
);





CREATE TABLE dg_ak_stock_zt_pool_sub_new_em (
    td DATE NOT NULL,
    s_code VARCHAR(20) NOT NULL,
    s_name VARCHAR(100),
    pct_chg FLOAT,
    c FLOAT,
    zt_price FLOAT,
    a FLOAT,
    circulation_mv NUMERIC(18,2),
    total_mv NUMERIC(18,2),
    turnover_rate FLOAT,
    days_since_open INT,
    open_date DATE,
    ipo_date DATE,
    is_new_high BOOLEAN,
    zt_stat VARCHAR(20),
    industry VARCHAR(100),
    PRIMARY KEY (td, s_code)
);




CREATE TABLE dg_ak_stock_zt_pool_zbgc_em (
    td DATE NOT NULL,
    s_code VARCHAR(20) NOT NULL,
    s_name VARCHAR(100),
    pct_chg FLOAT,
    c FLOAT,
    zt_price FLOAT,
    a FLOAT,
    circulation_mv NUMERIC(18,2),
    total_mv NUMERIC(18,2),
    turnover_rate FLOAT,
    increase_speed FLOAT,
    first_lock_time TIME,
    failed_account INT,
    zt_stat VARCHAR(20),
    amplitude FLOAT,
    industry VARCHAR(100),
    PRIMARY KEY (td, s_code)
);


