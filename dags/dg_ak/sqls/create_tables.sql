
-- create dg ak tracing tables
DROP TABLE IF EXISTS dg_ak_tracing_dt;
CREATE TABLE dg_ak_tracing_dt (
    ak_func_name VARCHAR NOT NULL,
    dt DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, dt)
);

DROP TABLE IF EXISTS dg_ak_tracing_scode_dt;
CREATE TABLE dg_ak_tracing_scode_dt (
    ak_func_name VARCHAR NOT NULL,
    scode VARCHAR NOT NULL,
    dt DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, scode, dt)
);

-- create dg ak tables
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





DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_ths;
CREATE TABLE ak_dg_stock_board_concept_name_ths (
    date DATE NOT NULL,
    b_create_date DATE NOT NULL,
    b_code VARCHAR(50),
    b_name VARCHAR(255) NOT NULL,
    stock_count INT,
    url VARCHAR(255),
    PRIMARY KEY (date,  b_code)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_ths_store;
CREATE TABLE ak_dg_stock_board_concept_name_ths_store (
    date DATE NOT NULL,
    b_create_date DATE NOT NULL,
    b_code VARCHAR(50),
    b_name VARCHAR(255) NOT NULL,
    stock_count INT,
    url VARCHAR(255),
    PRIMARY KEY (date,  b_code)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_em;
CREATE TABLE ak_dg_stock_board_concept_name_em (
    date DATE NOT NULL,
    rank INT,
    b_code VARCHAR(50),
    b_name VARCHAR(255),
    c DECIMAL,
    change_amount DECIMAL,
    change_percentage DECIMAL,
    total_mv BIGINT,
    turnover_rate DECIMAL,
    risers INT,
    fallers INT,
    leader_s VARCHAR(255),
    leader_s_change DECIMAL,
    PRIMARY KEY (date, b_code)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_em_store;
CREATE TABLE ak_dg_stock_board_concept_name_em_store (
    date DATE NOT NULL,
    rank INT,
    b_code VARCHAR(50),
    b_name VARCHAR(255),
    c DECIMAL,
    change_amount DECIMAL,
    change_percentage DECIMAL,
    total_mv BIGINT,
    turnover_rate DECIMAL,
    risers INT,
    fallers INT,
    leader_s VARCHAR(255),
    leader_s_change DECIMAL,
    PRIMARY KEY (date, b_code)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_summary_ths;
CREATE TABLE ak_dg_stock_board_industry_summary_ths (
    date DATE NOT NULL,
    b_name VARCHAR(255),
    change_percentage DECIMAL,
    v DECIMAL,
    a DECIMAL,
    net_inflow DECIMAL,
    risers INT,
    fallers INT,
    average_price DECIMAL,
    leader_s VARCHAR(255),
    leader_s_price DECIMAL,
    leader_s_change DECIMAL,
    PRIMARY KEY (date, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_summary_ths_store;
CREATE TABLE ak_dg_stock_board_industry_summary_ths_store (
    date DATE NOT NULL,
    b_name VARCHAR(255),
    change_percentage DECIMAL,
    v DECIMAL,
    a DECIMAL,
    net_inflow DECIMAL,
    risers INT,
    fallers INT,
    average_price DECIMAL,
    leader_s VARCHAR(255),
    leader_s_price DECIMAL,
    leader_s_change DECIMAL,
    PRIMARY KEY (date, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_name_em;
CREATE TABLE ak_dg_stock_board_industry_name_em (
    date DATE NOT NULL,
    rank INT,
    b_code VARCHAR(50),
    b_name VARCHAR(255),
    c DECIMAL,
    change_amount DECIMAL,
    change_percentage DECIMAL,
    total_mv BIGINT,
    turnover_rate DECIMAL,
    risers INT,
    fallers INT,
    leader_s VARCHAR(255),
    leader_s_change DECIMAL,
    PRIMARY KEY (date, b_code)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_name_em_store;
CREATE TABLE ak_dg_stock_board_industry_name_em_store (
    date DATE NOT NULL,
    rank INT,
    b_code VARCHAR(50),
    b_name VARCHAR(255),
    c DECIMAL,
    change_amount DECIMAL,
    change_percentage DECIMAL,
    total_mv BIGINT,
    turnover_rate DECIMAL,
    risers INT,
    fallers INT,
    leader_s VARCHAR(255),
    leader_s_change DECIMAL,
    PRIMARY KEY (date, b_code)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_ths;
CREATE TABLE ak_dg_stock_board_concept_cons_ths (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (date, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_ths_store;
CREATE TABLE ak_dg_stock_board_concept_cons_ths_store (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (date, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_em;
CREATE TABLE ak_dg_stock_board_concept_cons_em (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (date, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_em_store;
CREATE TABLE ak_dg_stock_board_concept_cons_em_store (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (date, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_ths;
CREATE TABLE ak_dg_stock_board_industry_cons_ths (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    c DECIMAL(10, 2),
    pct_chg DECIMAL(10, 2),
    change DECIMAL(10, 2),
    increase_speed DECIMAL(10, 2),
    turnover_rate DECIMAL(10, 2),
    quantity_ratio DECIMAL(10, 2),
    amplitude DECIMAL(10, 2),
    a VARCHAR(50),
    circulating_shares VARCHAR(50),
    circulation_mv DECIMAL(20, 2),
    pe DECIMAL(10, 2),
    PRIMARY KEY (date, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_ths_store;
CREATE TABLE ak_dg_stock_board_industry_cons_ths_store (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    c DECIMAL(10, 2),
    pct_chg DECIMAL(10, 2),
    change DECIMAL(10, 2),
    increase_speed DECIMAL(10, 2),
    turnover_rate DECIMAL(10, 2),
    quantity_ratio DECIMAL(10, 2),
    amplitude DECIMAL(10, 2),
    a VARCHAR(50),
    circulating_shares VARCHAR(50),
    circulation_mv DECIMAL(20, 2),
    pe DECIMAL(10, 2),
    PRIMARY KEY (date, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_em;
CREATE TABLE ak_dg_stock_board_industry_cons_em (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    c DECIMAL(10, 2),
    pct_chg DECIMAL(10, 2),
    change DECIMAL(10, 2),
    v BIGINT,
    a DECIMAL(20, 2),
    amplitude DECIMAL(10, 2),
    h DECIMAL(10, 2),
    l DECIMAL(10, 2),
    o DECIMAL(10, 2),
    yesterday_c DECIMAL(10, 2),
    turnover_rate DECIMAL(10, 2),
    pe_dynamic DECIMAL(10, 2),
    pb DECIMAL(10, 2),
    PRIMARY KEY (date, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_em_store;
CREATE TABLE ak_dg_stock_board_industry_cons_em_store (
    date DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    c DECIMAL(10, 2),
    pct_chg DECIMAL(10, 2),
    change DECIMAL(10, 2),
    v BIGINT,
    a DECIMAL(20, 2),
    amplitude DECIMAL(10, 2),
    h DECIMAL(10, 2),
    l DECIMAL(10, 2),
    o DECIMAL(10, 2),
    yesterday_c DECIMAL(10, 2),
    turnover_rate DECIMAL(10, 2),
    pe_dynamic DECIMAL(10, 2),
    pb DECIMAL(10, 2),
    PRIMARY KEY (date, s_code, b_name)
);