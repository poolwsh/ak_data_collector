DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_ths;
CREATE TABLE ak_dg_stock_board_concept_name_ths (
    td DATE NOT NULL,
    b_code VARCHAR(50),
    b_name VARCHAR(255) NOT NULL,
    stock_count INT,
    url VARCHAR(255),
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_ths_store;
CREATE TABLE ak_dg_stock_board_concept_name_ths_store (
    td DATE NOT NULL,
    b_code VARCHAR(50),
    b_name VARCHAR(255) NOT NULL,
    stock_count INT,
    url VARCHAR(255),
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_em;
CREATE TABLE ak_dg_stock_board_concept_name_em (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_name_em_store;
CREATE TABLE ak_dg_stock_board_concept_name_em_store (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_summary_ths;
CREATE TABLE ak_dg_stock_board_industry_summary_ths (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_summary_ths_store;
CREATE TABLE ak_dg_stock_board_industry_summary_ths_store (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_name_em;
CREATE TABLE ak_dg_stock_board_industry_name_em (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_name_em_store;
CREATE TABLE ak_dg_stock_board_industry_name_em_store (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_ths;
CREATE TABLE ak_dg_stock_board_concept_cons_ths (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (td, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_ths_store;
CREATE TABLE ak_dg_stock_board_concept_cons_ths_store (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (td, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_em;
CREATE TABLE ak_dg_stock_board_concept_cons_em (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (td, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_concept_cons_em_store;
CREATE TABLE ak_dg_stock_board_concept_cons_em_store (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255) NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (td, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_ths;
CREATE TABLE ak_dg_stock_board_industry_cons_ths (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_ths_store;
CREATE TABLE ak_dg_stock_board_industry_cons_ths_store (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_em;
CREATE TABLE ak_dg_stock_board_industry_cons_em (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, s_code, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_board_industry_cons_em_store;
CREATE TABLE ak_dg_stock_board_industry_cons_em_store (
    td DATE NOT NULL,
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
    PRIMARY KEY (td, s_code, b_name)
);
