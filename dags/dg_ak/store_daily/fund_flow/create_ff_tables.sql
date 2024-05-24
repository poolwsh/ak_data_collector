DROP TABLE IF EXISTS ak_dg_stock_individual_fund_flow_store;
CREATE TABLE ak_dg_stock_individual_fund_flow_store (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    c DECIMAL,
    pct_chg DECIMAL,
    main_net_inflow DECIMAL,
    main_net_inflow_pct DECIMAL,
    huge_order_net_inflow DECIMAL,
    huge_order_net_inflow_pct DECIMAL,
    large_order_net_inflow DECIMAL,
    large_order_net_inflow_pct DECIMAL,
    medium_order_net_inflow DECIMAL,
    medium_order_net_inflow_pct DECIMAL,
    small_order_net_inflow DECIMAL,
    small_order_net_inflow_pct DECIMAL,
    PRIMARY KEY (td, s_code)
);

DROP TABLE IF EXISTS ak_dg_stock_individual_fund_flow_rank_store;
CREATE TABLE ak_dg_stock_individual_fund_flow_rank_store (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255),
    latest_price DECIMAL,
    today_pct_chg DECIMAL,
    today_main_net_inflow DECIMAL,
    today_main_net_inflow_pct DECIMAL,
    today_huge_order_net_inflow DECIMAL,
    today_huge_order_net_inflow_pct DECIMAL,
    today_large_order_net_inflow DECIMAL,
    today_large_order_net_inflow_pct DECIMAL,
    today_medium_order_net_inflow DECIMAL,
    today_medium_order_net_inflow_pct DECIMAL,
    today_small_order_net_inflow DECIMAL,
    today_small_order_net_inflow_pct DECIMAL,
    PRIMARY KEY (td, s_code)
);

DROP TABLE IF EXISTS ak_dg_stock_market_fund_flow_store;
CREATE TABLE ak_dg_stock_market_fund_flow_store (
    td DATE NOT NULL,
    shanghai_closing_price DECIMAL,
    shanghai_pct_chg DECIMAL,
    shenzhen_closing_price DECIMAL,
    shenzhen_pct_chg DECIMAL,
    main_net_inflow DECIMAL,
    main_net_inflow_pct DECIMAL,
    huge_order_net_inflow DECIMAL,
    huge_order_net_inflow_pct DECIMAL,
    large_order_net_inflow DECIMAL,
    large_order_net_inflow_pct DECIMAL,
    medium_order_net_inflow DECIMAL,
    medium_order_net_inflow_pct DECIMAL,
    small_order_net_inflow DECIMAL,
    small_order_net_inflow_pct DECIMAL,
    PRIMARY KEY (td)
);

DROP TABLE IF EXISTS ak_dg_stock_sector_fund_flow_rank_store;
CREATE TABLE ak_dg_stock_sector_fund_flow_rank_store (
    td DATE NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    sector_type VARCHAR(255),
    today_pct_chg DECIMAL,
    today_main_net_inflow DECIMAL,
    today_main_net_inflow_pct DECIMAL,
    today_huge_order_net_inflow DECIMAL,
    today_huge_order_net_inflow_pct DECIMAL,
    today_large_order_net_inflow DECIMAL,
    today_large_order_net_inflow_pct DECIMAL,
    today_medium_order_net_inflow DECIMAL,
    today_medium_order_net_inflow_pct DECIMAL,
    today_small_order_net_inflow DECIMAL,
    today_small_order_net_inflow_pct DECIMAL,
    today_main_net_inflow_max_stock VARCHAR(255),
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_main_fund_flow_store;
CREATE TABLE ak_dg_stock_main_fund_flow_store (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255),
    latest_price DECIMAL,
    today_main_net_inflow_pct DECIMAL,
    today_rank INT,
    today_pct_chg DECIMAL,
    main_net_inflow_pct_5day DECIMAL,
    rank_5day INT,
    pct_chg_5day DECIMAL,
    main_net_inflow_pct_10day DECIMAL,
    rank_10day INT,
    pct_chg_10day DECIMAL,
    sector VARCHAR(255),
    PRIMARY KEY (td, s_code)
);

DROP TABLE IF EXISTS ak_dg_stock_sector_fund_flow_summary_store;
CREATE TABLE ak_dg_stock_sector_fund_flow_summary_store (
    td DATE NOT NULL,
    s_code VARCHAR(50) NOT NULL,
    s_name VARCHAR(255),
    b_name VARCHAR(255),
    latest_price DECIMAL,
    today_pct_chg DECIMAL,
    today_main_net_inflow DECIMAL,
    today_main_net_inflow_pct DECIMAL,
    today_huge_order_net_inflow DECIMAL,
    today_huge_order_net_inflow_pct DECIMAL,
    today_large_order_net_inflow DECIMAL,
    today_large_order_net_inflow_pct DECIMAL,
    today_medium_order_net_inflow DECIMAL,
    today_medium_order_net_inflow_pct DECIMAL,
    today_small_order_net_inflow DECIMAL,
    today_small_order_net_inflow_pct DECIMAL,
    PRIMARY KEY (td, s_code)
);

DROP TABLE IF EXISTS ak_dg_stock_sector_fund_flow_hist_store;
CREATE TABLE ak_dg_stock_sector_fund_flow_hist_store (
    td DATE NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    main_net_inflow DECIMAL,
    main_net_inflow_pct DECIMAL,
    huge_order_net_inflow DECIMAL,
    huge_order_net_inflow_pct DECIMAL,
    large_order_net_inflow DECIMAL,
    large_order_net_inflow_pct DECIMAL,
    medium_order_net_inflow DECIMAL,
    medium_order_net_inflow_pct DECIMAL,
    small_order_net_inflow DECIMAL,
    small_order_net_inflow_pct DECIMAL,
    PRIMARY KEY (td, b_name)
);

DROP TABLE IF EXISTS ak_dg_stock_concept_fund_flow_hist_store;
CREATE TABLE ak_dg_stock_concept_fund_flow_hist_store (
    td DATE NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    main_net_inflow DECIMAL,
    main_net_inflow_pct DECIMAL,
    huge_order_net_inflow DECIMAL,
    huge_order_net_inflow_pct DECIMAL,
    large_order_net_inflow DECIMAL,
    large_order_net_inflow_pct DECIMAL,
    medium_order_net_inflow DECIMAL,
    medium_order_net_inflow_pct DECIMAL,
    small_order_net_inflow DECIMAL,
    small_order_net_inflow_pct DECIMAL,
    PRIMARY KEY (td, b_name)
);
