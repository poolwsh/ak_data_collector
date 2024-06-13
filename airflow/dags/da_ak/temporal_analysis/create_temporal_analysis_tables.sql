

CREATE TABLE da_ak_stock_price_hl (
    s_code VARCHAR(20) NOT NULL,        -- Stock code
    td DATE NOT NULL,                   -- Trade date
    interval INT NOT NULL,              -- Time interval
    hs_h FLOAT,                         -- Historical high
    hs_l FLOAT,                         -- Historical low
    d_hs_h INT,                         -- Days since historical high
    d_hs_l INT,                         -- Days since historical low
    chg_from_hs FLOAT,                  -- Change from historical high/low
    pct_chg_from_hs FLOAT,              -- Percentage change from historical high/low
    tg_h FLOAT,                         -- Target high
    tg_l FLOAT,                         -- Target low
    d_tg_h INT,                         -- Days to target high
    d_tg_l INT,                         -- Days to target low
    chg_to_tg FLOAT,                    -- Change to target high/low
    pct_chg_to_tg FLOAT,                -- Percentage change to target high/low
    PRIMARY KEY (s_code, td, interval)  -- Composite primary key: stock code, trade date, and interval
);
SELECT create_hypertable('da_ak_stock_price_hl', 'td');

CREATE TABLE da_ak_stock_price_hl_store (
    s_code VARCHAR(20) NOT NULL,        -- Stock code
    td DATE NOT NULL,                   -- Trade date
    interval INT NOT NULL,              -- Time interval
    hs_h FLOAT,                         -- Historical high
    hs_l FLOAT,                         -- Historical low
    d_hs_h INT,                         -- Days since historical high
    d_hs_l INT,                         -- Days since historical low
    chg_from_hs FLOAT,                  -- Change from historical high/low
    pct_chg_from_hs FLOAT,              -- Percentage change from historical high/low
    tg_h FLOAT,                         -- Target high
    tg_l FLOAT,                         -- Target low
    d_tg_h INT,                         -- Days to target high
    d_tg_l INT,                         -- Days to target low
    chg_to_tg FLOAT,                    -- Change to target high/low
    pct_chg_to_tg FLOAT,                -- Percentage change to target high/low
    PRIMARY KEY (s_code, td, interval)  -- Composite primary key: stock code, trade date, and interval
);
SELECT create_hypertable('da_ak_stock_price_hl_store', 'td');
