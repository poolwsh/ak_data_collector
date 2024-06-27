

CREATE TABLE da_ak_stock_price_hl (
    s_code VARCHAR(20) NOT NULL,        -- Stock code
    td DATE NOT NULL,                   -- Trade date
    interval INT NOT NULL,              -- Time interval
    c FLOAT,
    hs_h FLOAT,                         -- Historical high
    hs_l FLOAT,                         -- Historical low
    d_hs_h INT,                         -- Days since historical high
    d_hs_l INT,                         -- Days since historical low
    td_hs_h DATE,
    td_hs_l DATE,
    chg_hl_hs FLOAT,
    pct_chg_hl_hs FLOAT,
    tg_h FLOAT,                         -- Target high
    tg_l FLOAT,                         -- Target low
    d_tg_h INT,                         -- Days to target high
    d_tg_l INT,                         -- Days to target low
    td_tg_h DATE,
    td_tg_l DATE,
    chg_hl_tg FLOAT, 
    pct_chg_hl_tg FLOAT,
    PRIMARY KEY (s_code, td, interval)  -- Composite primary key: stock code, trade date, and interval
);
SELECT create_hypertable('da_ak_stock_price_hl', 'td');

