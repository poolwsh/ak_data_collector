

CREATE TABLE da_ak_rs_daily (
    s_code VARCHAR(20) NOT NULL,  -- Stock code
    i_code VARCHAR(20) NOT NULL,  -- Index code
    td DATE NOT NULL,             -- Trading date
    rs FLOAT,                     -- Relative strength
    rs_rank INTEGER,              -- Rank of relative strength
    PRIMARY KEY (s_code, i_code, td) -- Composite primary key
);
SELECT create_hypertable('da_ak_rs_daily', 'td');

CREATE TABLE da_ak_board_a_industry_em_daily (
    td DATE NOT NULL,
    b_name VARCHAR(255) NOT NULL,
    a FLOAT,
    a_pre FLOAT,
    PRIMARY KEY (td, b_name)
);
SELECT create_hypertable('da_ak_board_a_industry_em_daily', 'td');


