

CREATE TABLE da_ak_rs_daily (
    s_code VARCHAR(20) NOT NULL,  -- Stock code
    i_code VARCHAR(20) NOT NULL,  -- Index code
    td DATE NOT NULL,             -- Trading date
    rs FLOAT,                     -- Relative strength
    rs_rank INTEGER,              -- Rank of relative strength
    PRIMARY KEY (s_code, i_code, td) -- Composite primary key
);
SELECT create_hypertable('da_ak_rs_daily', 'td');


