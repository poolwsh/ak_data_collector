

CREATE TABLE da_ak_tracing_stock_price_peak (
    s_code VARCHAR NOT NULL,          -- Stock code
    min_td DATE NOT NULL,             -- Calculated minimum trade date
    max_td DATE NOT NULL,             -- Calculated maximum trade date
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Record creation time
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Record update time
    host_name VARCHAR,                -- Host name
    PRIMARY KEY (s_code)              -- Primary key: stock code
);