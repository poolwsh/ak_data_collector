
-- create dg ak tracing tables
DROP TABLE IF EXISTS ak_dg_tracing_by_date;
CREATE TABLE ak_dg_tracing_by_date (
    ak_func_name VARCHAR NOT NULL,
    last_td DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name)
);

DROP TABLE IF EXISTS ak_dg_tracing_by_date_1_param;
CREATE TABLE ak_dg_tracing_by_date_1_param (
    ak_func_name VARCHAR NOT NULL,
    param_name VARCHAR NOT NULL,
    param_value VARCHAR NOT NULL,
    last_td DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, param_name, param_value)
);

DROP TABLE IF EXISTS ak_dg_tracing_by_scode_date;
CREATE TABLE ak_dg_tracing_by_scode_date (
    ak_func_name VARCHAR NOT NULL,
    scode VARCHAR NOT NULL,
    last_td DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, scode)
);

DROP TABLE IF EXISTS ak_dg_tracing_s_zh_a;
CREATE TABLE ak_dg_tracing_s_zh_a (
    ak_func_name VARCHAR NOT NULL,
    scode VARCHAR NOT NULL,
    period VARCHAR NOT NULL,
    adjust VARCHAR NOT NULL,
    last_td DATE,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, scode, period, adjust)
);

