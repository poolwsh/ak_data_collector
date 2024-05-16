
-- create dg ak tracing tables
DROP TABLE IF EXISTS dg_ak_tracing_date;
CREATE TABLE dg_ak_tracing_date (
    ak_func_name VARCHAR NOT NULL,
    date DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, date)
);

DROP TABLE IF EXISTS dg_ak_tracing_date_1_param;
CREATE TABLE dg_ak_tracing_date_1_param (
    ak_func_name VARCHAR NOT NULL,
    param_name VARCHAR NOT NULL,
    param_value VARCHAR NOT NULL,
    date DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, param_name, param_value, date)
);

DROP TABLE IF EXISTS dg_ak_tracing_scode_date;
CREATE TABLE dg_ak_tracing_scode_date (
    ak_func_name VARCHAR NOT NULL,
    scode VARCHAR NOT NULL,
    date DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, scode, date)
);


