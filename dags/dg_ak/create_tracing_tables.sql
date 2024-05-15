
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
