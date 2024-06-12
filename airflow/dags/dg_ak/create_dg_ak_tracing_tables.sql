


CREATE TABLE dg_ak_tracing_by_date (
    ak_func_name VARCHAR NOT NULL,
    last_td DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name)
);



CREATE TABLE dg_ak_tracing_by_date_1_param (
    ak_func_name VARCHAR NOT NULL,
    param_name VARCHAR NOT NULL,
    param_value VARCHAR NOT NULL,
    last_td DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, param_name, param_value)
);



CREATE TABLE dg_ak_tracing_by_scode_date (
    ak_func_name VARCHAR NOT NULL,
    scode VARCHAR NOT NULL,
    last_td DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, scode)
);



CREATE TABLE dg_ak_tracing_s_zh_a (
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



CREATE TABLE dg_ak_tracing_i_zh_a (
    ak_func_name VARCHAR NOT NULL,
    icode VARCHAR NOT NULL,
    period VARCHAR NOT NULL,
    last_td DATE,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, icode, period)
);

