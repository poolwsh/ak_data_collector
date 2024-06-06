
DROP TABLE IF EXISTS fy_dg_tracing_s_zh_a;
CREATE TABLE fy_dg_tracing_s_zh_a (
    ak_func_name VARCHAR NOT NULL,
    s_code VARCHAR NOT NULL,
    period VARCHAR NOT NULL,
    adjust VARCHAR NOT NULL,
    last_td DATE,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    host_name VARCHAR,
    PRIMARY KEY (ak_func_name, scode, period, adjust)
);
