


CREATE TABLE dg_ak_index_zh_a_hist_daily (
    i_code VARCHAR(20) NOT NULL,   
    td DATE NOT NULL,            
    o FLOAT,                    
    c FLOAT,                    
    h FLOAT,                   
    l FLOAT,                    
    v BIGINT,                  
    a NUMERIC(18, 2),             
    amplitude FLOAT,            
    pct_chg FLOAT,               
    change FLOAT,              
    turnover_rate FLOAT,        
    PRIMARY KEY (i_code, td)     
);
SELECT create_hypertable('dg_ak_index_zh_a_hist_daily', 'td');




CREATE TABLE dg_ak_index_zh_a_code_name (
    i_code VARCHAR(20) NOT NULL PRIMARY KEY, 
    i_name VARCHAR(100) NOT NULL,           
    symbol VARCHAR(100),
    create_time TIMESTAMP,                   
    update_time TIMESTAMP                  
);