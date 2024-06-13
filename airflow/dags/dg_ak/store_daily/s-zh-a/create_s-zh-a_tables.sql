





CREATE TABLE dg_ak_stock_zh_a_hist_daily_bfq (
    s_code VARCHAR(20) NOT NULL,    
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
    PRIMARY KEY (s_code, td)     
);
SELECT create_hypertable('dg_ak_stock_zh_a_hist_daily_bfq', 'td');





CREATE TABLE dg_ak_stock_zh_a_hist_daily_hfq (
    s_code VARCHAR(20) NOT NULL,    
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
    PRIMARY KEY (s_code, td)    
);
SELECT create_hypertable('dg_ak_stock_zh_a_hist_daily_hfq', 'td');



CREATE TABLE dg_ak_stock_zh_a_trade_date (
    trade_date DATE NOT NULL,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    PRIMARY KEY (trade_date)
);



CREATE TABLE dg_ak_stock_zh_a_code_name (
    s_code VARCHAR(20) NOT NULL PRIMARY KEY, 
    s_name VARCHAR(100) NOT NULL,            
    create_time TIMESTAMP,                  
    update_time TIMESTAMP                  
);
