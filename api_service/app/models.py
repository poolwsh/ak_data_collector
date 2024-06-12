from sqlalchemy import Column, Integer, Float, String, Date, DECIMAL, BigInteger, Numeric, TIMESTAMP, DateTime
from sqlalchemy.orm import declarative_base
from utils.db import db

Base = declarative_base()

class Da_ak_tracing_stock_price_hl(Base):
    __tablename__ = 'da_ak_tracing_stock_price_hl'
    s_code = Column(String, primary_key=True)
    min_td = Column(Date)
    max_td = Column(Date)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    host_name = Column(String)

class Da_ak_stock_price_hl(Base):
    __tablename__ = 'da_ak_stock_price_hl'
    s_code = Column(String, primary_key=True)
    td = Column(Date, primary_key=True)
    interval = Column(Integer, primary_key=True)
    hs_h = Column(Float)
    hs_l = Column(Float)
    d_hs_h = Column(Integer)
    d_hs_l = Column(Integer)
    chg_from_hs = Column(Float)
    pct_chg_from_hs = Column(Float)
    tg_h = Column(Float)
    tg_l = Column(Float)
    d_tg_h = Column(Integer)
    d_tg_l = Column(Integer)
    chg_to_tg = Column(Float)
    pct_chg_to_tg = Column(Float)

class Da_ak_stock_price_hl_store(Base):
    __tablename__ = 'da_ak_stock_price_hl_store'
    s_code = Column(String, primary_key=True)
    td = Column(Date, primary_key=True)
    interval = Column(Integer, primary_key=True)
    hs_h = Column(Float)
    hs_l = Column(Float)
    d_hs_h = Column(Integer)
    d_hs_l = Column(Integer)
    chg_from_hs = Column(Float)
    pct_chg_from_hs = Column(Float)
    tg_h = Column(Float)
    tg_l = Column(Float)
    d_tg_h = Column(Integer)
    d_tg_l = Column(Integer)
    chg_to_tg = Column(Float)
    pct_chg_to_tg = Column(Float)

class Dg_ak_tracing_by_date(Base):
    __tablename__ = 'dg_ak_tracing_by_date'
    ak_func_name = Column(String, primary_key=True)
    last_td = Column(Date)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    host_name = Column(String)

class Dg_ak_tracing_by_date_1_param(Base):
    __tablename__ = 'dg_ak_tracing_by_date_1_param'
    ak_func_name = Column(String, primary_key=True)
    param_name = Column(String, primary_key=True)
    param_value = Column(String, primary_key=True)
    last_td = Column(Date)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    host_name = Column(String)

class Dg_ak_tracing_by_scode_date(Base):
    __tablename__ = 'dg_ak_tracing_by_scode_date'
    ak_func_name = Column(String, primary_key=True)
    scode = Column(String, primary_key=True)
    last_td = Column(Date)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    host_name = Column(String)

class Dg_ak_tracing_s_zh_a(Base):
    __tablename__ = 'dg_ak_tracing_s_zh_a'
    ak_func_name = Column(String, primary_key=True)
    scode = Column(String, primary_key=True)
    period = Column(String, primary_key=True)
    adjust = Column(String, primary_key=True)
    last_td = Column(Date)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    host_name = Column(String)

class Dg_ak_tracing_i_zh_a(Base):
    __tablename__ = 'dg_ak_tracing_i_zh_a'
    ak_func_name = Column(String, primary_key=True)
    icode = Column(String, primary_key=True)
    period = Column(String, primary_key=True)
    last_td = Column(Date)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    host_name = Column(String)

class Dg_ak_stock_board_concept_name_ths(Base):
    __tablename__ = 'dg_ak_stock_board_concept_name_ths'
    td = Column(Date, primary_key=True)
    b_code = Column(String)
    b_name = Column(String, primary_key=True)
    stock_count = Column(Integer)
    url = Column(String)

class Dg_ak_stock_board_concept_name_ths_store(Base):
    __tablename__ = 'dg_ak_stock_board_concept_name_ths_store'
    td = Column(Date, primary_key=True)
    b_code = Column(String)
    b_name = Column(String, primary_key=True)
    stock_count = Column(Integer)
    url = Column(String)

class Dg_ak_stock_board_concept_name_em(Base):
    __tablename__ = 'dg_ak_stock_board_concept_name_em'
    td = Column(Date, primary_key=True)
    rank = Column(Integer)
    b_code = Column(String)
    b_name = Column(String, primary_key=True)
    c = Column(DECIMAL)
    change_amount = Column(DECIMAL)
    change_percentage = Column(DECIMAL)
    total_mv = Column(BigInteger)
    turnover_rate = Column(DECIMAL)
    risers = Column(Integer)
    fallers = Column(Integer)
    leader_s = Column(String)
    leader_s_change = Column(DECIMAL)

class Dg_ak_stock_board_concept_name_em_store(Base):
    __tablename__ = 'dg_ak_stock_board_concept_name_em_store'
    td = Column(Date, primary_key=True)
    rank = Column(Integer)
    b_code = Column(String)
    b_name = Column(String, primary_key=True)
    c = Column(DECIMAL)
    change_amount = Column(DECIMAL)
    change_percentage = Column(DECIMAL)
    total_mv = Column(BigInteger)
    turnover_rate = Column(DECIMAL)
    risers = Column(Integer)
    fallers = Column(Integer)
    leader_s = Column(String)
    leader_s_change = Column(DECIMAL)

class Dg_ak_stock_board_industry_summary_ths(Base):
    __tablename__ = 'dg_ak_stock_board_industry_summary_ths'
    td = Column(Date, primary_key=True)
    b_name = Column(String, primary_key=True)
    change_percentage = Column(DECIMAL)
    v = Column(DECIMAL)
    a = Column(DECIMAL)
    net_inflow = Column(DECIMAL)
    risers = Column(Integer)
    fallers = Column(Integer)
    average_price = Column(DECIMAL)
    leader_s = Column(String)
    leader_s_price = Column(DECIMAL)
    leader_s_change = Column(DECIMAL)

class Dg_ak_stock_board_industry_summary_ths_store(Base):
    __tablename__ = 'dg_ak_stock_board_industry_summary_ths_store'
    td = Column(Date, primary_key=True)
    b_name = Column(String, primary_key=True)
    change_percentage = Column(DECIMAL)
    v = Column(DECIMAL)
    a = Column(DECIMAL)
    net_inflow = Column(DECIMAL)
    risers = Column(Integer)
    fallers = Column(Integer)
    average_price = Column(DECIMAL)
    leader_s = Column(String)
    leader_s_price = Column(DECIMAL)
    leader_s_change = Column(DECIMAL)

class Dg_ak_stock_board_industry_name_em(Base):
    __tablename__ = 'dg_ak_stock_board_industry_name_em'
    td = Column(Date, primary_key=True)
    rank = Column(Integer)
    b_code = Column(String)
    b_name = Column(String, primary_key=True)
    c = Column(DECIMAL)
    change_amount = Column(DECIMAL)
    change_percentage = Column(DECIMAL)
    total_mv = Column(BigInteger)
    turnover_rate = Column(DECIMAL)
    risers = Column(Integer)
    fallers = Column(Integer)
    leader_s = Column(String)
    leader_s_change = Column(DECIMAL)

class Dg_ak_stock_board_industry_name_em_store(Base):
    __tablename__ = 'dg_ak_stock_board_industry_name_em_store'
    td = Column(Date, primary_key=True)
    rank = Column(Integer)
    b_code = Column(String)
    b_name = Column(String, primary_key=True)
    c = Column(DECIMAL)
    change_amount = Column(DECIMAL)
    change_percentage = Column(DECIMAL)
    total_mv = Column(BigInteger)
    turnover_rate = Column(DECIMAL)
    risers = Column(Integer)
    fallers = Column(Integer)
    leader_s = Column(String)
    leader_s_change = Column(DECIMAL)

class Dg_ak_stock_board_concept_cons_ths(Base):
    __tablename__ = 'dg_ak_stock_board_concept_cons_ths'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)

class Dg_ak_stock_board_concept_cons_ths_store(Base):
    __tablename__ = 'dg_ak_stock_board_concept_cons_ths_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)

class Dg_ak_stock_board_concept_cons_em(Base):
    __tablename__ = 'dg_ak_stock_board_concept_cons_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)

class Dg_ak_stock_board_concept_cons_em_store(Base):
    __tablename__ = 'dg_ak_stock_board_concept_cons_em_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)

class Dg_ak_stock_board_industry_cons_ths(Base):
    __tablename__ = 'dg_ak_stock_board_industry_cons_ths'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)
    a = Column(String)
    circulating_shares = Column(String)

class Dg_ak_stock_board_industry_cons_ths_store(Base):
    __tablename__ = 'dg_ak_stock_board_industry_cons_ths_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)
    a = Column(String)
    circulating_shares = Column(String)

class Dg_ak_stock_board_industry_cons_em(Base):
    __tablename__ = 'dg_ak_stock_board_industry_cons_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)
    v = Column(BigInteger)

class Dg_ak_stock_board_industry_cons_em_store(Base):
    __tablename__ = 'dg_ak_stock_board_industry_cons_em_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String, primary_key=True)
    v = Column(BigInteger)

class Dg_ak_stock_individual_fund_flow_store(Base):
    __tablename__ = 'dg_ak_stock_individual_fund_flow_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    c = Column(DECIMAL)
    pct_chg = Column(DECIMAL)
    main_net_inflow = Column(DECIMAL)
    main_net_inflow_pct = Column(DECIMAL)
    huge_order_net_inflow = Column(DECIMAL)
    huge_order_net_inflow_pct = Column(DECIMAL)
    large_order_net_inflow = Column(DECIMAL)
    large_order_net_inflow_pct = Column(DECIMAL)
    medium_order_net_inflow = Column(DECIMAL)
    medium_order_net_inflow_pct = Column(DECIMAL)
    small_order_net_inflow = Column(DECIMAL)
    small_order_net_inflow_pct = Column(DECIMAL)

class Dg_ak_stock_individual_fund_flow_rank_store(Base):
    __tablename__ = 'dg_ak_stock_individual_fund_flow_rank_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    latest_price = Column(DECIMAL)
    today_pct_chg = Column(DECIMAL)
    today_main_net_inflow = Column(DECIMAL)
    today_main_net_inflow_pct = Column(DECIMAL)
    today_huge_order_net_inflow = Column(DECIMAL)
    today_huge_order_net_inflow_pct = Column(DECIMAL)
    today_large_order_net_inflow = Column(DECIMAL)
    today_large_order_net_inflow_pct = Column(DECIMAL)
    today_medium_order_net_inflow = Column(DECIMAL)
    today_medium_order_net_inflow_pct = Column(DECIMAL)
    today_small_order_net_inflow = Column(DECIMAL)
    today_small_order_net_inflow_pct = Column(DECIMAL)

class Dg_ak_stock_market_fund_flow_store(Base):
    __tablename__ = 'dg_ak_stock_market_fund_flow_store'
    td = Column(Date, primary_key=True)
    shanghai_closing_price = Column(DECIMAL)
    shanghai_pct_chg = Column(DECIMAL)
    shenzhen_closing_price = Column(DECIMAL)
    shenzhen_pct_chg = Column(DECIMAL)
    main_net_inflow = Column(DECIMAL)
    main_net_inflow_pct = Column(DECIMAL)
    huge_order_net_inflow = Column(DECIMAL)
    huge_order_net_inflow_pct = Column(DECIMAL)
    large_order_net_inflow = Column(DECIMAL)
    large_order_net_inflow_pct = Column(DECIMAL)
    medium_order_net_inflow = Column(DECIMAL)
    medium_order_net_inflow_pct = Column(DECIMAL)
    small_order_net_inflow = Column(DECIMAL)
    small_order_net_inflow_pct = Column(DECIMAL)

class Dg_ak_stock_sector_fund_flow_rank_store(Base):
    __tablename__ = 'dg_ak_stock_sector_fund_flow_rank_store'
    td = Column(Date, primary_key=True)
    b_name = Column(String, primary_key=True)
    sector_type = Column(String)
    today_pct_chg = Column(DECIMAL)
    today_main_net_inflow = Column(DECIMAL)
    today_main_net_inflow_pct = Column(DECIMAL)
    today_huge_order_net_inflow = Column(DECIMAL)
    today_huge_order_net_inflow_pct = Column(DECIMAL)
    today_large_order_net_inflow = Column(DECIMAL)
    today_large_order_net_inflow_pct = Column(DECIMAL)
    today_medium_order_net_inflow = Column(DECIMAL)
    today_medium_order_net_inflow_pct = Column(DECIMAL)
    today_small_order_net_inflow = Column(DECIMAL)
    today_small_order_net_inflow_pct = Column(DECIMAL)
    today_main_net_inflow_max_stock = Column(String)

class Dg_ak_stock_main_fund_flow_store(Base):
    __tablename__ = 'dg_ak_stock_main_fund_flow_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    latest_price = Column(DECIMAL)
    today_main_net_inflow_pct = Column(DECIMAL)
    today_rank = Column(Integer)
    today_pct_chg = Column(DECIMAL)
    main_net_inflow_pct_5day = Column(DECIMAL)
    rank_5day = Column(Integer)
    pct_chg_5day = Column(DECIMAL)
    main_net_inflow_pct_10day = Column(DECIMAL)
    rank_10day = Column(Integer)
    pct_chg_10day = Column(DECIMAL)
    sector = Column(String)

class Dg_ak_stock_sector_fund_flow_summary_store(Base):
    __tablename__ = 'dg_ak_stock_sector_fund_flow_summary_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    b_name = Column(String)
    latest_price = Column(DECIMAL)
    today_pct_chg = Column(DECIMAL)
    today_main_net_inflow = Column(DECIMAL)
    today_main_net_inflow_pct = Column(DECIMAL)
    today_huge_order_net_inflow = Column(DECIMAL)
    today_huge_order_net_inflow_pct = Column(DECIMAL)
    today_large_order_net_inflow = Column(DECIMAL)
    today_large_order_net_inflow_pct = Column(DECIMAL)
    today_medium_order_net_inflow = Column(DECIMAL)
    today_medium_order_net_inflow_pct = Column(DECIMAL)
    today_small_order_net_inflow = Column(DECIMAL)
    today_small_order_net_inflow_pct = Column(DECIMAL)

class Dg_ak_stock_sector_fund_flow_hist_store(Base):
    __tablename__ = 'dg_ak_stock_sector_fund_flow_hist_store'
    td = Column(Date, primary_key=True)
    b_name = Column(String, primary_key=True)
    main_net_inflow = Column(DECIMAL)
    main_net_inflow_pct = Column(DECIMAL)
    huge_order_net_inflow = Column(DECIMAL)
    huge_order_net_inflow_pct = Column(DECIMAL)
    large_order_net_inflow = Column(DECIMAL)
    large_order_net_inflow_pct = Column(DECIMAL)
    medium_order_net_inflow = Column(DECIMAL)
    medium_order_net_inflow_pct = Column(DECIMAL)
    small_order_net_inflow = Column(DECIMAL)
    small_order_net_inflow_pct = Column(DECIMAL)

class Dg_ak_stock_concept_fund_flow_hist_store(Base):
    __tablename__ = 'dg_ak_stock_concept_fund_flow_hist_store'
    td = Column(Date, primary_key=True)
    b_name = Column(String, primary_key=True)
    main_net_inflow = Column(DECIMAL)
    main_net_inflow_pct = Column(DECIMAL)
    huge_order_net_inflow = Column(DECIMAL)
    huge_order_net_inflow_pct = Column(DECIMAL)
    large_order_net_inflow = Column(DECIMAL)
    large_order_net_inflow_pct = Column(DECIMAL)
    medium_order_net_inflow = Column(DECIMAL)
    medium_order_net_inflow_pct = Column(DECIMAL)
    small_order_net_inflow = Column(DECIMAL)
    small_order_net_inflow_pct = Column(DECIMAL)

class Dg_ak_stock_hold_num_cninfo(Base):
    __tablename__ = 'dg_ak_stock_hold_num_cninfo'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    cur_sh = Column(Integer)
    pre_sh = Column(Integer)
    num_of_sh_chg_pct = Column(Float)
    cur_avg_holdings = Column(Float)
    pre_avg_holdings = Column(Float)
    avg_holdings_chg_pct = Column(Float)

class Dg_ak_stock_hold_num_cninfo_store(Base):
    __tablename__ = 'dg_ak_stock_hold_num_cninfo_store'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    cur_sh = Column(Integer)
    pre_sh = Column(Integer)
    num_of_sh_chg_pct = Column(Float)
    cur_avg_holdings = Column(Float)
    pre_avg_holdings = Column(Float)
    avg_holdings_chg_pct = Column(Float)

class Dg_ak_index_zh_a_hist_daily(Base):
    __tablename__ = 'dg_ak_index_zh_a_hist_daily'
    i_code = Column(String, primary_key=True)
    td = Column(Date, primary_key=True)
    o = Column(Float)
    c = Column(Float)
    h = Column(Float)
    l = Column(Float)
    v = Column(BigInteger)
    amplitude = Column(Float)
    pct_chg = Column(Float)
    change = Column(Float)
    turnover_rate = Column(Float)

class Dg_ak_stock_zh_a_hist_daily_bfq(Base):
    __tablename__ = 'dg_ak_stock_zh_a_hist_daily_bfq'
    s_code = Column(String, primary_key=True)
    td = Column(Date, primary_key=True)
    o = Column(Float)
    c = Column(Float)
    h = Column(Float)
    l = Column(Float)
    v = Column(BigInteger)
    amplitude = Column(Float)
    pct_chg = Column(Float)
    change = Column(Float)
    turnover_rate = Column(Float)

class Dg_ak_stock_zh_a_hist_daily_hfq(Base):
    __tablename__ = 'dg_ak_stock_zh_a_hist_daily_hfq'
    s_code = Column(String, primary_key=True)
    td = Column(Date, primary_key=True)
    o = Column(Float)
    c = Column(Float)
    h = Column(Float)
    l = Column(Float)
    v = Column(BigInteger)
    amplitude = Column(Float)
    pct_chg = Column(Float)
    change = Column(Float)
    turnover_rate = Column(Float)

class Dg_ak_stock_zh_a_trade_date(Base):
    __tablename__ = 'dg_ak_stock_zh_a_trade_date'
    trade_date = Column(Date, primary_key=True)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)

class Dg_ak_stock_zt_pool_dtgc_em(Base):
    __tablename__ = 'dg_ak_stock_zt_pool_dtgc_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    pct_chg = Column(Float)
    c = Column(Float)
    a = Column(Float)
    dynamic_pe = Column(Float)
    turnover_rate = Column(Float)
    lock_fund = Column(BigInteger)
    board_amount = Column(BigInteger)
    continuous_dt = Column(Integer)
    open_account = Column(Integer)
    industry = Column(String)

class Dg_ak_stock_zt_pool_em(Base):
    __tablename__ = 'dg_ak_stock_zt_pool_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    pct_chg = Column(Float)
    c = Column(Float)
    a = Column(Float)
    turnover_rate = Column(Float)
    lock_fund = Column(BigInteger)
    failed_account = Column(Integer)
    zt_stat = Column(String)
    zt_account = Column(Integer)
    industry = Column(String)

class Dg_ak_stock_zt_pool_previous_em(Base):
    __tablename__ = 'dg_ak_stock_zt_pool_previous_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    pct_chg = Column(Float)
    c = Column(Float)
    zt_price = Column(Float)
    a = Column(Float)
    circulation_mv = Column(Numeric)
    total_mv = Column(Numeric)
    turnover_rate = Column(Float)
    increase_speed = Column(Float)
    amplitude = Column(Float)
    yesterday_zt_account = Column(Integer)
    zt_stat = Column(String)
    industry = Column(String)

class Dg_ak_stock_zt_pool_strong_em(Base):
    __tablename__ = 'dg_ak_stock_zt_pool_strong_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    pct_chg = Column(Float)
    c = Column(Float)
    zt_price = Column(Float)
    a = Column(Float)
    circulation_mv = Column(Numeric)
    total_mv = Column(Numeric)
    turnover_rate = Column(Float)
    increase_speed = Column(Float)
    quantity_ratio = Column(Float)
    zt_stat = Column(String)
    selection_reason = Column(String)
    industry = Column(String)

class Dg_ak_stock_zt_pool_sub_new_em(Base):
    __tablename__ = 'dg_ak_stock_zt_pool_sub_new_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    pct_chg = Column(Float)
    c = Column(Float)
    zt_price = Column(Float)
    a = Column(Float)
    circulation_mv = Column(Numeric)
    total_mv = Column(Numeric)
    turnover_rate = Column(Float)
    days_since_open = Column(Integer)
    open_date = Column(Date)
    ipo_date = Column(Date)
    zt_stat = Column(String)
    industry = Column(String)

class Dg_ak_stock_zt_pool_zbgc_em(Base):
    __tablename__ = 'dg_ak_stock_zt_pool_zbgc_em'
    td = Column(Date, primary_key=True)
    s_code = Column(String, primary_key=True)
    s_name = Column(String)
    pct_chg = Column(Float)
    c = Column(Float)
    zt_price = Column(Float)
    a = Column(Float)
    circulation_mv = Column(Numeric)
    total_mv = Column(Numeric)
    turnover_rate = Column(Float)
    increase_speed = Column(Float)
    failed_account = Column(Integer)
    zt_stat = Column(String)
    amplitude = Column(Float)
    industry = Column(String)

class Dg_fy_tracing_s_us(Base):
    __tablename__ = 'dg_fy_tracing_s_us'
    symbol = Column(String, primary_key=True)
    last_td = Column(Date)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    host_name = Column(String)

class Dg_fy_stock_us_trade_date(Base):
    __tablename__ = 'dg_fy_stock_us_trade_date'
    trade_date = Column(Date, primary_key=True)
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)

class Dg_fy_stock_us_hist_daily_bfq(Base):
    __tablename__ = 'dg_fy_stock_us_hist_daily_bfq'
    symbol = Column(String, primary_key=True)
    td = Column(Date, primary_key=True)
    o = Column(Float)
    c = Column(Float)
    h = Column(Float)
    l = Column(Float)
    v = Column(BigInteger)
    adj_close = Column(Float)
    dividends = Column(Float)
    stock_splits = Column(Float)

Base.metadata.create_all(bind=db.engine)
