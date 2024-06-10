from pydantic import BaseModel
from datetime import date, datetime
from decimal import Decimal

class Da_ak_tracing_stock_price_hlBase(BaseModel):
    s_code: str
    min_td: date
    max_td: date
    create_time: datetime
    update_time: datetime
    host_name: str

class Da_ak_tracing_stock_price_hlCreate(Da_ak_tracing_stock_price_hlBase):
    pass

class Da_ak_tracing_stock_price_hl(Da_ak_tracing_stock_price_hlBase):
    class Config:
        orm_mode = True

class Da_ak_stock_price_hlBase(BaseModel):
    s_code: str
    td: date
    interval: int
    hs_h: float
    hs_l: float
    d_hs_h: int
    d_hs_l: int
    chg_from_hs: float
    pct_chg_from_hs: float
    tg_h: float
    tg_l: float
    d_tg_h: int
    d_tg_l: int
    chg_to_tg: float
    pct_chg_to_tg: float

class Da_ak_stock_price_hlCreate(Da_ak_stock_price_hlBase):
    pass

class Da_ak_stock_price_hl(Da_ak_stock_price_hlBase):
    class Config:
        orm_mode = True

class Da_ak_stock_price_hl_storeBase(BaseModel):
    s_code: str
    td: date
    interval: int
    hs_h: float
    hs_l: float
    d_hs_h: int
    d_hs_l: int
    chg_from_hs: float
    pct_chg_from_hs: float
    tg_h: float
    tg_l: float
    d_tg_h: int
    d_tg_l: int
    chg_to_tg: float
    pct_chg_to_tg: float

class Da_ak_stock_price_hl_storeCreate(Da_ak_stock_price_hl_storeBase):
    pass

class Da_ak_stock_price_hl_store(Da_ak_stock_price_hl_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_tracing_by_dateBase(BaseModel):
    ak_func_name: str
    last_td: date
    create_time: datetime
    update_time: datetime
    host_name: str

class Dg_ak_tracing_by_dateCreate(Dg_ak_tracing_by_dateBase):
    pass

class Dg_ak_tracing_by_date(Dg_ak_tracing_by_dateBase):
    class Config:
        orm_mode = True

class Dg_ak_tracing_by_date_1_paramBase(BaseModel):
    ak_func_name: str
    param_name: str
    param_value: str
    last_td: date
    create_time: datetime
    update_time: datetime
    host_name: str

class Dg_ak_tracing_by_date_1_paramCreate(Dg_ak_tracing_by_date_1_paramBase):
    pass

class Dg_ak_tracing_by_date_1_param(Dg_ak_tracing_by_date_1_paramBase):
    class Config:
        orm_mode = True

class Dg_ak_tracing_by_scode_dateBase(BaseModel):
    ak_func_name: str
    scode: str
    last_td: date
    create_time: datetime
    update_time: datetime
    host_name: str

class Dg_ak_tracing_by_scode_dateCreate(Dg_ak_tracing_by_scode_dateBase):
    pass

class Dg_ak_tracing_by_scode_date(Dg_ak_tracing_by_scode_dateBase):
    class Config:
        orm_mode = True

class Dg_ak_tracing_s_zh_aBase(BaseModel):
    ak_func_name: str
    scode: str
    period: str
    adjust: str
    last_td: date
    create_time: datetime
    update_time: datetime
    host_name: str

class Dg_ak_tracing_s_zh_aCreate(Dg_ak_tracing_s_zh_aBase):
    pass

class Dg_ak_tracing_s_zh_a(Dg_ak_tracing_s_zh_aBase):
    class Config:
        orm_mode = True

class Dg_ak_tracing_i_zh_aBase(BaseModel):
    ak_func_name: str
    icode: str
    period: str
    last_td: date
    create_time: datetime
    update_time: datetime
    host_name: str

class Dg_ak_tracing_i_zh_aCreate(Dg_ak_tracing_i_zh_aBase):
    pass

class Dg_ak_tracing_i_zh_a(Dg_ak_tracing_i_zh_aBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_name_thsBase(BaseModel):
    td: date
    b_code: str
    b_name: str
    stock_count: int
    url: str

class Dg_ak_stock_board_concept_name_thsCreate(Dg_ak_stock_board_concept_name_thsBase):
    pass

class Dg_ak_stock_board_concept_name_ths(Dg_ak_stock_board_concept_name_thsBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_name_ths_storeBase(BaseModel):
    td: date
    b_code: str
    b_name: str
    stock_count: int
    url: str

class Dg_ak_stock_board_concept_name_ths_storeCreate(Dg_ak_stock_board_concept_name_ths_storeBase):
    pass

class Dg_ak_stock_board_concept_name_ths_store(Dg_ak_stock_board_concept_name_ths_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_name_emBase(BaseModel):
    td: date
    rank: int
    b_code: str
    b_name: str
    c: Decimal
    change_amount: Decimal
    change_percentage: Decimal
    total_mv: int
    turnover_rate: Decimal
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: Decimal

class Dg_ak_stock_board_concept_name_emCreate(Dg_ak_stock_board_concept_name_emBase):
    pass

class Dg_ak_stock_board_concept_name_em(Dg_ak_stock_board_concept_name_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_name_em_storeBase(BaseModel):
    td: date
    rank: int
    b_code: str
    b_name: str
    c: Decimal
    change_amount: Decimal
    change_percentage: Decimal
    total_mv: int
    turnover_rate: Decimal
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: Decimal

class Dg_ak_stock_board_concept_name_em_storeCreate(Dg_ak_stock_board_concept_name_em_storeBase):
    pass

class Dg_ak_stock_board_concept_name_em_store(Dg_ak_stock_board_concept_name_em_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_summary_thsBase(BaseModel):
    td: date
    b_name: str
    change_percentage: Decimal
    v: Decimal
    a: Decimal
    net_inflow: Decimal
    risers: int
    fallers: int
    average_price: Decimal
    leader_s: str
    leader_s_price: Decimal
    leader_s_change: Decimal

class Dg_ak_stock_board_industry_summary_thsCreate(Dg_ak_stock_board_industry_summary_thsBase):
    pass

class Dg_ak_stock_board_industry_summary_ths(Dg_ak_stock_board_industry_summary_thsBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_summary_ths_storeBase(BaseModel):
    td: date
    b_name: str
    change_percentage: Decimal
    v: Decimal
    a: Decimal
    net_inflow: Decimal
    risers: int
    fallers: int
    average_price: Decimal
    leader_s: str
    leader_s_price: Decimal
    leader_s_change: Decimal

class Dg_ak_stock_board_industry_summary_ths_storeCreate(Dg_ak_stock_board_industry_summary_ths_storeBase):
    pass

class Dg_ak_stock_board_industry_summary_ths_store(Dg_ak_stock_board_industry_summary_ths_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_name_emBase(BaseModel):
    td: date
    rank: int
    b_code: str
    b_name: str
    c: Decimal
    change_amount: Decimal
    change_percentage: Decimal
    total_mv: int
    turnover_rate: Decimal
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: Decimal

class Dg_ak_stock_board_industry_name_emCreate(Dg_ak_stock_board_industry_name_emBase):
    pass

class Dg_ak_stock_board_industry_name_em(Dg_ak_stock_board_industry_name_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_name_em_storeBase(BaseModel):
    td: date
    rank: int
    b_code: str
    b_name: str
    c: Decimal
    change_amount: Decimal
    change_percentage: Decimal
    total_mv: int
    turnover_rate: Decimal
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: Decimal

class Dg_ak_stock_board_industry_name_em_storeCreate(Dg_ak_stock_board_industry_name_em_storeBase):
    pass

class Dg_ak_stock_board_industry_name_em_store(Dg_ak_stock_board_industry_name_em_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_cons_thsBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_thsCreate(Dg_ak_stock_board_concept_cons_thsBase):
    pass

class Dg_ak_stock_board_concept_cons_ths(Dg_ak_stock_board_concept_cons_thsBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_cons_ths_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_ths_storeCreate(Dg_ak_stock_board_concept_cons_ths_storeBase):
    pass

class Dg_ak_stock_board_concept_cons_ths_store(Dg_ak_stock_board_concept_cons_ths_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_cons_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_emCreate(Dg_ak_stock_board_concept_cons_emBase):
    pass

class Dg_ak_stock_board_concept_cons_em(Dg_ak_stock_board_concept_cons_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_concept_cons_em_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_em_storeCreate(Dg_ak_stock_board_concept_cons_em_storeBase):
    pass

class Dg_ak_stock_board_concept_cons_em_store(Dg_ak_stock_board_concept_cons_em_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_cons_thsBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str
    a: str
    circulating_shares: str

class Dg_ak_stock_board_industry_cons_thsCreate(Dg_ak_stock_board_industry_cons_thsBase):
    pass

class Dg_ak_stock_board_industry_cons_ths(Dg_ak_stock_board_industry_cons_thsBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_cons_ths_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str
    a: str
    circulating_shares: str

class Dg_ak_stock_board_industry_cons_ths_storeCreate(Dg_ak_stock_board_industry_cons_ths_storeBase):
    pass

class Dg_ak_stock_board_industry_cons_ths_store(Dg_ak_stock_board_industry_cons_ths_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_cons_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str
    v: int

class Dg_ak_stock_board_industry_cons_emCreate(Dg_ak_stock_board_industry_cons_emBase):
    pass

class Dg_ak_stock_board_industry_cons_em(Dg_ak_stock_board_industry_cons_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_board_industry_cons_em_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str
    v: int

class Dg_ak_stock_board_industry_cons_em_storeCreate(Dg_ak_stock_board_industry_cons_em_storeBase):
    pass

class Dg_ak_stock_board_industry_cons_em_store(Dg_ak_stock_board_industry_cons_em_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_individual_fund_flow_storeBase(BaseModel):
    td: date
    s_code: str
    c: Decimal
    pct_chg: Decimal
    main_net_inflow: Decimal
    main_net_inflow_pct: Decimal
    huge_order_net_inflow: Decimal
    huge_order_net_inflow_pct: Decimal
    large_order_net_inflow: Decimal
    large_order_net_inflow_pct: Decimal
    medium_order_net_inflow: Decimal
    medium_order_net_inflow_pct: Decimal
    small_order_net_inflow: Decimal
    small_order_net_inflow_pct: Decimal

class Dg_ak_stock_individual_fund_flow_storeCreate(Dg_ak_stock_individual_fund_flow_storeBase):
    pass

class Dg_ak_stock_individual_fund_flow_store(Dg_ak_stock_individual_fund_flow_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_individual_fund_flow_rank_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    latest_price: Decimal
    today_pct_chg: Decimal
    today_main_net_inflow: Decimal
    today_main_net_inflow_pct: Decimal
    today_huge_order_net_inflow: Decimal
    today_huge_order_net_inflow_pct: Decimal
    today_large_order_net_inflow: Decimal
    today_large_order_net_inflow_pct: Decimal
    today_medium_order_net_inflow: Decimal
    today_medium_order_net_inflow_pct: Decimal
    today_small_order_net_inflow: Decimal
    today_small_order_net_inflow_pct: Decimal

class Dg_ak_stock_individual_fund_flow_rank_storeCreate(Dg_ak_stock_individual_fund_flow_rank_storeBase):
    pass

class Dg_ak_stock_individual_fund_flow_rank_store(Dg_ak_stock_individual_fund_flow_rank_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_market_fund_flow_storeBase(BaseModel):
    td: date
    shanghai_closing_price: Decimal
    shanghai_pct_chg: Decimal
    shenzhen_closing_price: Decimal
    shenzhen_pct_chg: Decimal
    main_net_inflow: Decimal
    main_net_inflow_pct: Decimal
    huge_order_net_inflow: Decimal
    huge_order_net_inflow_pct: Decimal
    large_order_net_inflow: Decimal
    large_order_net_inflow_pct: Decimal
    medium_order_net_inflow: Decimal
    medium_order_net_inflow_pct: Decimal
    small_order_net_inflow: Decimal
    small_order_net_inflow_pct: Decimal

class Dg_ak_stock_market_fund_flow_storeCreate(Dg_ak_stock_market_fund_flow_storeBase):
    pass

class Dg_ak_stock_market_fund_flow_store(Dg_ak_stock_market_fund_flow_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_sector_fund_flow_rank_storeBase(BaseModel):
    td: date
    b_name: str
    sector_type: str
    today_pct_chg: Decimal
    today_main_net_inflow: Decimal
    today_main_net_inflow_pct: Decimal
    today_huge_order_net_inflow: Decimal
    today_huge_order_net_inflow_pct: Decimal
    today_large_order_net_inflow: Decimal
    today_large_order_net_inflow_pct: Decimal
    today_medium_order_net_inflow: Decimal
    today_medium_order_net_inflow_pct: Decimal
    today_small_order_net_inflow: Decimal
    today_small_order_net_inflow_pct: Decimal
    today_main_net_inflow_max_stock: str

class Dg_ak_stock_sector_fund_flow_rank_storeCreate(Dg_ak_stock_sector_fund_flow_rank_storeBase):
    pass

class Dg_ak_stock_sector_fund_flow_rank_store(Dg_ak_stock_sector_fund_flow_rank_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_main_fund_flow_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    latest_price: Decimal
    today_main_net_inflow_pct: Decimal
    today_rank: int
    today_pct_chg: Decimal
    main_net_inflow_pct_5day: Decimal
    rank_5day: int
    pct_chg_5day: Decimal
    main_net_inflow_pct_10day: Decimal
    rank_10day: int
    pct_chg_10day: Decimal
    sector: str

class Dg_ak_stock_main_fund_flow_storeCreate(Dg_ak_stock_main_fund_flow_storeBase):
    pass

class Dg_ak_stock_main_fund_flow_store(Dg_ak_stock_main_fund_flow_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_sector_fund_flow_summary_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    b_name: str
    latest_price: Decimal
    today_pct_chg: Decimal
    today_main_net_inflow: Decimal
    today_main_net_inflow_pct: Decimal
    today_huge_order_net_inflow: Decimal
    today_huge_order_net_inflow_pct: Decimal
    today_large_order_net_inflow: Decimal
    today_large_order_net_inflow_pct: Decimal
    today_medium_order_net_inflow: Decimal
    today_medium_order_net_inflow_pct: Decimal
    today_small_order_net_inflow: Decimal
    today_small_order_net_inflow_pct: Decimal

class Dg_ak_stock_sector_fund_flow_summary_storeCreate(Dg_ak_stock_sector_fund_flow_summary_storeBase):
    pass

class Dg_ak_stock_sector_fund_flow_summary_store(Dg_ak_stock_sector_fund_flow_summary_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_sector_fund_flow_hist_storeBase(BaseModel):
    td: date
    b_name: str
    main_net_inflow: Decimal
    main_net_inflow_pct: Decimal
    huge_order_net_inflow: Decimal
    huge_order_net_inflow_pct: Decimal
    large_order_net_inflow: Decimal
    large_order_net_inflow_pct: Decimal
    medium_order_net_inflow: Decimal
    medium_order_net_inflow_pct: Decimal
    small_order_net_inflow: Decimal
    small_order_net_inflow_pct: Decimal

class Dg_ak_stock_sector_fund_flow_hist_storeCreate(Dg_ak_stock_sector_fund_flow_hist_storeBase):
    pass

class Dg_ak_stock_sector_fund_flow_hist_store(Dg_ak_stock_sector_fund_flow_hist_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_concept_fund_flow_hist_storeBase(BaseModel):
    td: date
    b_name: str
    main_net_inflow: Decimal
    main_net_inflow_pct: Decimal
    huge_order_net_inflow: Decimal
    huge_order_net_inflow_pct: Decimal
    large_order_net_inflow: Decimal
    large_order_net_inflow_pct: Decimal
    medium_order_net_inflow: Decimal
    medium_order_net_inflow_pct: Decimal
    small_order_net_inflow: Decimal
    small_order_net_inflow_pct: Decimal

class Dg_ak_stock_concept_fund_flow_hist_storeCreate(Dg_ak_stock_concept_fund_flow_hist_storeBase):
    pass

class Dg_ak_stock_concept_fund_flow_hist_store(Dg_ak_stock_concept_fund_flow_hist_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_hold_num_cninfoBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    cur_sh: int
    pre_sh: int
    num_of_sh_chg_pct: float
    cur_avg_holdings: float
    pre_avg_holdings: float
    avg_holdings_chg_pct: float

class Dg_ak_stock_hold_num_cninfoCreate(Dg_ak_stock_hold_num_cninfoBase):
    pass

class Dg_ak_stock_hold_num_cninfo(Dg_ak_stock_hold_num_cninfoBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_hold_num_cninfo_storeBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    cur_sh: int
    pre_sh: int
    num_of_sh_chg_pct: float
    cur_avg_holdings: float
    pre_avg_holdings: float
    avg_holdings_chg_pct: float

class Dg_ak_stock_hold_num_cninfo_storeCreate(Dg_ak_stock_hold_num_cninfo_storeBase):
    pass

class Dg_ak_stock_hold_num_cninfo_store(Dg_ak_stock_hold_num_cninfo_storeBase):
    class Config:
        orm_mode = True

class Dg_ak_index_zh_a_hist_dailyBase(BaseModel):
    i_code: str
    td: date
    o: float
    c: float
    h: float
    l: float
    v: int
    amplitude: float
    pct_chg: float
    change: float
    turnover_rate: float

class Dg_ak_index_zh_a_hist_dailyCreate(Dg_ak_index_zh_a_hist_dailyBase):
    pass

class Dg_ak_index_zh_a_hist_daily(Dg_ak_index_zh_a_hist_dailyBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zh_a_hist_daily_bfqBase(BaseModel):
    s_code: str
    td: date
    o: float
    c: float
    h: float
    l: float
    v: int
    amplitude: float
    pct_chg: float
    change: float
    turnover_rate: float

class Dg_ak_stock_zh_a_hist_daily_bfqCreate(Dg_ak_stock_zh_a_hist_daily_bfqBase):
    pass

class Dg_ak_stock_zh_a_hist_daily_bfq(Dg_ak_stock_zh_a_hist_daily_bfqBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zh_a_hist_daily_hfqBase(BaseModel):
    s_code: str
    td: date
    o: float
    c: float
    h: float
    l: float
    v: int
    amplitude: float
    pct_chg: float
    change: float
    turnover_rate: float

class Dg_ak_stock_zh_a_hist_daily_hfqCreate(Dg_ak_stock_zh_a_hist_daily_hfqBase):
    pass

class Dg_ak_stock_zh_a_hist_daily_hfq(Dg_ak_stock_zh_a_hist_daily_hfqBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zh_a_trade_dateBase(BaseModel):
    trade_date: date
    create_time: datetime
    update_time: datetime

class Dg_ak_stock_zh_a_trade_dateCreate(Dg_ak_stock_zh_a_trade_dateBase):
    pass

class Dg_ak_stock_zh_a_trade_date(Dg_ak_stock_zh_a_trade_dateBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zt_pool_dtgc_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    a: float
    dynamic_pe: float
    turnover_rate: float
    lock_fund: int
    board_amount: int
    continuous_dt: int
    open_account: int
    industry: str

class Dg_ak_stock_zt_pool_dtgc_emCreate(Dg_ak_stock_zt_pool_dtgc_emBase):
    pass

class Dg_ak_stock_zt_pool_dtgc_em(Dg_ak_stock_zt_pool_dtgc_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zt_pool_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    a: float
    turnover_rate: float
    lock_fund: int
    failed_account: int
    zt_stat: str
    zt_account: int
    industry: str

class Dg_ak_stock_zt_pool_emCreate(Dg_ak_stock_zt_pool_emBase):
    pass

class Dg_ak_stock_zt_pool_em(Dg_ak_stock_zt_pool_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zt_pool_previous_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: Decimal
    total_mv: Decimal
    turnover_rate: float
    increase_speed: float
    amplitude: float
    yesterday_zt_account: int
    zt_stat: str
    industry: str

class Dg_ak_stock_zt_pool_previous_emCreate(Dg_ak_stock_zt_pool_previous_emBase):
    pass

class Dg_ak_stock_zt_pool_previous_em(Dg_ak_stock_zt_pool_previous_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zt_pool_strong_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: Decimal
    total_mv: Decimal
    turnover_rate: float
    increase_speed: float
    quantity_ratio: float
    zt_stat: str
    selection_reason: str
    industry: str

class Dg_ak_stock_zt_pool_strong_emCreate(Dg_ak_stock_zt_pool_strong_emBase):
    pass

class Dg_ak_stock_zt_pool_strong_em(Dg_ak_stock_zt_pool_strong_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zt_pool_sub_new_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: Decimal
    total_mv: Decimal
    turnover_rate: float
    days_since_open: int
    open_date: date
    ipo_date: date
    zt_stat: str
    industry: str

class Dg_ak_stock_zt_pool_sub_new_emCreate(Dg_ak_stock_zt_pool_sub_new_emBase):
    pass

class Dg_ak_stock_zt_pool_sub_new_em(Dg_ak_stock_zt_pool_sub_new_emBase):
    class Config:
        orm_mode = True

class Dg_ak_stock_zt_pool_zbgc_emBase(BaseModel):
    td: date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: Decimal
    total_mv: Decimal
    turnover_rate: float
    increase_speed: float
    failed_account: int
    zt_stat: str
    amplitude: float
    industry: str

class Dg_ak_stock_zt_pool_zbgc_emCreate(Dg_ak_stock_zt_pool_zbgc_emBase):
    pass

class Dg_ak_stock_zt_pool_zbgc_em(Dg_ak_stock_zt_pool_zbgc_emBase):
    class Config:
        orm_mode = True

class Dg_fy_tracing_s_usBase(BaseModel):
    symbol: str
    last_td: date
    create_time: datetime
    update_time: datetime
    host_name: str

class Dg_fy_tracing_s_usCreate(Dg_fy_tracing_s_usBase):
    pass

class Dg_fy_tracing_s_us(Dg_fy_tracing_s_usBase):
    class Config:
        orm_mode = True

class Dg_fy_stock_us_trade_dateBase(BaseModel):
    trade_date: date
    create_time: datetime
    update_time: datetime

class Dg_fy_stock_us_trade_dateCreate(Dg_fy_stock_us_trade_dateBase):
    pass

class Dg_fy_stock_us_trade_date(Dg_fy_stock_us_trade_dateBase):
    class Config:
        orm_mode = True

class Dg_fy_stock_us_hist_daily_bfqBase(BaseModel):
    symbol: str
    td: date
    o: float
    c: float
    h: float
    l: float
    v: int
    adj_close: float
    dividends: float
    stock_splits: float

class Dg_fy_stock_us_hist_daily_bfqCreate(Dg_fy_stock_us_hist_daily_bfqBase):
    pass

class Dg_fy_stock_us_hist_daily_bfq(Dg_fy_stock_us_hist_daily_bfqBase):
    class Config:
        orm_mode = True

