from pydantic import BaseModel
from datetime import Date, DateTime
from decimal import DECIMAL

class Da_ak_tracing_stock_price_hlBase(BaseModel):
    s_code: str
    min_td: Date
    max_td: Date
    create_time: DateTime
    update_time: DateTime
    host_name: str

class Da_ak_tracing_stock_price_hlCreate(Da_ak_tracing_stock_price_hlBase):
    pass

class Da_ak_tracing_stock_price_hlRead(Da_ak_tracing_stock_price_hlBase):
    class Config:
        from_attributes = True

class Da_ak_stock_price_hlBase(BaseModel):
    s_code: str
    td: Date
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

class Da_ak_stock_price_hlRead(Da_ak_stock_price_hlBase):
    class Config:
        from_attributes = True

class Da_ak_stock_price_hl_storeBase(BaseModel):
    s_code: str
    td: Date
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

class Da_ak_stock_price_hl_storeRead(Da_ak_stock_price_hl_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_tracing_by_dateBase(BaseModel):
    ak_func_name: str
    last_td: Date
    create_time: DateTime
    update_time: DateTime
    host_name: str

class Dg_ak_tracing_by_dateCreate(Dg_ak_tracing_by_dateBase):
    pass

class Dg_ak_tracing_by_dateRead(Dg_ak_tracing_by_dateBase):
    class Config:
        from_attributes = True

class Dg_ak_tracing_by_date_1_paramBase(BaseModel):
    ak_func_name: str
    param_name: str
    param_value: str
    last_td: Date
    create_time: DateTime
    update_time: DateTime
    host_name: str

class Dg_ak_tracing_by_date_1_paramCreate(Dg_ak_tracing_by_date_1_paramBase):
    pass

class Dg_ak_tracing_by_date_1_paramRead(Dg_ak_tracing_by_date_1_paramBase):
    class Config:
        from_attributes = True

class Dg_ak_tracing_by_scode_dateBase(BaseModel):
    ak_func_name: str
    scode: str
    last_td: Date
    create_time: DateTime
    update_time: DateTime
    host_name: str

class Dg_ak_tracing_by_scode_dateCreate(Dg_ak_tracing_by_scode_dateBase):
    pass

class Dg_ak_tracing_by_scode_dateRead(Dg_ak_tracing_by_scode_dateBase):
    class Config:
        from_attributes = True

class Dg_ak_tracing_s_zh_aBase(BaseModel):
    ak_func_name: str
    scode: str
    period: str
    adjust: str
    last_td: Date
    create_time: DateTime
    update_time: DateTime
    host_name: str

class Dg_ak_tracing_s_zh_aCreate(Dg_ak_tracing_s_zh_aBase):
    pass

class Dg_ak_tracing_s_zh_aRead(Dg_ak_tracing_s_zh_aBase):
    class Config:
        from_attributes = True

class Dg_ak_tracing_i_zh_aBase(BaseModel):
    ak_func_name: str
    icode: str
    period: str
    last_td: Date
    create_time: DateTime
    update_time: DateTime
    host_name: str

class Dg_ak_tracing_i_zh_aCreate(Dg_ak_tracing_i_zh_aBase):
    pass

class Dg_ak_tracing_i_zh_aRead(Dg_ak_tracing_i_zh_aBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_name_thsBase(BaseModel):
    td: Date
    b_code: str
    b_name: str
    stock_count: int
    url: str

class Dg_ak_stock_board_concept_name_thsCreate(Dg_ak_stock_board_concept_name_thsBase):
    pass

class Dg_ak_stock_board_concept_name_thsRead(Dg_ak_stock_board_concept_name_thsBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_name_ths_storeBase(BaseModel):
    td: Date
    b_code: str
    b_name: str
    stock_count: int
    url: str

class Dg_ak_stock_board_concept_name_ths_storeCreate(Dg_ak_stock_board_concept_name_ths_storeBase):
    pass

class Dg_ak_stock_board_concept_name_ths_storeRead(Dg_ak_stock_board_concept_name_ths_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_name_emBase(BaseModel):
    td: Date
    rank: int
    b_code: str
    b_name: str
    c: DECIMAL
    change_amount: DECIMAL
    change_percentage: DECIMAL
    total_mv: int
    turnover_rate: DECIMAL
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: DECIMAL

class Dg_ak_stock_board_concept_name_emCreate(Dg_ak_stock_board_concept_name_emBase):
    pass

class Dg_ak_stock_board_concept_name_emRead(Dg_ak_stock_board_concept_name_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_name_em_storeBase(BaseModel):
    td: Date
    rank: int
    b_code: str
    b_name: str
    c: DECIMAL
    change_amount: DECIMAL
    change_percentage: DECIMAL
    total_mv: int
    turnover_rate: DECIMAL
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: DECIMAL

class Dg_ak_stock_board_concept_name_em_storeCreate(Dg_ak_stock_board_concept_name_em_storeBase):
    pass

class Dg_ak_stock_board_concept_name_em_storeRead(Dg_ak_stock_board_concept_name_em_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_summary_thsBase(BaseModel):
    td: Date
    b_name: str
    change_percentage: DECIMAL
    v: DECIMAL
    a: DECIMAL
    net_inflow: DECIMAL
    risers: int
    fallers: int
    average_price: DECIMAL
    leader_s: str
    leader_s_price: DECIMAL
    leader_s_change: DECIMAL

class Dg_ak_stock_board_industry_summary_thsCreate(Dg_ak_stock_board_industry_summary_thsBase):
    pass

class Dg_ak_stock_board_industry_summary_thsRead(Dg_ak_stock_board_industry_summary_thsBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_summary_ths_storeBase(BaseModel):
    td: Date
    b_name: str
    change_percentage: DECIMAL
    v: DECIMAL
    a: DECIMAL
    net_inflow: DECIMAL
    risers: int
    fallers: int
    average_price: DECIMAL
    leader_s: str
    leader_s_price: DECIMAL
    leader_s_change: DECIMAL

class Dg_ak_stock_board_industry_summary_ths_storeCreate(Dg_ak_stock_board_industry_summary_ths_storeBase):
    pass

class Dg_ak_stock_board_industry_summary_ths_storeRead(Dg_ak_stock_board_industry_summary_ths_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_name_emBase(BaseModel):
    td: Date
    rank: int
    b_code: str
    b_name: str
    c: DECIMAL
    change_amount: DECIMAL
    change_percentage: DECIMAL
    total_mv: int
    turnover_rate: DECIMAL
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: DECIMAL

class Dg_ak_stock_board_industry_name_emCreate(Dg_ak_stock_board_industry_name_emBase):
    pass

class Dg_ak_stock_board_industry_name_emRead(Dg_ak_stock_board_industry_name_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_name_em_storeBase(BaseModel):
    td: Date
    rank: int
    b_code: str
    b_name: str
    c: DECIMAL
    change_amount: DECIMAL
    change_percentage: DECIMAL
    total_mv: int
    turnover_rate: DECIMAL
    risers: int
    fallers: int
    leader_s: str
    leader_s_change: DECIMAL

class Dg_ak_stock_board_industry_name_em_storeCreate(Dg_ak_stock_board_industry_name_em_storeBase):
    pass

class Dg_ak_stock_board_industry_name_em_storeRead(Dg_ak_stock_board_industry_name_em_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_cons_thsBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_thsCreate(Dg_ak_stock_board_concept_cons_thsBase):
    pass

class Dg_ak_stock_board_concept_cons_thsRead(Dg_ak_stock_board_concept_cons_thsBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_cons_ths_storeBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_ths_storeCreate(Dg_ak_stock_board_concept_cons_ths_storeBase):
    pass

class Dg_ak_stock_board_concept_cons_ths_storeRead(Dg_ak_stock_board_concept_cons_ths_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_cons_emBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_emCreate(Dg_ak_stock_board_concept_cons_emBase):
    pass

class Dg_ak_stock_board_concept_cons_emRead(Dg_ak_stock_board_concept_cons_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_concept_cons_em_storeBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str

class Dg_ak_stock_board_concept_cons_em_storeCreate(Dg_ak_stock_board_concept_cons_em_storeBase):
    pass

class Dg_ak_stock_board_concept_cons_em_storeRead(Dg_ak_stock_board_concept_cons_em_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_cons_thsBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str
    a: str
    circulating_shares: str

class Dg_ak_stock_board_industry_cons_thsCreate(Dg_ak_stock_board_industry_cons_thsBase):
    pass

class Dg_ak_stock_board_industry_cons_thsRead(Dg_ak_stock_board_industry_cons_thsBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_cons_ths_storeBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str
    a: str
    circulating_shares: str

class Dg_ak_stock_board_industry_cons_ths_storeCreate(Dg_ak_stock_board_industry_cons_ths_storeBase):
    pass

class Dg_ak_stock_board_industry_cons_ths_storeRead(Dg_ak_stock_board_industry_cons_ths_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_cons_emBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str
    v: int

class Dg_ak_stock_board_industry_cons_emCreate(Dg_ak_stock_board_industry_cons_emBase):
    pass

class Dg_ak_stock_board_industry_cons_emRead(Dg_ak_stock_board_industry_cons_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_board_industry_cons_em_storeBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str
    v: int

class Dg_ak_stock_board_industry_cons_em_storeCreate(Dg_ak_stock_board_industry_cons_em_storeBase):
    pass

class Dg_ak_stock_board_industry_cons_em_storeRead(Dg_ak_stock_board_industry_cons_em_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_individual_fund_flow_storeBase(BaseModel):
    td: Date
    s_code: str
    c: DECIMAL
    pct_chg: DECIMAL
    main_net_inflow: DECIMAL
    main_net_inflow_pct: DECIMAL
    huge_order_net_inflow: DECIMAL
    huge_order_net_inflow_pct: DECIMAL
    large_order_net_inflow: DECIMAL
    large_order_net_inflow_pct: DECIMAL
    medium_order_net_inflow: DECIMAL
    medium_order_net_inflow_pct: DECIMAL
    small_order_net_inflow: DECIMAL
    small_order_net_inflow_pct: DECIMAL

class Dg_ak_stock_individual_fund_flow_storeCreate(Dg_ak_stock_individual_fund_flow_storeBase):
    pass

class Dg_ak_stock_individual_fund_flow_storeRead(Dg_ak_stock_individual_fund_flow_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_individual_fund_flow_rank_storeBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    latest_price: DECIMAL
    today_pct_chg: DECIMAL
    today_main_net_inflow: DECIMAL
    today_main_net_inflow_pct: DECIMAL
    today_huge_order_net_inflow: DECIMAL
    today_huge_order_net_inflow_pct: DECIMAL
    today_large_order_net_inflow: DECIMAL
    today_large_order_net_inflow_pct: DECIMAL
    today_medium_order_net_inflow: DECIMAL
    today_medium_order_net_inflow_pct: DECIMAL
    today_small_order_net_inflow: DECIMAL
    today_small_order_net_inflow_pct: DECIMAL

class Dg_ak_stock_individual_fund_flow_rank_storeCreate(Dg_ak_stock_individual_fund_flow_rank_storeBase):
    pass

class Dg_ak_stock_individual_fund_flow_rank_storeRead(Dg_ak_stock_individual_fund_flow_rank_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_market_fund_flow_storeBase(BaseModel):
    td: Date
    shanghai_closing_price: DECIMAL
    shanghai_pct_chg: DECIMAL
    shenzhen_closing_price: DECIMAL
    shenzhen_pct_chg: DECIMAL
    main_net_inflow: DECIMAL
    main_net_inflow_pct: DECIMAL
    huge_order_net_inflow: DECIMAL
    huge_order_net_inflow_pct: DECIMAL
    large_order_net_inflow: DECIMAL
    large_order_net_inflow_pct: DECIMAL
    medium_order_net_inflow: DECIMAL
    medium_order_net_inflow_pct: DECIMAL
    small_order_net_inflow: DECIMAL
    small_order_net_inflow_pct: DECIMAL

class Dg_ak_stock_market_fund_flow_storeCreate(Dg_ak_stock_market_fund_flow_storeBase):
    pass

class Dg_ak_stock_market_fund_flow_storeRead(Dg_ak_stock_market_fund_flow_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_sector_fund_flow_rank_storeBase(BaseModel):
    td: Date
    b_name: str
    sector_type: str
    today_pct_chg: DECIMAL
    today_main_net_inflow: DECIMAL
    today_main_net_inflow_pct: DECIMAL
    today_huge_order_net_inflow: DECIMAL
    today_huge_order_net_inflow_pct: DECIMAL
    today_large_order_net_inflow: DECIMAL
    today_large_order_net_inflow_pct: DECIMAL
    today_medium_order_net_inflow: DECIMAL
    today_medium_order_net_inflow_pct: DECIMAL
    today_small_order_net_inflow: DECIMAL
    today_small_order_net_inflow_pct: DECIMAL
    today_main_net_inflow_max_stock: str

class Dg_ak_stock_sector_fund_flow_rank_storeCreate(Dg_ak_stock_sector_fund_flow_rank_storeBase):
    pass

class Dg_ak_stock_sector_fund_flow_rank_storeRead(Dg_ak_stock_sector_fund_flow_rank_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_main_fund_flow_storeBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    latest_price: DECIMAL
    today_main_net_inflow_pct: DECIMAL
    today_rank: int
    today_pct_chg: DECIMAL
    main_net_inflow_pct_5day: DECIMAL
    rank_5day: int
    pct_chg_5day: DECIMAL
    main_net_inflow_pct_10day: DECIMAL
    rank_10day: int
    pct_chg_10day: DECIMAL
    sector: str

class Dg_ak_stock_main_fund_flow_storeCreate(Dg_ak_stock_main_fund_flow_storeBase):
    pass

class Dg_ak_stock_main_fund_flow_storeRead(Dg_ak_stock_main_fund_flow_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_sector_fund_flow_summary_storeBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    b_name: str
    latest_price: DECIMAL
    today_pct_chg: DECIMAL
    today_main_net_inflow: DECIMAL
    today_main_net_inflow_pct: DECIMAL
    today_huge_order_net_inflow: DECIMAL
    today_huge_order_net_inflow_pct: DECIMAL
    today_large_order_net_inflow: DECIMAL
    today_large_order_net_inflow_pct: DECIMAL
    today_medium_order_net_inflow: DECIMAL
    today_medium_order_net_inflow_pct: DECIMAL
    today_small_order_net_inflow: DECIMAL
    today_small_order_net_inflow_pct: DECIMAL

class Dg_ak_stock_sector_fund_flow_summary_storeCreate(Dg_ak_stock_sector_fund_flow_summary_storeBase):
    pass

class Dg_ak_stock_sector_fund_flow_summary_storeRead(Dg_ak_stock_sector_fund_flow_summary_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_sector_fund_flow_hist_storeBase(BaseModel):
    td: Date
    b_name: str
    main_net_inflow: DECIMAL
    main_net_inflow_pct: DECIMAL
    huge_order_net_inflow: DECIMAL
    huge_order_net_inflow_pct: DECIMAL
    large_order_net_inflow: DECIMAL
    large_order_net_inflow_pct: DECIMAL
    medium_order_net_inflow: DECIMAL
    medium_order_net_inflow_pct: DECIMAL
    small_order_net_inflow: DECIMAL
    small_order_net_inflow_pct: DECIMAL

class Dg_ak_stock_sector_fund_flow_hist_storeCreate(Dg_ak_stock_sector_fund_flow_hist_storeBase):
    pass

class Dg_ak_stock_sector_fund_flow_hist_storeRead(Dg_ak_stock_sector_fund_flow_hist_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_concept_fund_flow_hist_storeBase(BaseModel):
    td: Date
    b_name: str
    main_net_inflow: DECIMAL
    main_net_inflow_pct: DECIMAL
    huge_order_net_inflow: DECIMAL
    huge_order_net_inflow_pct: DECIMAL
    large_order_net_inflow: DECIMAL
    large_order_net_inflow_pct: DECIMAL
    medium_order_net_inflow: DECIMAL
    medium_order_net_inflow_pct: DECIMAL
    small_order_net_inflow: DECIMAL
    small_order_net_inflow_pct: DECIMAL

class Dg_ak_stock_concept_fund_flow_hist_storeCreate(Dg_ak_stock_concept_fund_flow_hist_storeBase):
    pass

class Dg_ak_stock_concept_fund_flow_hist_storeRead(Dg_ak_stock_concept_fund_flow_hist_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_hold_num_cninfoBase(BaseModel):
    td: Date
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

class Dg_ak_stock_hold_num_cninfoRead(Dg_ak_stock_hold_num_cninfoBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_hold_num_cninfo_storeBase(BaseModel):
    td: Date
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

class Dg_ak_stock_hold_num_cninfo_storeRead(Dg_ak_stock_hold_num_cninfo_storeBase):
    class Config:
        from_attributes = True

class Dg_ak_index_zh_a_hist_dailyBase(BaseModel):
    i_code: str
    td: Date
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

class Dg_ak_index_zh_a_hist_dailyRead(Dg_ak_index_zh_a_hist_dailyBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zh_a_hist_daily_bfqBase(BaseModel):
    s_code: str
    td: Date
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

class Dg_ak_stock_zh_a_hist_daily_bfqRead(Dg_ak_stock_zh_a_hist_daily_bfqBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zh_a_hist_daily_hfqBase(BaseModel):
    s_code: str
    td: Date
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

class Dg_ak_stock_zh_a_hist_daily_hfqRead(Dg_ak_stock_zh_a_hist_daily_hfqBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zh_a_trade_dateBase(BaseModel):
    trade_date: Date
    create_time: DateTime
    update_time: DateTime

class Dg_ak_stock_zh_a_trade_dateCreate(Dg_ak_stock_zh_a_trade_dateBase):
    pass

class Dg_ak_stock_zh_a_trade_dateRead(Dg_ak_stock_zh_a_trade_dateBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zt_pool_dtgc_emBase(BaseModel):
    td: Date
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

class Dg_ak_stock_zt_pool_dtgc_emRead(Dg_ak_stock_zt_pool_dtgc_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zt_pool_emBase(BaseModel):
    td: Date
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

class Dg_ak_stock_zt_pool_emRead(Dg_ak_stock_zt_pool_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zt_pool_previous_emBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: DECIMAL
    total_mv: DECIMAL
    turnover_rate: float
    increase_speed: float
    amplitude: float
    yesterday_zt_account: int
    zt_stat: str
    industry: str

class Dg_ak_stock_zt_pool_previous_emCreate(Dg_ak_stock_zt_pool_previous_emBase):
    pass

class Dg_ak_stock_zt_pool_previous_emRead(Dg_ak_stock_zt_pool_previous_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zt_pool_strong_emBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: DECIMAL
    total_mv: DECIMAL
    turnover_rate: float
    increase_speed: float
    quantity_ratio: float
    zt_stat: str
    selection_reason: str
    industry: str

class Dg_ak_stock_zt_pool_strong_emCreate(Dg_ak_stock_zt_pool_strong_emBase):
    pass

class Dg_ak_stock_zt_pool_strong_emRead(Dg_ak_stock_zt_pool_strong_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zt_pool_sub_new_emBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: DECIMAL
    total_mv: DECIMAL
    turnover_rate: float
    days_since_open: int
    open_date: Date
    ipo_date: Date
    zt_stat: str
    industry: str

class Dg_ak_stock_zt_pool_sub_new_emCreate(Dg_ak_stock_zt_pool_sub_new_emBase):
    pass

class Dg_ak_stock_zt_pool_sub_new_emRead(Dg_ak_stock_zt_pool_sub_new_emBase):
    class Config:
        from_attributes = True

class Dg_ak_stock_zt_pool_zbgc_emBase(BaseModel):
    td: Date
    s_code: str
    s_name: str
    pct_chg: float
    c: float
    zt_price: float
    a: float
    circulation_mv: DECIMAL
    total_mv: DECIMAL
    turnover_rate: float
    increase_speed: float
    failed_account: int
    zt_stat: str
    amplitude: float
    industry: str

class Dg_ak_stock_zt_pool_zbgc_emCreate(Dg_ak_stock_zt_pool_zbgc_emBase):
    pass

class Dg_ak_stock_zt_pool_zbgc_emRead(Dg_ak_stock_zt_pool_zbgc_emBase):
    class Config:
        from_attributes = True

class Dg_fy_tracing_s_usBase(BaseModel):
    symbol: str
    last_td: Date
    create_time: DateTime
    update_time: DateTime
    host_name: str

class Dg_fy_tracing_s_usCreate(Dg_fy_tracing_s_usBase):
    pass

class Dg_fy_tracing_s_usRead(Dg_fy_tracing_s_usBase):
    class Config:
        from_attributes = True

class Dg_fy_stock_us_trade_dateBase(BaseModel):
    trade_date: Date
    create_time: DateTime
    update_time: DateTime

class Dg_fy_stock_us_trade_dateCreate(Dg_fy_stock_us_trade_dateBase):
    pass

class Dg_fy_stock_us_trade_dateRead(Dg_fy_stock_us_trade_dateBase):
    class Config:
        from_attributes = True

class Dg_fy_stock_us_hist_daily_bfqBase(BaseModel):
    symbol: str
    td: Date
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

class Dg_fy_stock_us_hist_daily_bfqRead(Dg_fy_stock_us_hist_daily_bfqBase):
    class Config:
        from_attributes = True

