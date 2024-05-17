
ak_cols_config = {
    "stock_zt_pool_dtgc_em": {
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值', '总市值', '动态市盈率', '换手率', '封单资金', '最后封板时间', '板上成交额', '连续跌停', '开板次数', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'a', 'circulation_mv', 'total_mv', 'dynamic_pe', 'turnover_rate', 'lock_fund', 'last_lock_time', 'board_amount', 'continuous_dt', 'open_account', 'industry']
    },
    "stock_zt_pool_em": {
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值', '总市值', '换手率', '封板资金', '首次封板时间', '最后封板时间', '炸板次数', '涨停统计', '连板数', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'a', 'circulation_mv', 'total_mv', 'turnover_rate', 'lock_fund', 'first_lock_time', 'last_lock_time', 'failed_account', 'zt_stat', 'zt_account', 'industry']
    },
    "stock_zt_pool_previous_em": {
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '换手率', '涨速', '振幅', '昨日封板时间', '昨日连板数', '涨停统计', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'zt_price', 'a', 'circulation_mv', 'total_mv', 'turnover_rate', 'increase_speed', 'amplitude', 'yesterday_lock_time', 'yesterday_zt_account', 'zt_stat', 'industry']
    },
    "stock_zt_pool_strong_em": {
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '换手率', '涨速', '是否新高', '量比', '涨停统计', '入选理由', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'zt_price', 'a', 'circulation_mv', 'total_mv', 'turnover_rate', 'increase_speed', 'is_new_high', 'quantity_ratio', 'zt_stat', 'selection_reason', 'industry']
    },
    "stock_zt_pool_sub_new_em": {
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '转手率', '开板几日', '开板日期', '上市日期', '是否新高', '涨停统计', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'zt_price', 'a', 'circulation_mv', 'total_mv', 'turnover_rate', 'days_since_open', 'open_date', 'ipo_date', 'is_new_high', 'zt_stat', 'industry']
    },
    "stock_zt_pool_zbgc_em": {
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '换手率', '涨速', '首次封板时间', '炸板次数', '涨停统计', '振幅', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'zt_price', 'a', 'circulation_mv', 'total_mv', 'turnover_rate', 'increase_speed', 'first_lock_time', 'failed_account', 'zt_stat', 'amplitude', 'industry']
    }
}
