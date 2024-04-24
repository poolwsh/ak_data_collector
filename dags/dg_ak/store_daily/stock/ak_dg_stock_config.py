
ak_cols_config = {
    "stock_zt_pool_em": {
        # cmv = circulation market value 流通市值
        # tv = total value 总市值
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值', '总市值', '换手率', '封板资金', '首次封板时间', '最后封板时间', '炸板次数', '涨停统计', '连板数', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'a',    'circulation_mv',    'total_mv',    'turnover_rate', 'lock_fund', 'first_lock_time', 'last_lock_time', 'failed_account', 'zt_stat', 'zt_account', 'industry']
    }
}
