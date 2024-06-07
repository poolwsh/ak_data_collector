
ak_cols_config = {
    "stock_board_concept_name_ths": {
        "org_list": ['日期', '概念名称', '成分股数量', '网址', '代码'],
        "remove_list": [],
        "target_list": ['td',  'b_name',  'stock_count',   'url',   'b_code']
    },
    "stock_board_concept_name_em": {
        "org_list": ['排名', '板块名称', '板块代码', '最新价', '涨跌额', '涨跌幅', 
                     '总市值', '换手率', '上涨家数', '下跌家数', '领涨股票', '领涨股票-涨跌幅'],
        "remove_list": [],
        "target_list": ['rank', 'b_name', 'b_code', 'c', 'change_amount', 'change_percentage', 
                    'total_mv', 'turnover_rate', 'risers', 'fallers', 'leader_s', 'leader_s_change']
    },
    "stock_board_industry_summary_ths": {
        "org_list": ['序号', '板块', '涨跌幅', '总成交量', '总成交额', '净流入', '上涨家数',
                      '下跌家数', '均价', '领涨股', '领涨股-最新价', '领涨股-涨跌幅'],
        "remove_list": ['序号'],
        "target_list": ['b_name', 'change_percentage', 'v', 'a', 'net_inflow', 'risers', 
                    'fallers', 'average_price', 'leader_s', 'leader_s_price', 'leader_s_change']
    },
    "stock_board_industry_name_em": {
        "org_list": ['排名', '板块名称', '板块代码', '最新价', '涨跌额', '涨跌幅', '总市值',
                      '换手率', '上涨家数', '下跌家数', '领涨股票', '领涨股票-涨跌幅'],
        "remove_list": [],
        "target_list": ['rank', 'b_name', 'b_code', 'c', 'change_amount', 'change_percentage', 'total_mv', 
                    'turnover_rate', 'risers', 'fallers', 'leader_s', 'leader_s_change']
    },
    "stock_board_concept_cons_ths": {
        "org_list": ['序号', '代码', '名称', '现价', '涨跌幅', '涨跌', '涨速', '换手', '量比', '振幅', '成交额',
       '流通股', '流通市值', '市盈率'],
        "remove_list": ['序号', '现价', '涨跌幅', '涨跌', '涨速', '换手', '量比', '振幅', '成交额',
       '流通股', '流通市值', '市盈率'],
        "target_list": ['s_code', 's_name']
    },
    "stock_board_concept_cons_em": {
        "org_list": ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低',
       '今开', '昨收', '换手率', '市盈率-动态', '市净率'],
        "remove_list": ['序号', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低',
       '今开', '昨收', '换手率', '市盈率-动态', '市净率'],
        "target_list": ['s_code', 's_name']
    },
    "stock_board_industry_cons_ths": {
        "org_list": ['序号', '代码', '名称', '现价', '涨跌幅', '涨跌', '涨速', '换手', 
                     '量比', '振幅', '成交额', '流通股', '流通市值', '市盈率'],
        "remove_list": ['序号'],
        "target_list": ['s_code', 's_name', 'c', 'pct_chg', 'change', 'increase_speed', 'turnover_rate',
                    'quantity_ratio', 'amplitude', 'a', 'circulating_shares', 'circulation_mv', 'pe']
    },
    "stock_board_industry_cons_em": {
        "org_list": ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', 
                     '最低', '今开', '昨收', '换手率', '市盈率-动态', '市净率'],
        "remove_list": ['序号'],
        "target_list": ['s_code', 's_name', 'c', 'pct_chg', 'change', 'v', 'a', 'amplitude', 'h', 
                    'l', 'o', 'yesterday_c', 'turnover_rate', 'pe_dynamic', 'pb']
    }
}
