ak_cols_config = {
    "stock_individual_fund_flow": {
        "org_list": ['日期', '收盘价', '涨跌幅', '主力净流入-净额', '主力净流入-净占比', 
                     '超大单净流入-净额', '超大单净流入-净占比', '大单净流入-净额', '大单净流入-净占比', 
                     '中单净流入-净额', '中单净流入-净占比', '小单净流入-净额', '小单净流入-净占比'],
        "remove_list": [],
        "en_list": ['td', 'c', 'pct_chg', 'main_net_inflow', 'main_net_inflow_pct', 
                    'huge_order_net_inflow', 'huge_order_net_inflow_pct', 'large_order_net_inflow', 'large_order_net_inflow_pct',
                    'medium_order_net_inflow', 'medium_order_net_inflow_pct', 'small_order_net_inflow', 'small_order_net_inflow_pct']
    },
    "stock_individual_fund_flow_rank": {
        "org_list": ['序号', '代码', '名称', '最新价', '今日涨跌幅', '今日主力净流入-净额', '今日主力净流入-净占比',
                     '今日超大单净流入-净额', '今日超大单净流入-净占比', '今日大单净流入-净额', '今日大单净流入-净占比',
                     '今日中单净流入-净额', '今日中单净流入-净占比', '今日小单净流入-净额', '今日小单净流入-净占比'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'latest_price', 'today_pct_chg', 'today_main_net_inflow', 'today_main_net_inflow_pct', 
                    'today_huge_order_net_inflow', 'today_huge_order_net_inflow_pct', 'today_large_order_net_inflow', 'today_large_order_net_inflow_pct', 
                    'today_medium_order_net_inflow', 'today_medium_order_net_inflow_pct', 'today_small_order_net_inflow', 'today_small_order_net_inflow_pct']
    },
    "stock_market_fund_flow": {
        "org_list": ['日期', '上证-收盘价', '上证-涨跌幅', '深证-收盘价', '深证-涨跌幅', 
                     '主力净流入-净额', '主力净流入-净占比', '超大单净流入-净额', '超大单净流入-净占比', 
                     '大单净流入-净额', '大单净流入-净占比', '中单净流入-净额', '中单净流入-净占比', 
                     '小单净流入-净额', '小单净流入-净占比'],
        "remove_list": [],
        "en_list": ['td', 'shanghai_closing_price', 'shanghai_pct_chg', 'shenzhen_closing_price', 'shenzhen_pct_chg', 
                    'main_net_inflow', 'main_net_inflow_pct', 'huge_order_net_inflow', 'huge_order_net_inflow_pct', 
                    'large_order_net_inflow', 'large_order_net_inflow_pct', 'medium_order_net_inflow', 'medium_order_net_inflow_pct', 
                    'small_order_net_inflow', 'small_order_net_inflow_pct']
    },
    "stock_sector_fund_flow_rank": {
        "org_list": ['序号', '名称', '今日涨跌幅', '今日主力净流入-净额', '今日主力净流入-净占比', 
                     '今日超大单净流入-净额', '今日超大单净流入-净占比', '今日大单净流入-净额', '今日大单净流入-净占比', 
                     '今日中单净流入-净额', '今日中单净流入-净占比', '今日小单净流入-净额', '今日小单净流入-净占比', 
                     '今日主力净流入最大股'],
        "remove_list": ['序号'],
        "en_list": ['b_name', 'today_pct_chg', 'today_main_net_inflow', 'today_main_net_inflow_pct',
                    'today_huge_order_net_inflow', 'today_huge_order_net_inflow_pct', 'today_large_order_net_inflow', 'today_large_order_net_inflow_pct', 
                    'today_medium_order_net_inflow', 'today_medium_order_net_inflow_pct', 'today_small_order_net_inflow', 'today_small_order_net_inflow_pct', 
                    'today_main_net_inflow_max_stock']
    },
    "stock_main_fund_flow": {
        "org_list": ['序号', '代码', '名称', '最新价', '今日排行榜-主力净占比', '今日排行榜-今日排名', '今日排行榜-今日涨跌',
                     '5日排行榜-主力净占比', '5日排行榜-5日排名', '5日排行榜-5日涨跌', 
                     '10日排行榜-主力净占比', '10日排行榜-10日排名', '10日排行榜-10日涨跌', '所属板块'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'latest_price', 'today_main_net_inflow_pct', 'today_rank', 'today_pct_chg', 
                    'main_net_inflow_pct_5day', 'rank_5day', 'pct_chg_5day', 
                    'main_net_inflow_pct_10day', 'rank_10day', 'pct_chg_10day', 'sector']
    },
    "stock_sector_fund_flow_summary": {
        "org_list": ['序号', '代码', '名称', '最新价', '今天涨跌幅', '今日主力净流入-净额', '今日主力净流入-净占比',
                     '今日超大单净流入-净额', '今日超大单净流入-净占比', '今日大单净流入-净额', '今日大单净流入-净占比',
                     '今日中单净流入-净额', '今日中单净流入-净占比', '今日小单净流入-净额', '今日小单净流入-净占比'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'latest_price', 'today_pct_chg', 'today_main_net_inflow', 'today_main_net_inflow_pct', 
                    'today_huge_order_net_inflow', 'today_huge_order_net_inflow_pct', 'today_large_order_net_inflow', 'today_large_order_net_inflow_pct', 
                    'today_medium_order_net_inflow', 'today_medium_order_net_inflow_pct', 'today_small_order_net_inflow', 'today_small_order_net_inflow_pct']
    },
    "stock_sector_fund_flow_hist": {
        "org_list": ['日期', '主力净流入-净额', '主力净流入-净占比', '超大单净流入-净额', '超大单净流入-净占比', 
                     '大单净流入-净额', '大单净流入-净占比', '中单净流入-净额', '中单净流入-净占比', 
                     '小单净流入-净额', '小单净流入-净占比'],
        "remove_list": [],
        "en_list": ['td', 'main_net_inflow', 'main_net_inflow_pct', 'huge_order_net_inflow', 'huge_order_net_inflow_pct', 
                    'large_order_net_inflow', 'large_order_net_inflow_pct', 'medium_order_net_inflow', 'medium_order_net_inflow_pct', 
                    'small_order_net_inflow', 'small_order_net_inflow_pct']
    },
    "stock_concept_fund_flow_hist": {
        "org_list": ['日期', '主力净流入-净额', '主力净流入-净占比', '超大单净流入-净额', '超大单净流入-净占比', 
                     '大单净流入-净额', '大单净流入-净占比', '中单净流入-净额', '中单净流入-净占比', 
                     '小单净流入-净额', '小单净流入-净占比'],
        "remove_list": [],
        "en_list": ['td', 'main_net_inflow', 'main_net_inflow_pct', 'huge_order_net_inflow', 'huge_order_net_inflow_pct', 
                    'large_order_net_inflow', 'large_order_net_inflow_pct', 'medium_order_net_inflow', 'medium_order_net_inflow_pct', 
                    'small_order_net_inflow', 'small_order_net_inflow_pct']
    }
}
