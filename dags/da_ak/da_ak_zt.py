
from __future__ import annotations

import os
import sys
from pathlib import Path
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..'))
print(project_root)
# 将项目根目录添加到sys.path中
sys.path.append(project_root)


import socket
import pandas as pd
from datetime import timedelta, datetime
from airflow.models.dag import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.dates import days_ago

from dags.da_ak.utils.da_ak_util_funcs import DaAkUtilFuncs as dauf
from utils.logger import logger
import utils.config as con


# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

def generate_fibonacci_sequence(max_val, min_val=0):
    fib_sequence = [0, 1]
    while True:
        next_val = fib_sequence[-1] + fib_sequence[-2]
        if next_val > max_val:
            break
        fib_sequence.append(next_val)
    # 过滤并确保唯一值
    fib_sequence = sorted(set([x for x in fib_sequence if min_val <= x <= max_val]))
    return fib_sequence

def fetch_and_process_all_stock_data():
    try:
        # 获取所有交易日期
        query_dates = "SELECT DISTINCT td FROM ak_dg_stock_zh_a_hist_store_daily_hfq ORDER BY td DESC"
        trade_dates = pd.read_sql(query_dates, pg_conn)['td'].tolist()

        results = {}
        total_dates = len(trade_dates)

        for index, trade_date in enumerate(trade_dates):
            if con.LOGGER_DEBUG and index > 5:
                break
            logger.info(f'({index + 1}/{total_dates}) Processing data for date: {trade_date}')
            
            # 查询每个交易日期的 pct_chg 数据
            query = f"""
            SELECT pct_chg
            FROM ak_dg_stock_zh_a_hist_store_daily_hfq
            WHERE td = '{trade_date}'
            """
            df = pd.read_sql(query, pg_conn)

            if df.empty:
                logger.info(f"No data found for date: {trade_date}")
                continue

            bb = [3, 5, 8, 10, 15, 20]
            bins = sorted(set([-x for x in bb if x != 0] + [0] + bb))
            bins = [-float('inf')] + bins + [float('inf')]
            if con.LOGGER_DEBUG:
                logger.debug(f"bins={bins}")
            labels = [f"{bins[i]}%~{bins[i+1]}%" for i in range(len(bins)-1)]
            
            # 对 pct_chg 按斐波那契间隔进行分组
            df['pct_chg_group'] = pd.cut(df['pct_chg'], bins=bins, labels=labels, include_lowest=True)

            # 计算涨跌幅的百分比分布
            pct_chg_distribution = df['pct_chg_group'].value_counts(normalize=True).sort_index() * 100
            results[trade_date] = pct_chg_distribution
            logger.info(f"Distribution for {trade_date}: {pct_chg_distribution}")

        # 返回结果，可以进一步处理
        return results

    except Exception as e:
        logger.error(f"Error fetching or processing stock data: {str(e)}")
        raise

# 调用函数
if __name__ == "__main__":
    print(fetch_and_process_all_stock_data())