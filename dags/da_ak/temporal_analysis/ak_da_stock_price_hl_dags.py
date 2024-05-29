from __future__ import annotations

import sys
import os
import socket
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook

# 动态添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

from dags.dg_ak.utils.dg_ak_util_funcs import UtilFuncs as uf
from dags.utils.logger import logger
import dags.utils.config as con

# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# 定义常量
TRACING_TABLE_NAME = 'xxx'
PRICE_HL_TABLE_NAME = 'ak_da_stock_price_hl'

def read_and_execute_sql(file_path: str):
    """读取 SQL 文件并执行其中的 SQL 语句"""
    try:
        with open(file_path, 'r') as file:
            sql_statements = file.read()
        cursor = pg_conn.cursor()
        cursor.execute(sql_statements)
        pg_conn.commit()
        cursor.close()
        logger.info("SQL statements executed successfully.")
    except Exception as e:
        logger.error(f"Failed to execute SQL statements: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def insert_data_to_db(df: pd.DataFrame, table_name: str, batch_size: int = 5000):
    """将数据批量插入数据库"""
    try:
        cursor = pg_conn.cursor()
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        data_tuples = [tuple(x) for x in df.to_numpy()]
        
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            cursor.executemany(sql, batch)
        
        pg_conn.commit()
        cursor.close()
        logger.info(f"Data inserted into {table_name} successfully.")
    except Exception as e:
        logger.error(f"Failed to insert data into {table_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def calculate_price_hl(stock_data: pd.DataFrame, intervals: list[int]) -> pd.DataFrame:
    """计算股票的历史高点、历史低点、目标高点和目标低点"""
    results = []
    for i, row in stock_data.iterrows():
        date = row['td']
        close = row['c']
        s_code = row['s_code']
        for interval in intervals:
            if i >= interval:
                historical_high = stock_data['h'][i-interval:i].max()
                historical_low = stock_data['l'][i-interval:i].min()
                distance_historical_high = interval - stock_data['h'][i-interval:i].idxmax() + i
                distance_historical_low = interval - stock_data['l'][i-interval:i].idxmin() + i
                change_from_historical = close - historical_high if close >= historical_high else close - historical_low
                pct_chg_from_historical = change_from_historical / historical_high if close >= historical_high else change_from_historical / historical_low

                target_high = stock_data['h'][i:i+interval].max() if i + interval < len(stock_data) else None
                target_low = stock_data['l'][i:i+interval].min() if i + interval < len(stock_data) else None
                distance_target_high = stock_data['h'][i:i+interval].idxmax() - i if target_high else None
                distance_target_low = stock_data['l'][i:i+interval].idxmin() - i if target_low else None
                change_to_target = target_high - close if target_high else target_low - close if target_low else None
                pct_chg_to_target = change_to_target / close if change_to_target else None

                result = {
                    's_code': s_code,
                    'td': date,
                    'interval': interval,
                    'historical_high': historical_high,
                    'historical_low': historical_low,
                    'distance_historical_high': distance_historical_high,
                    'distance_historical_low': distance_historical_low,
                    'change_from_historical': change_from_historical,
                    'pct_chg_from_historical': pct_chg_from_historical,
                    'target_high': target_high,
                    'target_low': target_low,
                    'distance_target_high': distance_target_high,
                    'distance_target_low': distance_target_low,
                    'change_to_target': change_to_target,
                    'pct_chg_to_target': pct_chg_to_target
                }
                results.append(result)
    return pd.DataFrame(results)

def init_ak_dg_price_hl(ak_func_name: str, period: str, adjust: str, intervals: list[int]):
    try:
        logger.info(f"Initializing data for {ak_func_name} with period={period} and adjust={adjust}")
        s_code_name_list = uf.get_s_code_name_list(redis_hook.get_conn())
        total_codes = len(s_code_name_list)

        for index, (s_code, s_name) in enumerate(s_code_name_list):
            if LOGGER_DEBUG and index >= 5:
                break
            logger.info(f'({index + 1}/{total_codes}) Fetching data for s_code={s_code}, s_name={s_name}')
            stock_data_df = uf.get_stock_data(s_code, adjust, pg_conn)
            if not stock_data_df.empty:
                stock_data_df['td'] = pd.to_datetime(stock_data_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')
                price_hl_df = calculate_price_hl(stock_data_df, intervals)
                insert_data_to_db(price_hl_df, PRICE_HL_TABLE_NAME)

    except Exception as e:
        logger.error(f"Failed to initialize data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

if __name__ == "__main__":
    # 读取 SQL 文件并执行，确保表结构存在
    sql_file_path = current_dir / 'create_tables.sql'
    read_and_execute_sql(sql_file_path)

    ak_func_name = 'stock_zh_a_hist'
    period = 'daily'
    intervals = [3, 5, 8, 13, 21, 34, 55, 89, 144, 233]  # 斐波那契间隔示例
    for adjust in ['bfq', 'hfq']:
        init_ak_dg_price_hl(ak_func_name, period, adjust, intervals)
