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

from dg_ak.utils.utils import UtilTools as ut
from dg_ak.utils.util_funcs import UtilFuncs as uf
from dg_ak.utils.logger import logger
import dg_ak.utils.config as con

# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# 定义常量
TRACING_TABLE_NAME = 'ak_dg_tracing_s_zh_a'
TRADE_DATE_TABLE_NAME = 'ak_dg_stock_zh_a_trade_date'
STOCK_CODE_NAME_TABLE = 'ak_dg_stock_zh_a_code_name'
DEFAULT_START_DATE = '19800101'

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

def update_tracing_data(ak_func_name: str, period: str, adjust: str, s_code: str, stock_data_df: pd.DataFrame):
    try:
        last_td = stock_data_df['td'].max()
        if not last_td:
            return

        date_value = (ak_func_name, s_code, period, adjust, last_td, datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))
        
        insert_sql = f"""
            INSERT INTO {TRACING_TABLE_NAME} (ak_func_name, scode, period, adjust, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, scode, period, adjust) DO UPDATE 
            SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time;
        """
        cursor = pg_conn.cursor()
        cursor.execute(insert_sql, date_value)
        pg_conn.commit()
        cursor.close()
        logger.info(f"Tracing data updated for s_code={s_code} with last_td={last_td}")
    except Exception as e:
        logger.error(f"Failed to update tracing data for s_code={s_code}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def update_all_trade_dates(trade_dates: list[str]):
    try:
        if LOGGER_DEBUG:
            logger.debug(f"Trade dates to update: {len(trade_dates)}, first 5 dates: {trade_dates[:5]}")
        trade_dates = list(set(trade_dates))  # 去重
        trade_dates.sort()  # 排序
        insert_date_sql = f"""
            INSERT INTO {TRADE_DATE_TABLE_NAME} (trade_date, create_time, update_time)
            VALUES (%s, NOW(), NOW())
            ON CONFLICT (trade_date) DO NOTHING;
        """
        cursor = pg_conn.cursor()
        cursor.executemany(insert_date_sql, [(date,) for date in trade_dates])
        pg_conn.commit()
        cursor.close()
        logger.info("All trade dates updated successfully.")
    except Exception as e:
        logger.error(f"Failed to update trade dates: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def insert_code_name_to_db(code_name_list: list[tuple[str, str]]):
    try:
        cursor = pg_conn.cursor()

        sql = f"""
            INSERT INTO {STOCK_CODE_NAME_TABLE} (s_code, s_name, create_time, update_time)
            VALUES (%s, %s, NOW(), NOW())
            ON CONFLICT (s_code) DO UPDATE 
            SET s_name = EXCLUDED.s_name, update_time = EXCLUDED.update_time;
        """
        cursor.executemany(sql, code_name_list)
        pg_conn.commit()
        cursor.close()
        logger.info(f"s_code and s_name inserted into {STOCK_CODE_NAME_TABLE} successfully.")
    except Exception as e:
        logger.error(f"Failed to insert s_code and s_name into {STOCK_CODE_NAME_TABLE}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def init_ak_dg_s_zh_a(ak_func_name: str, period: str, adjust: str):
    try:
        logger.info(f"Initializing data for {ak_func_name} with period={period} and adjust={adjust}")
        s_code_name_list = uf.get_s_code_name_list(redis_hook.get_conn())
        total_codes = len(s_code_name_list)
        all_trade_dates = set()

        # 定义 ak_cols_config_dict
        config_path = current_dir / 'ak_dg_s-zh-a_config.py'
        ak_cols_config_dict = uf.load_ak_cols_config(config_path.as_posix())

        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        for index, (s_code, s_name) in enumerate(s_code_name_list):
            if LOGGER_DEBUG and index >= 5:
                break
            logger.info(f'({index + 1}/{total_codes}) Fetching data for s_code={s_code}, s_name={s_name}')
            if adjust == 'bfq':
                stock_data_df = uf.get_s_code_data(
                    ak_func_name, ak_cols_config_dict, s_code, period, DEFAULT_START_DATE, yesterday_date, None
                )
            else:
                stock_data_df = uf.get_s_code_data(
                    ak_func_name, ak_cols_config_dict, s_code, period, DEFAULT_START_DATE, yesterday_date, adjust
                )

            if not stock_data_df.empty:
                stock_data_df['s_code'] = stock_data_df['s_code'].astype(str)
                stock_data_df = uf.convert_columns(stock_data_df, f'ak_dg_{ak_func_name}_store_{period}_{adjust}', pg_conn, redis_hook.get_conn())
                if 'td' in stock_data_df.columns:
                    stock_data_df['td'] = pd.to_datetime(stock_data_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')

                insert_data_to_db(stock_data_df, f'ak_dg_{ak_func_name}_store_{period}_{adjust}')
                update_tracing_data(ak_func_name, period, adjust, s_code, stock_data_df)
                all_trade_dates.update(stock_data_df['td'].tolist())

                if LOGGER_DEBUG:
                    logger.debug(f"Stock data for s_code={s_code}: {stock_data_df.shape[0]} rows\n{stock_data_df.head(5).to_string(index=False)}")

        insert_code_name_to_db(s_code_name_list)
        if LOGGER_DEBUG:
            logger.debug(f"Final trade dates list length: {len(all_trade_dates)}, first 5 dates: {list(all_trade_dates)[:5]}")
        update_all_trade_dates(list(all_trade_dates))

    except Exception as e:
        logger.error(f"Failed to initialize data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

if __name__ == "__main__":
    current_dir = Path(__file__).resolve().parent
    sql_file_path = current_dir / 'create_s-zh-a_tables.sql'
    read_and_execute_sql(sql_file_path)

    ak_func_name = 'stock_zh_a_hist'
    period = 'daily'
    for adjust in ['bfq', 'hfq']:
        init_ak_dg_s_zh_a(ak_func_name, period, adjust)
