import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.da_ak.utils.da_ak_config import daak_config as con
from dags.da_ak.utils.da_ak_util_funcs import DaAkUtilFuncs as daakuf
from dags.utils.config import Config

# Constants
ROLLBACK_DAYS = 7  # Number of days to rollback for recalculating fund data
BATCH_DAYS = 25
DAG_NAME = "da_ak_board_a_dag"  # Name of the DAG

def get_begin_end_date():
    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            # 获取表中最大交易日期
            cursor.execute("SELECT MAX(td) FROM da_ak_board_a_industry_em_daily")
            result = cursor.fetchone()
            end_dt = datetime.now().date()  # 结束日期为今天
            if result[0]:
                start_dt = result[0] - timedelta(days=ROLLBACK_DAYS)
            else:
                start_dt = datetime.strptime(con.ZH_A_DEFAULT_START_DATE, "%Y-%m-%d").date()
            logger.info(f"Calculated date range: {start_dt} to {end_dt}")
            return start_dt, end_dt

def get_stock_df(start_dt, end_dt):
    logger.info(f"Fetching data from {start_dt} to {end_dt}")
    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            # 获取个股交易数据
            cursor.execute("""
                SELECT s_code, td, a
                FROM dg_ak_stock_zh_a_hist_daily_hfq
                WHERE td BETWEEN %s AND %s
            """, (start_dt, end_dt))
            stock_data = cursor.fetchall()
            logger.debug(f"Fetched {len(stock_data)} stock data: \n{stock_data[:5]}")
    stock_data_df = pd.DataFrame(stock_data, columns=['s_code', 'td', 'a'])
    return stock_data_df

def get_board_df(start_dt, end_dt):
    logger.info(f"Fetching data from {start_dt} to {end_dt}")
    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            # 获取板块成分数据
            cursor.execute("""
                SELECT td, b_name, s_code
                FROM dg_ak_stock_board_industry_cons_em
                WHERE td BETWEEN %s AND %s
            """, (start_dt, end_dt))
            board_composition = cursor.fetchall()
            logger.debug(f"Fetched {len(board_composition)} board composition data: \n{board_composition[:5]}")
    board_comp_df = pd.DataFrame(board_composition, columns=['td', 'b_name', 's_code'])
    return board_comp_df

def calculate_a_pre_ma(board_a_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the percentage of total market transaction amount for each board and compute moving averages.

    Args:
        board_a_df (pd.DataFrame): DataFrame containing board names, transaction amounts, and dates.

    Returns:
        pd.DataFrame: DataFrame with calculated percentages and moving averages.
    """
    # 计算每天的市场总交易金额
    total_market_df = board_a_df.groupby('td').agg({'a': 'sum'}).reset_index()
    total_market_df.rename(columns={'a': 'total_market_a'}, inplace=True)
    logger.debug(f"Total {len(total_market_df)} market dataframe: \n{total_market_df.head()}")

    # 合并市场总数据和板块资金数据
    board_fund_df = pd.merge(board_a_df, total_market_df, on='td', how='inner')
    logger.debug(f"Board fund dataframe after merging with total market data: \n{board_fund_df.head()}")

    # 计算每个板块的交易金额占市场总交易金额的百分比
    board_fund_df['a_pre'] = board_fund_df['a'] / board_fund_df['total_market_a']
    logger.debug(f"Board fund dataframe with percentage of total market: \n{board_fund_df.head()}")

    return board_fund_df

def save_board_a_df(board_a_pre_df):
    # 插入结果到数据库
    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            for index, row in board_a_pre_df.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO da_ak_board_a_industry_em_daily (td, b_name, a, a_pre)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (td, b_name)
                        DO UPDATE SET
                            a = EXCLUDED.a,
                            a_pre = EXCLUDED.a_pre;
                    """, (row['td'], row['b_name'], row['a'], row['a_pre']))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error inserting/updating data for {row['td']}, {row['b_name']}: {e}")
                    raise AirflowException(f"Error inserting/updating data for {row['td']}, {row['b_name']}")


def calculate_board_fund_data(ds, **kwargs):
    """
    Calculate fund data for boards and insert into the database.
    """
    start_dt, end_dt = get_begin_end_date()
    logger.info(end_dt)
    batch_size = timedelta(days=BATCH_DAYS)
    current_start_dt = start_dt
    logger.info(current_start_dt)
    
    logger.info(current_start_dt <= end_dt)
    board_comp_df = get_board_df(start_dt, end_dt)
    board_cons_dict = daakuf.gen_board_cons_dict(board_comp_df)
    while current_start_dt <= end_dt:
        current_end_dt = min(current_start_dt + batch_size - timedelta(days=1), end_dt)
        logger.info(f"Processing batch from {current_start_dt} to {current_end_dt}")
        stock_data_df = get_stock_df(current_start_dt, current_end_dt)
        if len(stock_data_df)>0:
            board_a_df = daakuf.calculate_grouped_value_by_date(stock_data_df, board_cons_dict, sum, 's_code', 'a', ['b_name', 'a'])
            board_a_pre_df = calculate_a_pre_ma(board_a_df)
            save_board_a_df(board_a_pre_df)

        # 更新当前处理开始日期
        current_start_dt = current_end_dt + timedelta(days=1)

def generate_dag():
    """
    Generate the Airflow DAG with the default arguments.
    """
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': con.DEFAULT_RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag = DAG(
        DAG_NAME,
        default_args=default_args,
        description='Calculate and sort fund data for boards daily',
        start_date=days_ago(1),
        schedule_interval='0 10 * * *',
        catchup=False,
        tags=['a-akshare', 'fund', 'board'],
        max_active_runs=1,
    )

    # Define the task to calculate and sort fund data
    calculate_and_sort_fund_data_task = PythonOperator(
        task_id='calculate_board_a',
        python_callable=calculate_board_fund_data,
        provide_context=True,
        dag=dag,
    )

    return dag

# Instantiate the DAG
globals()[DAG_NAME] = generate_dag()
