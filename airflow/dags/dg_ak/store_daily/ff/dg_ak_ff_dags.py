import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.da_ak.utils.da_ak_config import daak_config as con
from dags.utils.config import Config

# Constants
ROLLBACK_DAYS = 7  # Number of days to rollback for recalculating fund data
DAG_NAME = "da_ak_board_a_dag"  # Name of the DAG

def get_dates():
    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            # 获取表中最大交易日期
            cursor.execute("SELECT MAX(td) FROM da_ak_board_ff_industry_em_daily")
            result = cursor.fetchone()
            end_dt = datetime.now().date()  # 结束日期为今天
            if result[0]:
                start_dt = result[0] - timedelta(days=ROLLBACK_DAYS)
            else:
                start_dt = datetime.strptime(con.ZH_A_DEFAULT_START_DATE, "%Y-%m-%d").date()
            logger.info(f"Calculated date range: {start_dt} to {end_dt}")
            return start_dt, end_dt

def calculate_ma(series, window):
    """
    Calculate the moving average for a specified column over a given window.
    """
    return series.rolling(window=window).mean()


def merge_board_and_stock_data(board_comp_df, stock_data_df):
    """
    合并板块成分数据和个股交易数据。

    参数:
    board_comp_df (pd.DataFrame): 板块成分数据的 DataFrame。
    stock_data_df (pd.DataFrame): 个股交易数据的 DataFrame。

    返回:
    pd.DataFrame: 合并后的 DataFrame。
    """
    # 将所有日期列转换为datetime类型
    board_comp_df['td'] = pd.to_datetime(board_comp_df['td'])
    stock_data_df['td'] = pd.to_datetime(stock_data_df['td'])

    # 确保没有重复标签并填充缺失的板块成分数据
    board_comp_df = board_comp_df.drop_duplicates(subset=['td', 'b_name', 's_code'])
    logger.debug(f"Board composition dataframe after removing duplicates: \n{board_comp_df.head()}")

    trading_days = stock_data_df['td'].drop_duplicates().sort_values()

    # 创建一个完整的 DataFrame 包含所有日期和板块名称
    complete_df = pd.MultiIndex.from_product([trading_days, board_comp_df['b_name'].unique()], names=['td', 'b_name']).to_frame(index=False)
    logger.debug(f"Complete dataframe with all dates and board names: \n{complete_df.head()}")

    # 合并完整的日期和板块 DataFrame 与原始板块成分 DataFrame
    board_comp_df = pd.merge(complete_df, board_comp_df, on=['td', 'b_name'], how='left')
    board_comp_df = board_comp_df.groupby('b_name').apply(lambda group: group.ffill()).reset_index(drop=True)
    logger.debug(f"Board composition dataframe after forward fill: \n{board_comp_df.head()}")

    # 合并板块成分和个股交易数据
    merged_df = pd.merge(board_comp_df, stock_data_df, on=['s_code', 'td'], how='inner')
    logger.debug(f"Merged dataframe with board composition and stock data: \n{merged_df.head()}")

    return merged_df


def calculate_fund_metrics(merged_df):
    """
    计算板块的资金和相关指标。

    参数:
    merged_df (pd.DataFrame): 合并后的 DataFrame。

    返回:
    pd.DataFrame: 计算结果的 DataFrame。
    """
    # 按板块名称和日期分组，然后汇总交易金额
    board_fund_df = merged_df.groupby(['td', 'b_name']).agg({'a': 'sum'}).reset_index()
    logger.debug(f"Total {len(board_fund_df)} board fund dataframe: \n{board_fund_df.head()}")

    # 计算每天的市场总交易金额
    total_market_df = board_fund_df.groupby('td').agg({'a': 'sum'}).reset_index()
    total_market_df.rename(columns={'a': 'total_market_a'}, inplace=True)
    logger.debug(f"Total {len(total_market_df)} market dataframe: \n{total_market_df.head()}")

    # 合并市场总数据和板块资金数据
    board_fund_df = pd.merge(board_fund_df, total_market_df, on='td', how='inner')
    logger.debug(f"Board fund dataframe after merging with total market data: \n{board_fund_df.head()}")

    # 计算每个板块的交易金额占市场总交易金额的百分比
    board_fund_df['a_pre'] = board_fund_df['a'] / board_fund_df['total_market_a']
    logger.debug(f"Board fund dataframe with percentage of total market: \n{board_fund_df.head()}")

    # 计算移动平均值
    for window in [5, 8, 13, 34, 55, 144, 233]:
        ma_column = f'a_pre_ma{window}'
        board_fund_df[ma_column] = board_fund_df.groupby('b_name')['a_pre'].transform(lambda x: calculate_ma(x, window))
        logger.debug(f"Board fund dataframe with {ma_column}: \n{board_fund_df[[ma_column]].dropna().head()}")

    return board_fund_df


def calculate_board_fund_data(ds, **kwargs):
    """
    Calculate fund data for boards and insert into the database.
    """
    begin_dt, end_dt = get_dates()  # 计算开始和结束日期
    logger.info(f"Calculating fund data for boards from {begin_dt} to {end_dt}")

    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            # 获取板块成分数据
            cursor.execute("""
                SELECT td, b_name, s_code
                FROM dg_ak_stock_board_industry_cons_em
                WHERE td BETWEEN %s AND %s
            """, (begin_dt, end_dt))
            board_composition = cursor.fetchall()
            logger.debug(f"Fetched {len(board_composition)} board composition data: \n{board_composition[:5]}")

            # 获取个股交易数据
            cursor.execute("""
                SELECT s_code, td, a
                FROM dg_ak_stock_zh_a_hist_daily_hfq
                WHERE td BETWEEN %s AND %s
            """, (begin_dt, end_dt))
            stock_data = cursor.fetchall()
            logger.debug(f"Fetched {len(stock_data)} stock data: \n{stock_data[:5]}")

    board_comp_df = pd.DataFrame(board_composition, columns=['td', 'b_name', 's_code'])
    stock_data_df = pd.DataFrame(stock_data, columns=['s_code', 'td', 'a'])

    # 调用合并函数
    merged_df = merge_board_and_stock_data(board_comp_df, stock_data_df)

    # 调用计算函数
    board_fund_df = calculate_fund_metrics(merged_df)

    # 插入结果到数据库
    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            for index, row in board_fund_df.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO da_ak_board_ff_industry_em_daily (td, b_name, a, a_pre, a_pre_ma5, a_pre_ma8, a_pre_ma13, a_pre_ma34, a_pre_ma55, a_pre_ma144, a_pre_ma233)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (td, b_name)
                        DO UPDATE SET
                            a = EXCLUDED.a,
                            a_pre = EXCLUDED.a_pre,
                            a_pre_ma5 = EXCLUDED.a_pre_ma5,
                            a_pre_ma8 = EXCLUDED.a_pre_ma8,
                            a_pre_ma13 = EXCLUDED.a_pre_ma13,
                            a_pre_ma34 = EXCLUDED.a_pre_ma34,
                            a_pre_ma55 = EXCLUDED.a_pre_ma55,
                            a_pre_ma144 = EXCLUDED.a_pre_ma144,
                            a_pre_ma233 = EXCLUDED.a_pre_ma233;
                    """, (row['td'], row['b_name'], row['a'], row['a_pre'], row['a_pre_ma5'], row['a_pre_ma8'], row['a_pre_ma13'], row['a_pre_ma34'], row['a_pre_ma55'], row['a_pre_ma144'], row['a_pre_ma233']))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error inserting/updating data for {row['td']}, {row['b_name']}: {e}")
                    raise AirflowException(f"Error inserting/updating data for {row['td']}, {row['b_name']}")



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
        task_id='calculate_and_sort_fund_data',
        python_callable=calculate_board_fund_data,
        provide_context=True,
        dag=dag,
    )

    return dag

# Instantiate the DAG
globals()[DAG_NAME] = generate_dag()
