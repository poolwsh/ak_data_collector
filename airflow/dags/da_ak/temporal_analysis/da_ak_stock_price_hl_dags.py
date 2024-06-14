import os
import sys
import random
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
sys.path.append(project_root)

from dags.da_ak.utils.da_ak_util_funcs import DaAkUtilFuncs as dauf
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.da_ak.utils.da_ak_config import daak_config as con

DEBUG_MODE = con.DEBUG_MODE

PRICE_HL_TABLE_NAME = 'da_ak_stock_price_hl_store'
TEMP_PRICE_HL_TABLE_NAME = 'da_ak_stock_price_hl'
TRACING_TABLE_NAME = 'da_ak_tracing_stock_price_hl'
MIN_INTERVAL = 3
NONE_RESULT = 'NULL'
ROUND_N = 5
CSV_ROOT = '/tmp/ak_da/p_hl'
os.makedirs(CSV_ROOT, exist_ok=True)

def get_stock_data(s_code: str) -> pd.DataFrame:
    sql = f"""
        SELECT * FROM ak_dg_stock_zh_a_hist_store_daily_hfq 
        WHERE s_code = '{s_code}';
    """
    try:
        conn = PGEngine.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        df[['l', 'o', 'c', 'h']] = df[['l', 'o', 'c', 'h']].astype(float)
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data for s_code={s_code}: {str(e)}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)


def clear_table(conn, table_name):
    try:
        _cursor = conn.cursor()
        _cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()
        logger.info(f"Table {table_name} cleared successfully.")
        _cursor.close()
    except Exception as _e:
        conn.rollback()
        logger.error(f"Failed to clear table {table_name}: {_e}")
        raise AirflowException(_e)


def insert_or_update_data_from_csv(csv_path):
    if not os.path.exists(csv_path):
        logger.error("CSV file does not exist.")
        return
    try:
        conn = PGEngine.get_conn()
        _cursor = conn.cursor()
 
        clear_table(conn, TEMP_PRICE_HL_TABLE_NAME)

        with open(csv_path, 'r') as _file:
            _copy_sql = f"COPY {TEMP_PRICE_HL_TABLE_NAME} FROM STDIN WITH CSV HEADER DELIMITER ','"
            _cursor.copy_expert(sql=_copy_sql, file=_file)

        _cursor.execute(f"""
            INSERT INTO {PRICE_HL_TABLE_NAME} 
            SELECT * FROM {TEMP_PRICE_HL_TABLE_NAME}
            ON CONFLICT (s_code, td, interval) DO UPDATE SET
                hs_h = EXCLUDED.hs_h,
                hs_l = EXCLUDED.hs_l,
                d_hs_h = EXCLUDED.d_hs_h,
                d_hs_l = EXCLUDED.d_hs_l,
                chg_from_hs = EXCLUDED.chg_from_hs,
                pct_chg_from_hs = EXCLUDED.pct_chg_from_hs,
                tg_h = EXCLUDED.tg_h,
                tg_l = EXCLUDED.tg_l,
                d_tg_h = EXCLUDED.d_tg_h,
                d_tg_l = EXCLUDED.d_tg_l,
                chg_to_tg = EXCLUDED.chg_to_tg,
                pct_chg_to_tg = EXCLUDED.pct_chg_to_tg
        """)
        
        conn.commit()
        logger.info(f"Data from {csv_path} successfully loaded and updated into {PRICE_HL_TABLE_NAME}.")
        
        _cursor.close()
    except Exception as _e:
        conn.rollback()
        logger.error(f"Failed to load data from CSV: {_e}")
        raise AirflowException(_e)
    finally:
        if conn:
            PGEngine.release_conn(conn)



def insert_or_update_tracing_data(s_code: str, min_td: str, max_td: str):
    try:
        conn = PGEngine.get_conn()
        host_name = os.uname().nodename
        logger.info(f'storing tracing data of s_code={s_code}, min_td={min_td}, max_td={max_td}')
        with conn.cursor() as cursor:
            sql = f"""
                INSERT INTO {TRACING_TABLE_NAME} (s_code, min_td, max_td, host_name)
                VALUES ('{s_code}', '{min_td}', '{max_td}', '{host_name}')
                ON CONFLICT (s_code) DO UPDATE SET
                min_td = EXCLUDED.min_td,
                max_td = EXCLUDED.max_td,
                host_name = EXCLUDED.host_name,
                update_time = CURRENT_TIMESTAMP;
            """
            cursor.execute(sql)
            conn.commit()
        logger.info(f"Tracing data inserted/updated for s_code={s_code}.")
    except Exception as e:
        logger.error(f"Failed to insert/update tracing data for s_code={s_code}: {str(e)}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

def get_tracing_data(s_code: str) -> tuple:
    try:
        sql = f"""
            SELECT min_td, max_td FROM {TRACING_TABLE_NAME} WHERE s_code = '{s_code}';
        """
        conn = PGEngine.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                return result[0], result[1]
            else:
                return None, None
    except Exception as e:
        logger.error(f"Failed to fetch tracing data for s_code={s_code}: {str(e)}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

def generate_fibonacci_intervals(n: int) -> list[int]:
    fibs = [1, 2]
    while fibs[-1] < n:
        fibs.append(fibs[-1] + fibs[-2])
    intervals = [f for f in fibs if f >= MIN_INTERVAL and f < n]
    return intervals


def process_and_store_data():
    logger.info("Starting to process and store data.")
    s_code_name_list = dauf.get_s_code_name_list(task_cache_conn)
    if not DEBUG_MODE:
        random.shuffle(s_code_name_list)
    
    for index, stock_code in enumerate(s_code_name_list):
        logger.info(f"Processing {index + 1}/{len(s_code_name_list)}: {stock_code}")
        s_code = stock_code[0]

        min_td, max_td = get_tracing_data(s_code)

        stock_data_df = get_stock_data(s_code)
        if stock_data_df.empty:
            logger.info(f"No stock data found for s_code {s_code}")
            continue
        
        current_min_td, current_max_td = stock_data_df['td'].min(), stock_data_df['td'].max()

        if min_td == current_min_td and max_td == current_max_td:
            logger.info(f"Data for s_code {s_code} already processed. Skipping.")
            continue

        if DEBUG_MODE:
            logger.debug(f"Fetched data for s_code {s_code}: \n{stock_data_df.head(3)}")

        intervals = generate_fibonacci_intervals(len(stock_data_df))
        price_hl_df = process_stock_data_internal(s_code, stock_data_df, intervals)

        price_hl_df = price_hl_df.replace("NULL", "")
        
        csv_file_path = os.path.join(CSV_ROOT, f'{s_code}.csv')
        price_hl_df.to_csv(csv_file_path, index=False)
        insert_or_update_data_from_csv(csv_file_path)
        if not DEBUG_MODE:
            os.remove(csv_file_path)
        insert_or_update_tracing_data(s_code, current_min_td, current_max_td)


def process_stock_data_internal(s_code: str, stock_data_df: pd.DataFrame, intervals: list[int]) -> pd.DataFrame:
    if not stock_data_df.empty:
        stock_data_df['td'] = pd.to_datetime(stock_data_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')
        logger.info(f'Starting calculation for {s_code}.')
        if DEBUG_MODE:
            logger.debug(f"\nintervals={intervals}")
        price_hl_df = calculate_price_hl(stock_data_df, intervals)
        if DEBUG_MODE:
            logger.debug(f"Processed data for s_code {s_code}: \n{price_hl_df.head(3)}")
        return price_hl_df


def calculate_price_hl(stock_data: pd.DataFrame, intervals: list[int]) -> pd.DataFrame:
    s_code = stock_data['s_code'][0]
    results = []
    
    stock_data_np = stock_data[['td', 'c', 's_code']].to_numpy()
    dates = stock_data_np[:, 0]
    prices = stock_data_np[:, 1].astype(float)
    s_codes = stock_data_np[:, 2]

    progress_bar = tqdm(range(len(prices)), desc="Calculating price HL")
    for i in progress_bar:
        for interval in intervals:
            if i >= interval or i + interval < len(prices):
                row = calculate_c(prices, i, interval, s_codes[i], dates[i])
                if row:
                    results.append(row)
    
    if DEBUG_MODE:
        logger.debug(f"Calculated price HL for s_code={s_code}")
    return pd.DataFrame(results)


def calculate_c(prices: np.ndarray, i: int, interval: int, s_code: str, date: str) -> dict:
    historical_high = prices[i-interval:i].max() if i >= interval else None
    historical_low = prices[i-interval:i].min() if i >= interval else None
    distance_historical_high = i - np.argmax(prices[i-interval:i]) if historical_high is not None else None
    distance_historical_low = i - np.argmin(prices[i-interval:i]) if historical_low is not None else None
    change_from_historical = prices[i] - historical_high if historical_high and prices[i] >= historical_high else (prices[i] - historical_low if historical_low else None)
    pct_chg_from_historical = change_from_historical / historical_high if historical_high and prices[i] >= historical_high else (change_from_historical / historical_low if historical_low else None)

    target_high = prices[i:i+interval].max() if i + interval <= len(prices) else None
    target_low = prices[i:i+interval].min() if i + interval <= len(prices) else None
    distance_target_high = np.argmax(prices[i:i+interval]) if target_high is not None else None
    distance_target_low = np.argmin(prices[i:i+interval]) if target_low is not None else None
    change_to_target = target_high - prices[i] if target_high is not None else (target_low - prices[i] if target_low is not None else None)
    pct_chg_to_tg = change_to_target / prices[i] if change_to_target is not None else None

    return {
        's_code': s_code,
        'td': date,
        'interval': interval,
        'hs_h': round(historical_high, ROUND_N) if historical_high is not None else NONE_RESULT,
        'hs_l': round(historical_low, ROUND_N) if historical_low is not None else NONE_RESULT,
        'd_hs_h': distance_historical_high if distance_historical_high is not None else NONE_RESULT,
        'd_hs_l': distance_historical_low if distance_historical_low is not None else NONE_RESULT,
        'chg_from_hs': round(change_from_historical, ROUND_N) if change_from_historical is not None else NONE_RESULT,
        'pct_chg_from_hs': round(pct_chg_from_historical, ROUND_N) if pct_chg_from_historical is not None else NONE_RESULT,
        'tg_h': round(target_high, ROUND_N) if target_high is not None else NONE_RESULT,
        'tg_l': round(target_low, ROUND_N) if target_low is not None else NONE_RESULT,
        'd_tg_h': distance_target_high if distance_target_high is not None else NONE_RESULT,
        'd_tg_l': distance_target_low if distance_target_low is not None else NONE_RESULT,
        'chg_to_tg': round(change_to_target, ROUND_N) if change_to_target is not None else NONE_RESULT,
        'pct_chg_to_tg': round(pct_chg_to_tg, ROUND_N) if pct_chg_to_tg is not None else NONE_RESULT
    }



def generate_dag():
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = "股价高低点"

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'计算股票历史高低点数据',
        start_date=days_ago(1),
        schedule=dauf.generate_random_minute_schedule(hour=8),
        catchup=False,
        tags=['akshare', '个股价格', '横向'],
        max_active_runs=1,
    )

    process_and_store_data_task = PythonOperator(
        task_id='process_and_store_data',
        python_callable=process_and_store_data,
        dag=dag,
    )

    return dag

globals()['ak_dg_price_hl'] = generate_dag()

if __name__ == "__main__":
    process_and_store_data()
