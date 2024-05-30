import os
import sys
import time
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import psycopg2
from sqlalchemy import create_engine
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook

# 动态添加项目根目录到Python路径
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
sys.path.append(project_root)

from dags.da_ak.utils.da_ak_util_funcs import DaAkUtilFuncs as dauf
from utils.logger import logger
import utils.config as con

# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# 定义常量
PRICE_HL_TABLE_NAME = 'ak_da_stock_price_hl'
TRACING_TABLE_NAME = 'ak_da_tracing_stock_price_hl'
MIN_INTERVAL = 3
NONE_RESULT = 'NULL'
ROUND_N = 5

def get_stock_data(s_code: str) -> pd.DataFrame:
    sql = f"""
        SELECT * FROM ak_dg_stock_zh_a_hist_store_daily_hfq 
        WHERE s_code = '{s_code}';
    """
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        df[['l', 'o', 'c', 'h']] = df[['l', 'o', 'c', 'h']].astype(float)
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data for s_code={s_code}: {str(e)}")
        raise AirflowException(e)

def insert_data_to_db(df: pd.DataFrame, table_name: str, retries: int = 3, delay: int = 5):
    if df.empty:
        logger.warning("No data to insert into database.")
        return

    attempt = 0
    while attempt < retries:
        try:
            df = df.round(ROUND_N)
            df = df.where(pd.notnull(df), None)  # Replace NaN with None

            with pg_conn.cursor() as cursor:
                for index, row in df.iterrows():
                    sql = f"""
                        INSERT INTO {table_name} (s_code, td, interval, hs_h, hs_l, d_hs_h, d_hs_l, chg_from_hs, pct_chg_from_hs, tg_h, tg_l, d_tg_h, d_tg_l, chg_to_tg, pct_chg_to_tg)
                        VALUES ('{row['s_code']}', '{row['td']}', {row['interval']}, {row['hs_h']}, {row['hs_l']}, {row['d_hs_h']}, {row['d_hs_l']}, {row['chg_from_hs']}, {row['pct_chg_from_hs']}, {row['tg_h']}, {row['tg_l']}, {row['d_tg_h']}, {row['d_tg_l']}, {row['chg_to_tg']}, {row['pct_chg_to_tg']})
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
                        pct_chg_to_tg = EXCLUDED.pct_chg_to_tg;
                    """
                    cursor.execute(sql)
                pg_conn.commit()
            logger.info(f"Data inserted into {table_name} successfully.")
            return
        except psycopg2.OperationalError as e:
            attempt += 1
            logger.error(f"Failed to insert data into {table_name}, attempt {attempt}/{retries}: {str(e)}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"All {retries} retries failed.")
                logger.error(f"Data that caused the error: {df.head()}")
                raise AirflowException(e)
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {str(e)}")
            logger.error(f"Data that caused the error: {df.head()}")
            raise AirflowException(e)

def insert_or_update_tracing_data(s_code: str, min_td: str, max_td: str, host_name: str):
    try:
        with pg_conn.cursor() as cursor:
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
            pg_conn.commit()
        logger.info(f"Tracing data inserted/updated for s_code={s_code}.")
    except Exception as e:
        logger.error(f"Failed to insert/update tracing data for s_code={s_code}: {str(e)}")
        raise AirflowException(e)

def get_tracing_data(s_code: str) -> tuple:
    try:
        sql = f"""
            SELECT min_td, max_td FROM {TRACING_TABLE_NAME} WHERE s_code = '{s_code}';
        """
        with pg_conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                return result[0], result[1]
            else:
                return None, None
    except Exception as e:
        logger.error(f"Failed to fetch tracing data for s_code={s_code}: {str(e)}")
        raise AirflowException(e)

def generate_fibonacci_intervals(n: int) -> list[int]:
    fibs = [1, 2]
    while fibs[-1] < n:
        fibs.append(fibs[-1] + fibs[-2])
    intervals = [f for f in fibs if f >= MIN_INTERVAL and f < n]
    return intervals


def process_and_store_data():
    logger.info("Starting to process and store data.")
    # 获取股票列表
    s_code_name_list = dauf.get_s_code_name_list(redis_hook.get_conn())

    for index, stock_code in enumerate(s_code_name_list):
        logger.info(f"Processing {index + 1}/{len(s_code_name_list)}: {stock_code}")
        s_code = stock_code[0]
        s_name = stock_code[1]

        # 获取追踪数据
        min_td, max_td = get_tracing_data(s_code)

        stock_data_df = get_stock_data(s_code)
        if stock_data_df.empty:
            logger.info(f"No stock data found for s_code {s_code}")
            continue
        
        current_min_td, current_max_td = stock_data_df['td'].min(), stock_data_df['td'].max()

        # 如果当前数据已经计算过了，则跳过
        if min_td == current_min_td and max_td == current_max_td:
            logger.info(f"Data for s_code {s_code} already processed. Skipping.")
            continue

        if LOGGER_DEBUG:
            logger.debug(f"Fetched data for s_code {s_code}: \n{stock_data_df.head(3)}")

        intervals = generate_fibonacci_intervals(len(stock_data_df))
        price_hl_df = process_stock_data_internal(s_code, stock_data_df, intervals)

        if not price_hl_df.empty:
            insert_data_to_db(price_hl_df, PRICE_HL_TABLE_NAME)
            min_td, max_td = price_hl_df['td'].min(), price_hl_df['td'].max()
            host_name = os.uname().nodename
            insert_or_update_tracing_data(s_code, min_td, max_td, host_name)
        else:
            logger.info(f"No data to insert for s_code {s_code}")



def process_stock_data_internal(s_code: str, stock_data_df: pd.DataFrame, intervals: list[int]) -> pd.DataFrame:
    if not stock_data_df.empty:
        stock_data_df['td'] = pd.to_datetime(stock_data_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')
        logger.info(f'Starting calculate of {s_code}.')
        if con.LOGGER_DEBUG:
            logger.debug(f"\nintervals={intervals}")
        price_hl_df = calculate_price_hl(stock_data_df, intervals)
        if LOGGER_DEBUG:
            logger.debug(f"Processed data for s_code {s_code}: \n{price_hl_df.head(3)}")
        return price_hl_df

def calculate_price_hl(stock_data: pd.DataFrame, intervals: list[int]) -> pd.DataFrame:
    s_code = stock_data['s_code'][0]
    results = []
    stock_data = stock_data[['td', 'c', 's_code']]
    
    progress_bar = tqdm(range(len(stock_data)), desc="Calculating price HL")
    for i in progress_bar:
        progress_bar.set_description(f"({i}/{len(stock_data)}) Calculating price HL")
        
        for interval in intervals:
            if i >= interval or i + interval < len(stock_data):
                row = calculate_c(stock_data, i, interval)
                if row:
                    results.append(row)

    if con.LOGGER_DEBUG:
        logger.debug(f"Calculated price HL for s_code={s_code}")
    return pd.DataFrame(results)


def calculate_c(stock_data: pd.DataFrame, i: int, interval: int) -> dict:
    """计算单行数据"""
    # if con.LOGGER_DEBUG:
    #     logger.debug(f"len_df={len(stock_data)}, i={i} current_interval={interval}")
        # logger.debug(f"stock_data in calculate_c:\n{stock_data.iloc[max(0, i-interval-5):min(len(stock_data), i+5)]}")

    historical_high = float(stock_data['c'][i-interval:i].max()) if i >= interval else None
    historical_low = float(stock_data['c'][i-interval:i].min()) if i >= interval else None
    distance_historical_high = i - stock_data['c'][i-interval:i].idxmax() if historical_high else None
    distance_historical_low = i - stock_data['c'][i-interval:i].idxmin() if historical_low else None
    change_from_historical = stock_data['c'][i] - historical_high if historical_high and stock_data['c'][i] >= historical_high else (stock_data['c'][i] - historical_low if historical_low else None)
    pct_chg_from_historical = change_from_historical / historical_high if historical_high and stock_data['c'][i] >= historical_high else (change_from_historical / historical_low if historical_low else None)

    target_high = float(stock_data['c'][i:i+interval].max()) if i + interval <= len(stock_data) else None
    target_low = float(stock_data['c'][i:i+interval].min()) if i + interval <= len(stock_data) else None
    distance_target_high = stock_data['c'][i:i+interval].idxmax() - i if target_high else None
    distance_target_low = stock_data['c'][i:i+interval].idxmin() - i if target_low else None
    change_to_target = target_high - stock_data['c'][i] if target_high else (target_low - stock_data['c'][i] if target_low else None)
    pct_chg_to_tg = change_to_target / stock_data['c'][i] if change_to_target else None

    # if con.LOGGER_DEBUG:
    #     logger.debug(f"historical_high={historical_high}, historical_low={historical_low}")
    #     logger.debug(f"distance_historical_high={distance_historical_high}, distance_historical_low={distance_historical_low}")
    #     logger.debug(f"change_from_historical={change_from_historical}, pct_chg_from_historical={pct_chg_from_historical}")
    #     logger.debug(f"target_high={target_high}, target_low={target_low}")
    #     logger.debug(f"distance_target_high={distance_target_high}, distance_target_low={distance_target_low}")
    #     logger.debug(f"change_to_target={change_to_target}, pct_chg_to_tg={pct_chg_to_tg}")

    return {
        's_code': stock_data['s_code'][i],
        'td': stock_data['td'][i],
        'interval': interval,
        'hs_h': round(historical_high, ROUND_N) if historical_high else NONE_RESULT,
        'hs_l': round(historical_low, ROUND_N) if historical_low else NONE_RESULT,
        'd_hs_h': distance_historical_high if distance_historical_high is not None else NONE_RESULT,
        'd_hs_l': distance_historical_low if distance_historical_low is not None else NONE_RESULT,
        'chg_from_hs': round(change_from_historical, ROUND_N) if change_from_historical else NONE_RESULT,
        'pct_chg_from_hs': round(pct_chg_from_historical, ROUND_N) if pct_chg_from_historical else NONE_RESULT,
        'tg_h': round(target_high, ROUND_N) if target_high else NONE_RESULT,
        'tg_l': round(target_low, ROUND_N) if target_low else NONE_RESULT,
        'd_tg_h': distance_target_high if distance_target_high is not None else NONE_RESULT,
        'd_tg_l': distance_target_low if distance_target_low is not None else NONE_RESULT,
        'chg_to_tg': round(change_to_target, ROUND_N) if change_to_target else NONE_RESULT,
        'pct_chg_to_tg': round(pct_chg_to_tg, ROUND_N) if pct_chg_to_tg else NONE_RESULT
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

    dag_name = "ak_dg_price_hl"

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'处理股票历史高低点数据',
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=False,
        tags=['akshare', 'price_hl'],
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
