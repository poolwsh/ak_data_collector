from __future__ import annotations
import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from airflow.exceptions import AirflowException
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.hooks.base import BaseHook
import redis
import json
from tqdm import tqdm

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
redis_conn = redis_hook.get_conn()

# 从 Airflow 连接 ID 获取 PostgreSQL 连接字符串
pg_conn = BaseHook.get_connection(con.TXY800_PGSQL_CONN_ID)
pg_url = f"postgresql+psycopg2://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
pg_engine = create_engine(pg_url)

# 定义常量
PRICE_HL_TABLE_NAME = 'ak_da_stock_price_hl'
pa_data_prefix = 'stock_pa_data'
REDIS_TTL = 3600  # 1 hour

def insert_data_to_db(df: pd.DataFrame, table_name: str):
    """将数据插入数据库"""
    if df.empty:
        logger.warning("No data to insert into database.")
        return
    try:
        # 格式化浮点数值，只保留小数点后两位
        df = df.round(2)
        
        with pg_engine.connect() as conn:
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
                conn.execute(sql)
        
        logger.info(f"Data inserted into {table_name} successfully.")
    except Exception as e:
        logger.error(f"Failed to insert data into {table_name}: {str(e)}")
        logger.error(f"Data that caused the error: {df.head()}")
        raise AirflowException(e)

def generate_fibonacci_intervals(n: int) -> list[int]:
    """生成小于 n 的斐波那契序列"""
    fibs = [1, 2]
    while fibs[-1] < n:
        fibs.append(fibs[-1] + fibs[-2])
    if con.LOGGER_DEBUG:
        logger.debug(f"Generated Fibonacci intervals: {fibs}")
    return [f for f in fibs if f < n]

def calculate_row(stock_data: pd.DataFrame, i: int, interval: int) -> dict:
    """计算单行数据"""
    historical_high = float(stock_data['h'][i-interval:i].max())
    historical_low = float(stock_data['l'][i-interval:i].min())
    distance_historical_high = i - stock_data['h'][i-interval:i].idxmax()
    distance_historical_low = i - stock_data['l'][i-interval:i].idxmin()

    change_from_historical = stock_data['c'][i] - historical_high if stock_data['c'][i] >= historical_high else stock_data['c'][i] - historical_low
    pct_chg_from_historical = change_from_historical / historical_high if stock_data['c'][i] >= historical_high else change_from_historical / historical_low

    target_high = float(stock_data['h'][i:i+interval].max()) if i + interval < len(stock_data) else None
    target_low = float(stock_data['l'][i:i+interval].min()) if i + interval < len(stock_data) else None
    distance_target_high = stock_data['h'][i:i+interval].idxmax() - i if target_high else None
    distance_target_low = stock_data['l'][i:i+interval].idxmin() - i if target_low else None
    change_to_target = target_high - stock_data['c'][i] if target_high else target_low - stock_data['c'][i] if target_low else None
    pct_chg_to_tg = change_to_target / stock_data['c'][i] if change_to_target else None

    return {
        's_code': stock_data['s_code'][i],
        'td': stock_data['td'][i],
        'interval': interval,
        'hs_h': round(historical_high, 2),
        'hs_l': round(historical_low, 2),
        'd_hs_h': distance_historical_high,
        'd_hs_l': distance_historical_low,
        'chg_from_hs': round(change_from_historical, 2),
        'pct_chg_from_hs': round(pct_chg_from_historical, 2) if pct_chg_from_historical else 0,
        'tg_h': round(target_high, 2) if target_high else 0,
        'tg_l': round(target_low, 2) if target_low else 0,
        'd_tg_h': distance_target_high if distance_target_high is not None else 0,
        'd_tg_l': distance_target_low if distance_target_low is not None else 0,
        'chg_to_tg': round(change_to_target, 2) if change_to_target else 0,
        'pct_chg_to_tg': round(pct_chg_to_tg, 2) if pct_chg_to_tg else 0
    }

def calculate_price_hl(stock_data: pd.DataFrame, intervals: list[int] = None) -> pd.DataFrame:
    """计算股票的历史高点、历史低点、目标高点和目标低点"""
    if intervals is None:
        intervals = generate_fibonacci_intervals(len(stock_data))

    results = []
    for i in tqdm(range(len(stock_data)), desc="Processing stock data"):
        for interval in intervals:
            if i >= interval:
                row = calculate_row(stock_data, i, interval)
                results.append(row)
                
    if con.LOGGER_DEBUG:
        logger.debug(f"Calculated price HL for s_code={stock_data['s_code'][0]}, intervals={intervals}")
    return pd.DataFrame(results)

def cache_stock_data(s_code: str, pg_engine, redis_conn):
    """从数据库读取数据并缓存到Redis"""
    sql = f"""
        SELECT * FROM ak_dg_stock_zh_a_hist_store_daily_hfq 
        WHERE s_code = '{s_code}';
    """
    try:
        with pg_engine.connect() as conn:
            result = conn.execute(sql)
            data = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(data, columns=columns)
    except Exception as e:
        logger.error(f"Failed to fetch data for s_code={s_code}: {str(e)}")
        raise AirflowException(e)

    redis_key = f"{pa_data_prefix}:{s_code}"
    dauf.write_df_to_redis(redis_key, df, redis_conn, REDIS_TTL)
    
    if con.LOGGER_DEBUG:
        logger.debug(f"Cached stock data for s_code={s_code} in Redis. DataFrame head:\n{df.head()}")
    
    return df

def get_stock_data_from_cache(s_code: str, redis_conn) -> pd.DataFrame:
    """从Redis缓存中获取股票数据"""
    redis_key = f"{pa_data_prefix}:{s_code}"
    df = dauf.read_df_from_redis(redis_key, redis_conn)
    if not df.empty:
        df[['l', 'o', 'c', 'h']] = df[['l', 'o', 'c', 'h']].astype(float)
        if con.LOGGER_DEBUG:
            logger.debug(f"Retrieved stock data for s_code={s_code} from Redis cache. DataFrame head:\n{df.head()}")
        return df
    else:
        if con.LOGGER_DEBUG:
            logger.debug(f"No cache found for s_code={s_code}, will fetch from database")
        return pd.DataFrame()

def process_single_stock(s_code: str, s_name: str, intervals: list[int], total_codes: int, index: int):
    logger.info(f"Processing data for s_code={s_code}, s_name={s_name} ({index+1}/{total_codes})")
    
    stock_data_df = get_stock_data_from_cache(s_code, redis_conn)
    if stock_data_df.empty:
        stock_data_df = cache_stock_data(s_code, pg_engine, redis_conn)
    
    process_stock_data(s_code, stock_data_df, intervals)

def process_stock_data(s_code: str, stock_data_df: pd.DataFrame, intervals: list[int]):
    if not stock_data_df.empty:
        stock_data_df['td'] = pd.to_datetime(stock_data_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')
        if con.LOGGER_DEBUG:
            logger.debug(f'Length of {s_code}: {len(stock_data_df)}')
        price_hl_df = calculate_price_hl(stock_data_df, intervals)
        try:
            insert_data_to_db(price_hl_df, PRICE_HL_TABLE_NAME)
            if con.LOGGER_DEBUG:
                logger.debug(f"Processed and inserted data for s_code={s_code}. DataFrame head:\n{price_hl_df.head()}")
        except AirflowException:
            logger.error(f"Failed to insert data for s_code={s_code}, rolling back transaction")
            raise
    else:
        logger.info(f"No data found for s_code={s_code}")

def init_ak_dg_price_hl(intervals: list[int] = None):
    try:
        s_code_name_list = dauf.get_s_code_name_list(redis_conn)
        total_codes = len(s_code_name_list)
        logger.info(f"Total stock codes to process: {total_codes}")
        
        for index, (s_code, s_name) in enumerate(s_code_name_list):
            logger.info(f"({index+1}/{total_codes}) Calculate stock price hl for stock {s_code}")
            if con.LOGGER_DEBUG and index > 5:
                break
            process_single_stock(s_code, s_name, intervals, total_codes, index)

    except Exception as e:
        raise AirflowException(e)

if __name__ == "__main__":
    init_ak_dg_price_hl()
