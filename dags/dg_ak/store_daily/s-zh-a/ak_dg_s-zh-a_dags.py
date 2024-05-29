
from __future__ import annotations

import os
import sys
from pathlib import Path
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
print(project_root)
# 将项目根目录添加到sys.path中
sys.path.append(project_root)


import socket
import pandas as pd
from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.dates import days_ago

from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dguf
from utils.logger import logger
import utils.config as con

# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# 配置路径
config_path = current_path / 'ak_dg_s-zh-a_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict =dguf.load_ak_cols_config(config_path.as_posix())

# Redis keys 前缀
ARG_LIST_CACHE_PREFIX = "ak_dg_s_zh_a_arg_list"
STORE_LIST_CACHE_PREFIX = "ak_dg_s_zh_a_store_list"

TRACING_TABLE_NAME = 'ak_dg_tracing_s_zh_a'
TRADE_DATE_TABLE_NAME = 'ak_dg_stock_zh_a_trade_date'
STOCK_CODE_NAME_TABLE = 'ak_dg_stock_zh_a_code_name'

# 定义昨天的日期、默认开始日期和批次大小
default_end_date =dguf.format_td8(datetime.now()) # - timedelta(days=1)
default_start_date = con.zh_a_default_start_date
batch_size = 50  # 根据需求调整批次大小
rollback_days = 15  # 回滚天数

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

def prepare_arg_list(ak_func_name: str, period: str, adjust: str):
    _tracing_df =dguf.get_tracing_data_df(pg_conn, TRACING_TABLE_NAME)
    _current_tracing_df = _tracing_df[
        (_tracing_df['ak_func_name'] == ak_func_name) &
        (_tracing_df['period'] == period) &
        (_tracing_df['adjust'] == adjust)
    ]
    _tracing_dict = dict(zip(_current_tracing_df['scode'].values, _current_tracing_df['last_td'].values))

    _s_code_name_list =dguf.get_s_code_name_list(redis_hook.get_conn())
    insert_code_name_to_db(_s_code_name_list)
    
    _arg_list = []
    for _s_code, _s_name in _s_code_name_list:
        _start_date = _tracing_dict.get(_s_code, default_start_date)

        if _start_date != default_start_date:
            _start_date = (datetime.strptime(str(_start_date), '%Y-%m-%d') - timedelta(days=rollback_days)).strftime('%Y-%m-%d')
        _arg_list.append((_s_code,dguf.format_td8(_start_date), default_end_date))

    _redis_key = f"{ARG_LIST_CACHE_PREFIX}@{ak_func_name}@{period}@{adjust}"
    dguf.write_list_to_redis(_redis_key, _arg_list, redis_hook.get_conn())
    logger.info(f"Argument list for {ak_func_name} with period={period} and adjust={adjust} has been prepared and cached.")

def get_stock_data(ak_func_name: str, period: str, adjust: str):
    logger.info(f"Starting to save data for {ak_func_name} with period={period} and adjust={adjust}")
    try:
        _redis_key = f"{ARG_LIST_CACHE_PREFIX}@{ak_func_name}@{period}@{adjust}"
        _arg_list = dguf.read_list_from_redis(_redis_key, redis_hook.get_conn())

        if not _arg_list:
            raise AirflowException(f"No arguments available for {ak_func_name}, skipping data fetch.")

        if LOGGER_DEBUG:
            logger.debug(f"Config dictionary for {ak_func_name}: {ak_cols_config_dict}")

        _total_codes = len(_arg_list)
        _all_data = []
        all_trade_dates = set()

        # 清空表内容
        clear_table_sql = f"TRUNCATE TABLE ak_dg_{ak_func_name}_{period}_{adjust};"
        with pg_conn.cursor() as cursor:
            cursor.execute(clear_table_sql)
            pg_conn.commit()
        logger.info(f"Table ak_dg_{ak_func_name}_{period}_{adjust} has been cleared.")

        for _index, (_s_code, _start_date, _end_date) in enumerate(_arg_list):
            logger.info(f'({_index + 1}/{_total_codes}) Fetching data for s_code={_s_code} from {_start_date} to {_end_date}')
            if adjust == 'bfq':
                _stock_data_df =dguf.get_s_code_data(
                    ak_func_name, ak_cols_config_dict, _s_code, period, _start_date, _end_date, None
                )
            else:
                _stock_data_df =dguf.get_s_code_data(
                    ak_func_name, ak_cols_config_dict, _s_code, period, _start_date, _end_date, adjust
                )

            if not _stock_data_df.empty:
                _all_data.append(_stock_data_df)
                all_trade_dates.update(_stock_data_df['td'].unique())

            # 如果达到批次大小，处理并清空缓存
            if len(_all_data) >= batch_size or (_index + 1) == _total_codes:
                _combined_df = pd.concat(_all_data, ignore_index=True)
                if LOGGER_DEBUG:
                    logger.debug(f"Combined DataFrame columns for {ak_func_name}: {_combined_df.columns}")

                _combined_df['s_code'] = _combined_df['s_code'].astype(str)
                _combined_df =dguf.convert_columns(_combined_df, f'ak_dg_{ak_func_name}_{period}_{adjust}', pg_conn, redis_hook.get_conn())

                if 'td' in _combined_df.columns:
                    _combined_df['td'] = pd.to_datetime(_combined_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')

                _temp_csv_path =dguf.save_data_to_csv(_combined_df, f'{ak_func_name}_{period}_{adjust}')
                if _temp_csv_path is None:
                    raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

                # 将数据从 CSV 导入数据库
                dguf.insert_data_from_csv(pg_conn, _temp_csv_path, f'ak_dg_{ak_func_name}_{period}_{adjust}')

                # 更新交易日期到 trade_date 表
                _trade_dates = list(all_trade_dates)
                _insert_date_sql = f"""
                    INSERT INTO {TRADE_DATE_TABLE_NAME} (trade_date, create_time, update_time)
                    VALUES (%s, NOW(), NOW())
                    ON CONFLICT (trade_date) DO NOTHING;
                """
                with pg_conn.cursor() as cursor:
                    cursor.executemany(_insert_date_sql, [(date,) for date in _trade_dates])
                    pg_conn.commit()

                # 清空缓存
                _all_data = []
                all_trade_dates.clear()

    except Exception as e:
        logger.error(f"Failed to process data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def store_stock_data(ak_func_name: str, period: str, adjust: str):
    logger.info(f"Starting data storage operations for {ak_func_name} with period={period} and adjust={adjust}")

    try:
        source_table = f'ak_dg_{ak_func_name}_{period}_{adjust}'
        store_table = f'ak_dg_{ak_func_name}_store_{period}_{adjust}'
        # Dynamically retrieve the column names from the table
        columns =dguf.get_columns_from_table(pg_conn, source_table, redis_hook.get_conn())
        column_names = [col[0] for col in columns]  # Extract only the column names
        columns_str = ', '.join(column_names)
        update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in column_names if col not in ['s_code', 'td']])

        _insert_sql = f"""
            INSERT INTO {store_table} ({columns_str})
            SELECT {columns_str} FROM {source_table}
            ON CONFLICT (s_code, td) DO UPDATE SET 
            {update_columns}
            RETURNING s_code, td;
        """

        _inserted_rows =dguf.store_ak_data(pg_conn, ak_func_name, _insert_sql, truncate=False)
        if LOGGER_DEBUG:
            logger.debug(f"Inserted rows for {ak_func_name}: {_inserted_rows}")

        # Extract the maximum td for each s_code
        _keys = {}
        for _row in _inserted_rows:
            s_code, td = _row
            if s_code not in _keys or td > _keys[s_code]:
                _keys[s_code] = td

        if LOGGER_DEBUG:
            logger.debug(f"Keys to store in Redis for {ak_func_name}: {_keys}")

        # Convert the date objects to string format
        _keys_str = {k: v.strftime('%Y-%m-%d') for k, v in _keys.items()}

        _redis_key = f"{STORE_LIST_CACHE_PREFIX}@{ak_func_name}@{period}@{adjust}"
        dguf.write_list_to_redis(_redis_key, list(_keys_str.items()), redis_hook.get_conn())
        logger.info(f"Data operation completed successfully for {ak_func_name}. Keys: {_keys}")

    except Exception as e:
        logger.error(f"Failed during data operations for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def update_tracing_date(ak_func_name: str, period: str, adjust: str):
    _redis_key = f"{STORE_LIST_CACHE_PREFIX}@{ak_func_name}@{period}@{adjust}"
    _stored_keys =dguf.read_list_from_redis(_redis_key, redis_hook.get_conn())
    if not _stored_keys:
        logger.info(f"No keys to process for {ak_func_name}")
        return

    # 将日期字符串转换回 date 对象
    _stored_keys = [(s_code, datetime.strptime(date_str, '%Y-%m-%d').date()) for s_code, date_str in _stored_keys]

    _date_values = [(ak_func_name, _s_code, period, adjust, _date, datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for _s_code, _date in _stored_keys]

    _insert_sql = f"""
        INSERT INTO {TRACING_TABLE_NAME} (ak_func_name, scode, period, adjust, last_td, create_time, update_time, host_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ak_func_name, scode, period, adjust) DO UPDATE 
        SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time;
    """
    try:
        with pg_conn.cursor() as cursor:
            cursor.executemany(_insert_sql, _date_values)
            pg_conn.commit()

        logger.info(f"Tracing data saved for {ak_func_name} on keys: {_stored_keys}")
    except Exception as e:
        logger.error(f"Failed to update tracing data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def generate_dag_name(stock_func, period, adjust) -> str:
    adjust_mapping = {
        'bfq': '除权',
        'hfq': '后复权'
    }
    period_mapping = {
        'daily': '日线',
        'weekly': '周线'
    }
    source_mapping = {
        'stock_zh_a_hist': '东方财富'
    }
    return f"个股行情-{source_mapping.get(stock_func, stock_func)}-{period_mapping.get(period, period)}-{adjust_mapping.get(adjust, adjust)}"

def generate_dag(stock_func, period, adjust):
    logger.info(f"Generating DAG for {stock_func} with period={period} and adjust={adjust}")
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(stock_func, period, adjust)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{stock_func}(period={period}, adjust={adjust})下载个股行情相关数据',
        start_date=days_ago(1),
        schedule=dguf.generate_random_minute_schedule(hour=8), # 北京时间: 8+8=16
        catchup=False,
        tags=['akshare', 'store_daily', '个股行情'],
        max_active_runs=1,
        params={},
    )

    tasks = {
        'prepare_arg_list': PythonOperator(
            task_id=f'prepare_arg_list_{stock_func}_{period}_{adjust}',
            python_callable=prepare_arg_list,
            op_kwargs={'ak_func_name': stock_func, 'period': period, 'adjust': adjust},
            dag=dag,
        ),
        'get_stock_data': PythonOperator(
            task_id=f'get_stock_data_{stock_func}_{period}_{adjust}',
            python_callable=get_stock_data,
            op_kwargs={'ak_func_name': stock_func, 'period': period, 'adjust': adjust},
            dag=dag,
        ),
        'store_stock_data': PythonOperator(
            task_id=f'store_stock_data_{stock_func}_{period}_{adjust}',
            python_callable=store_stock_data,
            op_kwargs={'ak_func_name': stock_func, 'period': period, 'adjust': adjust},
            dag=dag,
        ),
        'update_tracing_date': PythonOperator(
            task_id=f'update_tracing_date_{stock_func}_{period}_{adjust}',
            python_callable=update_tracing_date,
            op_kwargs={'ak_func_name': stock_func, 'period': period, 'adjust': adjust},
            dag=dag,
        ),
    }
    tasks['prepare_arg_list'] >> tasks['get_stock_data'] >> tasks['store_stock_data'] >> tasks['update_tracing_date']
    return dag

def create_dags(ak_func_name, period, adjust):
    globals()[f'ak_dg_s_zh_a_{ak_func_name}_{period}_{adjust}'] = generate_dag(ak_func_name, period, adjust)
    logger.info(f"DAG for {ak_func_name} successfully created and registered.")

create_dags('stock_zh_a_hist', 'daily', 'hfq')
create_dags('stock_zh_a_hist', 'daily', 'bfq')