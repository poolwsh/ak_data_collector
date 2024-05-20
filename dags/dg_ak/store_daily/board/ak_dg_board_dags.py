from __future__ import annotations

import sys
from pathlib import Path
from datetime import timedelta, datetime
from dg_ak.utils.util_funcs import UtilFuncs as uf
from dg_ak.utils.logger import logger
import dg_ak.utils.config as con

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.dates import days_ago

# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# 配置路径
config_path = Path(__file__).resolve().parent / 'ak_dg_board_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = uf.load_ak_cols_config(config_path.as_posix())

# 统一定义Redis keys
BOARD_LIST_KEY_PREFIX = "board_list"
STORED_KEYS_KEY_PREFIX = "stored_keys"

def get_redis_key(base_key: str, identifier: str) -> str:
    return f"{base_key}@{identifier}"

# region 板块

def get_board_list(board_list_func_name: str):
    logger.info(f"Downloading board list for {board_list_func_name}")
    try:
        board_list_data_df = uf.get_data_today(board_list_func_name, ak_cols_config_dict)
        board_list_data_df = uf.convert_columns(board_list_data_df, f'ak_dg_{board_list_func_name}', pg_conn, redis_hook.get_conn())
        
        if 'date' in board_list_data_df.columns:
            board_list_data_df['date'] = pd.to_datetime(board_list_data_df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        if board_list_data_df.empty:
            logger.warning(f"No data retrieved for {board_list_func_name}")
            return

        redis_key = get_redis_key(BOARD_LIST_KEY_PREFIX, board_list_func_name)
        uf.write_df_to_redis(redis_key, board_list_data_df, redis_hook.get_conn(), uf.default_redis_ttl)

        temp_csv_path = uf.save_data_to_csv(board_list_data_df, board_list_func_name, include_header=True)
        if temp_csv_path is None:
            logger.warning(f"No CSV file created for {board_list_func_name}, skipping database insertion.")
            return
        
        uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{board_list_func_name}')

    except Exception as e:
        logger.error(f"Failed to process data for {board_list_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def store_board_list(board_list_func_name: str):
    logger.info(f"Starting data storage operations for {board_list_func_name}")

    insert_sql = f"""
        INSERT INTO ak_dg_{board_list_func_name}_store
        SELECT * FROM ak_dg_{board_list_func_name}
        ON CONFLICT (date, b_code) DO NOTHING
        RETURNING date;
    """

    try:
        inserted_rows = uf.store_ak_data(pg_conn, board_list_func_name, insert_sql, truncate=True)
        dates = list(set(row[0] for row in inserted_rows))
        redis_key = get_redis_key(STORED_KEYS_KEY_PREFIX, board_list_func_name)
        uf.write_list_to_redis(redis_key, dates, redis_hook.get_conn(), uf.default_redis_ttl)
        logger.info(f"Data operation completed successfully for {board_list_func_name}. Dates: {dates}")
    except Exception as e:
        logger.error(f"Failed during data operations for {board_list_func_name}: {str(e)}")
        raise AirflowException(e)

def save_tracing_board_list(board_list_func_name: str):
    redis_key = get_redis_key(STORED_KEYS_KEY_PREFIX, board_list_func_name)
    date_list = uf.read_list_from_redis(redis_key, redis_hook.get_conn())
    logger.info(date_list)
    if not date_list:
        logger.info(f"No dates to process for {board_list_func_name}")
        return

    uf.insert_tracing_date_data(pg_conn, board_list_func_name, date_list)
    logger.info(f"Tracing data saved for {board_list_func_name} on dates: {date_list}")

def get_board_cons(board_list_func_name: str, board_cons_func_name: str):
    logger.info(f"Starting to save data for {board_cons_func_name}")
    try:
        redis_key = get_redis_key(BOARD_LIST_KEY_PREFIX, board_list_func_name)
        board_list_df = uf.get_df_from_redis(redis_key, redis_hook.get_conn())
        
        if board_list_df.empty:
            raise AirflowException(f"No dates available for {board_list_func_name}, skipping data fetch.")
        
        board_cons_df = uf.get_data_by_board_names(board_cons_func_name, ak_cols_config_dict, board_list_df['b_name'])
        board_cons_df = uf.convert_columns(board_cons_df, f'ak_dg_{board_cons_func_name}', pg_conn, redis_hook.get_conn())
        
        if 'date' in board_cons_df.columns:
            board_cons_df['date'] = pd.to_datetime(board_cons_df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        temp_csv_path = uf.save_data_to_csv(board_cons_df, board_cons_func_name)
        if temp_csv_path is None:
            raise AirflowException(f"No CSV file created for {board_cons_func_name}, skipping database insertion.")
        
        uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{board_cons_func_name}')

    except Exception as e:
        logger.error(f"Failed to process data for {board_cons_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def store_board_cons(board_cons_func_name: str):
    logger.info(f"Starting data storage operations for {board_cons_func_name}")

    insert_sql = f"""
        INSERT INTO ak_dg_{board_cons_func_name}_store
        SELECT * FROM ak_dg_{board_cons_func_name}
        ON CONFLICT (date, s_code, b_name) DO NOTHING
        RETURNING date, b_name;
    """

    try:
        inserted_rows = uf.store_ak_data(pg_conn, board_cons_func_name, insert_sql, truncate=True)
        keys = list(set({(row[0], row[1]) for row in inserted_rows}))
        redis_key = get_redis_key(STORED_KEYS_KEY_PREFIX, board_cons_func_name)
        uf.write_list_to_redis(redis_key, keys, redis_hook.get_conn(), uf.default_redis_ttl)
        logger.info(f"Data operation completed successfully for {board_cons_func_name}.")
    except Exception as e:
        logger.error(f"Failed during data operations for {board_cons_func_name}: {str(e)}")
        raise AirflowException(e)

def save_tracing_board_cons(board_cons_func_name: str, param_name: str):
    redis_key = get_redis_key(STORED_KEYS_KEY_PREFIX, board_cons_func_name)
    cons_data = uf.read_list_from_redis(redis_key, redis_hook.get_conn())
    if not cons_data:
        logger.info(f"No constituent data to process for {board_cons_func_name}")
        return

    uf.insert_tracing_date_1_param_data(pg_conn, board_cons_func_name, param_name, cons_data)
    logger.info(f"Tracing data saved for {board_cons_func_name} with parameter data.")

def generate_dag(board_list_func_name: str, board_cons_func_name: str):
    logger.info(f"Generating DAG for {board_list_func_name} and {board_cons_func_name}")
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'start_date': days_ago(0),
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY),
    }

    dag = DAG(
        f'ak_dg_board_{board_list_func_name}_{board_cons_func_name}',
        default_args=default_args,
        description=f'利用akshare的函数{board_list_func_name}和{board_cons_func_name}下载板块相关数据',
        schedule='0 15 * * *',  # 北京时间: 15+8=23
        catchup=False,
        tags=['akshare', 'store_daily', '板块'],
        max_active_runs=1,
        params={},
    )

    tasks = {
        'get_board_list': PythonOperator(
            task_id=f'get_board_list-{board_list_func_name}',
            python_callable=get_board_list,
            op_kwargs={'board_list_func_name': board_list_func_name},
            dag=dag,
        ),
        'store_board_list': PythonOperator(
            task_id=f'store_board_list-{board_list_func_name}',
            python_callable=store_board_list,
            op_kwargs={'board_list_func_name': board_list_func_name},
            dag=dag,
        ),
        'save_tracing_board_list': PythonOperator(
            task_id=f'save_tracing_board_list-{board_list_func_name}',
            python_callable=save_tracing_board_list,
            op_kwargs={'board_list_func_name': board_list_func_name},
            dag=dag,
        ),
        'get_board_cons': PythonOperator(
            task_id=f'get_board_cons-{board_cons_func_name}',
            python_callable=get_board_cons,
            op_kwargs={'board_list_func_name': board_list_func_name, 'board_cons_func_name': board_cons_func_name},
            dag=dag,
        ),
        'store_board_cons': PythonOperator(
            task_id=f'store_board_cons-{board_cons_func_name}',
            python_callable=store_board_cons,
            op_kwargs={'board_cons_func_name': board_cons_func_name},
            dag=dag,
        ),
        'save_tracing_board_cons': PythonOperator(
            task_id=f'save_tracing_board_cons-{board_cons_func_name}',
            python_callable=save_tracing_board_cons,
            op_kwargs={'board_cons_func_name': board_cons_func_name, 'param_name': 'symbol'},
            dag=dag,
        )
    }
    tasks['get_board_list'] >> tasks['store_board_list'] >> tasks['save_tracing_board_list'] >> tasks['get_board_cons'] >> tasks['store_board_cons'] >> tasks['save_tracing_board_cons']
    return dag

ak_func_name_list = [
    ['stock_board_concept_name_ths', 'stock_board_concept_cons_ths'],
    ['stock_board_concept_name_em', 'stock_board_concept_cons_em'],
    ['stock_board_industry_summary_ths', 'stock_board_industry_cons_ths'],
    ['stock_board_industry_name_em', 'stock_board_industry_cons_em']
]

for func_names in ak_func_name_list:
    words = func_names[0].split('_')
    source = words[-1]
    board_type = words[-3]
    dag_name = f'ak_dg_board_{board_type}_{source}'
    globals()[dag_name] = generate_dag(func_names[0], func_names[1])
    logger.info(f"DAG for {dag_name} successfully created and registered.")
# endregion 板块
