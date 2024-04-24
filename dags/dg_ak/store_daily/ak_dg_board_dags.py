from __future__ import annotations

import os
from datetime import timedelta
from dg_ak.store_daily.util_funcs import UtilFuncs as uf
from dg_ak.utils.logger import logger
import dg_ak.utils.config as con

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.dates import days_ago

redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# region 板块
# 行业板块 (Industry Sector) & 概念板块 (Concept Sector) 
def get_board_list(ti, ak_func_name: str):
    logger.info(f"Downloading board list for {ak_func_name}")
    try:
        # 获取今天的板块数据
        board_list_data_df = uf.get_data_today(ak_func_name)
        if board_list_data_df.empty:
            logger.warning(f"No data retrieved for {ak_func_name}")
            return

        _redis_key = f'{ak_func_name}_board_list'
        uf.write_df_to_redis(_redis_key, board_list_data_df, redis_hook.get_conn(), uf.default_redis_ttl)
        ti.xcom_push(key=f'{ak_func_name}_board_list', value=_redis_key)

        # 保存数据到CSV
        temp_csv_path = uf.save_data_to_csv(board_list_data_df, ak_func_name)
        if temp_csv_path is None:
            logger.warning(f"No CSV file created for {ak_func_name}, skipping database insertion.")
            return
        
        # 将数据从CSV导入数据库
        uf.insert_data_from_csv(pg_conn, temp_csv_path, ak_func_name)

    except Exception as e:
        logger.error(f"Failed to process data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

    finally:
        if 'temp_csv_path' in locals() and os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            logger.info(f"Temporary CSV file {temp_csv_path} has been removed.")

def store_board_data(ti, ak_func_name):
    """
    Store board list data.
    """
    insert_sql = f"""
        INSERT INTO board_list_{ak_func_name}
        SELECT * FROM temp_board_list_{ak_func_name}
        ON CONFLICT (key_columns) DO UPDATE
        SET columns = excluded.columns
        RETURNING key_columns;
        """
    uf.store_ak_data(ti, pg_conn, ak_func_name, insert_sql, truncate=True)

def save_tracing_board_data(ti, ak_func_name, category):
    """
    Update tracing data specific to board list processing.
    """
    uf.execute_tracing_updates(ti, pg_conn, ak_func_name, category=category)

def get_board_cons(ti, ak_func_name):
    logger.info(f"Starting to save data for {ak_func_name}")
    try:
        # 从XCom获取Redis键
        _redis_key = ti.xcom_pull(task_ids=f'get_board_list({ak_func_name})', key=f'{ak_func_name}_board_list')
        
        # 从Redis获取日期数据
        _date_df = uf.get_dates_from_redis(_redis_key, redis_hook.get_conn())
        if _date_df.empty:
            logger.warning(f"No dates available for {ak_func_name}, skipping data fetch.")
            return
        _board_cons_df = uf.get_data_today_by_symbol_list(ak_func_name, _date_df['b_name'])
         # 保存数据到CSV
        temp_csv_path = uf.save_data_to_csv(_board_cons_df, ak_func_name)
        if temp_csv_path is None:
            logger.warning(f"No CSV file created for {ak_func_name}, skipping database insertion.")
            return
        
        # 将数据从CSV导入数据库
        uf.insert_data_from_csv(pg_conn, temp_csv_path, ak_func_name)

    except Exception as e:
        logger.error(f"Failed to process data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

    finally:
        if 'temp_csv_path' in locals() and os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            logger.info(f"Temporary CSV file {temp_csv_path} has been removed.")

def generate_dag(board_list_func, board_con_func):
    logger.info(f"Generating DAG for {board_list_func} and {board_con_func}")
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
        f'下载板块相关数据',
        default_args=default_args,
        description=f'利用akshare的函数{board_list_func}和{board_con_func}下载板块相关数据',
        schedule='0 15 * * *', # 北京时间: 15+8=23
        catchup=False,
        tags=['akshare', 'store_daily', '板块'],
        max_active_runs=1,
        params={},
    )

    tasks = {
        'get_board_list':PythonOperator(
            task_id=f'get_board_list({board_list_func})',
            python_callable=get_board_list,
            op_kwargs={'ak_func_name': board_list_func},
            dag=dag,
        ),
        'store_board_list':PythonOperator(
            task_id=f'store_board_list({board_list_func})',
            python_callable=store_board_data,
            op_kwargs={'ak_func_name': board_list_func},
            dag=dag,
        ),
        'save_tracing_board_list':PythonOperator(
            task_id=f'save_tracing_board_list({board_list_func})',
            python_callable=save_tracing_board_data,
            op_kwargs={'ak_func_name': board_list_func, 'category': 'board_list'},
            dag=dag,
        ),
        'get_board_cons':PythonOperator(
            task_id=f'get_board_cons({board_con_func})',
            python_callable=get_board_cons,
            op_kwargs={'ak_func_name': board_con_func},
            dag=dag,
        ),
        'store_board_cons':PythonOperator(
            task_id=f'store_board_cons({board_con_func})',
            python_callable=store_board_data,
            op_kwargs={'ak_func_name': board_con_func},
            dag=dag,
        ),
        'save_tracing_board_cons':PythonOperator(
            task_id=f'save_tracing_board_cons({board_con_func})',
            python_callable=save_tracing_board_data,
            op_kwargs={'ak_func_name': board_con_func, 'category': 'board_cons'},
            dag=dag,
        )
    }
    tasks['get_board_list'] >> tasks['store_board_list'] >> tasks['save_tracing_board_list'] >> tasks['get_board_cons'] >> tasks['store_board_cons'] >> tasks['save_tracing_board_cons']
    return dag


ak_funk_name_list = [
    ['stock_board_concept_name_ths', 'stock_board_concept_cons_ths'],
    ['stock_board_concept_name_em', 'stock_board_concept_cons_em'],
    ['stock_board_industry_summary_ths', 'stock_board_industry_cons_ths'],
    ['stock_board_industry_name_em', 'stock_board_industry_cons_em']
]

for func_names in ak_funk_name_list:
    words = func_names[0].split('_')
    source = words[-1]
    board_type = words[-3]
    dag_name = f'ak_dg_board_{board_type}_{source}'
    globals()[dag_name] = generate_dag(func_names[0], func_names[1])
    logger.info(f"DAG for {dag_name} successfully created and registered.")
# endregion 板块