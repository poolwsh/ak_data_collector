from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta
import pandas as pd
from dg_ak.utils.util_funcs import UtilFuncs as uf
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


# region 涨跌停
def get_tracing_data(ti, ak_func_name: str):
    logger.info(f"Starting to get tracing dataframe for {ak_func_name}")
    try:
        _date_list = uf.get_date_list(pg_conn, ak_func_name)
        _date_df = pd.DataFrame(_date_list, columns=['Date'])
        _redis_key = f'{ak_func_name}_date_list'
        uf.write_df_to_redis(_redis_key, _date_df, redis_hook.get_conn(), uf.default_redis_ttl)
        ti.xcom_push(key=f'{ak_func_name}_date_list', value=_redis_key)
        logger.info(f"Tracing dataframe for {ak_func_name} successfully written to Redis.")
    except Exception as e:
        logger.error(f"Failed to get tracing dataframe for {ak_func_name}: {str(e)}")
        raise AirflowException(e)

def get_ak_data(ti, ak_func_name):
    logger.info(f"Starting to save data for {ak_func_name}")
    try:
        _redis_key = ti.xcom_pull(task_ids=f'get_tracing_data_{ak_func_name}', key=f'{ak_func_name}_date_list')
        _temp_csv_path = uf.get_data_and_save2csv(_redis_key, ak_func_name, pg_conn, redis_hook.get_conn())

        _cursor = pg_conn.cursor()
        with open(_temp_csv_path, 'r') as f:
            _cursor.copy_expert((f"COPY ak_dg_{ak_func_name} FROM STDIN WITH CSV"), f)
        pg_conn.commit()
        logger.info("Data successfully inserted into PostgreSQL from CSV.")
    except Exception as e:
        logger.error(f"Failed to save data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)
    finally:
        _cursor.close()
        os.remove(_temp_csv_path)
        logger.info(f"{_temp_csv_path} has been removed.")

def store_zdt_data(ti, ak_func_name: str):
    logger.info(f"Starting copy and clean data operations for {ak_func_name}")

    insert_sql = f"""
        INSERT INTO ak_dg_{ak_func_name}_store
        SELECT * FROM ak_dg_{ak_func_name}
        ON CONFLICT (date, b_code) DO UPDATE
        SET b_create_date = EXCLUDED.b_create_date,
            b_name = EXCLUDED.b_name,
            stock_count = EXCLUDED.stock_count,
            url = EXCLUDED.url
        RETURNING date, b_code;
        """

    try:
        _inserted_rows = uf.store_ak_data(pg_conn, ak_func_name, insert_sql, truncate=False)
        _tds_codes = {(row[0], row[1]) for row in _inserted_rows}
        uf.push_data(ti, f'{ak_func_name}_stored_tds_codes', list(_tds_codes))  # Replace direct ti.xcom_push call

        logger.info("Data operation completed successfully for {ak_func_name}.")

    except Exception as e:
        logger.error(f"Failed during copy and clean operations for {ak_func_name}: {str(e)}")
        raise AirflowException(e)

# def store_ak_data(ti, ak_func_name):
#     logger.info(f"Starting copy and clean data operations for {ak_func_name}")

#     try:
#         _conn = pgsql_hook.get_conn()
#         _cursor = _conn.cursor()

#         insert_sql = f"""
#             INSERT INTO ak_dg_{ak_func_name}_store
#             SELECT * FROM ak_dg_{ak_func_name}
#             ON CONFLICT (td, s_code) DO NOTHING
#             RETURNING td;
#             """
#         _cursor.execute(insert_sql)
#         _inserted_tds = _cursor.fetchall()  # Fetch all returned tds
#         _tds = {td[0] for td in _inserted_tds}
#         logger.debug(f"stored_tds: {_tds}")

#         # Push the list of inserted TDS to XCom
#         ti.xcom_push(key=f'{ak_func_name}_stored_tds', value=list(_tds))

#         _conn.commit()
#         logger.info("Data operation completed successfully, starting truncation.")

#         truncate_sql = f"TRUNCATE TABLE ak_dg_{ak_func_name};"
#         _cursor.execute(truncate_sql)
#         _conn.commit()

#     except Exception as e:
#         logger.error(f"Failed during copy and clean operations for {ak_func_name}: {str(e)}")
#         if _conn:
#             _conn.rollback()
#         raise AirflowException(e)
#     finally:
#         if _cursor is not None:
#             _cursor.close()
#         if _conn is not None:
#             _conn.close()

def update_tracing_data(ti, ak_func_name):
    logger.info(f"Preparing to insert tracing data for {ak_func_name}")
    pg_hook = PostgresHook(postgres_conn_id='txy800_pgsql_ak')
    date_list = ti.xcom_pull(task_ids=f'store_data_{ak_func_name}', key=f'{ak_func_name}_stored_tds')
    logger.debug(f"td_list:{date_list}")
    host_name = os.getenv('HOSTNAME', socket.gethostname())
    category = "zdt"
    current_time = datetime.now()

    if not date_list:
        logger.warning("No dates to process for tracing data.")
        return

    data = [(ak_func_name, date, current_time, current_time, category, True, host_name) for date in date_list]

    insert_sql = """
        INSERT INTO dg_ak_tracing_dt (ak_func_name, dt, create_time, update_time, category, is_active, host_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ak_func_name, dt) DO UPDATE 
        SET update_time = EXCLUDED.update_time;
        """

    # 获取数据库连接和游标
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # 执行批量插入
    try:
        cursor.executemany(insert_sql, data)
        conn.commit()
        logger.info(f"Inserted/Updated tracing data for {len(date_list)} dates in {ak_func_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing bulk insert for {ak_func_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def generate_dag(ak_func_name):
    logger.info(f"Generating DAG for {ak_func_name}")
    default_args = {
        'owner': 'wsh',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email': ['wshmxgz@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'max_active_runs': 1,
        'retry_delay': timedelta(minutes=5),
        'tags': ['akshare', 'store_daily', '涨跌停'],
        'schedule': '17 0 * * *', # 北京时间: 17+8=25-24=1
        'catchup': False,
    }

    dag = DAG(
        f'ak_dg_zdt_{ak_func_name}',
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载涨跌停相关数据'
    )

    t1 = PythonOperator(
        task_id=f'get_tracing_data_{ak_func_name}',
        python_callable=get_tracing_data,
        op_kwargs={'ak_func_name': ak_func_name},
        dag=dag,
    )

    t2 = PythonOperator(
        task_id=f'get_data_{ak_func_name}',
        python_callable=get_ak_data,
        op_kwargs={'ak_func_name': ak_func_name},
        dag=dag,
    )

    t3 = PythonOperator(
        task_id=f'store_data_{ak_func_name}',
        python_callable=store_ak_data,
        op_kwargs={'ak_func_name': ak_func_name},
        dag=dag,
    )

    t4 = PythonOperator(
        task_id=f'tracing_data_{ak_func_name}',
        python_callable=update_tracing_data,
        op_kwargs={'ak_func_name': ak_func_name},
        dag=dag,
    )

    t1 >> t2 >> t3 >> t4
    return dag

ak_funk_name_list = [
    'stock_zt_pool_em', 'stock_zt_pool_previous_em', 'stock_zt_pool_strong_em', 
    'stock_zt_pool_sub_new_em', 'stock_zt_pool_zbgc_em', 'stock_zt_pool_dtgc_em']

# for func_name in ak_funk_name_list:
#     globals()[f'ak_dg_zdt_{func_name}'] = generate_dag(func_name)
#     logger.info(f"DAG for {func_name} successfully created and registered.")

# endregion 涨跌停
