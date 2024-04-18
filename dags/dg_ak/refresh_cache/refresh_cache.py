from __future__ import annotations

import redis
from typing import Optional, Union
from datetime import datetime, timedelta, date
import akshare as ak
import pandas as pd
from functools import partial
from sqlalchemy import create_engine


from dg_ak.store_daily.util_funcs import UtilFuncs as uf

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ExternalPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook


POSTGRES_CONN_ID = "local_pgsql"
REDIS_CONN_ID = "local_redis_3"

redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

def download_and_cache_stock_zt_pool_em_by_td(ti, td:Union[str, date]) -> str:
    _redis_key = uf.download_and_cache_ak_data_by_td(
        'stock_zt_pool_em', 
        td, 
        redis_conn = redis_hook.get_conn(), 
        ttl = uf.default_redis_ttl)
    ti.xcom_push(key='ak_zt_pool_em_cache_key', value=_redis_key)
    # return _redis_key

# download_and_cache_stock_zt_pool_em_by_td = partial(uf.download_and_cache_ak_data_by_td, ak_func_name='stock_zt_pool_em')
def get_sqlalchemy_engine():
    hook = PostgresHook(postgres_conn_id="local_pgsql")
    sqlalchemy_url = hook.get_uri()
    return create_engine(sqlalchemy_url)

def save_df_to_postgres(ti):
    # _redis_key = ti.xcom_pull(key='ak_zt_pool_em_cache_key', task_ids='ak_zt_pool_em_download')
    _df = uf.get_data_by_td('stock_zt_pool_em',td='20240402')
    engine = get_sqlalchemy_engine()
    _df.to_sql(name="ak_dg_stock_zt_pool_em", con=engine, schema='public', if_exists='replace', index=False)

def save_ak_zt_pool_em2(ti):
    _redis_key = ti.xcom_pull(key='ak_zt_pool_em_cache_key', task_ids='ak_zt_pool_em_download')
    _df = uf.read_df_from_redis(_redis_key, redis_hook.get_conn())
    
    # Establish PostgreSQL connection using Airflow's hook
    _conn = pgsql_hook.get_conn()
    _cursor = _conn.cursor()

    try:
        for _index, _row in _df.iterrows():
            _cursor.execute(
                """
                INSERT INTO ak_dg_stock_zt_pool_em (
                    td, s_code, s_name, pct_chg, c, a, 
                    circulation_mv, total_mv, turnover_rate, lock_fund,
                    first_lock_time, last_lock_time, failed_account, zt_stat, zt_account, industry
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    _row['td'], _row['s_code'], _row['s_name'], _row['pct_chg'], _row['c'], _row['a'],
                    _row['circulation_mv'], _row['total_mv'], _row['turnover_rate'], _row['lock_fund'],
                    _row['first_lock_time'], _row['last_lock_time'], _row['failed_account'], _row['zt_stat'], 
                    _row['zt_account'], _row['industry']
                )
            )

        # Commit the transaction
        _conn.commit()

    except Exception as _e:
        print(f"An error occurred: {_e}")
        _conn.rollback()

    finally:
        _cursor.close()
        _conn.close()

def save_ak_zt_pool_em1(ti):
    _redis_key = ti.xcom_pull(key='ak_zt_pool_em_cache_key', task_ids='ak_zt_pool_em_downdload')
    _df = uf.read_df_from_redis(_redis_key, redis_hook.get_conn())
    # 获取PostgreSQL连接和游标
    # _conn = pgsql_hook.get_connection(POSTGRES_CONN_ID)
    _cursor = _conn.cursor()
    try:
        # 循环遍历DataFrame，为每一行数据执行INSERT语句
        for _index, _row in _df.iterrows():
            _cursor.execute(
                """
                INSERT INTO ak_dg_stock_zt_pool_em (
                    td, s_code, s_name, pct_chg, c, a, 
                    circulation_mv, total_mv, turnover_rate, lock_fund,
                    first_lock_time, last_lock_time, failed_account, zt_stat, zt_account, industry
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    _row['td'], _row['s_code'], _row['s_name'], _row['pct_chg'], _row['c'], _row['a'],
                    _row['circulation_mv'], _row['total_mv'], _row['turnover_rate'], _row['lock_fund'],
                    _row['first_lock_time'], _row['last_lock_time'], _row['failed_account'], _row['zt_stat'], 
                    _row['zt_account'], _row['industry']
                )
            )

        # 提交事务
        _conn.commit()

    except Exception as _e:
        # 如果发生错误则回滚
        print(f"An error occurred: {_e}")
        _conn.rollback()

    finally:
        # 最后，无论成功还是失败，都关闭游标和连接
        _cursor.close()
        _conn.close()
    pass

with DAG(
    dag_id="ak_refresh_cache",
    default_args={
        "owner": "wsh",
        "depends_on_past": False,
        "email": ["mxgzmxgz@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    # [END default_args]
    description="利用akshare下载每日数据.",
    schedule="15 15 * * *",
    start_date=datetime(2024, 4, 1),
    catchup=False,
    tags=["akshare", "daily"],
    max_active_runs=1,
    params={
        #  "stocks": ['a'],
        #  "my_int_param": 6
     },
) as dag:
    # [END instantiate_dag]
    dag.doc_md = """
    依据设定每日运行，下载相关数据
    """  # otherwise, type it like this

    ak_zt_pool_em_downdload_task = PythonOperator(
        task_id = "ak_zt_pool_em_downdload",
        python_callable=download_and_cache_stock_zt_pool_em_by_td,
        op_kwargs={'td': '{{ ds }}', 'redis_conn': redis_hook.get_conn(), 'ttl': uf.default_redis_ttl},
        provide_context=True
    )

    ak_zt_pool_em_save_task = PythonOperator(
        task_id = "ak_zt_pool_em_save",
        python_callable=save_df_to_postgres,
        # python='/data/.virtualenvs/jpt_lab/bin/python3',
        provide_context=True,
    )


    ak_zt_pool_em_downdload_task >> ak_zt_pool_em_save_task