import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from dags.da_ak.utils.da_ak_util_funcs import DaAkUtilFuncs as dauf
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.da_ak.utils.da_ak_config import daak_config as con
from dags.utils.config import Config

# Constants
ROLLBACK_DAYS = 10  # Number of days to rollback for recalculating RS
DEFAULT_INDEX_LIST = ['000001']  # Default list of index codes
DAG_NAME = "calculate_and_sort_rs_daily"  # Name of the DAG

def get_dates(i_code):
    """
    Calculate the start and end dates for the RS calculation for a given i_code.
    The start date is MAX(td) from the table minus the rollback days.
    If the table is empty for the given i_code, use the default start date from the config.
    The end date is today's date.
    """
    with PGEngine.managed_conn() as conn:
        with conn.cursor() as cursor:
            # Get the maximum trading date for the given i_code from the table
            cursor.execute("SELECT MAX(td) FROM da_ak_rs_daily WHERE i_code = %s", (i_code,))
            result = cursor.fetchone()
            end_dt = datetime.now().date()  # End date is today's date
            if result[0]:
                start_dt = result[0] - timedelta(days=ROLLBACK_DAYS)
            else:
                start_dt = datetime.strptime(con.ZH_A_DEFAULT_START_DATE, "%Y-%m-%d").date()
            logger.info(f"Calculated date range for i_code {i_code}: {start_dt} to {end_dt}")
            return start_dt, end_dt


def calculate_and_sort_rs(ds, **kwargs):
    """
    Calculate Relative Strength (RS) and rank it, then insert into the database.
    If there are conflicts with existing data, update the existing records.
    """
    i_codes = kwargs.get('i_codes', DEFAULT_INDEX_LIST)  # Get the list of index codes
    target_date = kwargs['logical_date'].strftime('%Y-%m-%d')  # Get the target date from Airflow context
    
    for i_code in i_codes:
        begin_dt, end_dt = get_dates(i_code)  # Calculate the start and end dates for the current i_code
        logger.info(f"Calculating RS for index code: {i_code}")
        sql_query = f"""
        WITH time_period AS (
            SELECT 
                '{begin_dt}'::date AS start_date, 
                '{end_dt}'::date AS end_date
        ),
        index_daily_performance AS (
            SELECT 
                i_code,
                td,
                c AS index_close,
                pct_chg AS index_pct_chg
            FROM 
                dg_ak_index_zh_a_hist_daily, time_period
            WHERE 
                td BETWEEN time_period.start_date AND time_period.end_date
                AND i_code = '{i_code}'
        ),
        stock_daily_performance AS (
            SELECT 
                s_code,
                td,
                c AS stock_close,
                pct_chg AS stock_pct_chg
            FROM 
                dg_ak_stock_zh_a_hist_daily_hfq, time_period
            WHERE 
                td BETWEEN time_period.start_date AND time_period.end_date
        ),
        relative_strength_calculation AS (
            SELECT 
                sp.s_code,
                sp.td,
                sp.stock_pct_chg,
                ip.i_code,
                ip.index_pct_chg,
                CASE 
                    WHEN ip.index_pct_chg = 0 THEN NULL
                    ELSE (sp.stock_pct_chg / ip.index_pct_chg) 
                END AS rs
            FROM 
                stock_daily_performance sp
            JOIN 
                index_daily_performance ip 
            ON 
                sp.td = ip.td
        ),
        ranked_rs AS (
            SELECT s_code, i_code, td, rs,
                   RANK() OVER (PARTITION BY td ORDER BY rs DESC) AS rs_rank
            FROM relative_strength_calculation
        )
        INSERT INTO da_ak_rs_daily (s_code, i_code, td, rs, rs_rank)
        SELECT 
            s_code,
            i_code,
            td,
            rs,
            rs_rank
        FROM 
            ranked_rs
        ON CONFLICT (s_code, i_code, td)
        DO UPDATE SET
            rs = EXCLUDED.rs,
            rs_rank = EXCLUDED.rs_rank;
        """
        
        with PGEngine.managed_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql_query)
                    conn.commit()
                    logger.info(f"Inserted and ranked RS data for index code: {i_code} on {target_date}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error inserting and ranking RS data for index code: {i_code} on {target_date}, error: {e}")
                    raise AirflowException(f"Error inserting and ranking RS data for index code: {i_code}")


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
        description='Calculate and sort relative strength daily',
        start_date=days_ago(0),
        schedule_interval=dauf.generate_random_minute_schedule(hour=10),
        catchup=False,
        tags=['a-akshare', 'rs', 'vertical'],
        max_active_runs=1,
    )

    # Define the task to calculate and sort RS
    calculate_and_sort_rs_task = PythonOperator(
        task_id='calculate_and_sort_rs',
        python_callable=calculate_and_sort_rs,
        provide_context=True,
        op_kwargs={'i_codes': DEFAULT_INDEX_LIST},
        dag=dag,
    )

    return dag

# Instantiate the DAG
globals()[DAG_NAME] = generate_dag()

