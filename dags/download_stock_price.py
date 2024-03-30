
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
from __future__ import annotations

# [START tutorial]
# [START import_module]
import os
from datetime import datetime, timedelta

import yfinance as yf

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG
from airflow.models import Variable

# Operators; we need this to operate!
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# [END import_module]
POSTGRES_CONN_ID = "local_pgsql"

def get_tickers(**context):
    stock_list = Variable.get("stock_list_json", deserialize_json=True)
    stocks = context["dag_run"].conf.get("stocks", None) if "dag_run" in context and context["dag_run"] is not None else None
    if stocks:
        stock_list = stocks
    return stock_list

def download_prices(**context):
    stock_list = get_tickers(**context)
    for ticker in stock_list:
        dat = yf.Ticker(ticker)
        hist = dat.history(period="max")
        hist.reset_index(inplace=True)
        print(hist.columns)
        hist.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
        hist.rename(columns={"Date": "td", "Open":'o', "High":'h', "Low":'l', "Close":'c', 'Volume':'v'}, inplace=True)
        print(hist.columns)
        print(hist.shape[0])
        with open(f'/data/workspace/airflow/csv/{ticker}.csv', 'w') as writer:
            hist.to_csv(writer, index=False)
        print(f"Finished downloading price data for {ticker}.")
    
def get_file_path(ticker):
    return f'/data/workspace/airflow/csv/{ticker}.csv'

def load_price_data(ticker):
    print(f"Loading price data for {ticker}.")
    ticker_df = pd.read_csv(get_file_path(ticker))
    print(ticker_df.head(3))
    return ticker_df
    # with open(get_file_path(ticker), 'r') as reader:
    #     lines = reader.readlines()
    #     print(lines)
    #     return [[ticker]+lines.split(',')[:5] for line in lines if line[:4]!='Date']

def save_to_postgresql_stage(**context):
    tickers = get_tickers(**context)
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    for ticker in tickers:
        ticker_df = load_price_data(ticker)
        ticker_df.to_sql('data_table', pg_hook.get_conn(), if_exists='replace', chunksize=1000)
    
        # print(f"{ticker} length={len(val)}  {val[1]}")
        # sql = """INSERT INTO stock_prices_stage
        #         (s_code, td, o, h, l, c)
        #         VALUES (%s, %s, %s, %s, %s, %s)"""

# [START instantiate_dag]
with DAG(
    dag_id="Download_Stock_Price",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["mxgzmxgz@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    # [END default_args]
    description="Download stock price and save to local csv files.",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["wshdata"],
    params={
        #  "stocks": ['a'],
        #  "my_int_param": 6
     },
) as dag:
    # [END instantiate_dag]
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this

    download_task = PythonOperator(
        task_id = "download_prices",
        python_callable=download_prices,
        provide_context=True
    )

    save_to_postgresql_task = PythonOperator(
        task_id='save_to_database',
        python_callable=save_to_postgresql_stage,
        provide_context=True,
    )

    postgresql_task = PostgresOperator(
        task_id = 'merge_stock_price',
        postgres_conn_id='local_pgsql',
        sql='merge_stock_price.sql',
        dag=dag,
    )
    download_task >> save_to_postgresql_task >> postgresql_task
# [END tutorial]
