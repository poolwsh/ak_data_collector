##########################################
### test
import csv
import sys
sys.path.append('/data/workspace/git/ak_data_collector/dags')
#######################################

import os
import json
import time
import redis
import socket
import traceback
import pandas as pd
import akshare as ak
from io import BytesIO
from datetime import date, datetime, timedelta
from typing import Optional, Union

import dg_ak.utils.config as con
from dg_ak.utils.utils import UtilTools as ut
from dg_ak.utils.logger import logger

from airflow.exceptions import AirflowException

class UtilFuncs(object):
    default_redis_ttl = 60*60 # 1 hour

    # region stock funcs
    @staticmethod
    def get_s_code_list(redis_conn: redis.Redis, ttl: int = default_redis_ttl):
        try:
            _df = UtilFuncs.read_df_from_redis(con.STOCK_A_REALTIME_KEY, redis_conn)
            logger.info('Read stock real-time data from Redis successfully.')
            return _df['s_code']
        except Exception as e:
            logger.warning(f"Failed to read stock real-time data from Redis: {e}")
            try:
                _df = getattr(ak, 'stock_zh_a_spot_em')()
                if _df is not None and '代码' in _df.columns:
                    _df.rename(columns={'代码': 's_code'}, inplace=True)
                    _df['s_code'] = _df['s_code'].astype(str)
                    UtilFuncs.write_df_to_redis(con.STOCK_A_REALTIME_KEY, _df, redis_conn, ttl)
                    return _df['s_code']
                else:
                    logger.error("Failed to fetch or process data from the source.")
                    return pd.Series()  # 返回一个空的序列以避免进一步错误
            except Exception as inner_e:
                logger.error(f"Error while fetching or writing data: {inner_e}")
                raise  # 可能需要重新抛出异常或处理错误
        
    @staticmethod
    def get_s_code_data(ak_func_name, ak_cols_config_dict, s_code, period, start_date, end_date, adjust):
        _ak_func = getattr(ak, ak_func_name, None)
        if _ak_func is None:
            error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(error_msg)
            raise AirflowException(error_msg)
        
        _s_df = UtilFuncs.try_to_call(
            _ak_func,
            {'symbol': s_code, 'period': period, 
             'start_date': start_date, 'end_date': end_date, 'adjust': adjust
            })
        if _s_df is None or _s_df.empty:
            if _s_df is None:
                error_msg = f'Data function {ak_func_name} returned None with params(s_code={s_code}, period={period}, start_date={start_date}, end_date={end_date}, adjust={adjust}).'
                logger.error(error_msg)
                # raise AirflowException(error_msg)
            else:
                warning_msg = f'No data found for {ak_func_name} with params(s_code={s_code}, period={period}, start_date={start_date}, end_date={end_date}, adjust={adjust}).'
                logger.warning(warning_msg)
            return pd.DataFrame()  # 返回空的 DataFrame，以避免进一步的处理出错
        
        _s_df = ut.remove_cols(_s_df, ak_cols_config_dict[ak_func_name])
        _s_df.rename(columns=ut.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
        # _df['s_code'] = _df['s_code'].apply(UtilFuncs.pref_s_code)
        # _s_df = ut.add_td(_s_df, td)
        return _s_df

    @staticmethod
    def merge_s_code_data(ak_func_name, config_dict, s_code, period, start_date, end_date, adjust):
        _s_df = UtilFuncs.get_s_code_data(ak_func_name, config_dict, s_code, period, start_date, end_date, adjust)
        _hfq_s_df = UtilFuncs.get_s_code_data(ak_func_name, config_dict, s_code, period, start_date, end_date, adjust)
        _s_df.set_index('td', inplace=True)
        _hfq_s_df.set_index('td', inplace=True)
        _hfq_s_df = _hfq_s_df.add_suffix('_hfq')
        _merged_df = pd.merge(_s_df, _hfq_s_df, left_index=True, right_index=True, how='outer')
        return _merged_df
    
    @staticmethod
    def get_all_merged_s_code_data(s_code_list, ak_func_name, config_dict, period, start_date, end_date, adjust):
        _len_s_code_list = len(s_code_list)
        _df_result = pd.DataFrame()
        for _index, _s_code in enumerate(s_code_list, start=1):
            logger.info(f'({_index}/{_len_s_code_list}) downloading data with s_code={_s_code}')
            _merge_s_code_df = UtilFuncs.merge_s_code_data(ak_func_name, config_dict, _s_code, period, start_date, end_date, adjust)
            _df_result = pd.concat([_df_result, _merge_s_code_df], axis=0)
        return _df_result

    @staticmethod
    def store_trade_date():
        raise NotImplementedError
    # endregion stock funcs

    # region tool funcs
    @staticmethod
    def get_data_and_save2csv(redis_key, ak_func_name, pg_conn, redis_conn, temp_dir = '/tmp/ak_dg'):
        _table_name = f"ak_dg_{ak_func_name}"
        _date_df = UtilFuncs.read_df_from_redis(redis_key, redis_conn)

        _date_list = [ut.format_td8(date) for date in _date_df['Date'].sort_values(ascending=False).tolist()]
        logger.debug(f"date_list:{_date_list}")
        _ak_data_df = UtilFuncs.get_data_by_td_list(ak_func_name, _date_list)
        _desired_columns = UtilFuncs.get_columns_from_table(pg_conn, _table_name)
        _ak_data_df = _ak_data_df[_desired_columns]

        os.makedirs(temp_dir, exist_ok=True)
        _temp_csv_path = os.path.join(temp_dir, f'{ak_func_name}.csv')
        _ak_data_df.to_csv(_temp_csv_path, index=False, header=False)

        return _temp_csv_path

    @staticmethod
    def get_columns_from_table(pg_conn, table_name):
        """
        Retrieve the column names of a specified table in the PostgreSQL database.
        
        :param pg_conn: A connection object to the PostgreSQL database.
        :param table_name: The name of the table to retrieve column names from.
        :return: A list of column names in the order they appear in the database.
        """
        columns = []
        cursor = pg_conn.cursor()
        try:
            # SQL query to get the column names ordered by their ordinal position
            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
                (table_name,)
            )
            columns = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print("Error fetching column names from table '{}': {}".format(table_name, e))
        finally:
            cursor.close()
        return columns

    @staticmethod
    def pref_s_code(s_code):
        if s_code.startswith('00'):
            _pref = 'szmb_'
        elif s_code.startswith('30'):
            _pref = 'szcy_'
        elif s_code.startswith('60'):
            _pref = 'shmb_'
        elif s_code.startswith('68'):
            _pref = 'shkc_'
        elif s_code.startswith(('83', '87', '88')):
            _pref = 'bj_'
        else:
            _pref = 'unknown_'
        return _pref + s_code
    
    @staticmethod
    def generate_dt_list(begin_dt, end_dt, dt_format='%Y-%m-%d', ascending=False):
        start = datetime.strptime(ut.format_td10(begin_dt), '%Y-%m-%d')
        end = datetime.strptime(ut.format_td10(end_dt), '%Y-%m-%d')
        date_list = []
        if ascending:
            while start <= end:
                date_list.append(start.strftime(dt_format))
                start += timedelta(days=1)
        else:
            while end >= start:
                date_list.append(end.strftime(dt_format))
                end -= timedelta(days=1)
        return date_list

    @staticmethod
    def try_to_call(
            ak_func,
            param_dict: Optional[dict] = None,
            num_retries: int = 5,
            retry_delay: int = 5) -> Optional[pd.DataFrame]:
        param_dict = param_dict or {}
        for attempt in range(num_retries):
            try:
                logger.info(f'Trying to call function {ak_func.__name__} with params {param_dict}, attempt {attempt + 1}')
                result = ak_func(**param_dict)
                logger.info(f'get {len(result)} data.')
                return result
            except ConnectionError as e:
                _rd = retry_delay * (1 + num_retries)
                logger.warning(f"Attempt {attempt + 1}: ConnectionError encountered. Retrying after {_rd} seconds...")
                time.sleep(_rd)
            except Exception as e:
                logger.error(f"Error calling function {ak_func.__name__} with parameters: {param_dict}. Error: {e}")
                raise AirflowException()
        logger.error(f'Failed to call function {ak_func.__name__} after {num_retries} attempts with parameters: {param_dict}')
        raise AirflowException(f'Function {ak_func.__name__} failed after {num_retries} attempts')

    @staticmethod
    def get_data_today(ak_func_name: str, ak_cols_config_dict:dict, date_format: str = '%Y-%m-%d') -> pd.DataFrame:
        _today_date = datetime.now().strftime(date_format)  # Get today's date in the specified format
        _ak_func = getattr(ak, ak_func_name, None)
        
        if _ak_func is None:
            error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(error_msg)
            raise AirflowException(error_msg)
        
        try:
            _df = UtilFuncs.try_to_call(_ak_func)
            if _df is None or _df.empty:
                if _df is None:
                    error_msg = f'Data function {ak_func_name} returned None for today ({_today_date}).'
                    logger.error(error_msg)
                    raise AirflowException(error_msg)
                else:
                    warning_msg = f'No data found for {ak_func_name} for today ({_today_date}).'
                    logger.warning(warning_msg)
                    return pd.DataFrame()  # Return empty DataFrame to avoid further errors
            
            _df = ut.remove_cols(_df, ak_cols_config_dict[ak_func_name])
            _df.rename(columns=ut.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            _df['date'] = _today_date  # Add a new column 'date' with today's date
            # _df['date'] = pd.to_datetime(_df['date'], errors='coerce')
            return _df
        except Exception as e:
            logger.error(f"Error calling function {ak_func_name} for today ({_today_date}): {e}\n{traceback.format_exc()}")
            raise AirflowException(f"Error calling function {ak_func_name} for today ({_today_date}): {e}")

    @staticmethod
    def get_data_by_td(ak_func_name: str, ak_cols_config_dict:dict, td: str, td_pa_name: str = 'date') -> pd.DataFrame:
        _ak_func = getattr(ak, ak_func_name, None)
        if _ak_func is None:
            error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(error_msg)
            raise AirflowException(error_msg)
        
        _df = UtilFuncs.try_to_call(_ak_func, {td_pa_name: td})
        if _df is None or _df.empty:
            if _df is None:
                error_msg = f'Data function {ak_func_name} returned None for date {td}.'
                logger.error(error_msg)
                # raise AirflowException(error_msg)
            else:
                warning_msg = f'No data found for {ak_func_name} on {td}.'
                logger.warning(warning_msg)
            return pd.DataFrame()  # 返回空的 DataFrame，以避免进一步的处理出错
        
        _df = ut.remove_cols(_df, ak_cols_config_dict[ak_func_name])
        _df.rename(columns=ut.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
        # _df['s_code'] = _df['s_code'].apply(UtilFuncs.pref_s_code)
        _df = ut.add_td(_df, td)
        return _df

    @staticmethod
    def get_data_by_td_list(ak_func_name: str, td_list: list[str], td_pa_name: str = 'date', max_retry=20) -> pd.DataFrame:
        combined_df = pd.DataFrame()
        retry_count = 0

        for td in td_list:
            try:
                df = UtilFuncs.get_data_by_td(ak_func_name, td, td_pa_name)
                if df.empty:
                    retry_count += 1
                    if retry_count >= max_retry:
                        error_msg = f"Max retries reached ({max_retry}) with no data for {ak_func_name}."
                        logger.error(error_msg)
                        return combined_df
                    continue
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                retry_count = 0  # reset retry count after a successful fetch
            except AirflowException as e:
                logger.error(f"Error fetching data for date {td}: {str(e)}")
                if retry_count >= max_retry:
                    raise
                else:
                    retry_count += 1

        return combined_df


    @staticmethod
    def get_data_by_board_names(ak_func_name: str, ak_cols_config_dict:dict, board_names: list[str], date_format: str = '%Y-%m-%d') -> pd.DataFrame:
        """
        Retrieve stock constituent data for a list of board names using specified akshare function.

        :param ak_func_name: The name of the akshare function to use (e.g., 'stock_board_concept_cons_ths').
        :param board_names: List of board names (e.g., ['AI手机', '车联网']).
        :return: A DataFrame containing combined data for all board names.
        """
        all_data = []
        for b_name in board_names:
            try:
                # Fetch the akshare function dynamically
                data_func = getattr(ak, ak_func_name, None)
                if data_func is None:
                    logger.error(f"Function {ak_func_name} not found in akshare.")
                    continue
                
                # Call the function using try_to_call for retries and error handling
                data = UtilFuncs.try_to_call(data_func, {'symbol': b_name})
                if data is not None and not data.empty:
                    data['b_name'] = b_name  # Add the board name as a column to the DataFrame
                    all_data.append(data)
                else:
                    logger.warning(f"No data found for board {b_name}")
            except Exception as e:
                logger.error(f"Failed to fetch data for board {b_name}: {e}")

        # Combine all data into a single DataFrame
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = ut.remove_cols(combined_df, ak_cols_config_dict[ak_func_name])
            combined_df.rename(columns=ut.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            _today_date = datetime.now().strftime(date_format)
            combined_df['date'] = _today_date
            return combined_df
        else:
            return pd.DataFrame()  # Return an empty DataFrame if no data was fetched

    @staticmethod
    def write_df_to_redis(key: str, df: pd.DataFrame, conn: redis.Redis, ttl: int):
        try:
            _data_json = df.to_json(date_format='iso')
            conn.setex(key, ttl, _data_json)
            logger.info(f"DataFrame written to Redis under key: {key} with TTL {ttl} seconds.")
            # logger.debug(f"_data_json:{_data_json}")
        except Exception as e:
            error_msg = f"Error writing DataFrame to Redis for key: {key}. Traceback: {traceback.format_exc()}"
            logger.error(error_msg)
            raise AirflowException(error_msg)

    @staticmethod
    def read_df_from_redis(key: str, conn: redis.Redis) -> Optional[pd.DataFrame]:
        try:
            _data_json = conn.get(key)
            if _data_json:
                _data_json = _data_json.decode('utf-8') if isinstance(_data_json, bytes) else _data_json
                dtype_spec = {col: str for col in con.POSSIBLE_CODE_COLUMNS} 
                _df = pd.read_json(BytesIO(_data_json.encode()), dtype=dtype_spec)
                logger.info(f"DataFrame read from Redis for key: {key}")
                return _df
            else:
                logger.warning(f"No data found in Redis for key: {key}")
                return None
        except Exception as e:
            error_msg = f"Error reading DataFrame from Redis for key: {key}. Traceback: {traceback.format_exc()}"
            logger.error(error_msg)
            raise AirflowException(error_msg)

    @staticmethod
    def download_and_cache_ak_data_by_td(
            ak_func_name: str, 
            td: Union[str, date], 
            redis_conn: redis.Redis, 
            ttl: int = default_redis_ttl
        ) -> str:        
        try:
            _td = ut.format_td8(td)
        except Exception as e:
            logger.error(f"Error formatting date '{td}': {e}\n{traceback.format_exc()}")
            raise AirflowException(f"Error formatting date '{td}': {e}")

        try:
            _df = UtilFuncs.get_data_by_td(ak_func_name, _td)
            if _df is None or _df.empty:
                if _df is None:
                    logger.error(f"Failed to obtain data for {ak_func_name} on {_td}.")
                    raise AirflowException(f"Failed to obtain data for {ak_func_name} on {_td}.")
                else:
                    logger.warning(f"No data found for {ak_func_name} on {_td}, nothing written to Redis.")
                    return
            _redis_key = f'{ak_func_name}@{_td}'
            UtilFuncs.write_df_to_redis(_redis_key, _df, redis_conn, ttl)
            logger.info(f"Data for {ak_func_name} on {_td} written to Redis.")
            return _redis_key
        except Exception as e:
            logger.error(f"Error processing data for {ak_func_name} on {_td}: {e}\n{traceback.format_exc()}")
            raise AirflowException(f"Error processing data for {ak_func_name} on {_td}: {e}")
    # endregion tool funcs

# region store once
    @staticmethod
    def load_ak_cols_config(config_file_path: str) -> dict:
        config = {}
        with open(config_file_path, 'r', encoding='utf-8') as file:
            exec(file.read(), {}, config)
        return config['ak_cols_config']

    @staticmethod
    def write_df_to_redis(key: str, df: pd.DataFrame, conn, ttl: int):
        """
        Serialize a DataFrame to JSON and store it in Redis.
        """
        try:
            # Convert DataFrame to JSON
            data_json = df.to_json(date_format='iso')
            # Set in Redis with expiration time
            conn.setex(key, ttl, data_json)
            logger.info(f"DataFrame written to Redis under key: {key} with TTL {ttl} seconds.")
        except Exception as e:
            logger.error(f"Error writing DataFrame to Redis: {str(e)}")
            raise AirflowException(f"Error writing DataFrame to Redis: {str(e)}")

    @staticmethod
    def get_df_from_redis(key: str, conn):
        """
        Retrieve a DataFrame from Redis using the specified key and convert it from JSON.
        Check if the key is not None and data exists for the key.
        """
        if key is None:
            logger.error("Key is None, cannot retrieve data from Redis.")
            return pd.DataFrame()  # Return an empty DataFrame if the key is None

        try:
            data_json = conn.get(key)
            if not data_json:
                logger.warning(f"No data found in Redis for key: {key}")
                return pd.DataFrame()  # Return an empty DataFrame if no data found

            df = pd.read_json(BytesIO(data_json), dtype=str)
            logger.info(f"DataFrame retrieved from Redis for key: {key}")
            return df
        except Exception as e:
            logger.error(f"Error reading DataFrame from Redis for key: {key}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

        
    @staticmethod
    def store_ak_data(pg_conn, ak_func_name, insert_sql, truncate=False):
        cursor = pg_conn.cursor()
        try:
            cursor.execute(insert_sql)
            # Fetch all returned rows if RETURNING is used
            inserted_rows = cursor.fetchall()

            pg_conn.commit()
            logger.info(f"Data successfully inserted into table for {ak_func_name}")

            if truncate:
                truncate_sql = f"TRUNCATE TABLE ak_dg_{ak_func_name};"
                cursor.execute(truncate_sql)
                pg_conn.commit()
                logger.info(f"Table ak_dg_{ak_func_name} has been truncated")

            return inserted_rows  # Return the list of inserted rows

        except Exception as e:
            pg_conn.rollback()
            logger.error(f"Failed to store data for {ak_func_name}: {e}")
            raise
        finally:
            cursor.close()

    @staticmethod
    def push_data(context, key, value):
        if hasattr(context, 'xcom_push'):
            context.xcom_push(key=key, value=value)
        elif isinstance(context, redis.Redis):
            context.set(key, value)
        else:
            raise ValueError("Unsupported context provided for data push.")

    @staticmethod
    def pull_data(context, key):
        """
        Pull data from XCom based on the context and key provided.
        """
        if hasattr(context, 'xcom_pull'):
            return context.xcom_pull(key=key)
        else:
            raise ValueError("Context does not support xcom_pull operation.")

    @staticmethod
    def save_data_to_csv(df, filename, dir_path='/tmp/ak_data', include_header=True):
        if df.empty:
            logger.warning("No data to save to CSV.")
            return None
        try:
            os.makedirs(dir_path, exist_ok=True)  # Ensure the directory exists
            file_path = os.path.join(dir_path, f"{filename}.csv")
            df.to_csv(file_path, index=False, header=include_header)
            logger.info(f"Data saved to CSV at {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to save data to CSV: {e}")
            return None

    @staticmethod
    def insert_data_from_csv(conn, csv_path, table_name):
        if not os.path.exists(csv_path):
            logger.error("CSV file does not exist.")
            return
        try:
            cursor = conn.cursor()
            with open(csv_path, 'r') as file:
                # Use copy_expert to specify COPY command with CSV HEADER
                copy_sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','"
                cursor.copy_expert(sql=copy_sql, file=file)
                conn.commit()
                logger.info(f"Data from {csv_path} successfully loaded into {table_name}.")
            cursor.close()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to load data from CSV: {e}")
            raise AirflowException(e)
        

    @staticmethod
    def insert_data_from_csv1(conn, csv_path, table_name):
        if not os.path.exists(csv_path):
            logger.error("CSV file does not exist.")
            return
        try:
            cursor = conn.cursor()
            with open(csv_path, 'r') as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                
                for row in csvreader:
                    placeholders = ','.join(['%s'] * len(row))
                    insert_sql = f"""
                    INSERT INTO {table_name} ({','.join(header)})
                    VALUES ({placeholders});
                    """
                    cursor.execute(insert_sql, row)
            
            conn.commit()
            logger.info(f"Data from {csv_path} successfully loaded into {table_name}.")
            cursor.close()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to load data from CSV: {e}")
            raise AirflowException(e)
        
    @staticmethod
    def get_columns_from_table(pg_conn, table_name, redis_conn: redis.Redis, ttl: int = default_redis_ttl):
        redis_key = f"columns_{table_name}"
        
        try:
            # Try to get the column names from Redis
            cached_columns = redis_conn.get(redis_key)
            if cached_columns:
                columns = json.loads(cached_columns)
                logger.info(f"Retrieved column names for table '{table_name}' from Redis.")
                return columns
        except Exception as e:
            logger.warning(f"Failed to read column names from Redis for table '{table_name}': {e}")

        # If not found in Redis, fetch from PostgreSQL
        columns = []
        cursor = pg_conn.cursor()
        try:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
                (table_name,)
            )
            columns = [row[0] for row in cursor.fetchall()]
            # Cache the column names in Redis
            try:
                redis_conn.setex(redis_key, ttl, json.dumps(columns))
                logger.info(f"Retrieved column names for table '{table_name}' from PostgreSQL and cached in Redis.")
            except Exception as e:
                logger.warning(f"Failed to cache column names in Redis for table '{table_name}': {e}")
        except Exception as e:
            logger.error(f"Error fetching column names from table '{table_name}': {e}")
        finally:
            cursor.close()

        return columns


    @staticmethod
    def convert_columns(df, table_name, pg_conn, redis_conn: redis.Redis):
        columns = UtilFuncs.get_columns_from_table(pg_conn, table_name, redis_conn)
        # logger.warning(columns)
        if columns is None or len(columns) < 1 :
            raise AirflowException(f"Can't found columns using table_name {table_name}")

        df = df[columns]
        return df
    
# endregion store once


# region tracing data funcs
    @staticmethod
    def generate_date_list(start_date, end_date, ascending=True):
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        date_list = []
        step = timedelta(days=1)

        if ascending:
            while start <= end:
                date_list.append(start.strftime('%Y-%m-%d'))
                start += step
        else:
            while end >= start:
                date_list.append(end.strftime('%Y-%m-%d'))
                end -= step

        return date_list
    
    @staticmethod
    def get_date_list(pg_conn, key, begin='1990-01-01'):
        end_date = datetime.now() - timedelta(days=1)
        end_str = end_date.strftime('%Y-%m-%d')

        all_date_list = UtilFuncs.generate_date_list(begin, end_str, ascending=False)
        logger.debug(f"in func 'get_date_list', all_date_list[:30]:{all_date_list[:30]}")
        current_df = UtilFuncs.get_tracing_by_date(pg_conn, key)
        logger.debug(f"in func 'get_date_list', current_df.head(10):{current_df.head(10)}")
        
        if not current_df.empty:
            current_date_list = current_df['dt'].apply(ut.format_td10).tolist()
            logger.debug(f"in func 'get_date_list', current_date_list[:30]:{current_date_list[:30]}")
        else:
            current_date_list = []

        missing_date_list = [td for td in all_date_list if td not in current_date_list]
        logger.debug(f"in func 'get_date_list', missing_date_list:{missing_date_list}")

        return sorted(missing_date_list, reverse=True)

    @staticmethod
    def get_tracing_by_date(pg_conn, key):
        sql = """
        SELECT ak_func_name, dt, create_time, update_time, category, is_active, host_name
        FROM dg_ak_tracing_dt
        WHERE ak_func_name = %s;
        """

        # Use the provided PostgresHook to run the query and fetch data
        cursor = pg_conn.cursor()
        
        try:
            cursor.execute(sql, (key,))
            rows = cursor.fetchall()
            df = pd.DataFrame(
                rows, 
                columns=[
                    'ak_func_name', 'dt', 'create_time', 'update_time', 
                    'category', 'is_active', 'host_name'
                    ])
            return df
        finally:
            cursor.close()

    @staticmethod
    def prepare_tracing_data(ak_func_name, param_name, date_values):
        host_name = os.getenv('HOSTNAME', socket.gethostname())
        current_time = datetime.now()
        data = []
        for date, value in date_values:
            data.append((ak_func_name, param_name, value, date, current_time, current_time, host_name))
        return data

    @staticmethod
    def execute_tracing_data_insert(conn, insert_sql, data):
        cursor = conn.cursor()
        try:
            cursor.executemany(insert_sql, data)
            conn.commit()
            logger.info("Tracing data inserted/updated successfully.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert/update tracing data: {e}")
            raise AirflowException(e)
        finally:
            cursor.close()

    @staticmethod
    def insert_tracing_date_data(conn, ak_func_name, date_list):
        host_name = os.getenv('HOSTNAME', socket.gethostname())
        current_time = datetime.now()
        data = []
        for date in date_list:
            data.append((ak_func_name, date, current_time, current_time, host_name))
        insert_sql = """
            INSERT INTO dg_ak_tracing_date (ak_func_name, date, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, date) DO UPDATE 
            SET update_time = EXCLUDED.update_time;
            """
        UtilFuncs.execute_tracing_data_insert(conn, insert_sql, data)

    @staticmethod
    def insert_tracing_date_1_param_data(conn, ak_func_name, param_name, date_values):
        data = UtilFuncs.prepare_tracing_data(ak_func_name, param_name, date_values)
        insert_sql = """
            INSERT INTO dg_ak_tracing_date_1_param (ak_func_name, param_name, param_value, date, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, param_name, param_value, date) DO UPDATE 
            SET update_time = EXCLUDED.update_time;
            """
        UtilFuncs.execute_tracing_data_insert(conn, insert_sql, data)

    @staticmethod
    def insert_tracing_scode_date_data(conn, ak_func_name, scode_list, date):
        data = [(ak_func_name, scode, date, datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for scode in scode_list]
        insert_sql = """
            INSERT INTO dg_ak_tracing_scode_date (ak_func_name, scode, date, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, scode, date) DO UPDATE 
            SET update_time = EXCLUDED.update_time;
            """
        UtilFuncs.execute_tracing_data_insert(conn, insert_sql, data)
# endregion tracing data funcs

##################################################
#### test
# df = UtilFuncs.get_data_by_td('stock_zt_pool_em', '20240402')
# print(df.columns)
# print(df.head(3))

# from airflow.providers.postgres.hooks.postgres import PostgresHook
# pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
# pg_conn = pgsql_hook.get_conn()

# UtilFuncs.insert_data_from_csv(pg_conn, '/tmp/ak_data/stock_board_concept_name_em.csv', f'ak_dg_stock_board_concept_name_em')

# from airflow.providers.redis.hooks.redis import RedisHook
# REDIS_CONN_ID = "local_redis_3"
# redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
# s_code_list = UtilFuncs.get_s_code_list(redis_hook.get_conn())
# print(s_code_list)


# from dg_ak.store_daily.stock.ak_dg_stock_config import stock_cols_config
# s_df = UtilFuncs.get_s_code_data('stock_zh_a_hist', stock_cols_config['stock_zh_a_hist'], s_code='000004', period='daily', start_date='20240101', end_date='20240424', adjust="")
# hfq_s_df = UtilFuncs.get_s_code_data('stock_zh_a_hist', stock_cols_config['stock_zh_a_hist'], s_code='000004', period='daily', start_date='20240101', end_date='20240424', adjust='hfq')
# s_df.set_index('td', inplace=True)
# hfq_s_df.set_index('td', inplace=True)
# hfq_s_df = hfq_s_df.add_suffix('_hfq')
# print(s_df.head(1))
# print(hfq_s_df.head(1))
# merged_df = pd.merge(s_df, hfq_s_df, left_index=True, right_index=True, how='outer')
# print(merged_df[['o', 'o_hfq', 'c', 'c_hfq']].head())
# print(merged_df[['o', 'o_hfq', 'c', 'c_hfq']].tail())

# print(UtilFuncs.generate_dt_list('2024-04-01','2024-04-10',dt_format='%Y%m%d'))
##################################################