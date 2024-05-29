
import os
import sys
from pathlib import Path
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
print(project_root)
# 将项目根目录添加到sys.path中
sys.path.append(project_root)

import csv
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

import utils.config as con
from utils.ak_utils import AkUtilTools
from utils.logger import logger

from airflow.exceptions import AirflowException

# Logger debug switch
LOGGER_DEBUG = con.LOGGER_DEBUG

class DateOutOfRangeException(Exception):
    pass

class DgAkUtilFuncs(AkUtilTools):

    default_pause_time = 0.2  # 200ms

    # region stock funcs

    @staticmethod
    def get_s_code_data(ak_func_name, ak_cols_config_dict, s_code, period, start_date, end_date, adjust, pause_time: float = default_pause_time):
        if LOGGER_DEBUG:
            logger.debug(f"Fetching data for s_code: {s_code}, period: {period}, start_date: {start_date}, end_date: {end_date}, adjust: {adjust}")
        _ak_func = getattr(ak, ak_func_name, None)
        if _ak_func is None:
            _error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(_error_msg)
            raise AirflowException(_error_msg)
        if adjust is None:
            _s_df = DgAkUtilFuncs.try_to_call(
                _ak_func,
                {'symbol': s_code, 'period': period,
                 'start_date': start_date, 'end_date': end_date
                 }, pause_time=pause_time)
        else:
            _s_df = DgAkUtilFuncs.try_to_call(
                _ak_func,
                {'symbol': s_code, 'period': period,
                 'start_date': start_date, 'end_date': end_date, 'adjust': adjust
                 }, pause_time=pause_time)
        if _s_df is None or _s_df.empty:
            if _s_df is None:
                _error_msg = f'Data function {ak_func_name} returned None with params(s_code={s_code}, period={period}, start_date={start_date}, end_date={end_date}, adjust={adjust}).'
                logger.error(_error_msg)
            else:
                _warning_msg = f'No data found for {ak_func_name} with params(s_code={s_code}, period={period}, start_date={start_date}, end_date={end_date}, adjust={adjust}).'
                logger.warning(_warning_msg)
            return pd.DataFrame()  # 返回空的 DataFrame，以避免进一步的处理出错

        if LOGGER_DEBUG:
            logger.debug(f"Removing unnecessary columns for ak_func_name: {ak_func_name}")
        _s_df = DgAkUtilFuncs.remove_cols(_s_df, ak_cols_config_dict[ak_func_name])
        _s_df.rename(columns=DgAkUtilFuncs.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
        _s_df['s_code'] = s_code
        if LOGGER_DEBUG:
            logger.debug(f'Processed s_code data for {s_code}, length: {len(_s_df)}, first 5 rows: {_s_df.head().to_dict(orient="records")}')
        return _s_df

    @staticmethod
    def merge_s_code_data(ak_func_name, config_dict, s_code, period, start_date, end_date, adjust, pause_time: float = default_pause_time):
        if LOGGER_DEBUG:
            logger.debug(f"Merging data for s_code: {s_code}")
        _s_df = DgAkUtilFuncs.get_s_code_data(ak_func_name, config_dict, s_code, period, start_date, end_date, adjust, pause_time=pause_time)
        _hfq_s_df = DgAkUtilFuncs.get_s_code_data(ak_func_name, config_dict, s_code, period, start_date, end_date, adjust, pause_time=pause_time)
        _s_df.set_index('td', inplace=True)
        _hfq_s_df.set_index('td', inplace=True)
        _hfq_s_df = _hfq_s_df.add_suffix('_hfq')
        _merged_df = pd.merge(_s_df, _hfq_s_df, left_index=True, right_index=True, how='outer')
        if LOGGER_DEBUG:
            logger.debug(f"Merged data for s_code: {s_code}, merged data shape: {_merged_df.shape}, first 5 rows: {_merged_df.head().to_dict(orient='records')}")
        return _merged_df

    @staticmethod
    def get_all_merged_s_code_data(s_code_list, ak_func_name, config_dict, period, start_date, end_date, adjust, pause_time: float = default_pause_time):
        if LOGGER_DEBUG:
            logger.debug(f"Getting all merged data for {len(s_code_list)} stock codes.")
        _len_s_code_list = len(s_code_list)
        _df_result = pd.DataFrame()
        for _index, _s_code in enumerate(s_code_list, start=1):
            logger.info(f'({_index}/{_len_s_code_list}) downloading data with s_code={_s_code}')
            _merge_s_code_df = DgAkUtilFuncs.merge_s_code_data(ak_func_name, config_dict, _s_code, period, start_date, end_date, adjust, pause_time=pause_time)
            _df_result = pd.concat([_df_result, _merge_s_code_df], axis=0)
        if LOGGER_DEBUG:
            logger.debug(f"Total merged data shape: {_df_result.shape}, first 5 rows: {_df_result.head().to_dict(orient='records')}")
        return _df_result

    @staticmethod
    def store_trade_date():
        raise NotImplementedError
    # endregion stock funcs

    # region tool funcs
    @staticmethod
    def get_trade_dates(pg_conn) -> list:
        _query = f"SELECT trade_date FROM ak_dg_stock_zh_a_trade_date;"
        if LOGGER_DEBUG:
            logger.debug(f"Executing query to get trade dates: {_query}")
        try:
            _df = pd.read_sql(_query, pg_conn)
            _trade_dates = _df['trade_date'].tolist()
            logger.info("Trade dates retrieved successfully from ak_dg_stock_zh_a_trade_date.")
            if LOGGER_DEBUG:
                logger.debug(f"Trade dates length: {len(_trade_dates)}, first 5 dates: {_trade_dates[:5]}")
            return _trade_dates
        except Exception as _e:
            logger.error(f"Failed to retrieve trade dates: {_e}")
            raise AirflowException(f"Failed to retrieve trade dates: {_e}")

    @staticmethod
    def is_valid_date(date, ak_func_name):
        try:
            _ak_func = getattr(ak, ak_func_name, None)
            if _ak_func:
                _ak_func(date=date)
            return True
        except ValueError as ve:
            if "Length mismatch" in str(ve):
                logger.warning(f"Date {date} is out of range for function {ak_func_name}")
                return False
        except Exception as e:
            logger.error(f"Error validating date {date} for function {ak_func_name}: {e}")
            return False
        return True

    @staticmethod
    def get_data_and_save2csv(redis_key, ak_func_name, ak_cols_config_dict, pg_conn, redis_conn, temp_dir='/tmp/ak_dg'):
        if LOGGER_DEBUG:
            logger.debug(f"Fetching data for redis_key: {redis_key}, ak_func_name: {ak_func_name}")
        _table_name = f"ak_dg_{ak_func_name}"
        _date_df = DgAkUtilFuncs.read_df_from_redis(redis_key, redis_conn)

        _date_list = [DgAkUtilFuncs.format_td8(_date) for _date in _date_df['td'].sort_values(ascending=False).tolist()]
        if LOGGER_DEBUG:
            logger.debug(f"date_list length: {len(_date_list)}, first 5 dates: {_date_list[:5]}")
        
        # Check if dates are within the valid range
        _date_list = [date for date in _date_list if DgAkUtilFuncs.is_valid_date(date, ak_func_name)]

        _ak_data_df = DgAkUtilFuncs.get_data_by_td_list(ak_func_name, ak_cols_config_dict, _date_list)
        
        # Flatten the tuple list into column names
        _desired_columns = [col[0] for col in DgAkUtilFuncs.get_columns_from_table(pg_conn, _table_name, redis_conn)]
        
        try:
            _ak_data_df = _ak_data_df[_desired_columns]
        except KeyError as e:
            logger.error(f"KeyError while selecting columns for {ak_func_name}: {str(e)}")
            raise

        os.makedirs(temp_dir, exist_ok=True)
        _temp_csv_path = os.path.join(temp_dir, f'{ak_func_name}.csv')
        _ak_data_df.to_csv(_temp_csv_path, index=False, header=False)

        if LOGGER_DEBUG:
            logger.debug(f"Data saved to CSV at {_temp_csv_path}, length: {len(_ak_data_df)}, first 5 rows: {_ak_data_df.head().to_dict(orient='records')}")
        return _temp_csv_path


    @staticmethod
    def pref_s_code(s_code):
        _pref = 'unknown_'
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
        if LOGGER_DEBUG:
            logger.debug(f"Prefixed s_code: {_pref + s_code}")
        return _pref + s_code

    @staticmethod
    def generate_dt_list(begin_dt, end_dt, dt_format='%Y-%m-%d', ascending=False):
        _start = datetime.strptime(DgAkUtilFuncs.format_td10(begin_dt), '%Y-%m-%d')
        _end = datetime.strptime(DgAkUtilFuncs.format_td10(end_dt), '%Y-%m-%d')
        _date_list = []
        if ascending:
            while _start <= _end:
                _date_list.append(_start.strftime(dt_format))
                _start += timedelta(days=1)
        else:
            while _end >= _start:
                _date_list.append(_end.strftime(dt_format))
                _end -= timedelta(days=1)
        if LOGGER_DEBUG:
            logger.debug(f"Generated date list length: {len(_date_list)}, first 5 dates: {_date_list[:5]}")
        return _date_list


    @staticmethod
    def try_to_call(
            ak_func,
            param_dict: Optional[dict] = None,
            num_retries: int = 5,
            retry_delay: int = 5,
            pause_time: float = default_pause_time) -> Optional[pd.DataFrame]:
        _param_dict = param_dict or {}
        for _attempt in range(num_retries):
            try:
                logger.info(f'Trying to call function {ak_func.__name__} with params {_param_dict}, attempt {_attempt + 1}')
                _result = ak_func(**_param_dict)
                time.sleep(pause_time)  # Pause after each call
                if _result is not None and not _result.empty:
                    logger.info(f'get {len(_result)} data.')
                    if LOGGER_DEBUG:
                        logger.debug(f"Retrieved data length: {len(_result)}, first 5 rows: {_result.head().to_dict(orient='records')}")
                    return _result
                else:
                    logger.warning(f"No data retrieved in attempt {_attempt + 1}.")
            except ConnectionError as _e:
                _retry_delay = retry_delay * (1 + _attempt)
                logger.warning(f"Attempt {_attempt + 1}: ConnectionError encountered. Retrying after {_retry_delay} seconds...")
                time.sleep(_retry_delay)
            except ValueError as ve:
                if "Length mismatch" in str(ve):
                    logger.error(f"Date range exceeded for function {ak_func.__name__} with params {_param_dict}.")
                    raise DateOutOfRangeException(f"Date range exceeded for function {ak_func.__name__} with params {_param_dict}.")
                elif "只能获取最近 30 个交易日的数据" in str(ve):
                    logger.error(f"ValueError calling function {ak_func.__name__} with parameters: {_param_dict}. Error: {ve}")
                    raise DateOutOfRangeException(f"ValueError calling function {ak_func.__name__} with parameters: {_param_dict}. Error: {ve}")
                else:
                    logger.error(f"ValueError calling function {ak_func.__name__} with parameters: {_param_dict}. Error: {ve}")
                    raise AirflowException()
            except Exception as _e:
                logger.error(f"Error calling function {ak_func.__name__} with parameters: {_param_dict}. Error: {_e}")
                raise AirflowException()
        logger.error(f'Failed to call function {ak_func.__name__} after {num_retries} attempts with parameters: {_param_dict}')
        return None




    @staticmethod
    def get_data_today(ak_func_name: str, ak_cols_config_dict: dict, date_format: str = '%Y-%m-%d') -> pd.DataFrame:
        _today_date = datetime.now().strftime(date_format)  # Get today's date in the specified format
        _ak_func = getattr(ak, ak_func_name, None)

        if _ak_func is None:
            _error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(_error_msg)
            raise AirflowException(_error_msg)

        try:
            _df = DgAkUtilFuncs.try_to_call(_ak_func)
            if _df is None or _df.empty:
                if _df is None:
                    _error_msg = f'Data function {ak_func_name} returned None for today ({_today_date}).'
                    logger.error(_error_msg)
                else:
                    _warning_msg = f'No data found for {ak_func_name} for today ({_today_date}).'
                    logger.warning(_warning_msg)
                return pd.DataFrame()  # Return empty DataFrame to avoid further errors

            _df = DgAkUtilFuncs.remove_cols(_df, ak_cols_config_dict[ak_func_name])
            _df.rename(columns=DgAkUtilFuncs.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            _df['td'] = _today_date  # Add a new column 'date' with today's date
            if LOGGER_DEBUG:
                logger.debug(f"Retrieved data for today length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
            return _df
        except Exception as _e:
            logger.error(f"Error calling function {ak_func_name} for today ({_today_date}): {_e}\n{traceback.format_exc()}")
            raise AirflowException(f"Error calling function {ak_func_name} for today ({_today_date}): {_e}")

    @staticmethod
    def get_data_by_td(ak_func_name: str, ak_cols_config_dict: dict, td: str, td_pa_name: str = 'date') -> pd.DataFrame:
        if LOGGER_DEBUG:
            logger.debug(f"Fetching data for date: {td}")
        _ak_func = getattr(ak, ak_func_name, None)
        if _ak_func is None:
            _error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(_error_msg)
            raise AirflowException(_error_msg)

        _df = DgAkUtilFuncs.try_to_call(_ak_func, {td_pa_name: td})
        if _df is None or _df.empty:
            if _df is None:
                _error_msg = f'Data function {ak_func_name} returned None for date {td}.'
                logger.error(_error_msg)
            else:
                _warning_msg = f'No data found for {ak_func_name} on {td}.'
                logger.warning(_warning_msg)
            return pd.DataFrame()  # 返回空的 DataFrame，以避免进一步的处理出错

        _df = DgAkUtilFuncs.remove_cols(_df, ak_cols_config_dict[ak_func_name])
        _df.rename(columns=DgAkUtilFuncs.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
        _df = DgAkUtilFuncs.add_td(_df, td)
        if LOGGER_DEBUG:
            logger.debug(f"Retrieved data for date {td}, length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
        return _df

    @staticmethod
    def get_data_by_td_list(ak_func_name: str, ak_cols_config_dict: dict,  td_list: list[str], td_pa_name: str = 'date', max_retry=20) -> pd.DataFrame:
        _combined_df = pd.DataFrame()
        _retry_count = 0
        if LOGGER_DEBUG:
            logger.debug(f"Fetching data for date list length: {len(td_list)}, first 5 dates: {td_list[:5]}")

        for _td in td_list:
            try:
                _df = DgAkUtilFuncs.get_data_by_td(ak_func_name, ak_cols_config_dict, _td, td_pa_name)
                if _df.empty:
                    _retry_count += 1
                    if _retry_count >= max_retry:
                        _error_msg = f"Max retries reached ({max_retry}) with no data for {ak_func_name}."
                        logger.error(_error_msg)
                        return _combined_df
                    continue
                _combined_df = pd.concat([_combined_df, _df], ignore_index=True)
                _retry_count = 0  # reset retry count after a successful fetch
            except DateOutOfRangeException:
                logger.warning(f"Data for date {_td} is out of range. Skipping this date.")
                continue
            except AirflowException as _e:
                logger.error(f"Error fetching data for date {_td}: {str(_e)}")
                if _retry_count >= max_retry:
                    raise
                else:
                    _retry_count += 1

        if LOGGER_DEBUG:
            logger.debug(f"Combined data length: {len(_combined_df)}, first 5 rows: {_combined_df.head().to_dict(orient='records')}")
        return _combined_df

    @staticmethod
    def get_data_by_board_names(ak_func_name: str, ak_cols_config_dict: dict, board_names: list[str], date_format: str = '%Y-%m-%d') -> pd.DataFrame:
        all_data = []
        for _b_name in board_names:
            try:
                _data_func = getattr(ak, ak_func_name, None)
                if _data_func is None:
                    logger.error(f"Function {ak_func_name} not found in akshare.")
                    continue

                _data = DgAkUtilFuncs.try_to_call(_data_func, {'symbol': _b_name})
                if (_data is not None) and (not _data.empty):
                    _data['b_name'] = _b_name  # Add the board name as a column to the DataFrame
                    all_data.append(_data)
                    if LOGGER_DEBUG:
                        logger.debug(f"Retrieved data for board {_b_name}, length: {len(_data)}, rows: {_data.head().to_dict(orient='records')}")
                else:
                    logger.warning(f"No data found for board {_b_name}")
            except Exception as _e:
                logger.error(f"Failed to fetch data for board {_b_name}: {_e}")

        if all_data:
            _combined_df = pd.concat(all_data, ignore_index=True)
            _combined_df = DgAkUtilFuncs.remove_cols(_combined_df, ak_cols_config_dict[ak_func_name])
            _combined_df.rename(columns=DgAkUtilFuncs.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            _combined_df['s_code'] = _combined_df['s_code'].astype(str) 
            _today_date = datetime.now().strftime(date_format)
            _combined_df['td'] = _today_date
            if LOGGER_DEBUG:
                logger.debug(f"Combined data for all boards length: {len(_combined_df)}, first 5 rows: {_combined_df.head().to_dict(orient='records')}")
            return _combined_df
        else:
            return pd.DataFrame()  # Return an empty DataFrame if no data was fetched
    # endregion tool funcs

    # region cache tools
    @staticmethod
    def write_list_to_redis(key: str, data_list: list, conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        try:
            # 转换日期对象为字符串
            _data_list = [str(item) if isinstance(item, (datetime, date)) else item for item in data_list]
            _data_json = json.dumps(_data_list)
            conn.setex(key, ttl, _data_json)
            logger.info(f"List written to Redis under key: {key} with TTL {ttl} seconds.")
            if LOGGER_DEBUG:
                logger.debug(f"List length: {len(_data_list)}, first 5 items: {_data_list[:5]}")
        except Exception as _e:
            logger.error(f"Error writing list to Redis: {str(_e)}")
            raise AirflowException(f"Error writing list to Redis: {str(_e)}")


    @staticmethod
    def read_list_from_redis(key: str, conn: redis.Redis) -> Optional[list]:
        try:
            _data_json = conn.get(key)
            if (_data_json):
                _data_list = json.loads(_data_json)
                _data_list = [datetime.strptime(item, '%Y-%m-%d').date() if len(item) == 10 else item for item in _data_list]
                logger.info(f"List retrieved from Redis for key: {key}")
                if LOGGER_DEBUG:
                    logger.debug(f"List length: {len(_data_list)}, first 5 items: {_data_list[:5]}")
                return _data_list
            else:
                logger.warning(f"No data found in Redis for key: {key}")
                return None
        except Exception as _e:
            logger.error(f"Error reading list from Redis for key: {key}: {_e}")
            return None

    @staticmethod
    def write_df_to_redis(key: str, df: pd.DataFrame, conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        try:
            _data_json = df.to_json(date_format='iso')
            conn.setex(key, ttl, _data_json)
            logger.info(f"DataFrame written to Redis under key: {key} with TTL {ttl} seconds.")
            if LOGGER_DEBUG:
                logger.debug(f"DataFrame length: {len(df)}, first 5 rows: {df.head().to_dict(orient='records')}")
        except Exception as _e:
            _error_msg = f"Error writing DataFrame to Redis for key: {key}. Traceback: {traceback.format_exc()}"
            logger.error(_error_msg)
            raise AirflowException(_error_msg)



    @staticmethod
    def download_and_cache_ak_data_by_td(
            ak_func_name: str,
            td: Union[str, date],
            redis_conn: redis.Redis,
            ttl: int = con.DEFAULT_REDIS_TTL
    ) -> str:
        try:
            _td = DgAkUtilFuncs.format_td8(td)
        except Exception as _e:
            logger.error(f"Error formatting date '{td}': {_e}\n{traceback.format_exc()}")
            raise AirflowException(f"Error formatting date '{td}': {_e}")

        try:
            _df = DgAkUtilFuncs.get_data_by_td(ak_func_name, _td)
            if _df is None or _df.empty:
                if _df is None:
                    logger.error(f"Failed to obtain data for {ak_func_name} on {_td}.")
                    raise AirflowException(f"Failed to obtain data for {ak_func_name} on {_td}.")
                else:
                    logger.warning(f"No data found for {ak_func_name} on {_td}, nothing written to Redis.")
                    return
            _redis_key = f'{ak_func_name}@{_td}'
            DgAkUtilFuncs.write_df_to_redis(_redis_key, _df, redis_conn, ttl)
            logger.info(f"Data for {ak_func_name} on {_td} written to Redis.")
            if LOGGER_DEBUG:
                logger.debug(f"Redis key: {_redis_key}, Data length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
            return _redis_key
        except Exception as _e:
            logger.error(f"Error processing data for {ak_func_name} on {_td}: {_e}\n{traceback.format_exc()}")
            raise AirflowException(f"Error processing data for {ak_func_name} on {_td}: {_e}")
    # endregion cache tools

    # region store once
    @staticmethod
    def load_ak_cols_config(config_file_path: str) -> dict:
        _config = {}
        if LOGGER_DEBUG:
            logger.debug(f"Loading AK columns configuration from: {config_file_path}")
        with open(config_file_path, 'r', encoding='utf-8') as _file:
            exec(_file.read(), {}, _config)
        return _config['ak_cols_config']

    @staticmethod
    def write_df_to_redis(key: str, df: pd.DataFrame, conn, ttl: int):
        try:
            _data_json = df.to_json(date_format='iso')
            conn.setex(key, ttl, _data_json)
            logger.info(f"DataFrame written to Redis under key: {key} with TTL {ttl} seconds.")
            if LOGGER_DEBUG:
                logger.debug(f"DataFrame length: {len(df)}, first 5 rows: {df.head().to_dict(orient='records')}")
        except Exception as _e:
            logger.error(f"Error writing DataFrame to Redis: {str(_e)}")
            raise AirflowException(f"Error writing DataFrame to Redis: {str(_e)}")

    @staticmethod
    def get_df_from_redis(key: str, conn):
        if key is None:
            logger.error("Key is None, cannot retrieve data from Redis.")
            return pd.DataFrame()  # Return an empty DataFrame if the key is None

        try:
            _data_json = conn.get(key)
            if not _data_json:
                logger.warning(f"No data found in Redis for key: {key}")
                return pd.DataFrame()  # Return an empty DataFrame if no data found

            _df = pd.read_json(BytesIO(_data_json), dtype=str)
            logger.info(f"DataFrame retrieved from Redis for key: {key}")
            if LOGGER_DEBUG:
                logger.debug(f"DataFrame length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
            return _df
        except Exception as _e:
            logger.error(f"Error reading DataFrame from Redis for key: {key}: {_e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    @staticmethod
    def store_ak_data(pg_conn, ak_func_name, insert_sql, truncate=False):
        _cursor = pg_conn.cursor()
        if LOGGER_DEBUG:
            logger.debug(f"Storing data for {ak_func_name} with SQL: {insert_sql}")
        try:
            _cursor.execute(insert_sql)
            _inserted_rows = _cursor.fetchall()

            pg_conn.commit()
            logger.info(f"Data successfully inserted into table for {ak_func_name}")

            if truncate:
                _truncate_sql = f"TRUNCATE TABLE ak_dg_{ak_func_name};"
                _cursor.execute(_truncate_sql)
                pg_conn.commit()
                logger.info(f"Table ak_dg_{ak_func_name} has been truncated")

            if LOGGER_DEBUG:
                logger.debug(f"Inserted rows length: {len(_inserted_rows)}, first 5 rows: {_inserted_rows[:5]}")
            return _inserted_rows  # Return the list of inserted rows

        except Exception as _e:
            pg_conn.rollback()
            logger.error(f"Failed to store data for {ak_func_name}: {_e}")
            raise
        finally:
            _cursor.close()

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
            _file_path = os.path.join(dir_path, f"{filename}.csv")
            df.to_csv(_file_path, index=False, header=include_header)
            logger.info(f"Data saved to CSV at {_file_path}")
            if LOGGER_DEBUG:
                logger.debug(f"CSV data length: {len(df)}, first 5 rows: {df.head().to_dict(orient='records')}")
            return _file_path
        except Exception as _e:
            logger.error(f"Failed to save data to CSV: {_e}")
            return None

    @staticmethod
    def insert_data_from_csv(conn, csv_path, table_name):
        if not os.path.exists(csv_path):
            logger.error("CSV file does not exist.")
            return
        try:
            _cursor = conn.cursor()
            with open(csv_path, 'r') as _file:
                _copy_sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','"
                _cursor.copy_expert(sql=_copy_sql, file=_file)
                conn.commit()
                logger.info(f"Data from {csv_path} successfully loaded into {table_name}.")
            _cursor.close()
        except Exception as _e:
            conn.rollback()
            logger.error(f"Failed to load data from CSV: {_e}")
            raise AirflowException(_e)

    @staticmethod
    def insert_data_from_csv1(conn, csv_path, table_name):
        if not os.path.exists(csv_path):
            logger.error("CSV file does not exist.")
            return
        try:
            _cursor = conn.cursor()
            with open(csv_path, 'r') as _file:
                _csvreader = csv.reader(_file)
                _header = next(_csvreader)

                for _row in _csvreader:
                    _placeholders = ','.join(['%s'] * len(_row))
                    _insert_sql = f"""
                    INSERT INTO {table_name} ({','.join(_header)})
                    VALUES ({_placeholders});
                    """
                    _cursor.execute(_insert_sql, _row)

            conn.commit()
            logger.info(f"Data from {csv_path} successfully loaded into {table_name}.")
            _cursor.close()
        except Exception as _e:
            conn.rollback()
            logger.error(f"Failed to load data from CSV: {_e}")
            raise AirflowException(_e)

    @staticmethod
    def get_columns_from_table(pg_conn, table_name, redis_conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        _redis_key = f"columns_{table_name}"
        if LOGGER_DEBUG:
            logger.debug(f"Fetching column names and types for table: {table_name}")

        try:
            _cached_columns = redis_conn.get(_redis_key)
            if _cached_columns:
                _columns = json.loads(_cached_columns)
                logger.info(f"Retrieved column names and types for table '{table_name}' from Redis.")
                if LOGGER_DEBUG:
                    logger.debug(f"Cached columns length: {len(_columns)}, first 5 columns: {_columns[:5]}")
                return _columns
        except Exception as _e:
            logger.warning(f"Failed to read column names and types from Redis for table '{table_name}': {_e}")

        _columns = []
        _cursor = pg_conn.cursor()
        try:
            _cursor.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
                """,
                (table_name,)
            )
            _columns = [(row[0], row[1]) for row in _cursor.fetchall()]
            try:
                redis_conn.setex(_redis_key, ttl, json.dumps(_columns))
                logger.info(f"Retrieved column names and types for table '{table_name}' from PostgreSQL and cached in Redis.")
                if LOGGER_DEBUG:
                    logger.debug(f"Columns for table {table_name} length: {len(_columns)}, first 5 columns: {_columns[:5]}")
            except Exception as _e:
                logger.warning(f"Failed to cache column names and types in Redis for table '{table_name}': {_e}")
        except Exception as _e:
            logger.error(f"Error fetching column names and types from table '{table_name}': {_e}")
        finally:
            _cursor.close()

        return _columns



    @staticmethod
    def convert_columns(df, table_name, pg_conn, redis_conn: redis.Redis):
        _columns = UtilFuncs.get_columns_from_table(pg_conn, table_name, redis_conn)
        if LOGGER_DEBUG:
            logger.debug(f"Columns for table {table_name}: {_columns}")
        if _columns is None or len(_columns) < 1:
            raise AirflowException(f"Can't find columns using table_name {table_name}")

        # 提取列名和类型
        column_names = [col[0] for col in _columns]
        column_types = {col[0]: col[1] for col in _columns}

        if con.LOGGER_DEBUG:
            logger.debug('column_names')
            logger.debug(column_names)
        # 根据列名对 DataFrame 进行筛选
        df = df[column_names]

        # 根据类型信息进行转换
        for col, col_type in column_types.items():
            if col_type in ['bigint', 'integer']:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # 使用 Pandas 的整数类型
            elif col_type in ['decimal', 'numeric']:
                df[col] = pd.to_numeric(df[col], errors='coerce')  # 将无法转换的值转换为 NaN
            elif col_type == 'boolean':
                df[col] = df[col].astype(bool)
            elif col_type == 'date':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            elif col_type == 'timestamp':
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df


    # endregion store once

    # region tracing data funcs
    @staticmethod
    def get_tracing_data_df(pg_conn, tracing_table_name):
        _query = f"SELECT * FROM {tracing_table_name};"
        if LOGGER_DEBUG:
            logger.debug(f"Executing query to get tracing data: {_query}")
        try:
            _df = pd.read_sql(_query, pg_conn)
            logger.info(f"Data retrieved from {tracing_table_name} successfully.")
            if LOGGER_DEBUG:
                logger.debug(f"Tracing data length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
            return _df
        except Exception as _e:
            logger.error(f"Failed to retrieve data from {tracing_table_name}: {_e}")
            raise _e

    @staticmethod
    def generate_date_list(start_date, end_date, ascending=True):
        _start = datetime.strptime(start_date, '%Y-%m-%d')
        _end = datetime.strptime(end_date, '%Y-%m-%d')
        _date_list = []
        _step = timedelta(days=1)

        if ascending:
            while (_start <= _end):
                _date_list.append(_start.strftime('%Y-%m-%d'))
                _start += _step
        else:
            while (_end >= _start):
                _date_list.append(_end.strftime('%Y-%m-%d'))
                _end -= _step

        if LOGGER_DEBUG:
            logger.debug(f"Generated date list length: {len(_date_list)}, first 5 dates: {_date_list[:5]}")
        return _date_list

    @staticmethod
    def get_date_list(pg_conn, key, begin='1990-01-01'):
        _end_date = datetime.now() - timedelta(days=1)
        _end_str = _end_date.strftime('%Y-%m-%d')

        _all_date_list = UtilFuncs.generate_date_list(begin, _end_str, ascending=False)
        logger.debug(f"in func 'get_date_list', all_date_list length: {len(_all_date_list)}, first 5 dates: {_all_date_list[:5]}")
        _current_df = UtilFuncs.get_tracing_by_date(pg_conn, key)
        logger.debug(f"in func 'get_date_list', current_df length: {len(_current_df)}, first 5 rows: {_current_df.head().to_dict(orient='records')}")

        if not _current_df.empty:
            _current_date_list = _current_df['td'].apply(UtilTools.format_td10).tolist()
            logger.debug(f"in func 'get_date_list', current_date_list length: {len(_current_date_list)}, first 5 dates: {_current_date_list[:5]}")
        else:
            _current_date_list = []

        _missing_date_list = [td for td in _all_date_list if td not in _current_date_list]
        logger.debug(f"in func 'get_date_list', missing_date_list length: {len(_missing_date_list)}, first 5 dates: {_missing_date_list[:5]}")

        return sorted(_missing_date_list, reverse=True)

    @staticmethod
    def get_tracing_by_date(pg_conn, key):
        _sql = """
        SELECT ak_func_name, td, create_time, update_time, category, is_active, host_name
        FROM ak_dg_tracing_by_date
        WHERE ak_func_name = %s;
        """
        if LOGGER_DEBUG:
            logger.debug(f"Executing query to get tracing data by date: {_sql}")

        _cursor = pg_conn.cursor()
        try:
            _cursor.execute(_sql, (key,))
            _rows = _cursor.fetchall()
            _df = pd.DataFrame(
                _rows,
                columns=[
                    'ak_func_name', 'td', 'create_time', 'update_time',
                    'category', 'is_active', 'host_name'
                ])
            if LOGGER_DEBUG:
                logger.debug(f"Tracing data by date length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
            return _df
        finally:
            _cursor.close()

    @staticmethod
    def prepare_tracing_data(ak_func_name, param_name, date_values):
        _host_name = os.getenv('HOSTNAME', socket.gethostname())
        _current_time = datetime.now()
        _data = []
        for _date, _value in date_values:
            _data.append((ak_func_name, param_name, _value, _date, _current_time, _current_time, _host_name))
        if LOGGER_DEBUG:
            logger.debug(f"Prepared tracing data length: {len(_data)}, first 5 rows: {_data[:5]}")
        return _data

    @staticmethod
    def execute_tracing_data_insert(conn, insert_sql, data):
        _cursor = conn.cursor()
        if LOGGER_DEBUG:
            logger.debug(f"Executing insert SQL for tracing data: {insert_sql}")
        try:
            _cursor.executemany(insert_sql, data)
            conn.commit()
            logger.info("Tracing data inserted/updated successfully.")
            if LOGGER_DEBUG:
                logger.debug(f"Inserted tracing data length: {len(data)}, first 5 rows: {data[:5]}")
        except Exception as _e:
            conn.rollback()
            logger.error(f"Failed to insert/update tracing data: {_e}")
            raise AirflowException(_e)
        finally:
            _cursor.close()

    @staticmethod
    def insert_tracing_date_data(conn, ak_func_name, date_list):
        _host_name = os.getenv('HOSTNAME', socket.gethostname())
        _current_time = datetime.now()
        _data = []
        for _date in date_list:
            _data.append((ak_func_name, _date, _current_time, _current_time, _host_name))
        _insert_sql = """
            INSERT INTO ak_dg_tracing_by_date (ak_func_name, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name) DO UPDATE 
            SET last_td = EXCLUDED.last_td,
                update_time = EXCLUDED.update_time,
                host_name = EXCLUDED.host_name;
            """
        if LOGGER_DEBUG:
            logger.debug(f"Prepared insert SQL for tracing date data: {_insert_sql}")
        UtilFuncs.execute_tracing_data_insert(conn, _insert_sql, _data)

    @staticmethod
    def insert_tracing_date_1_param_data(conn, ak_func_name, param_name, date_values):
        _data = UtilFuncs.prepare_tracing_data(ak_func_name, param_name, date_values)
        _insert_sql = """
            INSERT INTO ak_dg_tracing_by_date_1_param (ak_func_name, param_name, param_value, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, param_name, param_value) DO UPDATE 
            SET last_td = EXCLUDED.last_td,
                update_time = EXCLUDED.update_time,
                host_name = EXCLUDED.host_name;
            """
        if LOGGER_DEBUG:
            logger.debug(f"Prepared insert SQL for tracing date with 1 param data: {_insert_sql}")
        UtilFuncs.execute_tracing_data_insert(conn, _insert_sql, _data)

    @staticmethod
    def insert_tracing_scode_date_data(conn, ak_func_name, scode_list, date):
        _data = [(ak_func_name, _scode, date, datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for _scode in scode_list]
        _insert_sql = """
            INSERT INTO ak_dg_tracing_by_scode_date (ak_func_name, scode, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, scode) DO UPDATE 
            SET last_td = EXCLUDED.last_td,
                update_time = EXCLUDED.update_time,
                host_name = EXCLUDED.host_name;
            """
        if LOGGER_DEBUG:
            logger.debug(f"Prepared insert SQL for tracing s_code date data: {_insert_sql}")
        UtilFuncs.execute_tracing_data_insert(conn, _insert_sql, _data)
    # endregion tracing data funcs

			 
															   

# region test
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
# endregion test