
import os
import sys
from pathlib import Path
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
print(project_root)
sys.path.append(project_root)

import time
import socket
import traceback
import pandas as pd
import akshare as ak
from datetime import date, datetime, timedelta
from typing import Optional, List
from sqlalchemy.exc import SQLAlchemyError


from dags.utils.dg_utils import AkUtilTools
from utils.logger import logger
from dags.dg_ak.utils.dg_ak_config import dgak_config as con

from airflow.exceptions import AirflowException

# Logger debug switch
DEBUG_MODE = con.DEBUG_MODE

class DateOutOfRangeException(Exception):
    pass

class DgAkUtilFuncs(AkUtilTools):

    default_pause_time = 0.2  # 200ms

    @staticmethod
    def get_selected_trade_dates(pg_conn, lookback_days: int, reference_date: str):
        try:
            td_list = DgAkUtilFuncs.get_trade_dates(pg_conn)
            td_list.sort(reverse=True)  
            if con.DEBUG_MODE:
                logger.debug(f'first 5 td_list:{td_list[:5]}')

            if reference_date is None or reference_date not in td_list:
                if reference_date is not None:
                    print(f"Warning: Reference date {reference_date} not found in trade dates, using the most recent date instead.")
                start_index = 0  
            else:
                start_index = td_list.index(reference_date)
            if con.DEBUG_MODE:
                logger.debug(f'lookback_days={lookback_days}, reference_date={reference_date}')
            selected_dates = td_list[:start_index + lookback_days + 1]
            if con.DEBUG_MODE:
                logger.debug(f'first 5 selected_dates:{selected_dates[:5]}')

            return selected_dates

        except Exception as e:
            print(f"Error: {str(e)}")
            return []
    
    @staticmethod
    def get_i_code_data(ak_func_name, ak_cols_config_dict, i_code, period, start_date, end_date, pause_time: float = default_pause_time):
        if DEBUG_MODE:
            logger.debug(f"Fetching data for i_code: {i_code}, period: {period}, start_date: {start_date}, end_date: {end_date}")
        _ak_func = getattr(ak, ak_func_name, None)
        if _ak_func is None:
            _error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(_error_msg)
            raise AirflowException(_error_msg)
        
        _i_df = DgAkUtilFuncs.try_to_call(
            _ak_func,
            {'symbol': i_code, 'period': period,
             'start_date': start_date, 'end_date': end_date
             }, pause_time=pause_time)
        
        if _i_df is None or _i_df.empty:
            if _i_df is None:
                _error_msg = f'Data function {ak_func_name} returned None with params(i_code={i_code}, period={period}, start_date={start_date}, end_date={end_date}).'
                logger.error(_error_msg)
            else:
                _warning_msg = f'No data found for {ak_func_name} with params(i_code={i_code}, period={period}, start_date={start_date}, end_date={end_date}).'
                logger.warning(_warning_msg)
            return pd.DataFrame() 

        if DEBUG_MODE:
            logger.debug(f"Removing unnecessary columns for ak_func_name: {ak_func_name}")
        _i_df = DgAkUtilFuncs.remove_cols(_i_df, ak_cols_config_dict[ak_func_name])
        _i_df.rename(columns=DgAkUtilFuncs.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
        _i_df['i_code'] = i_code
        if DEBUG_MODE:
            logger.debug(f'Processed i_code data for {i_code}, length: {len(_i_df)}, first 5 rows: {_i_df.head().to_dict(orient="records")}')
        return _i_df
    
    @staticmethod
    def get_s_code_data(ak_func_name, ak_cols_config_dict, s_code, period, start_date, end_date, adjust, pause_time: float = default_pause_time):
        if DEBUG_MODE:
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
            return pd.DataFrame()  

        if DEBUG_MODE:
            logger.debug(f"Removing unnecessary columns for ak_func_name: {ak_func_name}")
        _s_df = DgAkUtilFuncs.remove_cols(_s_df, ak_cols_config_dict[ak_func_name])
        _s_df.rename(columns=DgAkUtilFuncs.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
        _s_df['s_code'] = s_code
        if DEBUG_MODE:
            logger.debug(f'Processed s_code data for {s_code}, length: {len(_s_df)}, first 5 rows: {_s_df.head().to_dict(orient="records")}')
        return _s_df

    @staticmethod
    def get_trade_dates(pg_conn) -> list:
        with pg_conn.cursor() as cursor:
            _query = "SELECT trade_date FROM dg_ak_stock_zh_a_trade_date;"
            if DEBUG_MODE:
                logger.debug(f"Executing query to get trade dates: {_query}")
            try:
                cursor.execute(_query)
                rows = cursor.fetchall()
                _trade_dates = [row[0] for row in rows]  # Use index 0 to access the first element of the tuple
                logger.info("Trade dates retrieved successfully from dg_ak_stock_zh_a_trade_date.")
                if DEBUG_MODE:
                    logger.debug(f"Trade dates length: {len(_trade_dates)}, first 5 dates: {_trade_dates[:5]}")
                return _trade_dates
            except Exception as _e:
                logger.error(f"Failed to retrieve trade dates: {_e}")
                raise AirflowException(f"Failed to retrieve trade dates: {_e}")

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
                    if DEBUG_MODE:
                        logger.debug(f"Retrieved data length: {len(_result)}, first 5 rows: {_result.head().to_dict(orient='records')}")
                    return _result
                else:
                    logger.warning(f"No data retrieved in attempt {_attempt + 1}.")
            except ConnectionError as _e:
                _retry_delay = retry_delay * (1 + _attempt)
                logger.warning(f"Attempt {_attempt + 1}: ConnectionError encountered. Retrying after {_retry_delay} seconds...")
                time.sleep(_retry_delay)
            except KeyError as ke:
                logger.error(f"KeyError calling function {ak_func.__name__} with parameters: {_param_dict}. Error: {ke}")
                break  # Skip further retries for this specific KeyError
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
    def get_data_today(ak_func_name: str, ak_cols_config_dict: dict, date=datetime.now(), date_format: str = '%Y-%m-%d') -> pd.DataFrame:
        _today_date = date.strftime(date_format)  # Get today's date in the specified format
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
            if DEBUG_MODE:
                logger.debug(f"Retrieved data for today length: {len(_df)}, first 5 rows: {_df.head()}")
            return _df
        except Exception as _e:
            logger.error(f"Error calling function {ak_func_name} for today ({_today_date}): {_e}\n{traceback.format_exc()}")
            raise AirflowException(f"Error calling function {ak_func_name} for today ({_today_date}): {_e}")

    @staticmethod
    def get_data_by_none(ak_func_name: str, ak_cols_config_dict: dict) -> pd.DataFrame:
        _ak_func = getattr(ak, ak_func_name, None)
        if _ak_func is None:
            _error_msg = f'Function {ak_func_name} not found in module akshare.'
            logger.error(_error_msg)
            raise AirflowException(_error_msg)

        _df = DgAkUtilFuncs.try_to_call(_ak_func)
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
        if DEBUG_MODE:
            logger.debug(f"length: {len(_df)}, first 5 rows: {_df.head()}")
        return _df

    @staticmethod
    def get_data_by_td(ak_func_name: str, ak_cols_config_dict: dict, td: str, td_pa_name: str = 'date') -> pd.DataFrame:
        if DEBUG_MODE:
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
            return pd.DataFrame() 

        _df = DgAkUtilFuncs.remove_cols(_df, ak_cols_config_dict[ak_func_name])
        _df.rename(columns=DgAkUtilFuncs.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
        _df = DgAkUtilFuncs.add_td(_df, td)
        if DEBUG_MODE:
            logger.debug(f"Retrieved data for date {td}, length: {len(_df)}, first 5 rows: {_df.head()}")
        return _df

    @staticmethod
    def get_data_by_td_list(ak_func_name: str, ak_cols_config_dict: dict,  td_list: list[str], td_pa_name: str = 'date', max_retry=20) -> pd.DataFrame:
        _combined_df = pd.DataFrame()
        _retry_count = 0
        if DEBUG_MODE:
            logger.debug(f"Fetching data for date list length: {len(td_list)}\nfirst 5 dates: {td_list[:5]}")

        _total_list = len(td_list)
        for _index, _td in enumerate(td_list):
            try:
                logger.info(f'({_index + 1}/{_total_list}) Fetching data for td={_td}')
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

        if DEBUG_MODE:
            logger.debug(f"Combined data length: {len(_combined_df)}, \nfirst 5 rows: {_combined_df.head()}")
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
                    if DEBUG_MODE:
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
            if DEBUG_MODE:
                logger.debug(f"Combined data for all boards length: {len(_combined_df)}, first 5 rows: {_combined_df.head()}")
            return _combined_df
        else:
            return pd.DataFrame()  # Return an empty DataFrame if no data was fetched

    @staticmethod
    def load_ak_cols_config(config_file_path: str) -> dict:
        _config = {}
        if DEBUG_MODE:
            logger.debug(f"Loading AK columns configuration from: {config_file_path}")
        with open(config_file_path, 'r', encoding='utf-8') as _file:
            exec(_file.read(), {}, _config)
        return _config['ak_cols_config']


    # @staticmethod
    # def store_ak_data(pg_conn, ak_func_name, insert_sql, truncate=False):
    #     _cursor = pg_conn.cursor()
    #     if DEBUG_MODE:
    #         logger.debug(f"Storing data for {ak_func_name} with SQL: {insert_sql}")
    #     try:
    #         _cursor.execute(insert_sql)
    #         _inserted_rows = _cursor.fetchall()

    #         pg_conn.commit()
    #         logger.info(f"Data successfully inserted into table for {ak_func_name}")

    #         if truncate:
    #             _truncate_sql = f"TRUNCATE TABLE dg_ak_{ak_func_name};"
    #             _cursor.execute(_truncate_sql)
    #             pg_conn.commit()
    #             logger.info(f"Table dg_ak_{ak_func_name} has been truncated")

    #         if DEBUG_MODE:
    #             logger.debug(f"Inserted rows length: {len(_inserted_rows)}, first 5 rows: {_inserted_rows[:5]}")
    #         return _inserted_rows  # Return the list of inserted rows

    #     except Exception as _e:
    #         pg_conn.rollback()
    #         logger.error(f"Failed to store data for {ak_func_name}: {_e}")
    #         raise
    #     finally:
    #         _cursor.close()

    # endregion store once

    # region tracing data funcs
    @staticmethod
    def get_tracing_data_df(pg_conn, tracing_table_name):
        _query = f"SELECT * FROM {tracing_table_name};"
        if DEBUG_MODE:
            logger.debug(f"Executing query to get tracing data: {_query}")
        
        _cursor = pg_conn.cursor()
        try:
            _cursor.execute(_query)
            rows = _cursor.fetchall()
            columns = [desc[0] for desc in _cursor.description]
            _df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Data retrieved from {tracing_table_name} successfully.")
            if DEBUG_MODE:
                logger.debug(f"Tracing data length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
            return _df
        except SQLAlchemyError as _e:
            logger.error(f"Failed to retrieve data from {tracing_table_name}: {_e}")
            raise _e
        finally:
            _cursor.close()


    # @staticmethod
    # def get_tracing_by_date(pg_conn, key):
    #     _sql = """
    #     SELECT ak_func_name, td, create_time, update_time, category, is_active, host_name
    #     FROM dg_ak_tracing_by_date
    #     WHERE ak_func_name = %s;
    #     """
    #     if DEBUG_MODE:
    #         logger.debug(f"Executing query to get tracing data by date: {_sql}")

    #     _cursor = pg_conn.cursor()
    #     try:
    #         _cursor.execute(_sql, (key,))
    #         _rows = _cursor.fetchall()
    #         _df = pd.DataFrame(
    #             _rows,
    #             columns=[
    #                 'ak_func_name', 'td', 'create_time', 'update_time',
    #                 'category', 'is_active', 'host_name'
    #             ])
    #         if DEBUG_MODE:
    #             logger.debug(f"Tracing data by date length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
    #         return _df
    #     finally:
    #         _cursor.close()

    @staticmethod
    def prepare_tracing_data(ak_func_name, param_name, date_values):
        _host_name = os.getenv('HOSTNAME', socket.gethostname())
        _current_time = datetime.now()
        _data = []
        for _date, _value in date_values:
            _data.append((ak_func_name, param_name, _value, _date, _current_time, _current_time, _host_name))
        if DEBUG_MODE:
            logger.debug(f"Prepared tracing data length: {len(_data)}, first 5 rows: {_data[:5]}")
        return _data

    @staticmethod
    def execute_tracing_data_insert(conn, insert_sql, data):
        _cursor = conn.cursor()
        if DEBUG_MODE:
            logger.debug(f"Executing insert SQL for tracing data: {insert_sql}")
            logger.debug(f"data={data}")
        try:
            _cursor.executemany(insert_sql, data)
            conn.commit()
            logger.info("Tracing data inserted/updated successfully.")
            if DEBUG_MODE:
                logger.debug(f"Inserted tracing data length: {len(data)}, first 5 rows: {data[:5]}")
        except Exception as _e:
            conn.rollback()
            logger.error(f"Failed to insert/update tracing data: {_e}")
            raise AirflowException(_e)
        finally:
            _cursor.close()

    @staticmethod
    def insert_tracing_date_data(conn, ak_func_name, last_dt):
        _host_name = os.getenv('HOSTNAME', socket.gethostname())
        _current_time = datetime.now()
        _insert_sql = """
            INSERT INTO dg_ak_tracing_by_date (ak_func_name, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name) DO UPDATE 
            SET last_td = EXCLUDED.last_td,
                update_time = EXCLUDED.update_time,
                host_name = EXCLUDED.host_name;
            """
        if DEBUG_MODE:
            logger.debug(f"Prepared insert SQL for tracing date data: {_insert_sql}")
        DgAkUtilFuncs.execute_tracing_data_insert(conn, _insert_sql, [(ak_func_name, last_dt, _current_time, _current_time, _host_name)])

    @staticmethod
    def insert_tracing_date_1_param_data(conn, ak_func_name, param_name, date_values):
        _data = DgAkUtilFuncs.prepare_tracing_data(ak_func_name, param_name, date_values)
        _insert_sql = """
            INSERT INTO dg_ak_tracing_by_date_1_param (ak_func_name, param_name, param_value, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, param_name, param_value) DO UPDATE 
            SET last_td = EXCLUDED.last_td,
                update_time = EXCLUDED.update_time,
                host_name = EXCLUDED.host_name;
            """
        if DEBUG_MODE:
            logger.debug(f"Prepared insert SQL for tracing date with 1 param data: {_insert_sql}")
        DgAkUtilFuncs.execute_tracing_data_insert(conn, _insert_sql, _data)

    # @staticmethod
    # def insert_tracing_scode_date_data(conn, ak_func_name, scode_list, date):
    #     _data = [(ak_func_name, _scode, date, datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for _scode in scode_list]
    #     _insert_sql = """
    #         INSERT INTO dg_ak_tracing_by_scode_date (ak_func_name, scode, last_td, create_time, update_time, host_name)
    #         VALUES (%s, %s, %s, %s, %s, %s)
    #         ON CONFLICT (ak_func_name, scode) DO UPDATE 
    #         SET last_td = EXCLUDED.last_td,
    #             update_time = EXCLUDED.update_time,
    #             host_name = EXCLUDED.host_name;
    #         """
    #     if DEBUG_MODE:
    #         logger.debug(f"Prepared insert SQL for tracing s_code date data: {_insert_sql}")
    #     DgAkUtilFuncs.execute_tracing_data_insert(conn, _insert_sql, _data)
    # endregion tracing data funcs

	
    @staticmethod
    def is_trading_day(pg_conn, date=datetime.now().date()) -> str:
        trade_dates = DgAkUtilFuncs.get_trade_dates(pg_conn)
        if date in trade_dates:
            return True
        return False

    @staticmethod
    def get_b_names_from_date(pg_conn, table_name: str, date: str) -> List[str]:
        query = f"SELECT DISTINCT b_name FROM {table_name} WHERE td = %s"
        with pg_conn.cursor() as cursor:
            cursor.execute(query, (date,))
            results = cursor.fetchall()
        return [row[0] for row in results]

