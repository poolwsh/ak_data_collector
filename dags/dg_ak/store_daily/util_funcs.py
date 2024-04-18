##########################################
### test
# import sys
# sys.path.append('/data/workspace/git/ak_data_collector/dags')
#######################################

import os
import time
import redis
import traceback
import pandas as pd
import akshare as ak
from io import BytesIO
from datetime import date, datetime, timedelta
from typing import Optional, Union

from dg_ak.utils.utils import UtilTools as ut
from dg_ak.utils.logger import LogHelper

from airflow.exceptions import AirflowException

logger = LogHelper().logger

class UtilFuncs(object):
    default_redis_ttl = 60*60 # 1 hour

    # region tool funcs
    @staticmethod
    def get_data_and_save2csv(redis_key, ak_func_name, pg_conn, redis_conn, temp_dir = '/tmp/ak_dg'):
        _table_name = f"ak_dg_{ak_func_name}"
        _date_df = UtilFuncs.read_df_from_redis(redis_key, redis_conn)

        _date_list = [ut.format_td8(date) for date in _date_df['Date'].sort_values(ascending=False).tolist()]
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
                break
        logger.error(f'Failed to call function {ak_func.__name__} after {num_retries} attempts with parameters: {param_dict}')
        # raise AirflowException(f'Function {ak_func.__name__} failed after {num_retries} attempts')

    @staticmethod
    def get_data_by_td(ak_func_name: str, td: str, td_pa_name: str = 'date') -> pd.DataFrame:
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
        
        _df = ut.remove_cols(_df, ak_func_name)
        _df.rename(columns=ut.get_col_dict(ak_func_name), inplace=True)
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
    def write_df_to_redis(key: str, df: pd.DataFrame, conn: redis.Redis, ttl: int):
        try:
            _data_json = df.to_json(date_format='iso')
            conn.setex(key, ttl, _data_json)
            logger.info(f"DataFrame written to Redis under key: {key} with TTL {ttl} seconds.")
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
                _df = pd.read_json(BytesIO(_data_json.encode()))
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
        current_df = UtilFuncs.get_tracing_by_date(pg_conn, key)
        
        if not current_df.empty:
            current_date_list = current_df['dt'].tolist()
        else:
            current_date_list = []

        missing_date_list = [td for td in all_date_list if td not in current_date_list]

        return sorted(missing_date_list, reverse=True)

    @staticmethod
    def get_tracing_by_date(pg_conn, key):
        sql = """
        SELECT ak_func_name, dt, hash_value, create_time, update_time, host_name
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
                    'ak_func_name', 'dt', 'hash_value', 
                    'create_time', 'update_time', 'host_name'
                    ])
            return df
        finally:
            cursor.close()

    @staticmethod
    def set_tracing_by_date(key, dt):
        raise NotImplementedError

    @staticmethod
    def get_tracing_by_date_list(key):
        raise NotImplementedError

    @staticmethod
    def set_tracing_by_date_list(key):
        raise NotImplementedError

# endregion tracing data funcs

##################################################
#### test
# df = UtilFuncs.get_data_by_td('stock_zt_pool_em', '20240402')
# print(df.columns)
# print(df.head(3))


# print(UtilFuncs.generate_dt_list('2024-04-01','2024-04-10',dt_format='%Y%m%d'))
################################################3##