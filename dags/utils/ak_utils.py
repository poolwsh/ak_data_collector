

import os
import json
import redis
import traceback
import pandas as pd
import akshare as ak
from io import BytesIO
from datetime import date, datetime
from airflow.exceptions import AirflowException
from typing import Optional
import psycopg2.extensions


from utils.utils import UtilTools
import utils.config as con
from utils.utils import UtilTools
from utils.logger import logger


# Logger debug switch
DEBUG_MODE = con.DEBUG_MODE

class AkUtilTools(UtilTools):
    
    @staticmethod
    def get_s_code_list(redis_conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        if DEBUG_MODE:
            logger.debug("Attempting to get stock codes list from Redis.")
        try:
            _df = AkUtilTools.read_df_from_redis(con.STOCK_A_REALTIME_KEY, redis_conn)
            logger.info('Read stock real-time data from Redis successfully.')
            if DEBUG_MODE:
                logger.debug(f"Stock codes list length: {len(_df['s_code'])}, first 5 codes: {_df['s_code'].tolist()[:5]}")
            return _df['s_code']
        except Exception as _e:
            logger.warning(f"Failed to read stock real-time data from Redis: {_e}")
            try:
                _df = getattr(ak, 'stock_zh_a_spot_em')()
                if _df is not None and '代码' in _df.columns:
                    _df.rename(columns={'代码': 's_code'}, inplace=True)
                    _df['s_code'] = _df['s_code'].astype(str)
                    AkUtilTools.write_df_to_redis(con.STOCK_A_REALTIME_KEY, _df, redis_conn, ttl)
                    if DEBUG_MODE:
                        logger.debug(f"Fetched and cached stock codes list length: {len(_df['s_code'])}, first 5 codes: {_df['s_code'].tolist()[:5]}")
                    return _df['s_code']
                else:
                    logger.error("Failed to fetch or process data from the source.")
                    return pd.Series()  # 返回一个空的序列以避免进一步错误
            except Exception as _inner_e:
                logger.error(f"Error while fetching or writing data: {_inner_e}")
                raise  # 可能需要重新抛出异常或处理错误

    @staticmethod
    def get_s_code_name_list(redis_conn: redis.Redis, ttl: int = 60 * 60):
        if DEBUG_MODE:
            logger.debug("Attempting to get stock codes and names list from Redis.")
        try:
            _df = AkUtilTools.read_df_from_redis(con.STOCK_A_REALTIME_KEY, redis_conn)
            if _df is not None:
                logger.info('Read stock real-time data from Redis successfully.')
                if DEBUG_MODE:
                    logger.debug(f"Stock codes and names list length: {len(_df)}, first 5: {_df[['s_code', 's_name']].values.tolist()[:5]}")
                return _df[['s_code', 's_name']].values.tolist()
            else:
                logger.warning(f"No data found in Redis for key: {con.STOCK_A_REALTIME_KEY}")
        except Exception as _e:
            logger.warning(f"Failed to read stock real-time data from Redis: {_e}")

        # Fetch data from AkShare if Redis data is not available
        try:
            _df = getattr(ak, 'stock_zh_a_spot_em')()
            if _df is not None and '代码' in _df.columns and '名称' in _df.columns:
                _df.rename(columns={'代码': 's_code', '名称': 's_name'}, inplace=True)
                _df['s_code'] = _df['s_code'].astype(str)
                AkUtilTools.write_df_to_redis(con.STOCK_A_REALTIME_KEY, _df, redis_conn, ttl)
                if DEBUG_MODE:
                    logger.debug(f"Fetched and cached stock codes and names list length: {len(_df)}, first 5: {_df[['s_code', 's_name']].values.tolist()[:5]}")
                return _df[['s_code', 's_name']].values.tolist()
            else:
                logger.error("Failed to fetch or process data from the source.")
                return []
        except Exception as _inner_e:
            logger.error(f"Error while fetching or writing data: {_inner_e}")
            raise  # 重新抛出异常或处理错误


    @staticmethod
    def save_data_to_csv(df, filename, dir_path=con.CACHE_ROOT, include_header=True):
        if df.empty:
            logger.warning("No data to save to CSV.")
            return None
        try:
            os.makedirs(dir_path, exist_ok=True)  # Ensure the directory exists
            _file_path = os.path.join(dir_path, f"{filename}.csv")
            df.to_csv(_file_path, index=False, header=include_header)
            logger.info(f"Data saved to CSV at {_file_path}")
            if DEBUG_MODE:
                logger.debug(f"CSV data length: {len(df)}, first 5 rows: {df.head().to_dict(orient='records')}")
            return _file_path
        except Exception as _e:
            logger.error(f"Failed to save data to CSV: {_e}")
            return None


    @staticmethod
    def insert_data_from_csv(conn, csv_path, table_name):
        assert isinstance(conn, psycopg2.extensions.connection)
        if not os.path.exists(csv_path):
            logger.error("CSV file does not exist.")
            return
        try:
            with open(csv_path, 'r') as _file:
                _copy_sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','"
                with conn.cursor() as _cursor:
                    _cursor.copy_expert(sql=_copy_sql, file=_file)
                conn.commit()
                logger.info(f"Data from {csv_path} successfully loaded into {table_name}.")
        except Exception as _e:
            conn.rollback()
            logger.error(f"Failed to load data from CSV: {_e}")
            raise

    @staticmethod
    def get_columns_from_table(pg_conn, table_name, redis_conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        _redis_key = f"columns_{table_name}"
        if DEBUG_MODE:
            logger.debug(f"Fetching column names and types for table: {table_name}")

        try:
            _cached_columns = redis_conn.get(_redis_key)
            if _cached_columns:
                _columns = json.loads(_cached_columns)
                logger.info(f"Retrieved column names and types for table '{table_name}' from Redis.")
                if DEBUG_MODE:
                    logger.debug(f"Cached columns length: {len(_columns)}, first 5 columns: {_columns[:5]}")
                return _columns
        except Exception as _e:
            logger.warning(f"Failed to read column names and types from Redis for table '{table_name}': {_e}")

        _columns = []
        try:
            query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """
            result = pg_conn.execute(query, (table_name,))
            _columns = [(row['column_name'], row['data_type']) for row in result]
            try:
                redis_conn.setex(_redis_key, ttl, json.dumps(_columns))
                logger.info(f"Retrieved column names and types for table '{table_name}' from PostgreSQL and cached in Redis.")
                if DEBUG_MODE:
                    logger.debug(f"Columns for table {table_name} length: {len(_columns)}, first 5 columns: {_columns[:5]}")
            except Exception as _e:
                logger.warning(f"Failed to cache column names and types in Redis for table '{table_name}': {_e}")
        except Exception as _e:
            logger.error(f"Error fetching column names and types from table '{table_name}': {_e}")

        return _columns



    @staticmethod
    def convert_columns(df, table_name, pg_conn, redis_conn: redis.Redis):
        _columns = AkUtilTools.get_columns_from_table(pg_conn, table_name, redis_conn)
        if DEBUG_MODE:
            logger.debug(f"Columns for table {table_name}: {_columns}")
        if _columns is None or len(_columns) < 1:
            raise AirflowException(f"Can't find columns using table_name {table_name}")

        # 提取列名和类型
        column_names = [col[0] for col in _columns]
        column_types = {col[0]: col[1] for col in _columns}

        if DEBUG_MODE:
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



# region cache redis
    @staticmethod
    def read_df_from_redis(key: str, conn: redis.Redis) -> Optional[pd.DataFrame]:
        if key is None:
            logger.error("Key is None, cannot retrieve data from Redis.")
            return pd.DataFrame()  # Return an empty DataFrame if the key is None

        try:
            _data_json = conn.get(key)
            if _data_json:
                _df = pd.read_json(BytesIO(_data_json), dtype=str)
                logger.info(f"DataFrame retrieved from Redis for key: {key}")
                if DEBUG_MODE:
                    logger.debug(f"DataFrame length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
                return _df
            else:
                logger.warning(f"No data found in Redis for key: {key}")
                return pd.DataFrame()
        except Exception as _e:
            logger.error(f"Error reading DataFrame from Redis for key: {key}: {_e}")
            return pd.DataFrame()

    @staticmethod
    def write_df_to_redis(key: str, df: pd.DataFrame, conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        try:
            _data_json = df.to_json(date_format='iso')
            conn.setex(key, ttl, _data_json)
            logger.info(f"DataFrame written to Redis under key: {key} with TTL {ttl} seconds.")
            if DEBUG_MODE:
                logger.debug(f"DataFrame length: {len(df)}, first 5 rows: {df.head().to_dict(orient='records')}")
        except Exception as _e:
            _error_msg = f"Error writing DataFrame to Redis for key: {key}. Traceback: {traceback.format_exc()}"
            logger.error(_error_msg)
            raise AirflowException(_error_msg)

    @staticmethod
    def write_list_to_redis(key: str, data_list: list, conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        try:
            # 转换日期对象为字符串
            _data_list = [str(item) if isinstance(item, (datetime, date)) else item for item in data_list]
            _data_json = json.dumps(_data_list)
            conn.setex(key, ttl, _data_json)
            logger.info(f"List written to Redis under key: {key} with TTL {ttl} seconds.")
            if DEBUG_MODE:
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
                if DEBUG_MODE:
                    logger.debug(f"List length: {len(_data_list)}, first 5 items: {_data_list[:5]}")
                return _data_list
            else:
                logger.warning(f"No data found in Redis for key: {key}")
                return None
        except Exception as _e:
            logger.error(f"Error reading list from Redis for key: {key}: {_e}")
            return None

# endregion cache redis
