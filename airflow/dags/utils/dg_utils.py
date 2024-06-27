import os
import json
import redis
import traceback
import numpy as np
import pandas as pd
import akshare as ak
from io import BytesIO
from datetime import date, datetime, timedelta
from airflow.exceptions import AirflowException
from typing import Optional
from psycopg2 import sql


from utils.utils import UtilTools
from utils.config import config as con
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
                logger.debug(f"Stock codes list length: {len(_df['s_code'])}")
                logger.debug("First 5 codes:")
                for code in _df['s_code'].tolist()[:5]:
                    logger.debug(code)
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
                        logger.debug(f"Fetched and cached stock codes list length: {len(_df['s_code'])}")
                        logger.debug("First 5 codes:")
                        for code in _df['s_code'].tolist()[:5]:
                            logger.debug(code)
                    return _df['s_code']
                else:
                    logger.error("Failed to fetch or process data from the source.")
                    return pd.Series() 
            except Exception as _inner_e:
                logger.error(f"Error while fetching or writing data: {_inner_e}")
                raise 

    @staticmethod
    def get_s_code_name_list(redis_conn: redis.Redis, ttl: int = 60 * 60):
        if DEBUG_MODE:
            logger.debug("Attempting to get stock codes and names list from Redis.")
        try:
            _df = AkUtilTools.read_df_from_redis(con.STOCK_A_REALTIME_KEY, redis_conn)
            if _df is not None:
                logger.info('Read stock real-time data from Redis successfully.')
                if DEBUG_MODE:
                    logger.debug(f"Stock codes and names list length: {len(_df)}")
                    logger.debug("First 5:")
                    for item in _df[['s_code', 's_name']].values.tolist()[:5]:
                        logger.debug(item)
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
                    logger.debug(f"Fetched and cached stock codes and names list length: {len(_df)}")
                    logger.debug("First 5:")
                    for item in _df[['s_code', 's_name']].values.tolist()[:5]:
                        logger.debug(item)
                return _df[['s_code', 's_name']].values.tolist()
            else:
                logger.error("Failed to fetch or process data from the source.")
                return []
        except Exception as _inner_e:
            logger.error(f"Error while fetching or writing data: {_inner_e}")
            raise  

    @staticmethod
    def save_data_to_csv(df, filename, dir_path=con.CACHE_ROOT, include_header=True):
        if df.empty:
            logger.warning("No data to save to CSV.")
            return None
        try:
            os.makedirs(dir_path, exist_ok=True)  # Ensure the directory exists
            # Replace '-' with np.nan in all columns
            df.replace('-', np.nan, inplace=True)
            logger.debug(f'Replaced "-" with np.nan in the DataFrame')
            _file_path = os.path.join(dir_path, f"{filename}.csv")
            df.to_csv(_file_path, index=False, header=include_header)
            logger.info(f"Data saved to CSV at {_file_path}")
            if DEBUG_MODE:
                logger.debug(f"CSV data length: {len(df)}")
                logger.debug("First 5 rows:")
                logger.debug(df.head())
            return _file_path
        except Exception as _e:
            logger.error(f"Failed to save data to CSV: {_e}")
            return None


    @staticmethod
    def get_primary_key_columns(pg_conn, table_name: str):
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """
        with pg_conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            result = cursor.fetchall()
            return [row[0] for row in result]
        
    @staticmethod
    def insert_data_from_csv(conn, csv_path: str, table_name: str, redis_conn):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file does not exist: {csv_path}")

        temp_table_name = f"{table_name}_temp"

        try:
            with conn.cursor() as _cursor:
                columns = AkUtilTools.get_columns_from_table(conn, table_name, redis_conn)
                primary_keys = AkUtilTools.get_primary_key_columns(conn, table_name)

                if not primary_keys:
                    raise ValueError(f"No primary key found for table '{table_name}'")

                _cursor.execute(sql.SQL("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """), [temp_table_name])
                temp_table_exists = _cursor.fetchone()[0]

                if not temp_table_exists:
                    columns_def = ", ".join([f"{col[0]} {col[1]}" for col in columns])
                    logger.debug(f"Creating temp table with columns: {columns_def}")
                    _cursor.execute(sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(temp_table_name), sql.SQL(columns_def)))

                logger.debug(f"Truncating temp table {temp_table_name}")
                _cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(temp_table_name)))

                with open(csv_path, 'r') as _file:
                    for line in _file:
                        if '-,' in line:
                            logger.debug(f"Line with '-,' found: {line.strip()}")
                    _file.seek(0)  

                    _copy_sql = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER DELIMITER ','").format(sql.Identifier(temp_table_name))
                    logger.debug(f"Copying data from CSV to temp table {temp_table_name} using SQL: {_copy_sql}")
                    _cursor.copy_expert(sql=_copy_sql, file=_file)

                _cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 10").format(sql.Identifier(temp_table_name)))
                temp_table_sample = _cursor.fetchall()
                logger.debug(f"Sample data from temp table {temp_table_name}: {temp_table_sample}")

                conflict_target = sql.SQL(", ").join(map(sql.Identifier, primary_keys))
                update_columns = sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col[0]), sql.Identifier(col[0]))
                    for col in columns if col[0] not in primary_keys
                )

                insert_sql = sql.SQL("""
                    INSERT INTO {} 
                    SELECT * FROM {} 
                    ON CONFLICT ({}) DO UPDATE SET {}
                """).format(sql.Identifier(table_name), sql.Identifier(temp_table_name), conflict_target, update_columns)
                logger.debug(f"Executing insert SQL: {insert_sql}")
                _cursor.execute(insert_sql)

                conn.commit()
                logger.info(f"Data from {csv_path} successfully loaded into {table_name}.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to load data from CSV: {e}")
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
                    logger.debug(f"Cached columns length: {len(_columns)}")
                    for column in _columns:
                        logger.debug(column)
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
            with pg_conn.cursor() as cursor:
                cursor.execute(query, (table_name,))
                result = cursor.fetchall()
                _columns = [(row[0], row[1]) for row in result]
                if not _columns:
                    raise ValueError(f"No columns found for table '{table_name}'")

                try:
                    redis_conn.setex(_redis_key, ttl, json.dumps(_columns))
                    logger.info(f"Retrieved column names and types for table '{table_name}' from PostgreSQL and cached in Redis.")
                    if DEBUG_MODE:
                        logger.debug(f"Columns for table {table_name} length: {len(_columns)}")
                        for column in _columns:
                            logger.debug(column)
                except Exception as _e:
                    logger.warning(f"Failed to cache column names and types in Redis for table '{table_name}': {_e}")
        except Exception as _e:
            logger.error(f"Error fetching column names and types from table '{table_name}': {_e}")
            raise AirflowException(f"Error fetching column names and types from table '{table_name}': {_e}")

        return _columns


    @staticmethod
    def convert_columns(df, table_name, pg_conn, redis_conn: redis.Redis):
        _columns = AkUtilTools.get_columns_from_table(pg_conn, table_name, redis_conn)
        if DEBUG_MODE:
            logger.debug(f"Columns for table {table_name}: {_columns}")
        if _columns is None or len(_columns) < 1:
            raise AirflowException(f"Can't find columns using table_name {table_name}")

        column_names = [col[0] for col in _columns]
        column_types = {col[0]: col[1] for col in _columns}

        if DEBUG_MODE:
            logger.debug('column_names')
            logger.debug(column_names)

        df = df[column_names].copy()

        for col, col_type in column_types.items():
            if col_type in ['bigint', 'integer']:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  
            elif col_type in ['decimal', 'numeric']:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')  
            elif col_type == 'boolean':
                df.loc[:, col] = df[col].astype(bool)
            elif col_type == 'date':
                df.loc[:, col] = pd.to_datetime(df[col], errors='coerce').dt.date
            elif col_type == 'timestamp':
                df.loc[:, col] = pd.to_datetime(df[col], errors='coerce')

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
                    logger.debug(f"DataFrame length: {len(_df)}")
                    logger.debug("First 5 rows:")
                    logger.debug(_df.head())
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
                logger.debug(f"DataFrame length: {len(df)}")
                logger.debug("First 5 rows:")
                logger.debug(df.head())
        except Exception as _e:
            _error_msg = f"Error writing DataFrame to Redis for key: {key}. Traceback: {traceback.format_exc()}"
            logger.error(_error_msg)
            raise AirflowException(_error_msg)

    @staticmethod
    def write_list_to_redis(key: str, data_list: list, conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        try:
            _data_list = [str(item) if isinstance(item, (datetime, date)) else item for item in data_list]
            _data_json = json.dumps(_data_list)
            conn.setex(key, ttl, _data_json)
            logger.info(f"List written to Redis under key: {key} with TTL {ttl} seconds.")
            if DEBUG_MODE:
                logger.debug(f"List length: {len(_data_list)}")
                logger.debug("First 5 items:")
                for item in _data_list[:5]:
                    logger.debug(item)
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
                    logger.debug(f"List length: {len(_data_list)}")
                    logger.debug("First 5 items:")
                    for item in _data_list[:5]:
                        logger.debug(item)
                return _data_list
            else:
                logger.warning(f"No data found in Redis for key: {key}")
                return None
        except Exception as _e:
            logger.error(f"Error reading list from Redis for key: {key}: {_e}")
            return None

# endregion cache redis
