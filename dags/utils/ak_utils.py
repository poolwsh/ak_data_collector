


import redis
import traceback
import pandas as pd
import akshare as ak
from io import BytesIO
from airflow.exceptions import AirflowException
from typing import Optional, Union


from utils.utils import UtilTools
import utils.config as con
from utils.utils import UtilTools
from utils.logger import logger


# Logger debug switch
LOGGER_DEBUG = con.LOGGER_DEBUG

class AkUtilTools(UtilTools):
    
    @staticmethod
    def get_s_code_list(redis_conn: redis.Redis, ttl: int = con.DEFAULT_REDIS_TTL):
        if LOGGER_DEBUG:
            logger.debug("Attempting to get stock codes list from Redis.")
        try:
            _df = AkUtilTools.read_df_from_redis(con.STOCK_A_REALTIME_KEY, redis_conn)
            logger.info('Read stock real-time data from Redis successfully.')
            if LOGGER_DEBUG:
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
                    if LOGGER_DEBUG:
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
        if LOGGER_DEBUG:
            logger.debug("Attempting to get stock codes and names list from Redis.")
        try:
            _df = AkUtilTools.read_df_from_redis(con.STOCK_A_REALTIME_KEY, redis_conn)
            if _df is not None:
                logger.info('Read stock real-time data from Redis successfully.')
                if LOGGER_DEBUG:
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
                if LOGGER_DEBUG:
                    logger.debug(f"Fetched and cached stock codes and names list length: {len(_df)}, first 5: {_df[['s_code', 's_name']].values.tolist()[:5]}")
                return _df[['s_code', 's_name']].values.tolist()
            else:
                logger.error("Failed to fetch or process data from the source.")
                return []
        except Exception as _inner_e:
            logger.error(f"Error while fetching or writing data: {_inner_e}")
            raise  # 重新抛出异常或处理错误


    @staticmethod
    def read_df_from_redis(key: str, conn: redis.Redis) -> Optional[pd.DataFrame]:
        try:
            _data_json = conn.get(key)
            if _data_json:
                _data_json = _data_json.decode('utf-8') if isinstance(_data_json, bytes) else _data_json
                dtype_spec = {col: str for col in con.POSSIBLE_CODE_COLUMNS}
                _df = pd.read_json(BytesIO(_data_json.encode()), dtype=dtype_spec)
                logger.info(f"DataFrame read from Redis for key: {key}")
                if LOGGER_DEBUG:
                    logger.debug(f"DataFrame length: {len(_df)}, first 5 rows: {_df.head().to_dict(orient='records')}")
                return _df
            else:
                logger.warning(f"No data found in Redis for key: {key}")
                return None
        except Exception as _e:
            _error_msg = f"Error reading DataFrame from Redis for key: {key}. Traceback: {traceback.format_exc()}"
            logger.error(_error_msg)
            raise AirflowException(_error_msg)