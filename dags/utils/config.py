
redis_password='pi=3.14159'
redis_hostname='redis'
redis_port=6379

pg_sql_hostname = "127.0.0.1"
pg_sql_port = "25432"
pg_sql_user = "postgres"
pg_sql_password = "pi=3.14159"
pg_db_name='postgres'


ak_data_hostname = "timescaledb"
ak_data_port = "5432"
ak_data_user = "ak_data"
ak_data_password = "ak_data_pw"
ak_data_db_name='ak_data'

DEFAULT_EMAIL = "wshmxgz@gmail.com"
DEFAULT_OWNER = "wsh"
DEFAULT_RETRY_DELAY = 30  # in minutes

DEBUG_MODE = True

ZH_A_DEFAULT_START_DATE = "1980-01-01"
DEFAULT_REDIS_TTL = 60 * 60  # 1 hour

STOCK_A_REALTIME_KEY= "stock_a_realtime"

# POSSIBLE_CODE_COLUMNS = ['s_code', 'stock_code']


LOG_ROOT = '/data/airflow/log/'
CACHE_ROOT = '/data/airflow/cache/'