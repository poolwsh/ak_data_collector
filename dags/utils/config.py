
class Config:
    def __init__(self):
        self.redis_password = 'redis_pw'
        self.redis_hostname = 'redis'
        self.redis_port = 6379

        self.dag_s_data_hostname = "timescaledb"
        self.dag_s_data_port = "5432"
        self.dag_s_data_user = "dags_data_user"
        self.dag_s_data_password = "dags_data_pw"
        self.dag_s_data_db_name = 'dags_data'

        self.DEFAULT_EMAIL = "poolwsh@163.com"
        self.DEFAULT_OWNER = "poolwsh"
        self.DEFAULT_RETRY_DELAY = 30  # in minutes
        self.RETRIES = 1

        self.DEBUG_MODE = True

        self.DEFAULT_REDIS_TTL = 60 * 60  # 1 hour

        self.STOCK_A_REALTIME_KEY = "stock_a_realtime"
        self.INDEX_A_REALTIME_KEY = "index_a_realtime"

        self.LOG_ROOT = 'logs/airflow_dags'
        self.CACHE_ROOT = 'cache/airflow_dags'

# Instantiate the config
config = Config()