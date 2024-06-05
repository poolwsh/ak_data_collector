
class Config:
    def __init__(self):
        self.redis_password = 'redis_pw'
        self.redis_hostname = 'redis'
        self.redis_port = 6379

        self.ak_data_hostname = "timescaledb"
        self.ak_data_port = "5432"
        self.ak_data_user = "ak_data_user"
        self.ak_data_password = "ak_data_pw"
        self.ak_data_db_name = 'ak_data'

        self.DEFAULT_EMAIL = "poolwsh@163.com"
        self.DEFAULT_OWNER = "poolwsh"
        self.DEFAULT_RETRY_DELAY = 30  # in minutes

        self.DEBUG_MODE = True

        self.DEFAULT_REDIS_TTL = 60 * 60  # 1 hour

        self.STOCK_A_REALTIME_KEY = "stock_a_realtime"

        self.LOG_ROOT = '/data/airflow/log'
        self.CACHE_ROOT = '/data/airflow/cache'

# Instantiate the config
config = Config()