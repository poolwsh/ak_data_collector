class Config:
    # PostgreSQL configuration
    dag_s_data_hostname = "timescaledb"
    dag_s_data_port = "5432"
    dag_s_data_user = "dags_data_user"
    dag_s_data_password = "dags_data_pw"
    dag_s_data_db_name = 'dags_data'


    # Redis configuration
    redis_password = 'redis_pw'
    redis_hostname = 'redis'
    redis_port = 6379

    LOG_ROOT = 'logs/api_service'
    CACHE_ROOT = 'cache/api_service'

config = Config()