# script.py
import redis
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

def read_df_from_redis(redis_host, redis_port, redis_key, redis_db):
    pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)
    r = redis.Redis(connection_pool=pool)
    df_pickle = r.get(redis_key)
    if df_pickle:
        df = pd.read_pickle(df_pickle)
        return df
    else:
        raise Exception("No data found for key: {}".format(redis_key))

def write_df_to_postgres(df, postgres_uri, table_name):
    engine = create_engine(postgres_uri, poolclass=QueuePool, pool_size=10, max_overflow=20)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

if __name__ == "__main__":
    redis_key = 'your_redis_key'
    redis_host = 'your_redis_host'
    redis_port = 'your_redis_port'
    redis_db = 3
    postgres_uri = 'your_postgres_uri'
    table_name = 'your_table_name'
    
    df = read_df_from_redis(redis_host, redis_port, redis_key)
    write_df_to_postgres(df, postgres_uri, table_name)
