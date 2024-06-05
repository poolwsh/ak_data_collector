
import redis
from psycopg2 import pool
from dags.utils.config import config as con
from dags.utils.logger import logger

class RedisEngine(object):
    redis_pool = redis.ConnectionPool(
        host=con.redis_hostname,
        port=con.redis_port,
        password=con.redis_password,
        max_connections=20
    )
    connections = {}

    @staticmethod
    def get_connection(db):
        if db not in RedisEngine.connections or not RedisEngine.connections[db].ping():
            try:
                RedisEngine.connections[db] = redis.Redis(connection_pool=RedisEngine.redis_pool, db=db)
            except Exception as e:
                print(f"Error connecting to the Redis server (db {db}): {e}")
                RedisEngine.connections[db] = None
        return RedisEngine.connections[db]

task_cache_conn = RedisEngine.get_connection(3)


class PGEngine(object):
    pg_pool = pool.SimpleConnectionPool(
        1,  # minconn
        20, # maxconn
        user=con.ak_data_user,
        password=con.ak_data_password,
        host=con.ak_data_hostname,
        port=con.ak_data_port,
        database=con.ak_data_db_name
    )

    @staticmethod
    def get_conn():
        try:
            conn = PGEngine.pg_pool.getconn()
            if conn:
                logger.debug('create new postgresql connection')
                return conn
            else:
                logger.error("Failed to get connection from pool")
                return None
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            return None

    @staticmethod
    def release_conn(conn):
        try:
            if conn:
                PGEngine.pg_pool.putconn(conn)
                logger.debug('PostgreSQL connection returned to pool')
        except Exception as e:
            logger.error(f"Error returning connection to the pool: {e}")

