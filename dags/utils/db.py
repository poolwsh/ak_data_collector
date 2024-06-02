
import redis
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection as SQLAlchemyConnection
import psycopg2.extensions
import dags.utils.config as con

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
    db_engine = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            con.pg_sql_user, con.pg_sql_password, con.pg_sql_hostname, con.pg_sql_port, con.pg_db_name
        ), 
        echo=True if con.DEBUG_MODE else False,
        pool_size=20, 
        max_overflow=50
    )
    pg_conn = None

    @staticmethod
    def get_conn():
        if PGEngine.pg_conn is None or PGEngine.pg_conn.closed:
            try:
                PGEngine.pg_conn = PGEngine.db_engine.connect()
            except Exception as e:
                print(f"Error connecting to the database: {e}")
                PGEngine.pg_conn = None
        return PGEngine.pg_conn

    @staticmethod
    def get_psycopg2_conn(conn):
        if isinstance(conn, SQLAlchemyConnection):
            # If it's a SQLAlchemy connection, get the raw psycopg2 connection
            return conn.connection.connection
        elif isinstance(conn, psycopg2.extensions.connection):
            # If it's already a psycopg2 connection, return it directly
            return conn
        else:
            raise TypeError("Unsupported connection type")

