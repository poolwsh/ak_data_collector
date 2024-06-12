import redis
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from utils.config import config as con
from utils.logger import logger

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

api_service_cache_conn = RedisEngine.get_connection(2)

class Database:
    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{con.dag_s_data_user}:{con.dag_s_data_password}@{con.dag_s_data_hostname}:{con.dag_s_data_port}/{con.dag_s_data_db_name}",
            pool_size=20,
            max_overflow=0,
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_conn(self, func):
        """
        Provides a database connection to the function `func`.
        `func` should be a callable that accepts a connection as its first argument.
        """
        with self.engine.connect() as conn:
            return func(conn)
    
    def execute_query(self, query, params=None):
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            return result.fetchall()

    def get_session(self):
        """
        Provides a database session for ORM operations.
        """
        return self.SessionLocal()

# Initialize the database
db = Database()
