import os
import sys
import glob
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# # 获取项目根目录，并将其添加到 sys.path
current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_path, '..', 'dags'))
sys.path.append(project_root)

from dags.utils.config import config as con

pg_sql_hostname = "timescaledb"
pg_sql_port = "5432"
pg_sql_user = "postgres"
pg_sql_password = "postgres_pw"
pg_db_name='postgres'

# ak_data_hostname = "timescaledb"
# ak_data_port = "5432"
# ak_data_user = "ak_data_user"
# ak_data_password = "ak_data_pw"
# ak_data_db_name='ak_data'

# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 设置要遍历的目录
directories_to_scan = [
    os.path.join(project_root, 'da_ak'),
    os.path.join(project_root, 'dg_ak')
]

# 配置开关，控制是否删除并重建数据库
RECREATE_DB = True  # 如果需要删除并重建数据库，将此值设置为 True

def get_sql_files(directories):
    """
    获取指定目录下的所有 SQL 文件
    """
    sql_files = []
    for directory in directories:
        sql_files.extend(glob.glob(os.path.join(directory, '**', '*.sql'), recursive=True))
    
    # 将 create_s-zh-a_tables.sql 文件放在最前
    sql_files.sort(key=lambda x: 'create_s-zh-a_tables.sql' not in x)
    return sql_files

def create_ak_data_database():
    """
    创建 ak_data 数据库（如果不存在）
    """
    try:
        # 使用默认数据库连接，确保连接到 Postgres 实例而不是特定的数据库
        default_engine = create_engine(f'postgresql+psycopg2://{pg_sql_user}:{pg_sql_password}@{pg_sql_hostname}:{pg_sql_port}/{pg_db_name}', isolation_level='AUTOCOMMIT')
        conn = default_engine.connect()

        # 如果 RECREATE_DB 为 True，删除并重建数据库
        if RECREATE_DB:
            conn.execute(f"DROP DATABASE IF EXISTS {con.ak_data_db_name}")
            logger.info(f"Database '{con.ak_data_db_name}' dropped successfully.")

        # 检查 ak_data 数据库是否存在
        result = conn.execute(f"SELECT 1 FROM pg_database WHERE datname = '{con.ak_data_db_name}'")
        exists = result.fetchone()
        if not exists:
            # 如果 ak_data 数据库不存在，创建它
            conn.execute(f"CREATE DATABASE {con.ak_data_db_name}")
            logger.info(f"Database '{con.ak_data_db_name}' created successfully.")
        else:
            logger.info(f"Database '{con.ak_data_db_name}' already exists.")
        conn.close()
        
        # 创建数据库用户和权限
        ak_data_engine = create_engine(f'postgresql+psycopg2://{pg_sql_user}:{pg_sql_password}@{con.ak_data_hostname}:{con.ak_data_port}/{con.ak_data_db_name}', isolation_level='AUTOCOMMIT')
        ak_data_conn = ak_data_engine.connect()
        ak_data_conn.execute(f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{con.ak_data_user}') THEN CREATE USER {con.ak_data_user} WITH PASSWORD '{con.ak_data_password}'; END IF; END $$;")
        ak_data_conn.execute(f"GRANT ALL PRIVILEGES ON DATABASE {con.ak_data_db_name} TO {con.ak_data_user}")
        ak_data_conn.execute(f"GRANT ALL PRIVILEGES ON SCHEMA public TO {con.ak_data_user}")
        logger.info(f"User '{con.ak_data_user}' created and granted privileges.")
        ak_data_conn.close()
        
    except SQLAlchemyError as e:
        logger.error(f"Error creating database or user: {e}")
        if conn:
            conn.close()

def execute_sql_file(file_path, conn):
    """
    执行指定 SQL 文件中的内容
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        logger.info(f"Executing SQL file: {file_path}")
        conn.execute(text(sql_content))
        logger.info(f"Successfully executed SQL file: {file_path}")
    except Exception as e:
        logger.error(f"Error executing SQL file {file_path}: {e}")

def main():
    # 创建 ak_data 数据库（如果不存在）
    create_ak_data_database()

    # 获取数据库连接
    ak_data_engine = create_engine(f'postgresql+psycopg2://{con.ak_data_user}:{con.ak_data_password}@{con.ak_data_hostname}:{con.ak_data_port}/{con.ak_data_db_name}')
    conn = ak_data_engine.connect()

    # 获取所有 SQL 文件
    sql_files = get_sql_files(directories_to_scan)
    logger.info(f"Found {len(sql_files)} SQL files to execute.")

    # 执行每个 SQL 文件
    for sql_file in sql_files:
        execute_sql_file(sql_file, conn)

    # 关闭数据库连接
    if conn:
        conn.close()
        logger.info("Database connection closed.")

if __name__ == '__main__':
    main()
