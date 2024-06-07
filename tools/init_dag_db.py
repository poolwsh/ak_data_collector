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


# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 设置要遍历的目录
directories_to_scan = [
    os.path.join(project_root, 'da_ak'),
    os.path.join(project_root, 'dg_ak'),
    os.path.join(project_root, 'dg_fy')
]

# 配置开关，控制是否删除并重建数据库
RECREATE_DB = True  # 如果需要删除并重建数据库，将此值设置为 True

def get_sql_files(directories):
    sql_files = []
    for directory in directories:
        sql_files.extend(glob.glob(os.path.join(directory, '**', '*.sql'), recursive=True))
    
    # 将 create_s-zh-a_tables.sql 文件放在最前
    sql_files.sort(key=lambda x: 'create_s-zh-a_tables.sql' not in x)
    return sql_files

def create_dag_s_data_database():
    try:
        # 使用默认数据库连接，确保连接到 Postgres 实例而不是特定的数据库
        default_engine = create_engine(f'postgresql+psycopg2://{pg_sql_user}:{pg_sql_password}@{pg_sql_hostname}:{pg_sql_port}/{pg_db_name}', isolation_level='AUTOCOMMIT')
        conn = default_engine.connect()

        # 如果 RECREATE_DB 为 True，删除并重建数据库
        if RECREATE_DB:
            conn.execute(f"DROP DATABASE IF EXISTS {con.dag_s_data_db_name}")
            logger.info(f"Database '{con.dag_s_data_db_name}' dropped successfully.")

        # 检查 dag_s_data 数据库是否存在
        result = conn.execute(f"SELECT 1 FROM pg_database WHERE datname = '{con.dag_s_data_db_name}'")
        exists = result.fetchone()
        if not exists:
            # 如果 dag_s_data 数据库不存在，创建它
            conn.execute(f"CREATE DATABASE {con.dag_s_data_db_name}")
            logger.info(f"Database '{con.dag_s_data_db_name}' created successfully.")
        else:
            logger.info(f"Database '{con.dag_s_data_db_name}' already exists.")
        conn.close()
        
        # 创建数据库用户和权限
        dag_s_data_engine = create_engine(f'postgresql+psycopg2://{pg_sql_user}:{pg_sql_password}@{con.dag_s_data_hostname}:{con.dag_s_data_port}/{con.dag_s_data_db_name}', isolation_level='AUTOCOMMIT')
        dag_s_data_conn = dag_s_data_engine.connect()
        dag_s_data_conn.execute(f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{con.dag_s_data_user}') THEN CREATE USER {con.dag_s_data_user} WITH PASSWORD '{con.dag_s_data_password}'; END IF; END $$;")
        dag_s_data_conn.execute(f"GRANT ALL PRIVILEGES ON DATABASE {con.dag_s_data_db_name} TO {con.dag_s_data_user}")
        dag_s_data_conn.execute(f"GRANT ALL PRIVILEGES ON SCHEMA public TO {con.dag_s_data_user}")
        logger.info(f"User '{con.dag_s_data_user}' created and granted privileges.")
        dag_s_data_conn.close()
        
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
    # 创建 dag_s_data 数据库（如果不存在）
    create_dag_s_data_database()

    # 获取数据库连接
    dag_s_data_engine = create_engine(f'postgresql+psycopg2://{con.dag_s_data_user}:{con.dag_s_data_password}@{con.dag_s_data_hostname}:{con.dag_s_data_port}/{con.dag_s_data_db_name}')
    conn = dag_s_data_engine.connect()

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
