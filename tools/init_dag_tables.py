import os
import sys
import glob
import re
import psycopg2
from psycopg2 import sql
from dags.utils.config import config
from dags.utils.logger import logger

# 获取项目根目录，并将其添加到 sys.path
current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_path, '..', 'dags'))
sys.path.append(project_root)

# 设置要遍历的目录
directories_to_scan = [
    os.path.join(project_root, 'da_ak'),
    os.path.join(project_root, 'dg_ak'),
    os.path.join(project_root, 'dg_fy')
]

# 设置执行 SQL 的开关
drop_tables = False  # 控制是否删除现有表

def get_sql_files(directories):
    logger.debug(f"Scanning directories for SQL files: {directories}")
    sql_files = []
    for directory in directories:
        sql_files_in_dir = glob.glob(os.path.join(directory, '**', '*.sql'), recursive=True)
        logger.debug(f"Found {len(sql_files_in_dir)} SQL files in directory {directory}")
        sql_files.extend(sql_files_in_dir)
    
    # 将 create_s-zh-a_tables.sql 文件放在最前
    sql_files.sort(key=lambda x: 'create_s-zh-a_tables.sql' not in x)
    logger.debug(f"Total SQL files found: {len(sql_files)}")
    
    # 打印所有找到的 SQL 文件
    for sql_file in sql_files:
        logger.debug(f"SQL file found: {sql_file}")
    
    return sql_files

def extract_table_names(sql_content):
    """
    使用正则表达式从SQL内容中提取所有表名
    """
    table_names = re.findall(r'CREATE TABLE (\w+)', sql_content, re.IGNORECASE)
    return table_names

def execute_sql_file(file_path, conn):
    """
    执行指定 SQL 文件中的内容
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        # 提取表名并记录日志
        table_names = extract_table_names(sql_content)
        if table_names:
            logger.info(f"Tables found in SQL file {file_path}:\n" + "\n".join(table_names))
        
        # 如果启用了 drop_tables 选项，则添加删除表的SQL语句
        if drop_tables:
            drop_statements = ""
            for table_name in table_names:
                drop_statements += f"DROP TABLE IF EXISTS {table_name};\n"
            sql_content = drop_statements + sql_content
            if table_names:
                logger.info(f"Drop statements added for tables:\n" + "\n".join(table_names))
        else:
            if table_names:
                logger.info(f"Skipping create table statements for tables:\n" + "\n".join(table_names))
            logger.info(f"Executing non-create table statements in SQL file: {file_path}")
        
        with conn.cursor() as cur:
            cur.execute(sql.SQL(sql_content))
        
        if drop_tables or not table_names:
            logger.info(f"Successfully executed SQL file: {file_path}")
        else:
            logger.info(f"Successfully skipped create table statements in SQL file: {file_path}")
            
    except Exception as e:
        logger.error(f"Error executing SQL file {file_path}: {e}")

def main():
    logger.info("Starting table setup script")

    # 获取数据库连接
    logger.debug(f"Connecting to database {config.dag_s_data_db_name} as user {config.dag_s_data_user}")
    conn = psycopg2.connect(
        dbname=config.dag_s_data_db_name,
        user=config.dag_s_data_user,
        password=config.dag_s_data_password,
        host=config.dag_s_data_hostname,
        port=config.dag_s_data_port
    )
    conn.autocommit = True

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
