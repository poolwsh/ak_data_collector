import os
import glob
import logging
from sqlalchemy import text
from PGEngine import PGEngine  # 确保这行代码可以导入你的 PGEngine 类

# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 设置要遍历的目录
directories_to_scan = [
    'dags/da_ak',
    'dags/dg_ak'
]

def get_sql_files(directories):
    """
    获取指定目录下的所有 SQL 文件
    """
    sql_files = []
    for directory in directories:
        sql_files.extend(glob.glob(os.path.join(directory, '**', '*.sql'), recursive=True))
    return sql_files

def execute_sql_file(file_path, conn):
    """
    执行指定 SQL 文件中的内容
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        logger.info(f"Executing SQL file: {file_path}")
        with conn.begin() as trans:
            conn.execute(text(sql_content))
        logger.info(f"Successfully executed SQL file: {file_path}")
    except Exception as e:
        logger.error(f"Error executing SQL file {file_path}: {e}")

def main():
    # 获取数据库连接
    conn = PGEngine.get_conn()
    if not conn:
        logger.error("Failed to connect to the database. Exiting...")
        return

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
