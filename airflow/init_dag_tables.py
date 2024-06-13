import os
import sys
import glob
import re
import psycopg2
from psycopg2 import sql
from dags.utils.config import config
from dags.utils.logger import logger

current_path = os.path.dirname(os.path.abspath(__file__))
logger.debug(f'current_path={current_path}')
project_root = os.path.abspath(os.path.join(current_path, '..', 'dags'))
logger.debug(f'project_root={project_root}')
sys.path.append(project_root)

directories_to_scan = [
    os.path.join(project_root, 'da_ak'),
    os.path.join(project_root, 'dg_ak'),
    os.path.join(project_root, 'dg_fy')
]

drop_tables = False  

def get_sql_files(directories):
    logger.debug(f"Scanning directories for SQL files: {directories}")
    sql_files = []
    for directory in directories:
        sql_files_in_dir = glob.glob(os.path.join(directory, '**', '*.sql'), recursive=True)
        logger.debug(f"Found {len(sql_files_in_dir)} SQL files in directory {directory}")
        sql_files.extend(sql_files_in_dir)
    
    sql_files.sort(key=lambda x: 'create_s-zh-a_tables.sql' not in x)
    logger.debug(f"Total SQL files found: {len(sql_files)}")

    for sql_file in sql_files:
        logger.debug(f"SQL file found: {sql_file}")
    
    return sql_files

def extract_table_names(sql_content):
    table_names = re.findall(r'CREATE TABLE (\w+)', sql_content, re.IGNORECASE)
    return table_names

def split_sql_content(sql_content):
    segments = re.split(r'(CREATE TABLE \w+.*?;)', sql_content, flags=re.IGNORECASE | re.DOTALL)
    return [segment for segment in segments if segment.strip()]

def check_table_exists(conn, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);"), [table_name])
            exists = cur.fetchone()[0]
            return exists
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {e}")
        return False

def execute_sql_segment(conn, sql_segment, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL(sql_segment))
            logger.info(f"Successfully executed SQL segment for table: {table_name}")
    except Exception as e:
        logger.error(f"Error executing SQL segment for table {table_name}: {e}")

def execute_sql_file(file_path, conn):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        table_names = extract_table_names(sql_content)
        if table_names:
            logger.info(f"Tables found in SQL file {file_path}:\n" + "\n".join(table_names))
        
        segments = split_sql_content(sql_content)
        for segment in segments:
            table_name_match = extract_table_names(segment)
            if not table_name_match:
                continue
            
            table_name = table_name_match[0]
            if drop_tables:
                logger.info(f"Dropping and creating table: {table_name}")
                drop_statement = f"DROP TABLE IF EXISTS {table_name};\n"
                segment = drop_statement + segment
                execute_sql_segment(conn, segment, table_name)
            else:
                if check_table_exists(conn, table_name):
                    logger.info(f"Skipping create table statement for existing table: {table_name}")
                else:
                    logger.info(f"Table {table_name} does not exist, will be created.")
                    execute_sql_segment(conn, segment, table_name)
    except Exception as e:
        logger.error(f"Error executing SQL file {file_path}: {e}")

def main():
    logger.info("Starting table setup script")

    logger.debug(f"Connecting to database {config.dag_s_data_db_name} as user {config.dag_s_data_user}")
    conn = psycopg2.connect(
        dbname=config.dag_s_data_db_name,
        user=config.dag_s_data_user,
        password=config.dag_s_data_password,
        host=config.dag_s_data_hostname,
        port=config.dag_s_data_port
    )
    conn.autocommit = True

    sql_files = get_sql_files(directories_to_scan)
    logger.info(f"Found {len(sql_files)} SQL files to execute.")

    for sql_file in sql_files:
        execute_sql_file(sql_file, conn)

    if conn:
        conn.close()
        logger.info("Database connection closed.")

if __name__ == '__main__':
    main()
