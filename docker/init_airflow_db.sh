#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "postgres" <<-EOSQL
    CREATE USER airflow WITH PASSWORD 'airflow_pw';
    CREATE DATABASE airflow_data;
    GRANT ALL PRIVILEGES ON DATABASE airflow_data TO airflow;
    \c airflow_data
    GRANT ALL ON SCHEMA public TO airflow;
    ALTER USER airflow WITH SUPERUSER;
EOSQL
