#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "postgres_user" <<-EOSQL
    CREATE USER airflow_user WITH PASSWORD 'airflow_pw';
    CREATE DATABASE airflow_data;
    GRANT ALL PRIVILEGES ON DATABASE airflow_data TO airflow_user;
    \c airflow_data
    GRANT ALL ON SCHEMA public TO airflow_user;
EOSQL

