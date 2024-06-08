#!/bin/bash
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER airflow_user WITH PASSWORD 'airflow_pw';
    CREATE DATABASE airflow_data;
    GRANT ALL PRIVILEGES ON DATABASE airflow_data TO airflow_user;
    \c airflow_data
    GRANT ALL ON SCHEMA public TO airflow_user;
EOSQL
