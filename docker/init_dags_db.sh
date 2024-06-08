#!/bin/bash
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER dags_data_user WITH PASSWORD 'dags_data_pw';
    CREATE DATABASE dags_data;
    GRANT ALL PRIVILEGES ON DATABASE dags_data TO dags_data_user;
    \c dags_data
    GRANT ALL ON SCHEMA public TO dags_data_user;
EOSQL
