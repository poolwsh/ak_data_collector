#!/usr/bin/env bash
# Entry point for the Docker container

# Initialize the Airflow database
airflow db init

# Create an admin user if it does not exist
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start the scheduler
airflow scheduler &

# Start the web server
exec airflow webserver

# Run the custom initialization script
python /opt/airflow/tools/init_ak_dag_db.py
python /opt/airflow/dags/dg_ak/store_daily/s-zh-a/init_ak_dg_s_zh_a.py
