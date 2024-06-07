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

# Start the scheduler and webserver in background
airflow scheduler &

# Start the web server
airflow webserver &

# Run the custom initialization script after a delay to ensure Airflow is up
sleep 30
# python /opt/airflow/tools/init_ak_dag_db.py
# python /opt/airflow/dags/dg_ak/store_daily/s-zh-a/init_dg_ak_s_zh_a.py

# Keep the container running
tail -f /dev/null
