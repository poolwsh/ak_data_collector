#!/usr/bin/env bash
# Entry point for the Docker container

# Initialize the Airflow database
airflow db init

# Run the custom initialization script
python /opt/airflow/tools/init_ak_dag_db.py

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
