#!/usr/bin/env bash
# Entry point for the Docker container

# Initialize the database
airflow db init

# Start the scheduler
airflow scheduler &

# Start the web server
exec airflow webserver
