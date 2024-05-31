#!/usr/bin/env bash

# Initialize the database
airflow db init

# Start the scheduler and web server
airflow scheduler & airflow webserver
