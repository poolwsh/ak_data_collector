#!/bin/bash

# Set the USERPROFILE environment variable
export USERPROFILE=/mnt/c/Users/your-username

# Define the required directories
directories=(
    "$USERPROFILE/airflow_data/log"
    "$USERPROFILE/airflow_data/cache"
    "$USERPROFILE/airflow_data/db/redis/config"
    "$USERPROFILE/airflow_data/db/redis/data"
    "$USERPROFILE/airflow_data/db/timescaledb/data"
)

# Create the directories if they don't exist
for dir in "${directories[@]}"; do
  if [ ! -d "$dir" ]; then
    echo "Creating directory: $dir"
    mkdir -p "$dir"
  else
    echo "Directory already exists: $dir"
  fi
done

# Change to the directory containing the Dockerfile and docker-compose.yml
cd "$(dirname "$0")"

# Build the Docker image
docker build -t my_airflow_image -f Dockerfile .

# Start Docker Compose services
docker-compose up -d
