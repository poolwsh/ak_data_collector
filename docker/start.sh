#!/bin/bash

# Set the BASE_DIR variable
BASE_DIR="$HOME/airflow_data"

# Define the required directories
directories=(
    "$BASE_DIR/log"
    "$BASE_DIR/cache"
    "$BASE_DIR/db/redis/config"
    "$BASE_DIR/db/redis/data"
    "$BASE_DIR/db/timescaledb/data"
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

# Change to the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Changing to script directory: $SCRIPT_DIR"
cd "$SCRIPT_DIR"

# Move up one directory to the project root
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
echo "Changing to project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

# Print the current directory and list files in the docker directory
echo "Current directory: $(pwd)"
echo "Listing files in the docker directory:"
ls -l "$PROJECT_ROOT/docker"

# Check if docker-compose.yml exists
if [ ! -f "$PROJECT_ROOT/docker/docker-compose.yml" ]; then
  echo "docker-compose.yml not found in $PROJECT_ROOT/docker"
  exit 1
fi

# Define the docker-compose command with the correct context
DOCKER_COMPOSE_CMD="docker-compose -f $PROJECT_ROOT/docker/docker-compose.yml"

# Remove any existing network with the same name
docker network rm stock_data_service_network || true

# Bring down any existing containers
$DOCKER_COMPOSE_CMD down

# Build the Docker images
docker build --no-cache -t my_airflow_image -f "$PROJECT_ROOT/airflow/Dockerfile" .
docker build --no-cache -t my_api_service_image -f "$PROJECT_ROOT/api_service/Dockerfile" .

# Start Docker Compose services
$DOCKER_COMPOSE_CMD up -d

# Allow some time for the containers to fully start
sleep 10

# Ensure the log and cache directories inside the container have correct permissions
docker exec -u root $($DOCKER_COMPOSE_CMD ps -q airflow) bash -c "chown -R airflow:root /data/airflow/log && chmod -R 755 /data/airflow/log"
docker exec -u root $($DOCKER_COMPOSE_CMD ps -q airflow) bash -c "chown -R airflow:root /data/airflow/cache && chmod -R 755 /data/airflow/cache"

# Show logs for timescaledb and airflow
$DOCKER_COMPOSE_CMD logs timescaledb
$DOCKER_COMPOSE_CMD logs airflow
$DOCKER_COMPOSE_CMD logs api_service
