#!/bin/bash

# Set the BASE_DIR variable
BASE_DIR="$HOME/airflow_data"

# Define the directories to be cleared
directories=(
    "$BASE_DIR/db/redis"
    "$BASE_DIR/db/timescaledb"
)

# Confirm the clear operation
read -p "Are you sure you want to clear all data in the following directories? (y/n): " confirm
if [[ "$confirm" != "y" ]]; then
  echo "Operation canceled."
  exit 0
fi

# Clear the directories
for dir in "${directories[@]}"; do
  if [ -d "$dir" ]; then
    echo "Clearing directory: $dir"
    sudo rm -rf "$dir"/*
    echo "$dir cleared."
  else
    echo "Directory does not exist: $dir"
  fi
done

echo "All specified directories have been cleared."
