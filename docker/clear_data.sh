#!/bin/bash

# 设置BASE_DIR变量
BASE_DIR="$HOME/airflow_data"

# 定义需要清空的目录
directories=(
    "$BASE_DIR/db/redis"
    "$BASE_DIR/db/timescaledb"
)

# 确认清空操作
read -p "您确定要清空以下目录中的所有数据吗？(y/n): " confirm
if [[ "$confirm" != "y" ]]; then
  echo "操作已取消。"
  exit 0
fi

# 清空目录
for dir in "${directories[@]}"; do
  if [ -d "$dir" ]; then
    echo "清空目录: $dir"
    sudo rm -rf "$dir"/*
    echo "$dir 清空完成。"
  else
    echo "目录不存在: $dir"
  fi
done

echo "所有指定目录已清空。"
