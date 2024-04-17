import redis
import json
import pandas as pd
from datetime import datetime

# 配置 Redis 连接
redis_host = '101.35.169.170'
redis_port = 6379
redis_password = 'pi=3.14159'  # 确保是正确的密码
channel_name = 'data_channel'

# 创建 Redis 连接
r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

# 创建一些模拟数据
data = pd.DataFrame({
    'id': [1, 2, 3],
    'value': [100, 200, 300],
    'timestamp': [datetime.now().isoformat() for _ in range(3)]
})

# 转换数据为 JSON 格式
json_data = data.to_json(orient='records')

# 发送数据到 Redis 的指定频道
r.publish(channel_name, json_data)
print("Data published to Redis channel:", channel_name)
