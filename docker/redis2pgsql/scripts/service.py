import os
import redis
import pandas as pd
from sqlalchemy import create_engine

# 环境变量
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'your_redis_password') 
POSTGRES_URI = os.getenv('POSTGRES_URI', 'postgresql://user:password@localhost/dbname')

# 设置 Redis 连接
redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, password=REDIS_PASSWORD)  

# 设置 PostgreSQL 连接
engine = create_engine(POSTGRES_URI)

def listen_to_redis():
    pubsub = redis_conn.pubsub()
    pubsub.subscribe('data_channel')
    print("Listening for messages...")
    for message in pubsub.listen():
        if message['type'] == 'message':
            process_data(message['data'])

def process_data(data):
    print("Processing data...")
    # 数据从 Redis 订阅来是 bytes 类型，需要解码成字符串
    if data:
        try:
            # 解码 bytes 数据为 str
            json_str = data.decode('utf-8')
            # 将 JSON 字符串转换为 DataFrame
            df = pd.read_json(json_str, orient='records')  # 确保 JSON 格式与 pandas 兼容
            df.to_sql('table_name', engine, if_exists='append', index=False)
            print("Data saved to PostgreSQL.")
        except Exception as e:
            print("Error processing data:", e)


if __name__ == "__main__":
    listen_to_redis()
