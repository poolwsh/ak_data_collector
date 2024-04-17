# Dockerfile
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 安装必要的 Python 包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 暂不复制 Python 脚本，使用卷挂载在运行时提供
# CMD 指令也暂留空，通过 Docker 命令行参数提供

# 保持容器运行，等待外部命令
CMD ["tail", "-f", "/dev/null"]
