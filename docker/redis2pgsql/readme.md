* build docker
```shell
docker build -t redis2pgsql .
```

* run docker
```shell
docker run -d -v /path/to/your/scripts:/app --name local_redis2pgsql redis2pgsql
docker run -d --name local_redis2pgsql \
    -v /data/workspace/git/ak_data_collector/docker/redis2pgsql/scripts:/app \
    -e REDIS_HOST=101.35.169.170 \
    -e REDIS_PASSWORD=pi=3.14159 \
    -e POSTGRES_URI=postgresql://postgres:pi=3.14159@101.35.169.170:25432/postgres \
    redis2pgsql
```

* run python shell file
```shell
docker exec -it local_redis2pgsql python /app/service.py
```

* kill python shell instance
```shell
docker exec local_redis2pgsql pkill -f service.py
```

* rerun python shell file
```shell
docker exec -d local_redis2pgsql python /app/service.py
```


